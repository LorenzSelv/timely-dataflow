//! Rescaling of the computation, allowing new worker processes to setup connections to existing ones

pub mod bootstrap;

use crate::allocator::zero_copy::bytes_exchange::MergeQueue;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::Arc;
use std::net::{TcpListener, SocketAddrV4, TcpStream, Ipv4Addr};
use crate::networking::recv_handshake;
use crate::allocator::zero_copy::initialize::{LogSender, spawn_send_thread, spawn_recv_thread};
use abomonation::Abomonation;
use std::io::Read;
use crate::rescaling::bootstrap::encode_write;
use crate::buzzer::Buzzer;

/// Information to perform the rescaling
#[derive(Debug)]
pub struct RescaleMessage {
    /// to share a recv merge queue
    pub promise: Sender<MergeQueue>,
    /// to share a send merge queue
    pub future: Receiver<MergeQueue>,
    /// if Some, then the worker has been selected to bootstrap the new worker.
    /// It should connect to that address and init the new worker progress tracker.
    pub bootstrap_addr: Option<SocketAddrV4>
}

/// code to be executed in the acceptor (or rescaler) thread.
///
/// The thread would bind to the same address assigned to the worker thread, and listen
/// for incoming TCP connections.
/// When a new connection is established, a pair of send/recv network thread is spawned.
/// A vector of mpsc::channel is allocated, one for each worker thread internal to this process, and sent
/// to them using the `rescaler_tx` sender handle.
/// A worker using the TcpAllocator (cluster mode), will do an non-blocking read from this channel in the `rescale` function.
///
/// The payload of the message is a pair (promise, future) used to setup shared MergeQueue with the new network threads,
/// as done in the `initialize_networking` function at communication/src/allocator/zero_copy/initialize.rs
pub fn rescaler(
    my_index: usize,
    my_address: String,
    threads: usize,
    log_sender: Arc<LogSender>,
    rescaler_txs: Vec<Sender<RescaleMessage>>,
    buzzer_rxs: Vec<Receiver<Buzzer>>
) {
    let listener = TcpListener::bind(my_address).expect("Bind failed");

    let buzzers = buzzer_rxs.iter().map(|rx| rx.recv().expect("failed to recv buzzer")).collect::<Vec<_>>();

    loop {
        let mut stream = listener.accept().expect("Accept failed").0;
        let new_worker_index = recv_handshake(&mut stream).expect("Handshake failed");

        let bootstrap_addr = recv_bootstrap_addr(&mut stream, my_index);

        println!("worker {}:\tconnection from worker {}, bootstrap address is {:?}", my_index, new_worker_index, bootstrap_addr);

        // For queues from worker threads to the send network thread
        let (mut network_promise, worker_futures) = crate::promise_futures(1, threads);
        // For queues from recv network threads to worker threads
        let (worker_promises, mut network_future) = crate::promise_futures(threads, 1);

        // Only one additional remote process to talk to
        let network_promise = network_promise.remove(0);
        let network_future  = network_future.remove(0);

        spawn_send_thread(my_index, new_worker_index, stream.try_clone().unwrap(), log_sender.clone(), network_promise).expect("Spawn send thread");
        spawn_recv_thread(my_index, new_worker_index, stream.try_clone().unwrap(), log_sender.clone(), network_future, threads).expect("Spawn recv thread");


        // Send promises and futures to the workers, so that they can establish MergeQueues with the send/recv network threads
        rescaler_txs
            .iter()
            .zip(buzzers.iter())
            .zip(worker_promises.into_iter().flatten())
            .zip(worker_futures.into_iter().flatten())
            .for_each(|(((tx, buzzer), promise), future)| {
                let rescale_message = RescaleMessage {
                    promise,
                    future,
                    bootstrap_addr,
                };
                tx.send(rescale_message).expect("Send RescaleMessage failed");
                buzzer.buzz();
                // TODO(lorenzo): need to buzz the rx worker thread, in case it's parked
            });
    }
}

fn read_decode<T: Abomonation + Clone>(stream: &mut TcpStream) -> T {
    // note: supports only fixed-size types
    let mut buf = vec![0_u8; std::mem::size_of::<T>()];
    stream.read_exact(&mut buf[..]).expect("read_exact error");
    let (typed, remaining) = unsafe { abomonation::decode::<T>(&mut buf[..]) }.expect("decode error");
    assert_eq!(remaining.len(), 0);
    typed.clone()
}

fn recv_bootstrap_addr(mut stream: &mut TcpStream, my_index: usize) -> Option<SocketAddrV4> {
    let bootstrap_server_index: usize = read_decode(&mut stream);
    let ip: u32 = read_decode(&mut stream);
    let port: u16 = read_decode(&mut stream);

    // Some only if selected as bootstrap server
    if bootstrap_server_index == my_index { Some(SocketAddrV4::new(Ipv4Addr::from(ip), port)) }
    else { None }
}

/// write bootstrap server index and bootstrap client address to tcp stream
pub fn send_bootstrap_addr(mut stream: &mut TcpStream, bootstrap_server_index: usize, addr: SocketAddrV4) {
    encode_write(&mut stream, &bootstrap_server_index);
    encode_write(&mut stream, &u32::from(*addr.ip()));
    encode_write(&mut stream, &addr.port());
}
