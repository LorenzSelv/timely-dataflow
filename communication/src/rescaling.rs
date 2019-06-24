//! Rescaling of the computation, allowing new worker processes to setup connections to existing ones
use crate::allocator::zero_copy::bytes_exchange::MergeQueue;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::Arc;
use std::net::TcpListener;
use crate::networking::recv_handshake;
use crate::allocator::zero_copy::initialize::{LogSender, spawn_send_thread, spawn_recv_thread};

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
pub fn rescaler(my_index: usize,
                my_address: String,
                threads: usize,
                log_sender: Arc<LogSender>,
                rescaler_txs: Vec<Sender<(Sender<MergeQueue>, Receiver<MergeQueue>)>>)
{
    let listener = TcpListener::bind(my_address).expect("Bind failed");

    loop {
        let mut stream = listener.accept().expect("Accept failed").0;
        let new_worker_index = recv_handshake(&mut stream).expect("Handshake failed");
        println!("worker {}:\tconnection from worker {}", my_index, new_worker_index);

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
            .zip(worker_promises.into_iter().flatten())
            .zip(worker_futures.into_iter().flatten())
            .for_each(|((tx, promises), futures)| {
                tx.send((promises, futures)).expect("Send (promise/future) failed");
            });
    }
}
