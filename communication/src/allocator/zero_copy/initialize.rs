//! Network initialization.

use std::sync::Arc;
// use crate::allocator::Process;
use crate::allocator::process::ProcessBuilder;
use crate::networking::create_sockets;
use super::tcp::{send_loop, recv_loop};
use super::allocator::{TcpBuilder, new_vector};

/// Join handles for send and receive threads.
///
/// On drop, the guard joins with each of the threads to ensure that they complete
/// cleanly and send all necessary data.
pub struct CommsGuard {
    send_guards: Vec<::std::thread::JoinHandle<()>>,
    recv_guards: Vec<::std::thread::JoinHandle<()>>,
}

impl Drop for CommsGuard {
    fn drop(&mut self) {
        for handle in self.send_guards.drain(..) {
            handle.join().expect("Send thread panic");
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            handle.join().expect("Recv thread panic");
        }
        // println!("RECV THREADS JOINED");
    }
}

use crate::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;
use std::net::{TcpStream, SocketAddrV4};
use crate::allocator::zero_copy::bytes_exchange::MergeQueue;
use std::sync::mpsc::{Sender, Receiver};
use std::thread::JoinHandle;
use crate::rescaling::send_bootstrap_addr;
use crate::rescaling::bootstrap::BootstrapRecvEndpoint;

/// Returns a logger for communication events
pub type LogSender = Box<dyn Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>;

/// Initializes network connections
pub fn initialize_networking(
    addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    init_processes: usize,
    bootstrap_info: Option<(usize, SocketAddrV4)>,
    bootstrap_recv_endpoints: Vec<Option<BootstrapRecvEndpoint>>,
    noisy: bool,
    log_sender: LogSender)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    let log_sender = Arc::new(log_sender);
    let processes = addresses.len();

    let my_address = addresses[my_index].clone();

    // one per process (including local, which would be None)
    let mut results: Vec<Option<::std::net::TcpStream>> =
        create_sockets(addresses, my_index, noisy)?;

    let log_sender_clone = log_sender.clone();

    let (rescaler_txs, rescaler_rxs) = (0..threads).map(|_| std::sync::mpsc::channel()).unzip();
    let (buzzer_txs, buzzer_rxs)     = (0..threads).map(|_| std::sync::mpsc::channel()).unzip();

    // TODO maybe keep the handle of the acceptor thread?
    std::thread::spawn(move || {
        crate::rescaling::rescaler(my_index,
                                   my_address,
                                   threads,
                                   log_sender_clone,
                                   rescaler_txs,
                                   buzzer_rxs);
    });

    let is_rescaling = bootstrap_info.is_some();

    let process_allocators = crate::allocator::process::Process::new_vector(threads);

    let (builders, promises, futures) =
        new_vector(
            process_allocators,
            my_index,
            processes,
            init_processes,
            rescaler_rxs,
            buzzer_txs,
            bootstrap_recv_endpoints,
            is_rescaling,
        );

    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    let mut send_guards = Vec::new();
    let mut recv_guards = Vec::new();

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..results.len() {

        if let Some(mut stream) = results[index].take() {

            // if this new timely process is joining the cluster, communicate the selected bootstrap server
            // to all other worker processes before spawning send/recv threads.
            if let Some((bootstrap_server_index, bootstrap_addr)) = bootstrap_info {
                send_bootstrap_addr(&mut stream, bootstrap_server_index, bootstrap_addr);
            }

            let remote_recv = promises_iter.next().unwrap();
            let send_guard = spawn_send_thread(my_index, index, stream.try_clone()?, log_sender.clone(), remote_recv)?;
            send_guards.push(send_guard);

            let remote_send = futures_iter.next().unwrap();
            let recv_guard = spawn_recv_thread(my_index, index, stream.try_clone()?, log_sender.clone(), remote_send, threads)?;
            recv_guards.push(recv_guard);
        }
    }

    Ok((builders, CommsGuard { send_guards, recv_guards }))
}

/// send thread for communication to process `remote_index`
pub fn spawn_send_thread(my_index: usize, remote_index: usize, stream: TcpStream, log_sender: Arc<LogSender>, remote_recv: Vec<Sender<MergeQueue>>)
        -> Result<JoinHandle<()>, std::io::Error>
{
    ::std::thread::Builder::new()
        .name(format!("send thread {}", remote_index))
        .spawn(move || {
            let logger = log_sender(CommunicationSetup {
            process: my_index,
            sender: true,
            remote: Some(remote_index),
            });
            send_loop(stream, remote_recv, my_index, remote_index, logger);
        })
}

/// recv thread for communication from process `remote_index`
pub fn spawn_recv_thread(my_index: usize, remote_index: usize, stream: TcpStream, log_sender: Arc<LogSender>, remote_send: Vec<Receiver<MergeQueue>>, threads: usize)
        -> Result<JoinHandle<()>, std::io::Error>
{
    ::std::thread::Builder::new()
        .name(format!("recv thread {}", remote_index))
        .spawn(move || {
            let logger = log_sender(CommunicationSetup {
                process: my_index,
                sender: false,
                remote: Some(remote_index),
            });
            recv_loop(stream, remote_send, threads * my_index, my_index, remote_index, logger);
        })
}
