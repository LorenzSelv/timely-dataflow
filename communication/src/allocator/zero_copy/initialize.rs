//! Network initialization.

use std::sync::Arc;
// use crate::allocator::Process;
use crate::allocator::process::ProcessBuilder;
use crate::networking::create_sockets;
use super::tcp::{send_loop, recv_loop, pt_recv_loop};
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
use std::net::TcpStream;
use crate::allocator::zero_copy::bytes_exchange::MergeQueue;
use std::sync::mpsc::{Sender, Receiver};
use std::thread::JoinHandle;
use pubsub::queue::ring_log_queue::RingLogCursor;

/// Returns a logger for communication events
pub type LogSender = Box<Fn(CommunicationSetup)->Option<Logger<CommunicationEvent, CommunicationSetup>>+Send+Sync>;

/// Initializes network connections
pub fn initialize_networking(
    mut addresses: Vec<String>,
    my_index: usize,
    threads: usize,
    noisy: bool,
    log_sender: LogSender)
-> ::std::io::Result<(Vec<TcpBuilder<ProcessBuilder>>, CommsGuard)>
{
    let log_sender = Arc::new(log_sender);
    let processes = addresses.len();

    let my_address = addresses[my_index].clone();

    // append the progress tracker address so we setup connection to it
    // TODO: make sure we don't overflow -- message header should have a fixed target in the PT pusher that will be allocated
    let pt_index = std::usize::MAX; // avoid problems with indexes of workers during rescaling
    let pt_address = std::env::var("PROGRESS_TRACKER_ADDRESS").unwrap_or("127.0.0.1:9000".to_string());
    addresses.push(pt_address);


    // one per process (including local, which would be None)
    let mut results: Vec<Option<::std::net::TcpStream>> =
        create_sockets(addresses, my_index, noisy)?;

    let log_sender_clone = log_sender.clone();

    let (rescaler_txs, rescaler_rxs) =
        (0..threads)
            .map(|_| {
                let (tx, rx) = std::sync::mpsc::channel();
                (tx, Some(rx))
            })
            .unzip();

    // TODO maybe keep the handle of the acceptor thread?
    std::thread::spawn(move || {
        crate::rescaling::rescaler(my_index,
                                   my_address,
                                   threads,
                                   log_sender_clone,
                                   rescaler_txs);
    });

    let process_allocators = crate::allocator::process::Process::new_vector(threads);
    let (builders, promises, futures, pt_queue_promises, pt_cursor_promises) = new_vector(process_allocators, my_index, processes, rescaler_rxs);

    let mut promises_iter = promises.into_iter();
    let mut futures_iter = futures.into_iter();

    let mut send_guards = Vec::new();
    let mut recv_guards = Vec::new();

    // for each process, if a stream exists (i.e. not local) ...
    for index in 0..processes {

        if let Some(stream) = results[index].take() {
            let remote_recv = promises_iter.next().unwrap();
            let send_guard = spawn_send_thread(my_index, index, stream.try_clone()?, log_sender.clone(), remote_recv)?;
            send_guards.push(send_guard);

            let remote_send = futures_iter.next().unwrap();
            let recv_guard = spawn_recv_thread(my_index, index, stream.try_clone()?, log_sender.clone(), remote_send, threads)?;
            recv_guards.push(recv_guard);
        }
    }

    let pt_stream = results[pt_index].take().unwrap();
    // PT sender thread uses the same send_thread based on a shared MergeQueue as other the threads above
    let _pt_send_guard = spawn_send_thread(my_index, pt_index, pt_stream.try_clone()?, log_sender.clone(), pt_queue_promises)?;

    // PT receiver thread uses a different recv_thread based on a LogQueue.
    // It will hand-off cursors (handles to read from the LogQueue) to the worker threads
    let _pt_recv_guard = spawn_pt_recv_thread(my_index, pt_index, pt_stream.try_clone()?, log_sender.clone(), pt_cursor_promises)?;


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

/// recv thread to receive progress updates from the remote progress tracker
pub fn spawn_pt_recv_thread(my_index: usize, pt_index: usize, stream: TcpStream, log_sender: Arc<LogSender>, pt_cursor_promises: Vec<Sender<RingLogCursor<()>>>)
                         -> Result<JoinHandle<()>, std::io::Error>
{
    ::std::thread::Builder::new()
        .name(format!("recv pt thread {}", pt_index))
        .spawn(move || {
            let logger = log_sender(CommunicationSetup {
                process: my_index,
                sender: false,
                remote: Some(pt_index),
            });
            pt_recv_loop(stream, pt_cursor_promises, my_index, pt_index, logger);
        })
}
