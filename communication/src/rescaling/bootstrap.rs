//! Implementation of the bootstrapping protocol to initialize the progress tracking state
//! of a new worker threads joining the cluster.

use std::sync::mpsc::{Sender, Receiver};
use std::net::{SocketAddrV4, TcpStream, TcpListener};
use crate::networking::{recv_handshake, send_handshake};
use abomonation::Abomonation;
use std::io::{Read, Write};
use std::collections::HashMap;

/// Collection of channels to send encoded data (related to progress tracking)
/// from the bootstrap_worker_client to a worker thread. Used by the bootstrap client.
pub struct BootstrapSendEndpoint {
    state_tx: Sender<Vec<(usize,Vec<u8>)>>,
    range_req_rx: Receiver<ProgressUpdatesRange>,
    range_ans_tx: Sender<Vec<u8>>,
    _server_index: usize,
}

impl BootstrapSendEndpoint {

    /// Build a new bootstrap send endpoint
    pub fn new(state_tx: Sender<Vec<(usize,Vec<u8>)>>, range_req_rx: Receiver<ProgressUpdatesRange>, range_ans_tx: Sender<Vec<u8>>, _server_index: usize) -> Self {
        BootstrapSendEndpoint {
            state_tx,
            range_req_rx,
            range_ans_tx,
            _server_index,
        }
    }
}

/// Collection of channels to recv encoded data (related to progress tracking)
/// from the bootstrap_worker_client by a worker thread. Used by a worker thread.
pub struct BootstrapRecvEndpoint {
    state_rx: Receiver<Vec<(usize, Vec<u8>)>>,
    range_req_tx: Sender<ProgressUpdatesRange>,
    range_ans_rx: Receiver<Vec<u8>>,
    server_index: usize,
}

impl BootstrapRecvEndpoint {

    /// Build a new bootstrap recv endpoint
    pub fn new(state_rx: Receiver<Vec<(usize,Vec<u8>)>>, range_req_tx: Sender<ProgressUpdatesRange>, range_ans_rx: Receiver<Vec<u8>>, server_index: usize) -> Self {
        BootstrapRecvEndpoint {
            state_rx,
            range_req_tx,
            range_ans_rx,
            server_index,
        }
    }

    /// Receive progcaster states as a vector of (channel_id, state) pairs.
    pub fn recv_progcaster_states(&self) -> Vec<(usize,Vec<u8>)> {
        self.state_rx.recv().expect("recv_progcaster_states failed")
    }

    /// Send a progress update range request
    pub fn send_range_request(&self, range: ProgressUpdatesRange) {
        self.range_req_tx.send(range).expect("send_range_request failed")
    }

    /// Send a progress update range request
    pub fn recv_range_response(&self) -> Vec<u8> {
        self.range_ans_rx.recv().expect("recv_range_response failed")
    }

    /// Return the worker index of the bootstrap server
    pub fn get_server_index(&self) -> usize {
        self.server_index
    }
}

/// Setup connection with target address and perform sender handshake (send my_index)
pub fn start_connection(my_index: usize, address: SocketAddrV4) -> TcpStream {
    loop {
        match TcpStream::connect(address) {
            Ok(mut stream) => {
                send_handshake(&mut stream, my_index);
                eprintln!("[bootstrap server -- W{}] connected to new worker at {}", my_index, address);
                break stream
            },
            Err(error) => {
                eprintln!("[bootstap server -- W{}] error connecting to new worker at {}: {}", my_index, address, error);
                std::thread::sleep(std::time::Duration::from_millis(1));
            },
        }
    }
}

fn await_connection(address: SocketAddrV4) -> TcpStream {
    let listener = TcpListener::bind(address).expect("bind error");
    let mut stream = listener.accept().expect("accept error").0;
    let identifier = recv_handshake(&mut stream).expect("recv handshake error");
    eprintln!("[bootstrap client] connected to worker {}", identifier);
    stream
}

/// Identifies a range of progress updates
#[derive(Clone, Debug, Abomonation, PartialEq, Eq, Hash)]
pub struct ProgressUpdatesRange {
    /// identifier for the channel (also unique integer identifier for the scope)
    pub channel_id: usize,
    /// index of the worker
    pub worker_index: usize,
    /// inclusive start of the range start_seqno..end_seqno
    pub start_seqno: usize,
    /// exclusive end of the range seq_no_start..seq_no_end
    pub end_seqno: usize,
}

/// Simple wrapper to read and decode abomonated fixed-size types from a stream
pub fn read_decode<T: Abomonation + Copy>(stream: &mut TcpStream) -> T {
    // note: supports only fixed-size types
    let mut buf = vec![0_u8; std::mem::size_of::<T>()];
    stream.read_exact(&mut buf[..]).expect("read_exact error");
    let (&typed, remaining) = unsafe { abomonation::decode(&mut buf[..]) }.expect("decode error");
    assert_eq!(remaining.len(), 0);
    typed
}

/// Simple wrapper to encode and write abomonated types to a stream
pub fn encode_write<T: Abomonation>(stream: &mut TcpStream, typed: &T) {
    let mut buf = Vec::new();
    unsafe { abomonation::encode(typed, &mut buf) }.expect("encode error");
    stream.write(&buf[..]).expect("write error");
}

/// Client side of the bootstrapping protocol for a new worker process joining the cluster.
///
/// This function is executed by a separate thread that is spawned while building
/// the cluster configuration if the --join flag was passed as an argument.
/// This thread is unique to the whole process, thus multiple workers are talking to this
/// thread when performing the initialization protocol.
///
/// It takes an ip:port pair where it should listen to for incoming connection: the
/// bootstrap server will initiate a connection to that address (the address is the first thing sent
/// after setting up the TCP connection - `send_bootstrap_addr` function).
///
/// `bootstrap_send_endpoints` is a vector of endpoints to every worker thread in this timely process.
/// All the data received by the bootstrap server will be broadcasted to all workers.
///
/// Since the `communication` crate has no notion of timestamps and other timely data types,
/// the protocol is only dealing with encoded data types (prepended with their sizes).
/// The worker threads will read from the shared endpoint and perform the decode operation.
///
/// This is also necessary since every progcaster might have a different timestamp type
/// (e.g. nested scopes) and we would need some sort dynamic typing / generic mechanism which
/// would make things very complicated.
///
/// After forwarding the received state, it waits for range request from each endpoint.
/// A closed channel with the worker is the signal that there are no more range requests to satisfy.
///
pub fn bootstrap_worker_client(source_address: SocketAddrV4, bootstrap_send_endpoints: Vec<BootstrapSendEndpoint>) {

    // wait for the server to initiate the connection
    let mut tcp_stream = await_connection(source_address);

    // (0) read how many progcasters' states there are to receive
    let channel_num: usize = read_decode(&mut tcp_stream);

    let mut states = Vec::with_capacity(channel_num);

    for _ in 0..channel_num {

        // (1) read channel_id
        let channel_id: usize = read_decode(&mut tcp_stream);

        // (2) read encoded state size in bytes
        let state_size: usize = read_decode(&mut tcp_stream);

        // (3) read the encoded state
        let mut state_buf = vec![0_u8; state_size];
        tcp_stream.read_exact(&mut state_buf[..]).expect("read_exact failed");

        states.push((channel_id, state_buf));
    }

    // send the progcasters' state to every worker thread
    for endpoint in bootstrap_send_endpoints.iter() {
        endpoint.state_tx.send(states.clone()).expect("failed to send progcasters' state to worker");
    }

    // cache the range results so we avoid unnecessary request to the bootstrap server
    let mut range_cache = HashMap::<ProgressUpdatesRange, Vec<u8>>::new();

    for endpoint in bootstrap_send_endpoints.iter() {

        // the worker thread will drop the tx endpoint when it has no more range requests
        while let Ok(range_req) = endpoint.range_req_rx.recv() {

            let range_ans_buf = range_cache.entry(range_req.clone()).or_insert_with(|| {
                // (0) send range requests
                encode_write(&mut tcp_stream, &range_req);
                eprintln!("[bootstrap_worker_client] sent range request to bootstrap_server: {:?}", range_req);

                // (1) read size of the encoded updates_range
                let range_ans_size = read_decode(&mut tcp_stream);

                // (2) read encoded updates_range
                let mut range_ans_buf = vec![0_u8; range_ans_size];
                tcp_stream.read_exact(&mut range_ans_buf[..]).expect("read_exact error");

                // store buffer in the cache
                range_ans_buf
            });

            // (3) send back encoded range answer
            endpoint.range_ans_tx.send(range_ans_buf.clone()).expect("send failed");
        }

        eprintln!("done bootstrapping a worker");
    }
}
