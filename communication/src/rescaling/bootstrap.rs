//! TODO(lorenzo) doc

use std::sync::mpsc::{Sender, Receiver};
use std::net::{SocketAddrV4, TcpStream, TcpListener};
use crate::networking::{recv_handshake, send_handshake};
use abomonation::Abomonation;
use std::io::{Read, Write};

/// TODO
pub struct BootstrapSendEndpoint {
    state_tx: Sender<Vec<(usize,Vec<u8>)>>,
    range_req_rx: Receiver<ProgressUpdatesRange>,
    range_ans_tx: Sender<Vec<u8>>,
    _server_index: usize,
}

impl BootstrapSendEndpoint {
    /// TODO
    pub fn new(state_tx: Sender<Vec<(usize,Vec<u8>)>>, range_req_rx: Receiver<ProgressUpdatesRange>, range_ans_tx: Sender<Vec<u8>>, _server_index: usize) -> Self {
        BootstrapSendEndpoint {
            state_tx,
            range_req_rx,
            range_ans_tx,
            _server_index,
        }
    }
}

/// TODO
pub struct BootstrapRecvEndpoint {
    state_rx: Receiver<Vec<(usize, Vec<u8>)>>,
    range_req_tx: Sender<ProgressUpdatesRange>,
    range_ans_rx: Receiver<Vec<u8>>,
    server_index: usize,
}

impl BootstrapRecvEndpoint {
    /// TODO
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

/// TODO
pub fn start_connection(my_index: usize, address: SocketAddrV4) -> TcpStream {
    loop {
        match TcpStream::connect(address) {
            Ok(mut stream) => {
                send_handshake(&mut stream, my_index);
                println!("[bootstrap server -- W{}] connected to new worker at {}", my_index, address);
                break stream
            },
            Err(error) => {
                println!("[bootstap server -- W{}] error connecting to new worker at {}: {}", my_index, address, error);
                std::thread::sleep(std::time::Duration::from_millis(1));
            },
        }
    }
}

fn await_connection(address: SocketAddrV4) -> TcpStream {
    let listener = TcpListener::bind(address).expect("bind error");
    let mut stream = listener.accept().expect("accept error").0;
    let identifier = recv_handshake(&mut stream).expect("recv handshake error");
    println!("[bootstrap client] connected to worker {}", identifier);
    stream
}

/// Identifies a range of progress updates
#[derive(Clone, Debug, Abomonation)]
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

/// TODO
pub fn read_decode<T: Abomonation + Copy>(stream: &mut TcpStream) -> T {
    // note: supports only fixed-size types
    let mut buf = vec![0_u8; std::mem::size_of::<T>()];
    stream.read_exact(&mut buf[..]).expect("read_exact error");
    let (&typed, remaining) = unsafe { abomonation::decode(&mut buf[..]) }.expect("decode error");
    assert_eq!(remaining.len(), 0);
    typed
}

/// TODO
pub fn encode_write<T: Abomonation>(stream: &mut TcpStream, typed: &T) {
    let mut buf = Vec::new();
    unsafe { abomonation::encode(typed, &mut buf) }.expect("encode error");
    stream.write(&buf[..]).expect("write error");
}

/// TODO(lorenzo) doc
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

    // TODO(lorenzo) every worker in this process is asking for the same updates!
    //               send the request only once and cache the result
    for endpoint in bootstrap_send_endpoints.iter() {

        // the worker thread will drop the tx endpoint when it has no more range requests
        while let Ok(range_req) = endpoint.range_req_rx.recv() {

            // (0) send range requests
            encode_write(&mut tcp_stream, &range_req);

            // (1) read size of the encoded updates_range
            let range_ans_size = read_decode(&mut tcp_stream);

            // (2) read encoded updates_range
            let mut range_ans_buf = vec![0_u8; range_ans_size];
            tcp_stream.read_exact(&mut range_ans_buf[..]).expect("read_exact error");

            // (3) send back encoded range answer
            endpoint.range_ans_tx.send(range_ans_buf).expect("send failed");
        }

        println!("done bootstrapping a worker");
    }
}
