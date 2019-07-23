//! TODO
use std::net::{TcpStream, SocketAddrV4, TcpListener};
use abomonation::Abomonation;
use std::io::{Read, Write};
use std::sync::Mutex;
use std::collections::HashMap;
use crate::progress::broadcast::{ProgcasterServerHandle, ProgcasterClientHandle};
use timely_communication::networking::{send_handshake, recv_handshake};

fn start_connection(my_index: usize, address: SocketAddrV4) -> TcpStream {
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
#[derive(Clone,Abomonation)]
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

fn read_decode<T: Abomonation + Copy>(stream: &mut TcpStream) -> T {
    // note: supports only fixed-size types
    let mut buf = vec![0_u8; std::mem::size_of::<T>()];
    stream.read_exact(&mut buf[..]).expect("read_exact error");
    let (&typed, remaining) = unsafe { abomonation::decode(&mut buf[..]) }.expect("decode error");
    assert_eq!(remaining.len(), 0);
    typed
}

fn encode_write<T: Abomonation>(stream: &mut TcpStream, typed: &T) {
    let mut buf = vec![0_u8; abomonation::measure(typed)];
    unsafe { abomonation::encode(typed, &mut buf) }.expect("encode error");
    stream.write(&buf[..]).expect("write error");
}

/// TODO documentation
pub fn bootstrap_worker_server(my_index: usize, target_address: SocketAddrV4, progcasters: HashMap<usize, Box<dyn ProgcasterServerHandle>>) {

    // connect to target_address
    let mut tcp_stream = start_connection(my_index, target_address);

    let mut states = Vec::with_capacity(progcasters.len());

    for (&channel_id, progcaster) in progcasters.iter() {
        let progress_state = progcaster.get_progress_state();
        progcaster.start_recording();

        states.push((channel_id, progress_state));
    }


    for (channel_id, progress_state) in states.into_iter() {
        // (1) write channel_id
        encode_write(&mut tcp_stream, &channel_id);

        // (2) write size of the state in bytes
        encode_write(&mut tcp_stream, &progress_state.len());

        // (3) write the encoded state
        tcp_stream.write(&progress_state[..]).expect("write error");
    }

    // handle range requests
    let mut request_buf = vec![0_u8; std::mem::size_of::<ProgressUpdatesRange>()];

    loop {
        let read = tcp_stream.read(&mut request_buf[..]).expect("read error");

        // loop until the client closes the connection
        // TODO maybe send some close message instead
        if read == 0 {
            println!("bootstrap worker server done!");
            break;
        } else {
            assert_eq!(read, request_buf.len());

            let (range_req, remaining) = unsafe { abomonation::decode::<ProgressUpdatesRange>(&mut request_buf[..]) }.expect("decode error");
            assert_eq!(remaining.len(), 0);

            let progcaster = &progcasters[&range_req.channel_id];

            let updates_range = progcaster.get_updates_range(range_req.clone());

            // write the size of the encoded updates_range
            encode_write(&mut tcp_stream, &updates_range.len());

            // write the updates_range
            tcp_stream.write(&updates_range[..]).expect("failed to send range_updates to target worker");
        }
    }

    for (_, progcaster) in progcasters.iter() {
        progcaster.stop_recording();
    }
}

/// TODO(lorenzo) doc
pub fn bootstrap_worker_client(source_address: SocketAddrV4, progcasters: HashMap<usize, Mutex<Box<dyn ProgcasterClientHandle>>>) {

    // wait for the server to initiate the connection
    let mut tcp_stream = await_connection(source_address);

    let mut states = Vec::with_capacity(progcasters.len());

    for _ in 0..states.len() {

        // (1) read channel_id
        let channel_id: usize = read_decode(&mut tcp_stream);

        // (2) read encoded state size in bytes
        let state_size: usize = read_decode(&mut tcp_stream);

        // (3) read the encoded state
        let mut state_buf = vec![0_u8; state_size];
        tcp_stream.read_exact(&mut state_buf[..]).expect("read_exact failed");

        states.push((channel_id, state_buf));
    }

    for (channel_id, encoded_state) in states.into_iter() {
        let progcaster = progcasters[&channel_id].lock().ok().expect("mutex error");
        progcaster.set_progress_state(encoded_state);

        let missing_ranges = progcaster.get_missing_updates_ranges();

        for range in missing_ranges.into_iter() {

            // (0) send range requests
            encode_write(&mut tcp_stream, &range);

            // (1) read size of the encoded updates_range
            let updates_range_size = read_decode(&mut tcp_stream);

            // (2) read encoded updates_range
            let mut updates_range_buf = vec![0_u8; updates_range_size];
            tcp_stream.read_exact(&mut updates_range_buf[..]).expect("read_exact error");

            progcaster.apply_updates_range(range, updates_range_buf);
        }
    }
}