//! TODO
use std::net::{TcpStream, SocketAddrV4};
use std::io::{Read, Write};
use std::collections::HashMap;
use crate::progress::broadcast::ProgcasterServerHandle;
use timely_communication::networking::send_handshake;
use timely_communication::rescaling::bootstrap::{encode_write, ProgressUpdatesRange};

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


/// TODO documentation
pub fn bootstrap_worker_server(my_index: usize, target_address: SocketAddrV4, progcasters: HashMap<usize, Box<dyn ProgcasterServerHandle>>) {

    // connect to target_address
    let mut tcp_stream = start_connection(my_index, target_address);

    // write how many progcasters' state we are going to send
    encode_write(&mut tcp_stream, &progcasters.len());

    for (channel_id, progcaster) in progcasters.iter() {
        let progress_state = progcaster.get_progress_state();
        progcaster.start_recording();

        // (1) write channel_id
        encode_write(&mut tcp_stream, channel_id);

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

            let updates_range = progcaster.get_updates_range(range_req);

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
