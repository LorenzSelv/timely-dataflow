//!

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{Sender, Receiver};

use crate::networking::MessageHeader;

use super::bytes_slab::BytesSlab;
use super::bytes_exchange::MergeQueue;

use logging_core::Logger;

use crate::logging::{CommunicationEvent, CommunicationSetup, MessageEvent, StateEvent};
use pubsub::queue::ring_log_queue::{RingLogCursor, RingLogQueue, RingLogFolder};
use pubsub::queue::log_queue::LogQueue;

/// Repeatedly reads from a TcpStream and carves out messages.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
/// If the stream ends without being shut down, the receive thread panics in an attempt to
/// take down the computation and cause the failures to cascade.
pub fn recv_loop(
    mut reader: TcpStream,
    targets: Vec<Receiver<MergeQueue>>,
    worker_offset: usize,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: true }));

    let mut targets: Vec<MergeQueue> = targets.into_iter().map(|x| x.recv().expect("Failed to receive MergeQueue")).collect();

    let mut buffer = BytesSlab::new(20);

    // Where we stash Bytes before handing them off.
    let mut stageds = Vec::with_capacity(targets.len());
    for _ in 0 .. targets.len() {
        stageds.push(Vec::new());
    }

    // Each loop iteration adds to `self.Bytes` and consumes all complete messages.
    // At the start of each iteration, `self.buffer[..self.length]` represents valid
    // data, and the remaining capacity is available for reading from the reader.
    //
    // Once the buffer fills, we need to copy uncomplete messages to a new shared
    // allocation and place the existing Bytes into `self.in_progress`, so that it
    // can be recovered once all readers have read what they need to.
    let mut active = true;
    while active {

        buffer.ensure_capacity(1);

        assert!(!buffer.empty().is_empty());

        // Attempt to read some more bytes into self.buffer.
        let read = match reader.read(&mut buffer.empty()) {
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            },
        };

        // TODO(lorenzo) this guy is the one that notices the failure.. Ok(0) = channel closed
        //               should set a `shutdown` flag to signal the puller about the failure, => the puller will stop pull from this MergeQueue
        //               should inform the sender that the channel is broken

        assert!(read > 0);

        buffer.make_valid(read);

        // Consume complete messages from the front of self.buffer.
        while let Some(header) = MessageHeader::try_read(buffer.valid()) {

            // TODO: Consolidate message sequences sent to the same worker?
            let peeled_bytes = header.required_bytes();
            let bytes = buffer.extract(peeled_bytes);

            // Record message receipt.
            logger.as_mut().map(|logger| {
                logger.log(MessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                stageds[header.target - worker_offset].push(bytes);
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                if !buffer.valid().is_empty() {
                    panic!("Clean shutdown followed by data.");
                }
                buffer.ensure_capacity(1);
                if reader.read(&mut buffer.empty()).expect("read failure") > 0 {
                    panic!("Clean shutdown followed by data.");
                }
            }
        }

        // Pass bytes along to targets.
        for (index, staged) in stageds.iter_mut().enumerate() {
            // FIXME: try to merge `staged` before handing it to BytesPush::extend
            use crate::allocator::zero_copy::bytes_exchange::BytesPush;
            targets[index].extend(staged.drain(..));
        }
    }

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: false, }));
}

/// Repeatedly sends messages into a TcpStream.
///
/// The intended communication pattern is a sequence of (header, message)^* for valid
/// messages, followed by a header for a zero length message indicating the end of stream.
pub fn send_loop(
    // TODO: Maybe we don't need BufWriter with consolidation in writes.
    writer: TcpStream,
    sources: Vec<Sender<MergeQueue>>,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: true, }));

    let mut sources: Vec<MergeQueue> = sources.into_iter().map(|x| {
        let buzzer = crate::buzzer::Buzzer::new();
        let queue = MergeQueue::new(buzzer);
        x.send(queue.clone()).expect("failed to send MergeQueue");
        queue
    }).collect();

    let mut writer = ::std::io::BufWriter::with_capacity(1 << 16, writer);
    let mut stash = Vec::new();

    while !sources.is_empty() {

        // TODO: Round-robin better, to release resources fairly when overloaded.
        for source in sources.iter_mut() {
            use crate::allocator::zero_copy::bytes_exchange::BytesPull;
            source.drain_into(&mut stash);
        }

        if stash.is_empty() {
            // No evidence of records to read, but sources not yet empty (at start of loop).
            // We are going to flush our writer (to move buffered data), double check on the
            // sources for emptiness and wait on a signal only if we are sure that there will
            // still be a signal incoming.
            //
            // We could get awoken by more data, a channel closing, or spuriously perhaps.
            writer.flush().expect("Failed to flush writer.");
            sources.retain(|source| !source.is_complete());
            if !sources.is_empty() {
                std::thread::park();
            }
        }
        else {
            // TODO: Could do scatter/gather write here.
            for mut bytes in stash.drain(..) {

                // Record message sends.
                logger.as_mut().map(|logger| {
                    let mut offset = 0;
                    while let Some(header) = MessageHeader::try_read(&mut bytes[offset..]) {
                        logger.log(MessageEvent { is_send: true, header, });
                        offset += header.required_bytes();
                    }
                });

                writer.write_all(&bytes[..]).expect("Write failure in send_loop.");
            }
        }
    }

    // Write final zero-length header.
    // Would be better with meaningful metadata, but as this stream merges many
    // workers it isn't clear that there is anything specific to write here.
    let header = MessageHeader {
        channel:    0,
        source:     0,
        target:     0,
        length:     0,
        seqno:      0,
    };
    header.write_to(&mut writer).expect("Failed to write header!");
    writer.flush().expect("Failed to flush writer.");
    writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    logger.as_mut().map(|logger| logger.log(MessageEvent { is_send: true, header }));

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: true, process, remote, start: false, }));
}

/// TODO(lorenzo) doc
pub fn pt_recv_loop(
    mut reader: TcpStream,
    pt_cursor_promises: Vec<Sender<RingLogCursor<()>>>,
    process: usize,
    remote: usize,
    mut logger: Option<Logger<CommunicationEvent, CommunicationSetup>>)
{
    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: true }));

    // Allocate a LogQueue that discard progress updates that have been read by all workers (`no_fold()`)
    let capacity = 1 << 20;
    let mut log_queue = RingLogQueue::new(capacity, RingLogFolder::no_fold());

    // Send cursors to all worker threads, so they can read progress updated from the queue

    pt_cursor_promises.into_iter().for_each(|tx| tx.send(log_queue.new_cursor()).expect("Failed to send cursor"));

    // Buffer to read data from the tcp stream
    let mut buffer = [0_u8; 1<<20];

    let mut active = true;
    while active {

        let read = match reader.read(&mut buffer) {
            Ok(n) => n,
            Err(x) => {
                // We don't expect this, as socket closure results in Ok(0) reads.
                println!("Error: {:?}", x);
                0
            },
        };

        // TODO(lorenzo) this guy is the one that notices the failure.. Ok(0) = channel closed
        //               should set a `shutdown` flag to signal the puller about the failure, => the puller will stop pull from this MergeQueue
        //               should inform the sender that the channel is broken
        assert!(read > 0);

        let mut consumed = 0;

        // Consume complete messages from the front of self.buffer.
        while let Some(header) = MessageHeader::try_read(&mut buffer[consumed..read]) {

            let message_size = header.required_bytes();

            // Record message receipt.
            logger.as_mut().map(|logger| {
                logger.log(MessageEvent { is_send: false, header, });
            });

            if header.length > 0 {
                // try to append until it succeeds. It will only fail if the buffer is full
                while !log_queue.append_raw(&buffer[consumed..message_size]) {}

                consumed += message_size;
            }
            else {
                // Shutting down; confirm absence of subsequent data.
                active = false;
                // if !buffer.valid().is_empty() {
                if consumed + message_size != read {
                    panic!("Clean shutdown followed by data.");
                }
                if reader.read(&mut buffer).expect("read failure") > 0 {
                    panic!("Clean shutdown followed by data.");
                }
            }
        }

        assert_eq!(consumed, read, "consumed bytes differs from the read bytes");
    }

    // Log the receive thread's start.
    logger.as_mut().map(|l| l.log(StateEvent { send: false, process, remote, start: false, }));
}
