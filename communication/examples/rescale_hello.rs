///
/// Usage:
///
/// 1) start a computation with e.g. two process, each with a single worker
///
///    `cargo run --package timely_communication --example rescale_hello -- -w1 -p0 -n2`
///    `cargo run --package timely_communication --example rescale_hello -- -w1 -p1 -n2`
///
/// 2) after a bit, spawn the third process
///    `cargo run --package timely_communication --example rescale_hello -- -w1 -p2 -n3`
///
/// 3) after a bit, spawn the fourth process
///    `cargo run --package timely_communication --example rescale_hello -- -w1 -p3 -n4`
///
/// There are a few assumption we are relying on:
///   * each worker with higher index setup connection to workers with lower indexes
///   * addresses are chosen deterministically (one could provide host file and would work as well, though)
///     e.g for the example above the host file could look like this:
///     ```ignore
///     127.0.0.1:5000
///     127.0.0.1:5001
///     127.0.0.1:5002
///     127.0.0.1:5003
///     ```
///     Each worker would consider only the first `n` hosts in the file, where `n` is passed as an option.
///
extern crate timely_communication;

use std::ops::Deref;
use timely_communication::{Message, Allocate};
use std::rc::Rc;
use core::cell::RefCell;
use std::time::Duration;

fn main() {

    // extract the configuration from user-supplied arguments, initialize the computation.
    let config = timely_communication::Configuration::from_args(std::env::args()).unwrap();
    let guards = timely_communication::initialize(config, |mut allocator| {

        println!("worker {} of {} started", allocator.index(), allocator.peers());

        // If in cluster mode, channel allocations might have to be updated dynamically.
        // See `rescaling_hello.rs` for a more detailed explanation for the need of the closure below.
        let senders1 = Rc::new(RefCell::new(Vec::new()));
        let senders2 = Rc::clone(&senders1);

        let on_new_pusher = move |pusher| {
            senders1.borrow_mut().push(pusher);
        };

        // allocates pair of senders list and one receiver.
        let mut receiver = allocator.allocate(0, on_new_pusher);


        loop {

            std::thread::sleep(Duration::from_secs(2));
            allocator.rescale();

            let mut senders = senders2.borrow_mut();

            println!("senders len is {:?}", senders.len());
            println!("peers   num is {:?}", allocator.peers());

            // send typed data along each channel
            for i in 0..allocator.peers() {
                senders[i].send(Message::from_typed(format!("hello, {} (from {})", i, allocator.index())));
                senders[i].done();
            }

            // no support for termination notification,
            // we have to count down ourselves.
            let mut received = 0;
            while received < allocator.peers() {
                allocator.receive();

                if let Some(message) = receiver.recv() {
                    println!("worker {}: received: <{}>", allocator.index(), message.deref());
                    received += 1;
                }

                allocator.release();
            }
        }

        allocator.index()
    });

    // computation runs until guards are joined or dropped.
    if let Ok(guards) = guards {
        for guard in guards.join() {
            println!("result: {:?}", guard);
        }
    }
    else { println!("error in computation"); }
}
