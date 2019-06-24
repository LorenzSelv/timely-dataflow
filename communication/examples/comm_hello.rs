extern crate timely_communication;

use std::ops::Deref;
use timely_communication::{Message, Allocate};
use std::rc::Rc;
use core::cell::RefCell;

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

        let mut senders = senders2.borrow_mut();

        // send typed data along each channel
        for i in 0 .. allocator.peers() {
            senders[i].send(Message::from_typed(format!("hello, {}", i)));
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
