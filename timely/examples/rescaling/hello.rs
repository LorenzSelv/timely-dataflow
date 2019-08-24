//! Usage:
//!    Run in different terminals the following commands:
//!
//!   1) cargo run --package timely --example rescaling_hello -- -n2 -w2 -p0
//!   2) cargo run --package timely --example rescaling_hello -- -n2 -w2 -p1
//!
//! After a bit..
//!
//!   3) cargo run --package timely --example rescaling_hello -- -n2 -w2 -p2 --join 0 --nn 3
//!
//!      join the cluster with worker 0 as bootstrap server (--join 0), there are now 3 processes in
//!      the cluster (--nn 3)
//!
extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}: seen {}", index, x))
                 .probe_with(&mut probe);
        });

        // if the worker is not a new worker joining the cluster (or we are not in cluster mode)
        // than this is a no-op.
        if worker.bootstrap() {
            return
        }

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                std::thread::sleep(std::time::Duration::from_secs(1));
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
