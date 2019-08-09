extern crate timely;

use timely::dataflow::operators::*;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::channels::pact::Pipeline;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let index = worker.index();

        // circulate numbers, Collatz stepping each time.
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|x| *x)
                // receiving input is associated with a mint_ref operation so it's ok
                // but if you don't receive input then only cap you actually own
                .unary(Pipeline, "increment", |_capability, _info| {

                    let mut vector = Vec::new();
                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut vector);
                            let mut session = output.session(&time);
                            for datum in vector.drain(..) {
                                session.give(datum + 1);
                            }
                        }
                    }
                })
                .inspect(move |x| println!("[W{}] seen {:?}", index, x))
                .probe_with(&mut probe);
        });

        // if the worker is not a new worker joining the cluster (or we are not in cluster mode)
        // than this is a no-op.
        if worker.bootstrap() {
            return
        }

        // introduce data and watch!
        for round in 0..30 {
            if worker.index() == 0 {
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