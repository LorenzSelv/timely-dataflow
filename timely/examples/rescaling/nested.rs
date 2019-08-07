extern crate timely;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // circulate numbers, Collatz stepping each time.
        worker.dataflow(|scope| {
            let numbers = scope.input_from(&mut input).exchange(|x| *x);

            let numbers = scope.iterative::<u32,_,_>(|subscope| {
                numbers
                    .enter(subscope)
                    .inspect(|&n| println!("{:?}", (0..n).collect::<Vec<_>>()))
                    .leave()
            });

            numbers.inspect(|x| println!("N={:?} done", x)).probe_with(&mut probe);
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