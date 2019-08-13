extern crate timely;

use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::source;
use timely::scheduling::Scheduler;

use colored::*;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();

        worker.dataflow::<u32, _, _>(|scope| {

            source(scope, "Source", |capability, info| {

                println!("[Source] initial cap is {:?}", capability);

                let activator = scope.activator_for(&info.address[..]);

                let mut cap = Some(capability);
                move |output| {

                    let mut done = false;
                    if let Some(cap) = cap.as_mut() {
                        // get some data and send it.
                        let time = *cap.time();
                        output.session(&cap)
                              .give((time, index));

                        // downgrade capability.
                        cap.downgrade(&(time + 1));
                        done = time > 20;
                    }

                    if done { cap = None; }
                    else    { activator.activate(); }
                }
            })
            .exchange(|_| 0)
            .unary_notify(Pipeline, "hello", None, move |input, output, notificator| {

                input.for_each(|cap, data| {
                    output.session(&cap).give_vec(&mut data.replace(Vec::new()));
                    let next_time = *cap.time()+1;
                    notificator.notify_at(cap.delayed(&next_time));
                });

                notificator.for_each(|time, count, _self| {
                    println!("[W{}] notificator.for_each time={:?}, count={:?}", index, time.time(), count);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                });
            })
            .inspect_batch(move |t, x| println!("{}", format!("[W{}@inspect] seen {:?} at time {:?}", index, x, t).bold().red()));
        });

        worker.bootstrap();

    }).unwrap();
}