//! Broadcast records to all workers.

// use std::rc::Rc;
// use std::cell::RefCell;

// use communication::Pull;

use crate::ExchangeData;
// use progress::{Source, Target};
// use progress::{Timestamp, Operate, operate::{Schedule, SharedProgress}, Antichain};
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::channels::pact::Broadcast as BroadcastPact;

// use dataflow::channels::{Message, Bundle};
// use dataflow::channels::pushers::Counter as PushCounter;
// use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
// use dataflow::channels::pushers::Tee;
// use dataflow::channels::pullers::Counter as PullCounter;
// use dataflow::channels::pact::{LogPusher, LogPuller};

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Broadcast, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(&self) -> Self;
}

impl<G: Scope, D: ExchangeData> Broadcast<D> for Stream<G, D> {
    fn broadcast(&self) -> Stream<G, D> {

        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        // let peers = self.scope().peers() as u64;
        // self.flat_map(move |x| (0 .. peers).map(move |i| (i,x.clone())))
        //     .exchange(|ix| ix.0)
        //     .map(|(_i,x)| x)

        let mut vector = Vec::new();
        self.unary(BroadcastPact, "Broadcast", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_vec(&mut vector);
            });
        })
    }
}
