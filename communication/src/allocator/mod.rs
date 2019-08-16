//! Types and traits for the allocation of channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::VecDeque;

pub use self::thread::Thread;
pub use self::process::Process;
pub use self::generic::{Generic, GenericBuilder};

pub mod thread;
pub mod process;
pub mod generic;

pub mod canary;
pub mod counters;

pub mod zero_copy;

use crate::{Data, Push, Pull, Message};
use std::net::TcpStream;
use crate::rescaling::bootstrap::{BootstrapRecvEndpoint, ProgressUpdatesRange};

/// A proto-allocator, which implements `Send` and can be completed with `build`.
///
/// This trait exists because some allocators contain elements that do not implement
/// the `Send` trait, for example `Rc` wrappers for shared state. As such, what we
/// actually need to create to initialize a computation are builders, which we can
/// then move into new threads each of which then construct their actual allocator.
pub trait AllocateBuilder : Send {
    /// The type of allocator to be built.
    type Allocator: Allocate;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator;
}

/// Alias trait for the `on_new_push` closure expected by the `allocate` function.
///
/// The closure expects a (boxed) pusher to for messages of type `T`.
/// The intended behavior is to add the pusher to a list of pushers wrapped
/// in a Rc<RefCell<..>.
/// This enables an allocator to store the closure and call it in the future
/// to back-fill the channel allocation with new pushers.
///
/// The actual back-filling is performed on-demand when calling the `rescale` function.
pub trait OnNewPushFn<T>: FnMut(Box<dyn Push<Message<T>>>) + 'static {}
impl<T,                F: FnMut(Box<dyn Push<Message<T>>>) + 'static> OnNewPushFn<T> for F {}

/// Alias trait for the send_state_closure expected by the `rescale` function.
pub trait BootstrapSendStateClosure : FnOnce(&mut TcpStream) {}
impl <F: FnOnce(&mut TcpStream)> BootstrapSendStateClosure for F {}

/// Alias trait for the get_updates_range_closure expected by the `rescale` function.
pub trait BootstrapGetUpdatesRangeClosure : Fn(&ProgressUpdatesRange) -> Option<Vec<u8>> {}
impl <F: Fn(&ProgressUpdatesRange) -> Option<Vec<u8>>> BootstrapGetUpdatesRangeClosure for F {}

/// Alias trait for the done_closure (signaling the end of the protocol) expected by the `rescale` function.
pub trait BootstrapDoneClosure : FnOnce() {}
impl <F: FnOnce()> BootstrapDoneClosure for F {}

/// A type capable of allocating channels.
///
/// There is some feature creep, in that this contains several convenience methods about the nature
/// of the allocated channels, and maintenance methods to ensure that they move records around.
pub trait Allocate {
    /// The index of the worker out of `(0..self.peers())`.
    fn index(&self) -> usize;
    /// The number of workers in the communication group.
    fn peers(&self) -> usize;
    /// The number of workers in the communication group.
    fn peers_rc(&self) -> Rc<RefCell<usize>>;
    /// The number of workers per process in the communication group.
    fn inner_peers(&self) -> usize;
    /// The number of workers in the initial communication group.
    /// Defaults to peers, only in cluster mode with rescaling operations will change.
    fn init_peers(&self) -> usize { self.peers() }
    /// Indicates if we are in rescaling mode
    fn is_rescaling(&self) -> bool;
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data, F>(&mut self, identifier: usize, on_new_pusher: F) -> Box<dyn Pull<Message<T>>>
         where F: OnNewPushFn<T>;

    /// If a configuration change happens in the cluster, adapt existing channels to reflect that change.
    ///
    /// This function is implemented only by the `TcpAllocator` which allows the addition (and maybe removal?)
    /// of workers. When a new worker process joins the computation, it would initiate connection to every other process
    /// in the cluster. Each process, in turn, has an additional thread waiting for connections (rescaler thread, see communication/src/mod.rs).
    ///
    /// This function checks with the acceptor thread if a worker process joined, and if that is case it would
    /// update allocator internal state and back-fill existing channels, by calling the `on_new_pusher`
    /// closure that has been passed to the `allocate` function above.
    ///
    /// The number of peers (total number of worker threads in the computation) is also updated.
    /// As a result, you should *not* rely on the number of peers to remain unchanged.
    ///
    /// The `ExchangePusher` relies on the modulo operator, and thus on a constant number of peers
    /// to maintain correctness. This needs to be updated to use a routing table, or to require the usage
    /// of Megaphone that keeps the routing table for us.
    ///
    /// If a rescaling operations happened, the new worker process selected a bootstrap server to
    /// initialize its own progress tracking state.
    /// The bootstrap closures are used to perform the initialization protocol by the bootstrap server.
    /// We need to pass closures in order to hide the usage of data types that are only available
    /// in the timely crate (not in the communication crate) such as `Progcaster` or the `Timestamp` type itself.
    ///
    /// The closures are defined in the `Worker::rescale` function which is calling this function.
    ///
    fn rescale(&mut self,
               _: impl BootstrapSendStateClosure,
               _: impl BootstrapGetUpdatesRangeClosure,
               _: impl BootstrapDoneClosure) { /* nop by default */ }

    /// A shared queue of communication events with channel identifier.
    ///
    /// It is expected that users of the channel allocator will regularly
    /// drain these events in order to drive their computation. If they
    /// fail to do so the event queue may become quite large, and turn
    /// into a performance problem.
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>>;

    /// Awaits communication events.
    ///
    /// This method may park the current thread, for at most `duration`,
    /// until new events arrive.
    /// The method is not guaranteed to wait for any amount of time, but
    /// good implementations should use this as a hint to park the thread.
    fn await_events(&self, _duration: Option<Duration>) { }

    /// Ensure that received messages are surfaced in each channel.
    ///
    /// This method should be called to ensure that received messages are
    /// surfaced in each channel, but failing to call the method does not
    /// ensure that they are not surfaced.
    ///
    /// Generally, this method is the indication that the allocator should
    /// present messages contained in otherwise scarce resources (for example
    /// network buffers), under the premise that someone is about to consume
    /// the messages and release the resources.
    fn receive(&mut self) { }

    /// Signal the completion of a batch of reads from channels.
    ///
    /// Conventionally, this method signals to the communication fabric
    /// that the worker is taking a break from reading from channels, and
    /// the fabric should consider re-acquiring scarce resources. This can
    /// lead to the fabric performing defensive copies out of un-consumed
    /// buffers, and can be a performance problem if invoked casually.
    fn release(&mut self) { }

    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default, this method uses the thread-local channel constructor
    /// based on a shared `VecDeque` which updates the event queue.
    fn pipeline<T: 'static>(&mut self, identifier: usize) ->
        (thread::ThreadPusher<Message<T>>,
         thread::ThreadPuller<Message<T>>)
    {
        thread::Thread::new_from(identifier, self.events().clone())
    }

    /// If the worker is joining the cluster, return the endpoint to the bootstrapping thread
    fn get_bootstrap_endpoint(&mut self) -> Option<BootstrapRecvEndpoint> { None }
}

/// A communication channel event.
pub enum Event {
    /// A number of messages pushed into the channel.
    Pushed(usize),
    /// A number of messages pulled from the channel.
    Pulled(usize),
}
