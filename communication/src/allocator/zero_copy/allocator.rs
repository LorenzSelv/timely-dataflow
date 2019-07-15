//! Zero-copy allocator based on TCP.
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::sync::mpsc::{Sender, Receiver};

use bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Message, Data, Pull, Push};
use crate::allocator::{AllocateBuilder, OnNewPushFn};
use crate::allocator::Event;
use crate::allocator::canary::Canary;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};
use super::push_pull::{Pusher, PullerInner};

use pubsub::queue::ring_log_queue::{RingLogCursor, RawRingLogCursor};
use pubsub::queue::demux_cursor::{RawDemuxLogCursor, DemuxFn, OneViewCursor, RawOneViewCursor};
use crate::allocator::zero_copy::push_pull::CursorPullerInner;

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder<A: AllocateBuilder> {
    inner:  A,
    index:  usize,                      // number out of peers
    peers:  usize,                      // number of peer allocators.
    futures:   Vec<Receiver<MergeQueue>>,  // to receive queues to each network thread.
    promises:   Vec<Sender<MergeQueue>>,    // to send queues from each network thread.

    pt_cursor_future:  Receiver<RingLogCursor<()>>, // receive a cursor to read the progress updates queue.
                                                    // Empty type () because we use the raw interface
    pt_queue_future: Receiver<MergeQueue>,          // as the other promises above, we share a MergeQueue to
                                                    // send progress updates to the progress tracking thread

    // receiver side of the channel to the acceptor thread (see `rescale` method).
    rescaler_rx: Option<Receiver<(Sender<MergeQueue>, Receiver<MergeQueue>)>>,
}

/// Creates a vector of builders, sharing appropriate state.
///
/// `threads` is the number of workers in a single process, `processes` is the
/// total number of processes.
/// The returned tuple contains
/// ```ignore
/// (
///   AllocateBuilder for local threads,
///   info to spawn egress comm threads,
///   info to spawn ingress comm thresds,
/// )
/// ```
// TODO(lorenzo) format long functions like this
pub fn new_vector<A: AllocateBuilder>(
    allocators: Vec<A>,
    my_process: usize,
    processes: usize,
    rescaler_rxs: Vec<Option<Receiver<(Sender<MergeQueue>, Receiver<MergeQueue>)>>>)
-> (Vec<TcpBuilder<A>>,
    Vec<Vec<Sender<MergeQueue>>>,
    Vec<Vec<Receiver<MergeQueue>>>,
    Vec<Sender<MergeQueue>>,
    Vec<Sender<RingLogCursor<()>>>)
{
    let threads = allocators.len();

    // For queues from worker threads to network threads, and vice versa.
    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);

    let (mut pt_queue_promises, pt_queue_futures) = crate::promise_futures(1, threads);
    let (pt_cursor_promises, mut pt_cursor_futures) = crate::promise_futures(threads, 1);

    // futures go to the worker thread
    let pt_queue_futures  = pt_queue_futures.into_iter().flatten();
    let pt_cursor_futures  = pt_cursor_futures.pop().unwrap();

    let builders =
    allocators
        .into_iter()
        .zip(worker_promises)
        .zip(worker_futures)
        .zip(rescaler_rxs)
        .zip(pt_queue_futures)
        .zip(pt_cursor_futures)
        .enumerate()
        .map(|(index, (((((inner, promises), futures), rescaler_rx), pt_queue_future), pt_cursor_future))| {
            TcpBuilder {
                inner,
                index: my_process * threads + index,
                peers: threads * processes,
                promises,
                futures,
                rescaler_rx,
                pt_queue_future,
                pt_cursor_future,
            }})
        .collect();

    // promises go to the progress tracking thread
    let pt_queue_promises = pt_queue_promises.pop().unwrap();
    let pt_cursor_promises = pt_cursor_promises.into_iter().flatten().collect();

    (builders, network_promises, network_futures, pt_queue_promises, pt_cursor_promises)
}

impl<A: AllocateBuilder> TcpBuilder<A> {

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> TcpAllocator<A::Allocator> {

        let recvs = self.promises.into_iter().map(fulfill_promise).collect();

        let sends = self.futures.into_iter().map(extract_future).collect();

        // receive the cursor to the log queue from the process tracker recv thread
        let pt_cursor = self.pt_cursor_future.recv().expect("failed to receive cursor");

        let pt_cursor = RawDemuxLogCursor::new(pt_cursor, DemuxFn::raw(|raw_message| {
            MessageHeader::try_read(raw_message).map(|header| header.channel)
        }));

        let pt_queue  = extract_future(self.pt_queue_future);

        TcpAllocator {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            canaries: Rc::new(RefCell::new(Vec::new())),
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),
            pt_to_local: HashMap::new(),
            rescaler_rx: self.rescaler_rx,
            pt_cursor,
            pt_send: pt_queue,
            channels: Vec::new(),
        }
    }
}

// Allocate and send MergeQueue shared with the recv network thread
fn fulfill_promise(promise: Sender<MergeQueue>) -> MergeQueue {
    let buzzer = crate::buzzer::Buzzer::new();
    let queue = MergeQueue::new(buzzer);
    promise.send(queue.clone()).expect("Failed to send MergeQueue");
    queue
}

// Receive MergeQueue shared with the send network thread
fn extract_future(future: Receiver<MergeQueue>) -> Rc<RefCell<SendEndpoint<MergeQueue>>> {
    let queue = future.recv().expect("Failed to receive push queue");
    let sendpoint = SendEndpoint::new(queue);
    Rc::new(RefCell::new(sendpoint))
}

/// Alias trait for `on_new_pusher` function specialized to the Pusher concrete object.
///
/// Using the OnNewPushFn<T> is not possible as it would require to use a Vec of trait objects,
/// but this is not possible as we need to perform certain operation
/// (casting the allocated pusher to the appropriate type needs a generic trait method and it's not allowed
/// in trait objects, see https://doc.rust-lang.org/error-index.html#E0038)
pub trait OnNewPusherFn<T>: FnMut(Box<Pusher<Message<T>, MergeQueue>>) + 'static {}
impl<T,                  F: FnMut(Box<Pusher<Message<T>, MergeQueue>>) + 'static> OnNewPusherFn<T> for F {}

/// A TCP-based allocator for inter-process communication.
pub struct TcpAllocator<A: Allocate> {

    inner:      A,                                  // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    staged:     Vec<Bytes>,                         // staging area for incoming Bytes
    canaries:   Rc<RefCell<Vec<usize>>>,

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>,     // sends[x] -> goes to process x.
    recvs:      Vec<MergeQueue>,                                // recvs[x] <- from process x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,   // to worker-local typed pullers.

    pt_to_local:   HashMap<usize, Rc<RefCell<VecDeque<RawOneViewCursor>>>>,  // to worker-local progress tracker pullers.

    // receiver side of the channel to the acceptor thread (see `rescale` method).
    rescaler_rx: Option<Receiver<(Sender<MergeQueue>, Receiver<MergeQueue>)>>,

    pt_cursor:  RawDemuxLogCursor, // cursor to read the progress updates queue.
                                    // Empty type () because we use the raw interface

    pt_send: Rc<RefCell<SendEndpoint<MergeQueue>>>, // -> goes to progress tracker process

    // store channels allocated so far, so that we can back-fill them with
    // new pushers by calling the associated closur when a new worker process joins the cluster
    channels: Vec<(usize, Box<dyn OnNewPusherFn<()>>)>,
}

impl<A: Allocate> Allocate for TcpAllocator<A> {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data, F>(&mut self, identifier: usize, mut on_new_push: F) -> Box<Pull<Message<T>>>
        where F: OnNewPushFn<T>
    {
        // Inner exchange allocations.
        let inner_peers = self.inner.peers();

        // Create an `on_new_pusher` closure which will be repeatedly called by the `allocate` function;
        // the inner allocator will not store the closure, as intra-process channels do not change over time.
        let inner_sends1 = Rc::new(RefCell::new(Vec::with_capacity(inner_peers)));
        let inner_sends2 = Rc::clone(&inner_sends1);

        let on_new_inner_push = move |push| {
            inner_sends1.borrow_mut().push(push);
        };

        let inner_recv = self.inner.allocate(identifier, on_new_inner_push);

        // now inner had been filled-up
        let mut inner_sends = Rc::try_unwrap(inner_sends2).ok().unwrap().into_inner();

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            if process_id == self.index / inner_peers {
                on_new_push(inner_sends.remove(0));
            }
            else {
                // message header template.
                let header = MessageHeader {
                    channel:    identifier,
                    source:     self.index,
                    target:     target_index,
                    length:     0,
                    seqno:      0,
                };

                // create, box, and stash new process_binary pusher.
                if process_id > self.index / inner_peers { process_id -= 1; }
                on_new_push(Box::new(Pusher::new(header, self.sends[process_id].clone())));
            }
        }

        let channel =
        self.to_local
            .entry(identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(PullerInner::new(inner_recv, channel, canary), identifier, self.events().clone()));


        // This a tricky bit. The `channels` Vec must store closures of the same type (i.e. Pusher cannot be variant in `T`).
        // But the allocator must support allocation of channels of different types (and be able to back-fill them).
        // To circumvent the limitation above, the `rescale` function will allocate a `Pusher` for the empty type `()` (arbitrary)
        // and then call the `on_new_pusher` closure we are crafting below.
        //
        // The `on_new_pusher` closure takes a boxed pusher for type `()` and cast it to the type `T` requested
        // by the allocation. Once we have a pusher of the correct type, we call the `on_new_push` closure
        // that will insert the new pusher in the pushers list.
        //
        // The higher-order function below takes the `on_new_push` closure and crafts the `on_new_pusher` closure.
        let on_new_pusher_from = move |mut on_new_push: Box<dyn OnNewPushFn<T>>| {
            move |pusher: Box<Pusher<Message<()>, MergeQueue>>| {
                let pusher = pusher.into_typed::<T>();
                on_new_push(Box::new(pusher))
            }
        };

        let on_new_pusher = on_new_pusher_from(Box::new(on_new_push));

        // store the on_new_pusher closure so we can call it to back-fill the channel we just allocated
        self.channels.push((identifier, Box::new(on_new_pusher)));

        puller
    }

    fn progress_tracking_channel<T: Data + Clone>(&mut self, identifier: usize)
                                  -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {
        // Create an `on_new_pusher` closure which will be repeatedly called by the `allocate` function;
        // the inner allocator will not store the closure, as intra-process channels do not change over time.
        let inner_peers = self.inner.peers();
        let inner_sends1 = Rc::new(RefCell::new(Vec::with_capacity(inner_peers)));
        let inner_sends2 = Rc::clone(&inner_sends1);

        let on_new_inner_push = move |push| {
            inner_sends1.borrow_mut().push(push);
        };

        let inner_recv = self.inner.allocate(identifier, on_new_inner_push);

        // now inner has been filled-up
        let mut pushers = Rc::try_unwrap(inner_sends2).ok().unwrap().into_inner();


        // allocate a new pusher to the progress tracking send thread
        let pt_index = std::usize::MAX; // TODO(lorenzo) maybe make this a constant

        // message header template.
        let header = MessageHeader {
            channel:    identifier,
            source:     self.index,
            target:     pt_index,
            length:     0,
            seqno:      0,
        };

        let pt_pusher = Box::new(Pusher::new(header, Rc::clone(&self.pt_send)));

        pushers.push(pt_pusher);

        // TODO(lorenzo) progress_tracking_channel is called multiple times,
        //   once for each scope.
        //   => in the queue there are messages for several PT instances,
        //      demux should be done with channel.identifier
        //
        //      demux returns a OneTimeViewer, which can be sent to the
        //      CursorPuller via a shared Rc<RefCell<VecDeque<OneTimeViewer
        //      the puller will drain the vec and look at the messages
        //       (maybe implement read?)
        //
        // `await_events` will park and check for new events is the self.events `VecDeque`
        // progress updates are events we should unpark on
        // => pusher need to append an event when pushing something, puller needs to ??

        let channel =
            self.pt_to_local
                .entry(identifier)
                .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                .clone();

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(CursorPullerInner::new(inner_recv, channel, canary), identifier, self.events().clone()));

        (pushers, puller)
    }

    /// When a new worker process joins the computation, it would initiate connection to every other process
    /// in the cluster. Each process, in turn, has an additional thread waiting for connections (see communication/src/rescaling.rs).
    ///
    /// This function checks with the acceptor (or rescaler) thread if a worker process joined, and if that is case it would
    /// update allocator internal state and back-fill existing channels, by calling the `on_new_pusher`
    /// closure that has been passed to the `allocate` function above.
    ///
    /// The number of peers (total number of worker threads in the computation) is also updated.
    /// As a result, you should *not* rely on the number of peers to remain unchanged.
    fn rescale(&mut self) {
        // try receiving from the rescale thread - did any new worker process initiated a connection?
        if let Some(rescaler_rx) = &self.rescaler_rx {

            // a new process joined. The rescaler thread spawned a new pair of network thread
            // for sending/receiving from this new worker process. We need to setup shared `MergeQueue`
            // with those threads. The protocol is the same as the initialization code.
            if let Ok((promise, future)) = rescaler_rx.try_recv() {

                // update recvs and sends
                self.recvs.push(fulfill_promise(promise));

                let new_send = extract_future(future);
                self.sends.push(new_send.clone());

                let threads = self.inner.peers();
                let self_index = self.index;
                let self_peers = self.peers;

                // back-fill existing channels with `threads` new pushers pointing to the new send
                for (channel_id, on_new_pusher) in self.channels.iter_mut() {

                    // ASSUMPTION: if there are currently P processes, then
                    //             current processes have indexes [0..P-1]
                    //             and the new process has index P
                    //
                    // This will not be true when we allow an arbitrary worker to leave the cluster

                    // for each worker thread in the remote process (assumption: it has the same number of threads)
                    // allocate a pusher with appropriate message header and call the `on_new_pusher` closure
                    // to back-fill the channel
                    (0..threads).for_each(|thread_idx| {
                        let header = MessageHeader {
                            channel: *channel_id,
                            source: self_index,
                            target: self_peers + thread_idx, // see assumption above
                            length: 0,
                            seqno: 0,
                        };
                        on_new_pusher(Box::new(Pusher::new(header, new_send.clone())));
                    });
                }

                // the new process adds `threads` new workers to the cluster
                self.peers += threads;
            }
        }
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {

        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let dropped =
            self.to_local
                .remove(&dropped_channel)
                .expect("non-existent channel dropped");
            assert!(dropped.borrow().is_empty());
        }
        ::std::mem::drop(canaries);

        // receive intra-process messages
        self.inner.receive();

        // receive inter-process messages
        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        let mut events = self.inner.events().borrow_mut();

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(40);

                    // Increment message count for channel.
                    events.push_back((header.channel, Event::Pushed(1)));

                    // Ensure that a queue exists.
                    // We may receive data before allocating, and shouldn't block.
                    self.to_local
                        .entry(header.channel)
                        .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                        .borrow_mut()
                        .push_back(peel);
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }

        // receive progress update messages
        while let Some((channel_id, one_time_viewer)) = self.pt_cursor.demux_next() {

            if let Some(channel_id) = channel_id {
                // Increment message count for channel.
                events.push_back((channel_id, Event::Pushed(1)));

                // TODO(lorenzo) offset inside the one_time_viewer to discard the header!

                // Send one_time_viewer to the puller, so that it will be able to read the progress update
                self.pt_to_local
                    .entry(channel_id)
                    .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
                    .borrow_mut()
                    .push_back(one_time_viewer);
            }
            else {
                println!("failed to read full header!");
            }
        }
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        self.pt_send.borrow_mut().publish();

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for (index, list) in self.to_local.iter() {
        //     let len = list.borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        self.inner.events()
    }
    fn await_events(&self, duration: Option<std::time::Duration>) {
        self.inner.await_events(duration);
    }
}