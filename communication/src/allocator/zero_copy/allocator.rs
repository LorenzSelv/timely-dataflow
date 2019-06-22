//! Zero-copy allocator based on TCP.
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};
use std::sync::mpsc::{Sender, Receiver};

use bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Message, Data, Push, Pull};
use crate::allocator::{AllocateBuilder, OnNewPusherFn};
use crate::allocator::Event;
use crate::allocator::canary::Canary;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};
use super::push_pull::{Pusher, PullerInner};
use core::borrow::Borrow;

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

    // TODO(lorenzo) doc
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
    Vec<Vec<Receiver<MergeQueue>>>)
{
    let threads = allocators.len();

    // For queues from worker threads to network threads, and vice versa.
    let (network_promises, worker_futures) = crate::promise_futures(processes-1, threads);
    let (worker_promises, network_futures) = crate::promise_futures(threads, processes-1);

    let builders =
    allocators
        .into_iter()
        .zip(worker_promises)
        .zip(worker_futures)
        .zip(rescaler_rxs)
        .enumerate()
        .map(|(index, (((inner, promises), futures), rescaler_rx))| {
            TcpBuilder {
                inner,
                index: my_process * threads + index,
                peers: threads * processes,
                promises,
                futures,
                rescaler_rx,
            }})
        .collect();

    (builders, network_promises, network_futures)
}

impl<A: AllocateBuilder> TcpBuilder<A> {

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> TcpAllocator<A::Allocator> {

        let recvs = self.promises.into_iter().map(fulfill_promise).collect();

        let sends = self.futures.into_iter().map(extract_future).collect();

        TcpAllocator {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            canaries: Rc::new(RefCell::new(Vec::new())),
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),
            rescaler_rx: self.rescaler_rx,
            channels: Vec::new(),
        }
    }
}

fn fulfill_promise(promise: Sender<MergeQueue>) -> MergeQueue {
    let buzzer = crate::buzzer::Buzzer::new();
    let queue = MergeQueue::new(buzzer);
    promise.send(queue.clone()).expect("Failed to send MergeQueue");
    queue
}

fn extract_future(future: Receiver<MergeQueue>) -> Rc<RefCell<SendEndpoint<MergeQueue>>> {
    let queue = future.recv().expect("Failed to receive push queue");
    let sendpoint = SendEndpoint::new(queue);
    Rc::new(RefCell::new(sendpoint))
}

/// A trait with no generic type `T` so that on_new_pusher closures can be stored as Box<dyn OnNewPusher> in the same Vec
//trait OnNewPusher {}
//impl <T: 'static> OnNewPusherFn<T> for OnNewPusher {} // TODO static?

trait SizedData : Data + Sized {}

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

    // TODO(lorenzo) doc
    rescaler_rx: Option<Receiver<(Sender<MergeQueue>, Receiver<MergeQueue>)>>,

    // store channels allocated so far, so that we can back-fill them with
    // new pushers when a new worker process joins the cluster
    channels: Vec<(usize, Box<dyn OnNewPusherFn<SizedData>>)>,
}

impl<A: Allocate> Allocate for TcpAllocator<A> {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data, F>(&mut self, identifier: usize, on_new_pusher: F) -> Box<Pull<Message<T>>>
        where F: OnNewPusherFn<T>
    {
        // Inner exchange allocations.
        let inner_peers = self.inner.peers();

        // Create an `on_new_pusher` closure which will be repeatedly called by the `allocate` function;
        // the inner allocator will not store the closure, as intra-process channels do not change over time.
        let inner_sends1 = Rc::new(RefCell::new(Vec::with_capacity(inner_peers)));
        let inner_sends2 = Rc::clone(&inner_sends1);

        let on_new_pusher = move |pusher| {
            inner_sends1.borrow_mut().push(pusher);
        };

        let inner_recv = self.inner.allocate(identifier, on_new_pusher);

        // now inner had been filled-up
        let mut inner_sends = inner_sends1.borrow_mut();

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            if process_id == self.index / inner_peers {
                on_new_pusher(inner_sends.remove(0));
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
                on_new_pusher(Box::new(Pusher::new(header, self.sends[process_id].clone())));
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

        puller
    }

    // TODO(lorenzo) doc
    fn rescale(&mut self) {
        if let Some(rescaler_rx) = &self.rescaler_rx {

            if let Ok((promise, future)) = rescaler_rx.try_recv() {

                // update recvs and sends
                self.recvs.push(fulfill_promise(promise));

                let new_send = extract_future(future);
                self.sends.push(new_send.clone());

                let threads = self.inner.peers();

                // back-fill existing channels with `threads` new pushers pointing to the new send
                for (channel_id, on_new_pusher) in self.channels.iter() {

                    let on_new_pusher: &OnNewPusherFn<Data> = on_new_pusher.borrow();

                    // ASSUMPTION: if there are currently P processes, then
                    //             current processes have indexes [0..P-1]
                    //             and the new process has index P
                    //
                    // This will not be true when we allow an arbitrary worker to leave the cluster

                    (0..threads).map(|thread_idx| {
                        let header = MessageHeader {
                            channel: *channel_id,
                            source: self.index,
                            target: self.peers + thread_idx, // see assumption above
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

        self.inner.receive();

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
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

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