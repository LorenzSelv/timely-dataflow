//! Zero-copy allocator based on TCP.
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap};

use bytes::arc::Bytes;

use networking::MessageHeader;

use {Allocate, allocator::AllocateBuilder, Data, Push, Pull};
use allocator::Message;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue, Signal};
use super::push_pull::{Pusher, PullerInner};

/// Builds an instance of a TcpAllocator.
///
/// Builders are required because some of the state in a `TcpAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct TcpBuilder<A: AllocateBuilder> {
    inner:      A,
    index:      usize,              // number out of peers
    peers:      usize,              // number of peer allocators.
    sends:      Vec<MergeQueue>,    // for pushing bytes at remote processes.
    recvs:      Vec<MergeQueue>,    // for pulling bytes from remote processes.
    signal:     Signal,
}

impl<A: AllocateBuilder> TcpBuilder<A> {
    /// Creates a vector of builders, sharing appropriate state.
    pub fn new_vector(
        my_process: usize,
        threads: usize,
        processes: usize,
        inner_builders: Vec<A>,
        )
    // -> (Vec<TcpBuilder<Process>>, Vec<Receiver<Bytes>>, Vec<Sender<Bytes>>) {
    -> (Vec<Self>, Vec<(Vec<MergeQueue>, Signal)>, Vec<Vec<MergeQueue>>) {

        // The results are a vector of builders, as well as the necessary shared state to build each
        // of the send and receive communication threads, respectively.

        // Should have as many inner builders as threads.
        assert_eq!(threads, inner_builders.len());

        let worker_signals: Vec<Signal> = (0 .. threads).map(|_| Signal::new()).collect();
        let network_signals: Vec<Signal> = (0 .. processes-1).map(|_| Signal::new()).collect();

        let worker_to_network: Vec<Vec<_>> = (0 .. threads).map(|_| (0 .. processes-1).map(|p| MergeQueue::new(network_signals[p].clone())).collect()).collect();
        let network_to_worker: Vec<Vec<_>> = (0 .. processes-1).map(|_| (0 .. threads).map(|t| MergeQueue::new(worker_signals[t].clone())).collect()).collect();

        let worker_from_network: Vec<Vec<_>> = (0 .. threads).map(|t| (0 .. processes-1).map(|p| network_to_worker[p][t].clone()).collect()).collect();
        let network_from_worker: Vec<Vec<_>> = (0 .. processes-1).map(|p| (0 .. threads).map(|t| worker_to_network[t][p].clone()).collect()).collect();

        let builders =
        // Process::new_vector(threads)
        inner_builders
            .into_iter()
            .zip(worker_signals)
            .zip(worker_to_network)
            .zip(worker_from_network)
            .enumerate()
            .map(|(index, (((inner, signal), sends), recvs))| {
                TcpBuilder {
                    inner,
                    index: my_process * threads + index,
                    peers: threads * processes,
                    sends,
                    recvs,
                    signal,
                }})
            .collect();

        let sends = network_from_worker.into_iter().zip(network_signals).collect();

        (builders, sends, network_to_worker)
    }
}

impl<A: AllocateBuilder> AllocateBuilder for TcpBuilder<A> {

    type Allocator = TcpAllocator<A::Allocator>;

    /// Builds a `TcpAllocator`, instantiating `Rc<RefCell<_>>` elements.
    fn build(self) -> Self::Allocator {

        let mut sends = Vec::new();
        for send in self.sends.into_iter() {
            let sendpoint = SendEndpoint::new(send);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        TcpAllocator {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            // allocated: 0,
            _signal: self.signal,
            staged: Vec::new(),
            sends,
            recvs: self.recvs,
            to_local: HashMap::new(),
        }
    }
}

/// A TCP-based allocator for inter-process communication.
pub struct TcpAllocator<A: Allocate> {

    inner:      A,                                  // A non-serialized inner allocator for process-local peers.

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).
    // allocated:  usize,                              // indicates how many channels have been allocated (locally).

    _signal:     Signal,

    staged:     Vec<Bytes>,

    // sending, receiving, and responding to binary buffers.
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>,     // sends[x] -> goes to process x.
    recvs:      Vec<MergeQueue>,                                // recvs[x] <- from process x?.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,   // to worker-local typed pullers.
}

impl<A: Allocate> Allocate for TcpAllocator<A> {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<Push<Message<T>>>>, Box<Pull<Message<T>>>) {

        // Result list of boxed pushers.
        let mut pushes = Vec::<Box<Push<Message<T>>>>::new();

        // Inner exchange allocations.
        let inner_peers = self.inner.peers();
        let (mut inner_sends, inner_recv) = self.inner.allocate(identifier);

        for target_index in 0 .. self.peers() {

            // TODO: crappy place to hardcode this rule.
            let mut process_id = target_index / inner_peers;

            if process_id == self.index / inner_peers {
                pushes.push(inner_sends.remove(0));
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
                pushes.push(Box::new(Pusher::new(header, self.sends[process_id].clone())));
            }
        }

        let channel =
        self.to_local
            .entry(identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        let puller = Box::new(PullerInner::new(inner_recv, channel));

        (pushes, puller, )
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn pre_work(&mut self) {

        self.inner.pre_work();

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(40);

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
    fn post_work(&mut self) {
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
}