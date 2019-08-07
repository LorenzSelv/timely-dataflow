//! Broadcasts progress information among workers.

use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::Location;
use crate::communication::{Message, Push, Pull};
use crate::logging::TimelyLogger as Logger;
use std::rc::Rc;
use std::cell::RefCell;
use crate::communication::rescaling::bootstrap::ProgressUpdatesRange;
use std::collections::{HashMap, HashSet};
use abomonation::Abomonation;


#[derive(Clone, Debug, Eq, PartialEq)]
/// TODO(lorenzo) make private again
pub struct ProgressState<T: Timestamp> {
    /// compacted ChangeBatch: all updates ever sent/recved accumulated
    pub acc_updates: ChangeBatch<(Location, T)>,

    /// delta ChangeBatch: cleared every time progcaster.recv() is called
    pub delta_updates: ChangeBatch<(Location, T)>,

    /// hashmap of (worker_index) -> SeqNo
    ///                 ^source in the message
    ///                                ^guaranteed to be monotonically increasing (++), panic if not
    /// note: we also keep (my_index, SeqNo) to track which messages sent are included in the acc state
    worker_seqno: HashMap<usize, usize>, // [0..seq_no[ messages are included in the state
}

impl<T: Timestamp> ProgressState<T> {

    fn new() -> Self {
        ProgressState {
            acc_updates: ChangeBatch::new(),
            delta_updates: ChangeBatch::new(),
            worker_seqno: HashMap::new(),
        }
    }
}

impl<T: Timestamp+Abomonation> ProgressState<T> {

    fn encode(&mut self) -> Vec<u8> {
        // As HashMap does not implement Abomonation, we need to encode it as a vector.
        // We encode the change batch and the worker_seqno separately, one after the other.
        let mut buf = Vec::new();
        // encode change_batch
        unsafe { abomonation::encode(&self.acc_updates, &mut buf) }.expect("encode error");
        // encode worker_seqno
        let worker_seqno_vec: Vec<(usize,usize)> = self.worker_seqno.iter().map(|(x,y)| (*x, *y)).collect();
        unsafe { abomonation::encode(&worker_seqno_vec, &mut buf) }.expect("encode error");
        buf
    }

    fn decode(mut buf: Vec<u8>) -> Self {
        let (typed, mut remaining) = unsafe { abomonation::decode::<ChangeBatch<(Location,T)>>(&mut buf[..]) }.expect("decode error");
        let change_batch = typed.clone();
        let (typed, remaining) = unsafe { abomonation::decode::<Vec<(usize,usize)>>(&mut remaining) }.expect("decode error");
        let worker_seqno: HashMap<usize,usize> = typed.iter().map(|&x| x).collect();
        eprintln!("[decode] received worker_seqno = {:?}", worker_seqno);
        assert_eq!(remaining.len(), 0);
        ProgressState {
            acc_updates: change_batch.clone(),
            delta_updates: change_batch, // initially, all updates are new and should be `recv()`ed
            worker_seqno,
        }
    }

    fn contains_update(&self, worker_index: usize, seqno: usize) -> bool {
        self.worker_seqno.get(&worker_index).map(|&next_seqno| next_seqno > seqno).unwrap_or(false)
    }

    fn update(&mut self, progress_msg: &ProgressMsg<T>, my_index: usize) {
        let worker_index = progress_msg.0;
        let seq_no = progress_msg.1;
        let progress_vec = &progress_msg.2;
        eprintln!("[W{}] recved  ProgressMsg from w={} with seqno={} changes={:?}", my_index, worker_index, seq_no, progress_vec);

        // make sure the message is the next message we expect to read
        if let Some(expected_seqno) = self.worker_seqno.insert(worker_index, seq_no + 1) {
            if expected_seqno != seq_no {
                panic!("got wrong seqno: expected = {}, received = {}, state is {:?}", expected_seqno, seq_no, self.worker_seqno);
            }
        } else {
            if seq_no != 0 {
                panic!("[W{}] first seqno of worker {} should be 0! state is {:?}", my_index, worker_index, self.worker_seqno);
            }
        }

        // apply all updates in the message
        for (pointstamp, delta) in progress_vec.into_iter() {
            self.acc_updates.update(pointstamp.clone(), *delta);
            self.delta_updates.update(pointstamp.clone(), *delta);
        }
    }

    fn apply_updates_range(&mut self, range: ProgressUpdatesRange, mut buf: Vec<u8>) {
        // make sure we are applying the correct range and update the next sequence number
        // if the worker index is contained in the state, then it must be 0
        assert_eq!(*self.worker_seqno.get(&range.worker_index).unwrap_or(&0_usize), range.start_seqno);
        self.worker_seqno.insert(range.worker_index, range.end_seqno);

        let (updates_range, remaining) = unsafe { abomonation::decode::<ProgressVec<T>>(&mut buf[..]) }.expect("decode error");
        assert_eq!(remaining.len(), 0);

        for (pointstamp, delta) in updates_range.into_iter() {
            self.acc_updates.update(pointstamp.clone(), *delta);
            self.delta_updates.update(pointstamp.clone(), *delta);
        }
    }

    fn drain_delta_updates(&mut self) -> std::vec::Drain<((Location, T), i64)> {
        self.delta_updates.drain()
    }
}

struct ProgressRecorder<T: Timestamp> {
    worker_msgs: HashMap<usize, Vec<ProgressMsg<T>>>,
}

impl<T: Timestamp> ProgressRecorder<T> {
    fn new() -> Self {
        ProgressRecorder {
            worker_msgs: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.worker_msgs.clear();
    }

    fn append(&mut self, progress_msg: ProgressMsg<T>) {
        self.worker_msgs
            .entry(progress_msg.0)
            .or_insert(Vec::new())
            .push(progress_msg);
    }
}

impl<T: Timestamp+Abomonation> Abomonation for ProgressRecorder<T> {}

impl<T: Timestamp+Abomonation> ProgressRecorder<T> {

    fn has_updates_range(&mut self, range: &ProgressUpdatesRange) -> bool {
        eprintln!("[has_updates_range] asked for {:?} -- worker_msgs is {:?}", range, self.worker_msgs.iter().map(|(w,msgs)| (w, msgs.iter().map(|msg| msg.1).collect::<Vec<_>>())).collect::<Vec<_>>());

        let msgs = self.worker_msgs.entry(range.worker_index).or_insert(Vec::new());

        if msgs.len() > 0 {
            let first_seqno = msgs.first().unwrap().1;
            let last_seqno = msgs.last().unwrap().1;
            // `range.end_seqno` is exclusive: the new worker will read that message
            // from the direct connection with the other worker.
            // println!("[has_updates_range] first_seqno={} last_seqno={} range={:?}", first_seqno, last_seqno, range);
            first_seqno <= range.start_seqno && range.end_seqno - 1 <= last_seqno
        } else {
            // it's possible that this worker has not received progress updates by some workers yet
            // if the rescaling operation is happening right after starting the cluster or after another rescaling operation
            false
        }
    }

    fn get_updates_range(&mut self, range: &ProgressUpdatesRange) -> Vec<u8> {

        let msgs = self.worker_msgs.get(&range.worker_index).expect("requested a range for missing worker index");

        let first_seqno = msgs[0].1;
        let skip = range.start_seqno - first_seqno;
        let range_size = range.end_seqno - range.start_seqno;

        assert!(skip + range_size <= msgs.len());

        let mut change_batch = ChangeBatch::new();

        for progress_msg in msgs.into_iter().skip(skip).take(range_size) {
            for (pointstamp, delta) in &progress_msg.2 {
                change_batch.update(pointstamp.clone(), *delta);
            }
        }

        let acc = change_batch.into_inner();

        let mut acc_buf = Vec::new();
        unsafe { abomonation::encode(&acc, &mut acc_buf) }.expect("encode error");
        acc_buf
    }
}

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((Location, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of
/// message and internal updates
pub type ProgressMsg<T> = Message<(usize, usize, ProgressVec<T>)>;

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    // reuse allocations
    to_push: Option<ProgressMsg<T>>,

    // TODO(lorenzo): this will become an hashmap, and we get the IDs from there
    //                for now we assume indices are [0..pushers.len()[
    pushers: Rc<RefCell<Vec<Box<Push<ProgressMsg<T>>>>>>,
    puller: Box<Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Number of peers workers within the process
    local_peers: usize,
    /// Sequence number counter
    counter: Rc<RefCell<usize>>,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
    /// Communication channel identifier
    channel_identifier: usize,

    /// we need to maintain accumulate state, so that we can bootstrap workers during rescaling
    pub progress_state: ProgressState<T>,

    recorder: ProgressRecorder<T>,
    is_recording: bool,

    logging: Option<Logger>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, path: &Vec<usize>, mut logging: Option<Logger>) -> Progcaster<T> {

        let channel_identifier = worker.new_identifier();

        let pushers1 = Rc::new(RefCell::new(Vec::with_capacity(worker.peers())));
        let pushers2 = Rc::clone(&pushers1);

        let worker_index = worker.index();
        let local_peers = worker.inner_peers();

        let counter1 = Rc::new(RefCell::new(0_usize));
        let counter2 = Rc::clone(&counter1);

        let send_bootstrap_message1 = Rc::new(RefCell::new(false));
        let send_bootstrap_message2 = Rc::clone(&send_bootstrap_message1);

        let on_new_pusher = move |mut pusher: Box<dyn Push<ProgressMsg<T>>>| {
            // When a new worker joins, we send an empty progress message to let the worker
            // know which is the next seqno it should expect to pull from the channel.
            // We do it only for pushers added after the initial phase.
            if *send_bootstrap_message1.borrow() {
                let mut bootstrap_message = None;
                let seqno = *(*counter1).borrow();
                Progcaster::fill_message(&mut bootstrap_message, worker_index, seqno, &mut ChangeBatch::new());
                eprintln!("[W{}] BOOTSTRAP MESSAGE seqno={}", worker_index, seqno);
                pusher.push(&mut bootstrap_message);
                // (we do not increment the seqno, this is just a "flag" message
            }

            // Append the new pusher to the list of pushers.
            pushers1.borrow_mut().push(pusher);
        };

        let puller = worker.allocate(channel_identifier, &path[..], on_new_pusher);

        // `on_new_pusher` has been called multiple times to init the list of pushers.
        // The closure will be called again only on rescaling and we should send the bootstrap message.
        *send_bootstrap_message2.borrow_mut() = true;

        logging.as_mut().map(|l| l.log(crate::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: crate::logging::CommChannelKind::Progress,
        }));

        let addr = path.clone();
        Progcaster {
            to_push: None,
            pushers: pushers2,
            puller,
            source: worker_index,
            local_peers,
            counter: counter2,
            addr,
            channel_identifier,
            logging,
            progress_state: ProgressState::new(),
            recorder: ProgressRecorder::new(),
            is_recording: false, // not recording initially
        }
    }

    /// Get the channel identifier of the progcaster, unique among all of other progcasters.
    pub fn channel_id(&self) -> usize {
        self.channel_identifier
    }

    /// Sends pointstamp changes to all workers.
    pub fn send(&mut self, mut changes: &mut ChangeBatch<(Location, T)>) {
        assert!(!self.is_recording, "don't send during rescaling operations");

        changes.compact();
        if !changes.is_empty() {

            eprintln!("[W{}] sending ProgressMsg with seqno={} changes={:?}", self.source, *(*self.counter).borrow(), changes.clone().into_inner());
            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
                is_send: true,
                is_duplicate: false,
                source: self.source,
                channel: self.channel_identifier,
                seq_no: *(*self.counter).borrow(),
                addr: self.addr.clone(),
                // TODO: fill with additional data
                messages: Vec::new(),
                internal: Vec::new(),
            }));

            for pusher in self.pushers.borrow_mut().iter_mut() {

                let seqno = *self.counter.borrow();
                Progcaster::fill_message(&mut self.to_push, self.source, seqno, &mut changes);

                // TODO: This should probably use a broadcast channel.
                pusher.push(&mut self.to_push);
                pusher.done();
            }

            *self.counter.borrow_mut() += 1;
            changes.clear();
        }
    }

    fn fill_message(message: &mut Option<ProgressMsg<T>>, source: usize, counter: usize, changes: &mut ChangeBatch<(Location, T)>) {
        // Attempt to re-use allocations, if possible.
        if let Some(tuple) = message {
            let tuple = tuple.as_mut();
            tuple.0 = source;
            tuple.1 = counter;
            tuple.2.clear(); tuple.2.extend(changes.iter().cloned());
        }
        // If we don't have an allocation ...
        if message.is_none() {
            *message = Some(Message::from_typed((
                source,
                counter,
                changes.clone().into_inner(),
            )));
        }
    }

    /// Receives pointstamp changes from all workers.
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        // Then try to pull more changes from the channel.
        self.pull_loop();

        changes.extend(self.progress_state.drain_delta_updates());
    }

    fn pull_loop(&mut self) {
        while let Some(message) = self.puller.pull() {

            let source = message.0;
            let counter = message.1;
            let recv_changes = &message.2;

            let is_duplicate = self.progress_state.contains_update(source, counter);

            let addr = &mut self.addr;
            let channel = self.channel_identifier;
            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
                is_send: false,
                is_duplicate,
                source,
                seq_no: counter,
                channel,
                addr: addr.clone(),
                // TODO: fill with additional data
                messages: Vec::new(),
                internal: Vec::new(),
            }));

            // during rescaling, it could happen that the state for some worker (the last seqno associated with that worker)
            // received by the bootstrap server is ahead of the direct TCP connection
            // with that worker. In that case we should not re-apply the updates.
            if !is_duplicate {

                self.progress_state.update(&message, self.source);

                if self.is_recording {
                    let tuple = (source, counter, recv_changes.iter().cloned().collect());
                    self.recorder.append(Message::from_typed(tuple));
                }
            }
        }
    }
}

/// Handle to progcaster struct to be used for bootstrapping a new worker.
/// It exposes methods used on the server side (i.e. the worker already
/// in the cluster that should bootstrap the new worker).
pub trait ProgcasterServerHandle {

    /// Start recording progress update messages, so that we can answer update_ranges queries.
    fn start_recording(&self);

    /// Stop recording progress update messages.
    fn stop_recording(&self);

    /// Get the encoded (abomonation::encode) progress state.
    fn get_progress_state(&self) -> Vec<u8>;

    /// Return the encoded (abomonation::encode) vector of updates corresponding
    /// to all updates in the requested message range range.
    fn get_updates_range(&self, range: &ProgressUpdatesRange) -> Option<Vec<u8>>;

    /// Return a boxed clone of this handle.
    fn boxed_clone(&self) -> Box<ProgcasterServerHandle>;
}

impl Clone for Box<ProgcasterServerHandle> {
    fn clone(&self) -> Self {
        self.boxed_clone()
    }
}

/// Handle to progcaster struct to be used for bootstrapping a new worker.
/// It exposes methods used on the client side (i.e. the new worker being bootstrapped).
pub trait ProgcasterClientHandle {

    /// Set the encoded progress state of the progcaster
    fn set_progcaster_state(&self, state: Vec<u8>);

    /// Apply all updated provided in the encoded (abomonation::encode) vector of updates
    fn apply_updates_range(&self, range: ProgressUpdatesRange, updates_range: Vec<u8>);

    /// Return the worker indices in the progress state
    fn get_worker_indices(&self) -> HashSet<usize>;

    /// Return a the next missing range request that should be forwarded to the bootstrap server.
    /// `workers_todo` is the set of worker indices from which we expect to receive a bootstrap message.
    fn get_missing_updates_range(&self, workers_todo: &mut HashSet<usize>) -> Option<ProgressUpdatesRange>;

    /// Return a boxed clone of this handle.
    fn boxed_clone(&self) -> Box<ProgcasterClientHandle>;
}

impl Clone for Box<ProgcasterClientHandle> {
    fn clone(&self) -> Self {
        self.boxed_clone()
    }
}


impl<T: Timestamp> ProgcasterServerHandle for Rc<RefCell<Progcaster<T>>> {

    fn start_recording(&self) {
        let mut progcaster = self.borrow_mut();
        assert!(!progcaster.is_recording, "TODO: handle concurrent rescaling operation?");
        progcaster.is_recording = true;
        progcaster.recorder.reset();
    }

    fn stop_recording(&self) {
        let mut progcaster = self.borrow_mut();
        assert!(progcaster.is_recording);
        progcaster.is_recording = false;
        progcaster.recorder.reset();
    }

    fn get_progress_state(&self) -> Vec<u8> {
        let mut progcaster = self.borrow_mut();
        progcaster.progress_state.acc_updates.compact();
        progcaster.progress_state.encode()
    }

    fn get_updates_range(&self, range: &ProgressUpdatesRange) -> Option<Vec<u8>> {
        let mut progcaster = self.borrow_mut();

        assert!(progcaster.is_recording);

        if !progcaster.recorder.has_updates_range(range) {
            // try to pull for more progress messages
            progcaster.pull_loop();
        }

        if progcaster.recorder.has_updates_range(range) {
            Some(progcaster.recorder.get_updates_range(range))
        } else {
            None
        }
    }

    fn boxed_clone(&self) -> Box<ProgcasterServerHandle> {
        Box::new(Rc::clone(&self))
    }
}

impl<T: Timestamp> ProgcasterClientHandle for Rc<RefCell<Progcaster<T>>> {

    fn set_progcaster_state(&self, state: Vec<u8>) {
        let mut progcaster = self.borrow_mut();
        progcaster.progress_state = ProgressState::decode(state);
    }

    fn apply_updates_range(&self, range: ProgressUpdatesRange, updates_range: Vec<u8>) {
        let mut progcaster = self.borrow_mut();
        progcaster.progress_state.apply_updates_range(range, updates_range)
    }

    fn get_worker_indices(&self) -> HashSet<usize> {
        // TODO(lorenzo) assumptions:
        //   1) worker indices are 0..peers
        //   2) this process covers index range [peers - local_peers, peers[
        //       => we return all indices of workers that are not in this process
        //       => [0..peers-local_peers[
        let progcaster = self.borrow();
        let local_peers = progcaster.local_peers;
        let peers = progcaster.pushers.borrow().len();
        (0..peers-local_peers).collect()
    }

    fn get_missing_updates_range(&self, worker_todo: &mut HashSet<usize>) -> Option<ProgressUpdatesRange> {

        let progcaster = &mut *self.borrow_mut();

        if let Some(message) = progcaster.puller.pull() {
            let worker_index = message.0;
            let msg_seqno = message.1;
            let recv_changes = &message.2;

            // state_seqno is the next message seqno that we should read and apply to the state
            let state_seqno = *progcaster.progress_state.worker_seqno.get(&worker_index).unwrap_or(&0_usize);

            if worker_todo.contains(&worker_index) {
                // The first message received by a worker is the bootstrap_message.
                // The bootstrap_message carries no updates and is just used to signal what is the
                // next seqno the worker should expect to pull from the channel.
                assert_eq!(recv_changes.len(), 0);
                worker_todo.remove(&worker_index);

                eprintln!("Got bootstrap message: worker={} seqno={}", worker_index, msg_seqno);

                if state_seqno < msg_seqno {
                    // state is behind of the direct connection with `worker_index`
                    // we will read `msg_seqno` and all the following updates, but we need to ask
                    // for the missing range [state_seqno..msg_seqno[
                    let missing_range = ProgressUpdatesRange {
                        channel_id: progcaster.channel_identifier,
                        worker_index,
                        start_seqno: state_seqno,
                        end_seqno: msg_seqno,
                    };
                    Some(missing_range)
                } else {
                    // Message is a bootstrap message (contains no updates) and the state is up-to-date, nothing to do.
                    None
                }
            } else {
                // Message is not a bootstrap message.
                // Other workers are still making progress, so they might send more progress updates
                // before all workers get the chance to send their bootstrap message.
                // If that's the case we need to update the progress_state, but we need to make
                // sure that all updates from that worker have been already integrated in the progress state.
                // This is guaranteed if we fulfill the missing range request before pulling again.
                progcaster.progress_state.update(message, progcaster.source);
                None
            }
        } else {
            None
        }
    }

    fn boxed_clone(&self) -> Box<ProgcasterClientHandle> {
        Box::new(Rc::clone(&self))
    }
}
