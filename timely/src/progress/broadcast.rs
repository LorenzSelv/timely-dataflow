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
struct ProgressState<T: Timestamp> {
    /// compacted ChangeBatch: all updates ever sent/recved accumulated
    change_batch: ChangeBatch<(Location,T)>,

    /// hashmap of (worker_index) -> SeqNo
    ///                 ^source in the message
    ///                                ^guaranteed to be monotonically increasing (++), panic if not
    /// note: we also keep (my_index, SeqNo) to track which messages sent are included in the acc state
    worker_seqno: HashMap<usize, usize>, // [0..seq_no[ messages are included in the state
}

impl<T: Timestamp> ProgressState<T> {

    fn new() -> Self {
        ProgressState {
            change_batch: ChangeBatch::new(),
            worker_seqno: HashMap::new(),
        }
    }

    // TODO(lorenzo) after a rescaling operation is complete, we should track also the new worker in the state
    fn new_worker(&mut self, _worker_index: usize) {
        unimplemented!();
    }
}

impl<T: Timestamp+Abomonation> Abomonation for ProgressState<T> {}

impl<T: Timestamp+Abomonation> ProgressState<T> {

    fn encode(&mut self) -> Vec<u8> {
        let mut buf = Vec::new();
        println!("before encode: state is {:?}", self);
        unsafe { abomonation::encode(self, &mut buf) }.expect("encode error");
        println!("after encode: buffer is {:?}", buf);
        buf
    }

    fn decode(mut buf: Vec<u8>) -> Self {
        println!("before decode: buffer is {:?}", buf);
        let (typed, remaining) = unsafe { abomonation::decode::<Self>(&mut buf[..]) }.expect("decode error");
        assert_eq!(remaining.len(), 0);
        typed.clone()
    }

    fn contains_update(&self, worker_index: usize, seqno: usize) -> bool {
        self.worker_seqno.get(&worker_index).map(|&next_seqno| next_seqno > seqno).unwrap_or(false)
    }

    fn update(&mut self, progress_msg: &ProgressMsg<T>) {
        let worker_index = progress_msg.0;
        let seq_no = progress_msg.1;
        let progress_vec = &progress_msg.2;
        println!("state before update: {:?}", self.worker_seqno);
        println!("update {} {} {:?}", worker_index, seq_no, progress_vec);

        // make sure the message is the next message we expect to read
        if let Some(expected_seqno) = self.worker_seqno.insert(worker_index, seq_no + 1) {
            assert_eq!(expected_seqno, seq_no, "got wrong seqno!");
        } else {
            if seq_no != 0 {
                println!("first seqno of worker {} should be 0! state is {:?}", worker_index, self.worker_seqno);
                assert_eq!(seq_no, 0);
            }
        }

        // apply all updates in the message
        for (pointstamp, delta) in progress_vec.into_iter() {
            self.change_batch.update(pointstamp.clone(), *delta);
        }
    }

    fn apply_updates_range(&mut self, range: ProgressUpdatesRange, mut buf: Vec<u8>) {
        // make sure we are applying the correct range and update the next sequence number
        assert_eq!(self.worker_seqno[&range.worker_index], range.start_seqno);
        self.worker_seqno.insert(range.worker_index, range.end_seqno);

        let (updates_range, remaining) = unsafe { abomonation::decode::<ProgressVec<T>>(&mut buf[..]) }.expect("decode error");
        assert_eq!(remaining.len(), 0);

        for (pointstamp, delta) in updates_range.into_iter() {
            self.change_batch.update(pointstamp.clone(), *delta);
        }
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
        let msgs = &self.worker_msgs[&range.worker_index];

        if let Some(first_msg) = msgs.first() {

            let last_msg = msgs.last().unwrap();

            let first_seqno = first_msg.1;
            let last_seqno = last_msg.1;

            // `range.end_seqno` is exclusive: the new worker will read that message
            // from the direct connection with the other worker.
            first_seqno <= range.start_seqno && range.end_seqno - 1 <= last_seqno
        } else {
            false
        }
    }

    fn get_updates_range(&mut self, range: &ProgressUpdatesRange) -> Vec<u8> {

        let msgs = self.worker_msgs.remove(&range.worker_index).expect("requested a range for missing worker index");

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

    pushers: Rc<RefCell<Vec<Box<Push<ProgressMsg<T>>>>>>, // TODO: this will become and hashmap, and we get the IDs from there
    puller: Box<Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
    /// Communication channel identifier
    channel_identifier: usize,

    /// we need to maintain accumulate state, so that we can bootstrap workers during rescaling
    progress_state: ProgressState<T>,

    // where we stash messages that we should apply to the progress state after initialization
    progress_msg_stash: Vec<ProgressMsg<T>>,

    recorder: ProgressRecorder<T>,
    is_recording: bool,

    pulled_changes_stash: ChangeBatch<(Location, T)>,

    logging: Option<Logger>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied worker.
    pub fn new<A: crate::worker::AsWorker>(worker: &mut A, path: &Vec<usize>, mut logging: Option<Logger>) -> Progcaster<T> {

        let channel_identifier = worker.new_identifier();

        let pushers1 = Rc::new(RefCell::new(Vec::with_capacity(worker.peers())));
        let pushers2 = Rc::clone(&pushers1);

        let on_new_pusher = move |pusher| {
            pushers1.borrow_mut().push(pusher);
        };

        let puller= worker.allocate(channel_identifier, &path[..], on_new_pusher);

        logging.as_mut().map(|l| l.log(crate::logging::CommChannelsEvent {
            identifier: channel_identifier,
            kind: crate::logging::CommChannelKind::Progress,
        }));
        let worker_index = worker.index();
        let addr = path.clone();
        Progcaster {
            to_push: None,
            pushers: pushers2,
            puller,
            source: worker_index,
            counter: 0,
            addr,
            channel_identifier,
            logging,
            progress_state: ProgressState::new(),
            progress_msg_stash: Vec::new(),
            pulled_changes_stash: ChangeBatch::new(),
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
            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
                is_send: true,
                is_duplicate: false,
                source: self.source,
                channel: self.channel_identifier,
                seq_no: self.counter,
                addr: self.addr.clone(),
                // TODO: fill with additional data
                messages: Vec::new(),
                internal: Vec::new(),
            }));

            for pusher in self.pushers.borrow_mut().iter_mut() {

                Progcaster::fill_message(&mut self.to_push, self.source, self.counter, &mut changes);

                // TODO: This should probably use a broadcast channel.
                pusher.push(&mut self.to_push);
                pusher.done();
            }

            self.counter += 1;
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
    pub fn recv(&mut self, mut changes: &mut ChangeBatch<(Location, T)>) {
        // First drain all stashed changes that we already pulled, but not exposed yet.
        changes.extend(self.pulled_changes_stash.drain());

        // Then try to pull more changes from the channel.
        self.pull_loop(&mut changes);
    }

    fn pull_loop(&mut self, changes: &mut ChangeBatch<(Location, T)>) {
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

                // We clone rather than drain to avoid deserialization.
                for &(ref update, delta) in recv_changes.iter() {
                    changes.update(update.clone(), delta);
                }

                self.progress_state.update(&message);

                if self.is_recording {
                    let tuple = (self.source, self.counter, recv_changes.iter().cloned().collect());
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
    fn get_updates_range(&self, range: &ProgressUpdatesRange) -> Vec<u8>;

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

    /// Return a list of missing range requests. These requests, when combined to the accumulated
    /// state of the progcaster, would provide all the progress messages that need to be received
    /// to complete the initialization of the progcaster.
    /// `exclude_id` is the id of the worker acting as the bootstrap server. Since the
    /// bootstrapping is done synchronously, the
    fn get_missing_updates_ranges(&self, exclude_id: usize) -> Vec<ProgressUpdatesRange>;

    /// To figure out the missing updates, we pulled from the channel and stashed
    /// away progress messages. These messages should be applied to the state to
    /// complete the initialization.
    fn apply_stashed_progress_msgs(&self);

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
        progcaster.progress_state.encode()
    }

    fn get_updates_range(&self, range: &ProgressUpdatesRange) -> Vec<u8> {
        let mut progcaster = self.borrow_mut();

        assert!(progcaster.is_recording);

        // we might not have the requested range yet! If that's the case, we should
        // pull from the channel until we do (stashing changes for later).
        let mut changes_stash = ChangeBatch::new();

        while !progcaster.recorder.has_updates_range(range) {
            progcaster.pull_loop(&mut changes_stash);
        }

        progcaster.pulled_changes_stash.extend(changes_stash.drain());

        progcaster.recorder.get_updates_range(range)
    }

    fn boxed_clone(&self) -> Box<ProgcasterServerHandle> {
        Box::new(Rc::clone(&self))
    }
}

impl<T: Timestamp> ProgcasterClientHandle for Rc<RefCell<Progcaster<T>>> {

    fn set_progcaster_state(&self, state: Vec<u8>) {
        let mut progcaster = self.borrow_mut();
        progcaster.progress_state = ProgressState::decode(state);
        println!("decoded state is {:?}", progcaster.progress_state);
    }

    fn get_missing_updates_ranges(&self, server_index: usize) -> Vec<ProgressUpdatesRange> {

        let mut progcaster = self.borrow_mut();
        let progcaster = &mut *progcaster;

        let mut worker_todo: HashSet<usize> = progcaster.progress_state.worker_seqno.keys().map(|x| *x).collect();

        // The server worker will not push any progress message until bootstrapping is completed.
        // When the worker will start reading from its direct connection with the server worker,
        // it will find the next seqno after the one that appears in the state.
        // Thus there will be no missing range with the server worker, we can remove it from the list.
        worker_todo.remove(&server_index);

        let mut missing_ranges = Vec::new();

        while !worker_todo.is_empty() {

            while let Some(message) = progcaster.puller.pull() {
                let worker_index = message.0;
                let msg_seq_no = message.1;
                let recv_changes = &message.2;

                // state_seq_no is the next message that we should read and apply to the state
                let state_seq_no = *progcaster.progress_state.worker_seqno.get(&worker_index).expect("msg from unknown worker");

                // if the message is not included in the state, we need to stash it
                // so we can apply it later
                if state_seq_no <= msg_seq_no {
                    let tuple = (worker_index, msg_seq_no, recv_changes.iter().cloned().collect());
                    progcaster.progress_msg_stash.push(Message::from_typed(tuple));
                }

                if worker_todo.contains(&worker_index) {

                    if state_seq_no < msg_seq_no {
                        // state is behind of the direct connection with `worker_index`
                        // we will read `msg_seq_no` and all the following updates, but we need to ask
                        // for the missing range [state_seq_no..msg_seq_no[
                        let missing_range = ProgressUpdatesRange {
                            channel_id: progcaster.channel_identifier,
                            worker_index,
                            start_seqno: state_seq_no,
                            end_seqno: msg_seq_no,
                        };
                        missing_ranges.push(missing_range);
                    }

                    // in all cases, we are done with this worker
                    worker_todo.remove(&worker_index);
                }
            }
        }

        missing_ranges
    }

    fn apply_updates_range(&self, range: ProgressUpdatesRange, updates_range: Vec<u8>) {
        let mut progcaster = self.borrow_mut();
        progcaster.progress_state.apply_updates_range(range, updates_range)
    }

    fn apply_stashed_progress_msgs(&self) {
        let mut progcaster = self.borrow_mut();
        let progcaster = &mut *progcaster;

        for message in progcaster.progress_msg_stash.iter() {
            progcaster.progress_state.update(message);
        }

        progcaster.progress_msg_stash.clear();
    }

    fn boxed_clone(&self) -> Box<ProgcasterClientHandle> {
        Box::new(Rc::clone(&self))
    }
}

mod test {
    use crate::progress::broadcast::ProgressState;
    use crate::progress::{Location, Port, ChangeBatch};
    use crate::progress::Port::{Source, Target};
    use std::collections::HashMap;

    #[test]
    fn state_encode_decode() {

        let mut state = ProgressState::<u32>::new();
        
        state.change_batch.update((Location{ node: 0, port: Port::Target(2) }, 4_u32), 4);
        state.change_batch.update((Location{ node: 3, port: Port::Source(4) }, 8_u32), 9);
        state.change_batch.compact();

        state.worker_seqno.insert(7, 8);

        let buf = state.encode();

        let mut state_dec = ProgressState::decode(buf);
        state_dec.change_batch.compact();

        assert_eq!(state, state_dec);
    }

    #[test]
    fn hardcoded() {
        let  buf = vec![80, 94, 2, 224, 160, 127, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 124, 42, 30, 207, 220, 65, 152, 248, 5, 141, 125, 42, 33, 18, 154, 227, 31, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 160, 226, 1, 224, 160, 127, 0, 0];
        let mut state_dec = ProgressState::<u64>::decode(buf);
        println!("state_dec is {:?}", state_dec);
    }

    #[test]
    fn real() {
        let mut state: ProgressState<u64> = ProgressState { change_batch: ChangeBatch { updates: vec![((Location { node: 1, port: Source(0) }, 0), -1), ((Location { node: 1, port: Source(0) }, 1), 1), ((Location { node: 2, port: Source(0) }, 0), -1), ((Location { node: 3, port: Source(0) }, 0), -1), ((Location { node: 4, port: Source(0) }, 0), -1), ((Location { node: 1, port: Source(0) }, 0), -1), ((Location { node: 1, port: Source(0) }, 1), 1), ((Location { node: 2, port: Source(0) }, 0), -1), ((Location { node: 3, port: Source(0) }, 0), -1), ((Location { node: 4, port: Source(0) }, 0), -1), ((Location { node: 1, port: Source(0) }, 1), -1), ((Location { node: 1, port: Source(0) }, 2), 1), ((Location { node: 1, port: Source(0) }, 1), -1), ((Location { node: 1, port: Source(0) }, 2), 1), ((Location { node: 2, port: Target(0) }, 1), 1), ((Location { node: 2, port: Target(0) }, 1), -1)], clean: 0 }, worker_seqno: HashMap::new()};
        let buf = vec![208, 51, 2, 208, 0, 127, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 51, 32, 163, 25, 196, 39, 2, 248, 252, 4, 66, 86, 92, 72, 91, 169, 31, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 160, 226, 1, 208, 0, 127, 0, 0];
        state.worker_seqno.insert(1, 3);
        state.worker_seqno.insert(0, 2);
        state.change_batch.compact();
        let buf_enc = state.encode();
        assert_eq!(buf_enc, buf);
    }
}
