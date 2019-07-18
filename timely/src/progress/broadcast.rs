//! Broadcasts progress information among workers.

use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::Location;
use crate::communication::{Message, Push, Pull};
use crate::logging::TimelyLogger as Logger;
use std::rc::Rc;
use std::cell::RefCell;
use crate::progress::rescaling::ProgressUpdatesRange;
use std::collections::HashMap;
use abomonation::Abomonation;

struct ProgressState<T: Timestamp> {
    change_batch: ChangeBatch<(Location,T)>,
    worker_seqno: HashMap<usize, usize>,
}

impl<T: Timestamp> ProgressState<T> {

    fn new() -> Self {
        ProgressState {
            change_batch: ChangeBatch::new(),
            worker_seqno: HashMap::new(),
        }
    }

    // TODO(lorenzo) after a rescaling operation is complete, we should track also the new worker in the state
    fn new_worker(&mut self, worker_index: usize) {
        unimplemented!();
    }
}

impl<T: Timestamp+Abomonation> Abomonation for ProgressState<T> {}

impl<T: Timestamp+Abomonation> ProgressState<T> {

    fn encode(&mut self) -> Vec<u8> {
        let mut buf = vec![0_u8; abomonation::measure(self)];
        unsafe { abomonation::encode(self, &mut buf) }.expect("encode error");
        buf
    }

    fn decode(encoded_state: &mut [u8]) -> Self {
        unimplemented!();
    }

    fn update(&mut self, progress_msg: &ProgressMsg<T>) {
        self.change_batch.extend(progress_msg.2.into_iter());
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

    // TODO ref or clone?
    fn append(&mut self, progress_msg: ProgressMsg<T>) {
        self.worker_msgs
            .entry(progress_msg.0)
            .or_insert(Vec::new())
            .push(progress_msg);
    }
}

impl<T: Timestamp+Abomonation> Abomonation for ProgressRecorder<T> {}

impl<T: Timestamp+Abomonation> ProgressRecorder<T> {

    fn get_updates_range(&mut self, range: ProgressUpdatesRange) -> Vec<u8> {

        let msgs = self.worker_msgs.remove(&range.worker_index).expect("requested a range for missing worker index");

        let seq_no_first = msgs[0].1;
        let skip = range.seq_no_start - seq_no_first;
        let range_size = range.seq_no_end - range.seq_no_start;

        assert!(skip + range_size <= msgs.len());

        let updates_range =
            msgs
                .into_iter()
                .skip(skip)
                .take(range_size)
                .map(|msg| msg.2)
                .flatten(); // each message has a vector of updates


        let mut change_batch = ChangeBatch::new();
        change_batch.extend(updates_range);

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

    // TODO(lorenzo)
    //    we need to maintain accumulate state, so that we can bootstrap workers during rescaling
    //    state:
    //      * compacted ChangeBatch: all updates ever sent/recved accumulated
    //      * hashmap of (worker_index) -> SeqNo
    //                        ^source in the message
    //                                       ^guaranteed to be monotonically increasing (++), panic if not
    //        note: we also keep (my_index, SeqNo) to track which messages sent are included in the acc state
    progress_state: ProgressState<T>,

    recorder: Option<ProgressRecorder<T>>,

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
            recorder: None, // not recording initially
        }
    }

    /// Sends pointstamp changes to all workers.
    pub fn send(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        changes.compact();
        if !changes.is_empty() {

            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
                is_send: true,
                source: self.source,
                channel: self.channel_identifier,
                seq_no: self.counter,
                addr: self.addr.clone(),
                // TODO: fill with additional data
                messages: Vec::new(),
                internal: Vec::new(),
            }));

            for pusher in self.pushers.borrow_mut().iter_mut() {

                // Attempt to re-use allocations, if possible.
                if let Some(tuple) = &mut self.to_push {
                    let tuple = tuple.as_mut();
                    tuple.0 = self.source;
                    tuple.1 = self.counter;
                    tuple.2.clear(); tuple.2.extend(changes.iter().cloned());
                }
                // If we don't have an allocation ...
                if self.to_push.is_none() {
                    self.to_push = Some(Message::from_typed((
                        self.source,
                        self.counter,
                        changes.clone().into_inner(),
                    )));
                }

                if let Some(message) = &self.to_push {
                    // let clone = *message.clone();
                    self.progress_state.update(message);
                    if let Some(mut recorder) = &self.recorder {
                        recorder.append(*message.clone());
                    }
                }

                // TODO: This should probably use a broadcast channel.
                pusher.push(&mut self.to_push);
                pusher.done();
            }

            self.counter += 1;
            changes.clear();
        }
    }

    /// Receives pointstamp changes from all workers.
    pub fn recv(&mut self, changes: &mut ChangeBatch<(Location, T)>) {

        while let Some(message) = self.puller.pull() {

            let source = message.0;
            let counter = message.1;
            let recv_changes = &message.2;

            let addr = &mut self.addr;
            let channel = self.channel_identifier;
            self.logging.as_ref().map(|l| l.log(crate::logging::ProgressEvent {
                is_send: false,
                source: source,
                seq_no: counter,
                channel,
                addr: addr.clone(),
                // TODO: fill with additional data
                messages: Vec::new(),
                internal: Vec::new(),
            }));

            // We clone rather than drain to avoid deserialization.
            for &(ref update, delta) in recv_changes.iter() {
                changes.update(update.clone(), delta);
            }

            self.progress_state.update(&message);

            if let Some(mut recorder) = &self.recorder {
                recorder.append(*message);
            }
        }

    }
}

pub trait ProgcasterServerHandle {

    fn start_recording(&mut self);

    fn stop_recording(&mut self);

    fn get_progress_state(&mut self) -> Vec<u8>;

    fn get_updates_range(&mut self, range: ProgressUpdatesRange) -> Vec<u8>;
}

pub trait ProgcasterClientHandle {

    fn set_progress_state(&mut self, state: &[u8]);

    fn set_updates_range(&mut self, range: ProgressUpdatesRange, updates_range: &[u8]);

    fn get_missing_updates_ranges(&mut self) -> Vec<ProgressUpdatesRange>;
}


impl<T: Timestamp> ProgcasterServerHandle for Progcaster<T> {

    fn start_recording(&mut self) {
        assert!(self.recorder.is_none(), "TODO: handle concurrent rescaling operation?");
        self.recorder = Some(ProgressRecorder::new());
    }

    fn stop_recording(&mut self) {
        assert!(self.recorder.is_some());
        self.recorder = None;
    }

    fn get_progress_state(&mut self) -> Vec<u8> {
        self.progress_state.encode()
    }

    fn get_updates_range(&mut self, range: ProgressUpdatesRange) -> Vec<u8> {
        self.recorder.unwrap().get_updates_range(range)
    }
}

impl<T: Timestamp> ProgcasterClientHandle for Progcaster<T> {

    fn set_progress_state(&mut self, state: &[u8]) {
        unimplemented!()
    }

    fn get_missing_updates_ranges(&mut self) -> Vec<ProgressUpdatesRange> {
        unimplemented!()
    }

    fn set_updates_range(&mut self, range: ProgressUpdatesRange, updates_range: &[u8]) {
        unimplemented!()
    }
}
