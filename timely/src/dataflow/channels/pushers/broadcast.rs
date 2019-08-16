//! The exchange pattern distributes pushed data between many target pushees.

use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::{Bundle, Message};
use std::rc::Rc;
use std::cell::RefCell;

/// Broadcast records to all pushers
pub struct Broadcast<T, D, P: Push<Bundle<T, D>>> {
    pushers: Rc<RefCell<Vec<P>>>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>>  Broadcast<T, D, P> {
    /// Allocates a new `Broadcast` from a supplied set of pushers.
    pub fn new(pushers: Rc<RefCell<Vec<P>>>) -> Broadcast<T, D, P> {
        let mut buffers = vec![];
        // allocate buffers, one for each pusher. If new pushers are added,
        // we will extend the vector with more buffers in the `push` function
        for _ in 0..pushers.borrow().len() {
            buffers.push(Vec::with_capacity(Message::<T, D>::default_length()));
        }
        Broadcast {
            pushers,
            buffers,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            if let Some(ref time) = self.current {
                // TODO(lorenzo) this call might fail if the pusher is broken
                Message::push_at(&mut self.buffers[index], time.clone(), &mut self.pushers.borrow_mut()[index]);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>> Push<Bundle<T, D>> for Broadcast<T, D, P> {
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {

        // A `rescale` operation might have happened since the last call to push.
        // In that case, the `pushers` vector has been back-filled with the new pusher.
        // We need to allocate buffers for the new pushers.
        // let mut pushers = self.pushers.borrow_mut();

        let pushers_len = self.pushers.borrow().len();

        while self.buffers.len() < pushers_len {
            self.buffers.push(Vec::with_capacity(Message::<T, D>::default_length()));
        }

        debug_assert_eq!(self.buffers.len(), pushers_len);

        if let Some(message) = message {

            let message = message.as_mut();
            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            if self.current.as_ref().map_or(false, |x| x != time) {
                for index in 0..pushers_len {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            for datum in data.drain(..) {
                for index in 0..pushers_len {
                    self.buffers[index].push(datum.clone());
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }
                }
            }
        }
        else {
            // flush
            for index in 0..pushers_len {
                self.flush(index);
                self.pushers.borrow_mut()[index].push(&mut None);
            }
        }
    }
}
