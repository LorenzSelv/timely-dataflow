//! The exchange pattern distributes pushed data between many target pushees.

use crate::Data;
use crate::communication::Push;
use crate::dataflow::channels::{Bundle, Message};
use std::rc::Rc;
use std::cell::RefCell;

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D) -> u64> {
    pushers: Rc<RefCell<Vec<P>>>,
    buffers: Vec<Vec<D>>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, D, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64>  Exchange<T, D, P, H> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Rc<RefCell<Vec<P>>>, key: H) -> Exchange<T, D, P, H> {
        let mut buffers = vec![];
        // TODO(lorenzo) init buffers, might have to change later
        for _ in 0..pushers.borrow().len() {
            buffers.push(Vec::with_capacity(Message::<T, D>::default_length()));
        }
        Exchange {
            pushers,
            hash_func: key,
            buffers,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            if let Some(ref time) = self.current {
                Message::push_at(&mut self.buffers[index], time.clone(), &mut self.pushers.borrow()[index]);
            }
        }
    }
}

impl<T: Eq+Data, D: Data, P: Push<Bundle<T, D>>, H: FnMut(&T, &D)->u64> Push<Bundle<T, D>> for Exchange<T, D, P, H> {
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Bundle<T, D>>) {

        // A `rescale` operation might have happened since the last call to push.
        // In that case, the `pushers` vector has been back-filled with the new pusher.
        // We need to allocate buffers for the new pushers.
        let pushers = self.pushers.borrow();

        while self.buffers.len() < pushers.len() {
            self.buffers.push(Vec::with_capacity(Message::<T, D>::default_length()));
        }

        debug_assert_eq!(self.buffers.len(), pushers.len());

        // TODO(lorenzo) change below to use a routing table

        // if only one pusher, no exchange
        if pushers.len() == 1 {
            pushers[0].push(message);
        }
        else if let Some(message) = message {

            let message = message.as_mut();
            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            if self.current.as_ref().map_or(false, |x| x != time) {
                for index in 0..pushers.len() {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            // if the number of pushers is a power of two, use a mask
            if (pushers.len() & (pushers.len() - 1)) == 0 {
                let mask = (pushers.len() - 1) as u64;
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) & mask) as usize;

                    self.buffers[index].push(datum);
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }

                    // unsafe {
                    //     self.buffers.get_unchecked_mut(index).push(datum);
                    //     if self.buffers.get_unchecked(index).len() == self.buffers.get_unchecked(index).capacity() {
                    //         self.flush(index);
                    //     }
                    // }

                }
            }
            // as a last resort, use mod (%)
            else {
                for datum in data.drain(..) {
                    let index = (((self.hash_func)(time, &datum)) % pushers.len() as u64) as usize;
                    self.buffers[index].push(datum);
                    if self.buffers[index].len() == self.buffers[index].capacity() {
                        self.flush(index);
                    }
                }
            }

        }
        else {
            // flush
            for index in 0..pushers.len() {
                self.flush(index);
                pushers[index].push(&mut None);
            }
        }
    }
}
