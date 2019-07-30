//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::process::ProcessBuilder as TypedProcessBuilder;
use crate::allocator::{Allocate, AllocateBuilder, Event, Thread, Process, OnNewPushFn, BootstrapSendStateClosure, BootstrapGetUpdatesRangeClosure, BootstrapDoneClosure};
use crate::allocator::zero_copy::allocator_process::{ProcessBuilder, ProcessAllocator};
use crate::allocator::zero_copy::allocator::{TcpBuilder, TcpAllocator};

use crate::{Pull, Data, Message};
use crate::rescaling::bootstrap::BootstrapRecvEndpoint;

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Generic {
    /// Intra-thread allocator.
    Thread(Thread),
    /// Inter-thread, intra-process allocator.
    Process(Process),
    /// Inter-thread, intra-process serializing allocator.
    ProcessBinary(ProcessAllocator),
    /// Inter-process allocator.
    ZeroCopy(TcpAllocator<Process>),
}

impl Generic {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.index(),
            &Generic::Process(ref p) => p.index(),
            &Generic::ProcessBinary(ref pb) => pb.index(),
            &Generic::ZeroCopy(ref z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.peers(),
            &Generic::Process(ref p) => p.peers(),
            &Generic::ProcessBinary(ref pb) => pb.peers(),
            &Generic::ZeroCopy(ref z) => z.peers(),
        }
    }

    /// The initial number of workers.
    pub fn init_peers(&self) -> usize {
        match self {
            &Generic::Thread(ref t) => t.init_peers(),
            &Generic::Process(ref p) => p.init_peers(),
            &Generic::ProcessBinary(ref pb) => pb.init_peers(),
            &Generic::ZeroCopy(ref z) => z.init_peers(),
        }
    }

    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data, F>(&mut self, identifier: usize, on_new_pusher: F) -> Box<Pull<Message<T>>>
        where F: OnNewPushFn<T>
    {
        match self {
            &mut Generic::Thread(ref mut t) => t.allocate(identifier, on_new_pusher),
            &mut Generic::Process(ref mut p) => p.allocate(identifier, on_new_pusher),
            &mut Generic::ProcessBinary(ref mut pb) => pb.allocate(identifier, on_new_pusher),
            &mut Generic::ZeroCopy(ref mut z) => z.allocate(identifier, on_new_pusher),
        }
    }

    /// Rescale the allocator if a changed occurred in the cluster
    /// Only ZeroCopy (`TcpAllocator`) actually does something, for the others it's a nop.
    fn rescale(&mut self,
               a: impl BootstrapSendStateClosure,
               b: impl BootstrapGetUpdatesRangeClosure,
               c: impl BootstrapDoneClosure
    ) {
        match self {
            &mut Generic::ZeroCopy(ref mut z) => z.rescale(a, b, c),
            _ => {} // no-op for the others
        }
    }

    /// Perform work before scheduling operators.
    fn receive(&mut self) {
        match self {
            &mut Generic::Thread(ref mut t) => t.receive(),
            &mut Generic::Process(ref mut p) => p.receive(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.receive(),
            &mut Generic::ZeroCopy(ref mut z) => z.receive(),
        }
    }

    /// Perform work after scheduling operators.
    pub fn release(&mut self) {
        match self {
            &mut Generic::Thread(ref mut t) => t.release(),
            &mut Generic::Process(ref mut p) => p.release(),
            &mut Generic::ProcessBinary(ref mut pb) => pb.release(),
            &mut Generic::ZeroCopy(ref mut z) => z.release(),
        }
    }

    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> {
        match self {
            &Generic::Thread(ref t) => t.events(),
            &Generic::Process(ref p) => p.events(),
            &Generic::ProcessBinary(ref pb) => pb.events(),
            &Generic::ZeroCopy(ref z) => z.events(),
        }
    }

    fn get_bootstrap_endpoint(&mut self) -> Option<BootstrapRecvEndpoint> {
        match self {
            &mut Generic::ZeroCopy(ref mut z) => z.get_bootstrap_endpoint(),
            _ => None,
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn init_peers(&self) -> usize { self.init_peers() }
    fn allocate<T: Data, F>(&mut self, identifier: usize, on_new_pusher: F) -> Box<Pull<Message<T>>>
        where F: OnNewPushFn<T>
    {
        self.allocate(identifier, on_new_pusher)
    }

    fn rescale(&mut self,
               a: impl BootstrapSendStateClosure,
               b: impl BootstrapGetUpdatesRangeClosure,
               c: impl BootstrapDoneClosure
    ) {
        self.rescale(a, b, c);
    }

    fn receive(&mut self) { self.receive(); }
    fn release(&mut self) { self.release(); }
    fn events(&self) -> &Rc<RefCell<VecDeque<(usize, Event)>>> { self.events() }
    fn await_events(&self, _duration: Option<std::time::Duration>) {
        match self {
            &Generic::Thread(ref t) => t.await_events(_duration),
            &Generic::Process(ref p) => p.await_events(_duration),
            &Generic::ProcessBinary(ref pb) => pb.await_events(_duration),
            &Generic::ZeroCopy(ref z) => z.await_events(_duration),
        }
    }

    fn get_bootstrap_endpoint(&mut self) -> Option<BootstrapRecvEndpoint> {
        self.get_bootstrap_endpoint()
    }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the `ProcessBinary` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum GenericBuilder {
    /// Builder for `Thread` allocator.
    Thread(ThreadBuilder),
    /// Builder for `Process` allocator.
    Process(TypedProcessBuilder),
    /// Builder for `ProcessBinary` allocator.
    ProcessBinary(ProcessBuilder),
    /// Builder for `ZeroCopy` allocator.
    ZeroCopy(TcpBuilder<TypedProcessBuilder>),
}

impl AllocateBuilder for GenericBuilder {
    type Allocator = Generic;
    fn build(self) -> Generic {
        match self {
            GenericBuilder::Thread(t) => Generic::Thread(t.build()),
            GenericBuilder::Process(p) => Generic::Process(p.build()),
            GenericBuilder::ProcessBinary(pb) => Generic::ProcessBinary(pb.build()),
            GenericBuilder::ZeroCopy(z) => Generic::ZeroCopy(z.build()),
        }
    }
}
