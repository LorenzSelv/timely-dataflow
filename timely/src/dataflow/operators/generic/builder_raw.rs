//! Types to build operators with general shapes.
//!
//! These types expose some raw timely interfaces, and while public so that others can build on them,
//! they require some sophistication to use correctly. I recommend checking out `builder_rc.rs` for
//! an interface that is intentionally harder to mis-use.

use std::default::Default;
use std::rc::Rc;
use std::cell::RefCell;

use crate::Data;

use crate::scheduling::{Schedule, Activations};

use crate::progress::{Source, Target};
use crate::progress::ChangeBatch;
use crate::progress::{Timestamp, Operate, operate::SharedProgress, Antichain};

use crate::dataflow::{Stream, Scope};
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;
use crate::dataflow::operators::generic::operator_info::OperatorInfo;

/// Contains type-free information about the operator properties.
pub struct OperatorShape {
    name: String,   // A meaningful name for the operator.
    notify: bool,   // Does the operator require progress notifications.
    init_peers: usize,   // The total number of workers in the computation when it was first started.
                         // This will not match the actual number of peers in the computation if a rescaling operation
                         // has occurred. But this is fine, as it is used only to initialize initial capabilities of scopes.
    inputs: usize,  // The number of input ports.
    outputs: usize, // The number of output ports.
}

/// Core data for the structure of an operator, minus scope and logic.
impl OperatorShape {
    fn new(name: String, init_peers: usize) -> Self {
        OperatorShape {
            name,
            notify: true,
            init_peers,
            inputs: 0,
            outputs: 0,
        }
    }

    /// The number of inputs of this operator
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// The number of outputs of this operator
    pub fn outputs(&self) -> usize {
        self.outputs
    }
}

/// Builds operators with generic shape.
pub struct OperatorBuilder<G: Scope> {
    scope: G,
    index: usize,
    global: usize,
    address: Vec<usize>,    // path to the operator (ending with index).
    shape: OperatorShape,
    summary: Vec<Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, mut scope: G) -> Self {

        let global = scope.new_identifier();
        let index = scope.allocate_operator_index();
        let mut address = scope.addr();
        address.push(index);
        let init_peers = scope.init_peers();

        println!("Operator shape has init_peers = {}", init_peers);

        OperatorBuilder {
            scope,
            index,
            global,
            address,
            shape: OperatorShape::new(name, init_peers),
            summary: vec![],
        }
    }

    /// The operator's scope-local index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize {
        self.global
    }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape {
        &self.shape
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.shape.notify = notify;
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P) -> P::Puller
        where
            P: ParallelizationContract<G::Timestamp, D> {
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.outputs];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<D: Data, P>(&mut self, stream: &Stream<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> P::Puller
    where
        P: ParallelizationContract<G::Timestamp, D> {

        let channel_id = self.scope.new_identifier();
        let logging = self.scope.logging();
        let (sender, receiver) = pact.connect(&mut self.scope, channel_id, &self.address[..], logging);
        let target = Target { index: self.index, port: self.shape.inputs };
        stream.connect_to(target, sender, channel_id);

        self.shape.inputs += 1;
        assert_eq!(self.shape.outputs, connection.len());
        self.summary.push(connection);

        receiver
    }

    /// Adds a new input to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Data>(&mut self) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        let connection = vec![Antichain::from_elem(Default::default()); self.shape.inputs];
        self.new_output_connection(connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output_connection<D: Data>(&mut self, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> (Tee<G::Timestamp, D>, Stream<G, D>) {

        let (targets, registrar) = Tee::<G::Timestamp,D>::new();
        let source = Source { index: self.index, port: self.shape.outputs };
        let stream = Stream::new(source, registrar, self.scope.clone());

        self.shape.outputs += 1;
        assert_eq!(self.shape.inputs, connection.len());
        for (summary, entry) in self.summary.iter_mut().zip(connection.into_iter()) {
            summary.push(entry);
        }

        (targets, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<PEP, PIP>(mut self, push_external: PEP, pull_internal: PIP)
    where
        PEP: FnMut(&mut [ChangeBatch<G::Timestamp>])+'static,
        PIP: FnMut(
            &mut [ChangeBatch<G::Timestamp>],
            &mut [ChangeBatch<G::Timestamp>],
            &mut [ChangeBatch<G::Timestamp>],
        )->bool+'static
    {
        let inputs = self.shape.inputs;
        let outputs = self.shape.outputs;

        let operator = OperatorCore {
            shape: self.shape,
            address: self.address,
            activations: self.scope.activations().clone(),
            push_external,
            pull_internal,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            summary: self.summary,
        };

        self.scope.add_operator_with_indices(Box::new(operator), self.index, self.global);
    }

    /// Information describing the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        OperatorInfo::new(self.index, self.global, &self.address[..])
    }
}

struct OperatorCore<T, PEP, PIP>
    where
        T: Timestamp,
{
    shape: OperatorShape,
    address: Vec<usize>,
    push_external: PEP,
    pull_internal: PIP,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    activations: Rc<RefCell<Activations>>,
    summary: Vec<Vec<Antichain<T::Summary>>>,
}

impl<T, PEP, PIP> Schedule for OperatorCore<T, PEP, PIP>
where
    T: Timestamp,
    PEP: FnMut(&mut [ChangeBatch<T>])+'static,
    PIP: FnMut(&mut [ChangeBatch<T>], &mut [ChangeBatch<T>], &mut [ChangeBatch<T>])->bool+'static
{
    fn name(&self) -> &str { &self.shape.name }
    fn path(&self) -> &[usize] { &self.address[..] }
    fn schedule(&mut self) -> bool {

        let shared_progress = &mut *self.shared_progress.borrow_mut();

        let frontier = &mut shared_progress.frontiers[..];
        let consumed = &mut shared_progress.consumeds[..];
        let internal = &mut shared_progress.internals[..];
        let produced = &mut shared_progress.produceds[..];

        (self.push_external)(frontier);
        (self.pull_internal)(consumed, internal, produced)
    }
}

impl<T, PEP, PIP> Operate<T> for OperatorCore<T, PEP, PIP>
where
    T: Timestamp,
    PEP: FnMut(&mut [ChangeBatch<T>])+'static,
    PIP: FnMut(&mut [ChangeBatch<T>], &mut [ChangeBatch<T>], &mut [ChangeBatch<T>])->bool+'static
{
    fn inputs(&self) -> usize { self.shape.inputs }
    fn outputs(&self) -> usize { self.shape.outputs }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Rc<RefCell<SharedProgress<T>>>) {

        // Request the operator to be scheduled at least once.
        self.activations.borrow_mut().activate(&self.address[..]);

        // by default, we reserve a capability for each output port at `Default::default()`.
        self.shared_progress
            .borrow_mut()
            .internals
            .iter_mut()
            .for_each(|output| output.update(Default::default(), self.shape.init_peers as i64));
            // TODO(lorenzo) important
        println!("self.shape.init_peers is {}", self.shape.init_peers);

        (self.summary.clone(), self.shared_progress.clone())
    }

    // initialize self.frontier antichains as indicated by hosting scope.
    fn set_external_summary(&mut self) {
        (self.push_external)(&mut self.shared_progress.borrow_mut().frontiers[..]);
    }

    fn notify_me(&self) -> bool { self.shape.notify }
}
