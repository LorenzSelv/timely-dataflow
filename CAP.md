
## Capability & Rescaling

Capability are created with the `mint` function: increment the change batch `internal` with a `+1` for the associated timestamp.

Where capabilities are minted:

* `OperatorBuilder::build` -- the builder has, for each output, a change batch `internal` used to keep track
   of capability changes. Initially, a capability at time 0 (assuming integer timestamp) is minted.
   The capability takes a reference to the `internal` change batch, so that when downgraded / dropped
   it can update `internal` (suppose `cap.time` is `t`, `internal.update(t, -1)`).
 
* `Capability::clone` -- mint a clone of the capability (`internal` is updated).

* `Capability::delayed` -- mint a capability for a later (greater or equal) timestamp

* `CapabilityRef::delayed_for_output` -- as `Capability::delayed` but for a specific output port.
  Note that `internal` is now a vector of change batches, one for each output. TODO: find usage
  
* `CapabilityRef::retain_for_output` -- mint a capability consuming a capref.

* `new_unordered_input` -- mint a capability with timestamp 0

Where capabilities are used:

* The `Operator::build` takes a constructor which expects a vector of capabilities (has length 1 -- TODO always ?)
  => TODO look up all usages of `OperatorBuilder`.. probably `unary_*`, etc.
  
* `Notificator` uses capabilities to setup notifications / iterate through closed timestamps
                (gives a capability to produce output for that timestamp)
       
* Generic operators:
    * `unary_frontier` -- uses the `OperatorBuilder` interface. The relevant part
       is the closure passed to the `build` method: it takes a single-element vector of capabilities
       as argument, pops the capability out and pass it to the user-supplied
       constructor for the operator (which returns the `logic`: a closure expecting input and output handles).
       
       => anything that uses capabilities is the first part of the constructor
       
    * `unary` -- same as `unary_frontier`
    
    * `unary_notify` -- implementation uses `unary_frontier`. The constructor
       passed initializes a `Notificator` calling `notify_at` for every
       requested time: it tries to `delay` the capability passed to the constructor
       closure.. this will panic if the requested time is smaller than the capability time.
       => we should check before calling `delayed` that the capability is for a previous timestamp.
       => if that's not the case then we should not try to setup the notification.
       => this would make `unary_notify` safe to use but `unary_frontier` not: it would
          panic if tried to delay the capability to a previous time.
       => possible alternative: `delayed` does not panic and returns an optional value (if you want to panic you can `unwrap` it)
       
    * `binary_*` -- versions work similarly
       
    * `source` -- exactly the same as `unary` 
                   
       
       
       
       
`CapabilityRef` is an unowned capability (TODO: seems to be owned to me since you can call `retain`)
It says that it increments the internal change batch but it does not.

Used only in input handles: pull some data, mint a capref for that time and pass it to user logic closure in e.g. `for_each`.

When creating an output session (`session` method of `InputHandle`)
it checks that the capability is `valid_for_output`

------------------------------------------------------------------------------
`InputHandle` mints capref when pulling something from the channel: receiving
input allows you to produce output. This is _relatively_ safe if everything else is done properly,
i.e. no-one can produce output for time timestamps.

Thus, we need to constrain the capabilities each operator of a new worker has.

`scope.input_from` should ensure the supplied input handle will produce only input for current or future timestamps.
Options:
* call the `advance_to` with the current timestamp (the bootstrap server should ensure that the global frontier would not advance further, we should
emit a progress update (current t at input, +1) so that everybody has to wait for the new worker to send the corresponding (t, -1) before consider the epoch closed)
* assign `now_at` directly with the new timestamp

TODO: do we want the (timestamp::default(), -1) update ? probably not if we are consistent with the operator's capabilities (which should be initialized)


Summary:
* operators _should_ be initialized with (output) capabilities at the current time `t'` for that operator source (output) at the bootstrap server.
  Unfortunately, at the time of building the dataflow graph this information is not available yet: we discover `t` only after bootstrapping, which in turn
  needs the dataflow graph to have been built already. Chicken-and-egg problem.
  Solution: we initialize with the default timestamp, we keep an handle to every minted capability, we downgrade the capabilities after bootstrapping
  according to the actual current time `t`.
  
* `input_handles` are created with the default timestamp. They are then registered as input of (potentially more than one) dataflows (`scope::input_from`).
   During the registration we store an handle to the capability so that we can downgrade it later (same reasoning as above).
   During bootstrapping, we downgrade the capability as above.
   (TODO: note that this would cause a problem if downgrading the second time. We need to downgrade iff the current time is actually lower that the downgraded-to time).
  
* `new_unordered_input` mints a capability for the default timestamp, which is then downgraded according using the stored handle 

* TODO other places?

==============================
Things to deal with:

1) We need to acquire this "actual current time `t`" during bootstrapping
  
    * the bootstrap server emits a progress update `(t', +1)` to `shared_progress.internal` (internal capabilities):
      it is allowed to do so as it has not gone past that timestamp.
      Since the bootstrap server will not emit the corresponding decrement `(t', -1)`, other workers will
      have to wait until the newly bootstrapped worker has downgraded/dropped/advanced past that timestamp (i.e. emitted `(t', -1)`).
      
------------------------------

2) Capability can be cloned, delayed, dropped. We need to make sure we handle all such cases in the "keep an handle" mechanism.

  `Capability` should be a wrapper to a shared `Rc<RefCell<>>` handle
  ...TODO

------------------------------
  
3) method should be safe also if someone uses the operator builder directly
  
------------------------------

############################################################################

ALTERNATIVE
===========

* New worker has no capability at all!
    - Change all APIs to accept an Option<Capability> instead: mint them only if *not* in rescale mode
    - Is it enough to store a flag in the Scope?
       - yes for operator [DONE]
       - yes for unordered input [DONE]
       - TODO input handle? [TODO]
       - TODO probe [TODO]
       - TODO all usages of builder_raw [TODO]

* All notifications are cancelled

* Operator, upon receiving input, are provided with capabilities (already in the current implementation) which allow them
  to produce output, request notifications, etc.
  




