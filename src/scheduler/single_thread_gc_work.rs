use crate::global_state::GcStatus;
use crate::plan::{Mutator, MutatorContext};
use crate::plan::{Plan, PlanTraceObject};
use crate::policy::gc_work::{TraceKind, DEFAULT_TRACE};
use crate::scheduler::*;
use crate::util::ObjectReference;
use crate::vm::*;
use crate::vm::slot::Slot;
use crate::ObjectQueue;
use crate::MMTK;

use std::marker::PhantomData;
use std::sync::atomic::Ordering;

pub struct STDoCollection<VM, P, const KIND: TraceKind>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STDoCollection<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

// Unconstrained,
// /// Preparation work.  Plans, spaces, GC workers, mutators, etc. should be prepared for GC at
// /// this stage.
// Prepare,
// /// Clear the VO bit metadata.  Mainly used by ImmixSpace.
// #[cfg(feature = "vo_bit")]
// ClearVOBits,
// /// Compute the transtive closure starting from transitively pinning (TP) roots following only strong references.
// /// No objects in this closure are allow to move.
// TPinningClosure,
// /// Trace (non-transitively) pinning roots. Objects pointed by those roots must not move, but their children may. To ensure correctness, these must be processed after TPinningClosure
// PinningRootsTrace,
// /// Compute the transtive closure following only strong references.
// Closure,
// /// Handle Java-style soft references, and potentially expand the transitive closure.
// SoftRefClosure,
// /// Handle Java-style weak references.
// WeakRefClosure,
// /// Resurrect Java-style finalizable objects, and potentially expand the transitive closure.
// FinalRefClosure,
// /// Handle Java-style phantom references.
// PhantomRefClosure,
// /// Let the VM handle VM-specific weak data structures, including weak references, weak
// /// collections, table of finalizable objects, ephemerons, etc.  Potentially expand the
// /// transitive closure.
// ///
// /// NOTE: This stage is intended to replace the Java-specific weak reference handling stages
// /// above.
// VMRefClosure,
// /// Compute the forwarding addresses of objects (mark-compact-only).
// CalculateForwarding,
// /// Scan roots again to initiate another transitive closure to update roots and reference
// /// after computing the forwarding addresses (mark-compact-only).
// SecondRoots,
// /// Update Java-style weak references after computing forwarding addresses (mark-compact-only).
// ///
// /// NOTE: This stage should be updated to adapt to the VM-side reference handling.  It shall
// /// be kept after removing `{Soft,Weak,Final,Phantom}RefClosure`.
// RefForwarding,
// /// Update the list of Java-style finalization cadidates and finalizable objects after
// /// computing forwarding addresses (mark-compact-only).
// FinalizableForwarding,
// /// Let the VM handle the forwarding of reference fields in any VM-specific weak data
// /// structures, including weak references, weak collections, table of finalizable objects,
// /// ephemerons, etc., after computing forwarding addresses (mark-compact-only).
// ///
// /// NOTE: This stage is intended to replace Java-specific forwarding phases above.
// VMRefForwarding,
// /// Compact objects (mark-compact-only).
// Compact,
// /// Work packets that should be done just before GC shall go here.  This includes releasing
// /// resources and setting states in plans, spaces, GC workers, mutators, etc.
// Release,
// /// Resume mutators and end GC.
// Final,

impl<VM, P, const KIND: TraceKind> GCWork<VM>
    for STDoCollection<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut closure = STObjectGraphTraversalClosure::<VM, P, KIND>::new(mmtk, worker);
        STStopMutators::<VM, P>::new().execute(worker, mmtk);
        STPrepare::<VM, P>::new(mmtk).execute(worker, mmtk);
        STScanMutatorRoots::<VM, P, KIND>::new().execute(&mut closure, worker, mmtk);
        STScanVMSpecificRoots::<VM, P, KIND>::new().execute(&mut closure, worker, mmtk);
        STScanVMSpaceObjects::<VM, P, KIND>::new().execute(&mut closure, worker, mmtk);
        STProcessWeakReferences::<VM, P, KIND>::new().execute(&mut closure, worker, mmtk);
        STRelease::<VM, P>::new(mmtk).execute(worker, mmtk);
        // We implicitly resume mutators in Scheduler::on_gc_finished so we don't have a separate
        // implementation for that
    }
}

pub(crate) struct STPrepare<
    VM: VMBinding,
    P: Plan<VM = VM>,
> {
    plan: *const P,
    phantom: PhantomData<VM>,
}

impl<VM, P> STPrepare<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM>,
{
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            plan: mmtk.get_plan().downcast_ref::<P>().unwrap(),
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        probe!(mmtk, prepare_start);
        // SAFETY: We're a single threaded GC, so no other thread can access the plan
        let plan_mut: &mut P = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.prepare(worker);

        // PrepareMutator
        if plan_mut.constraints().needs_prepare_mutator {
            <VM as VMBinding>::VMActivePlan::mutators()
                .for_each(|mutator| mutator.prepare(worker.tls));
        }

        // PrepareCollector
        worker.get_copy_context_mut().prepare();
        mmtk.get_plan().prepare_worker(worker);

        // Set GC status
        mmtk.set_gc_status(GcStatus::GcProper);
        probe!(mmtk, prepare_end);
    }
}

pub(crate) struct STRelease<
    VM: VMBinding,
    P: Plan<VM = VM>,
> {
    plan: *const P,
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P> STRelease<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM>,
{
    pub fn new(mmtk: &'static MMTK<VM>) -> Self {
        Self {
            plan: mmtk.get_plan().downcast_ref::<P>().unwrap(),
            phantom: PhantomData,
        }
    }

    pub fn execute(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        probe!(mmtk, release_start);
        mmtk.gc_trigger.policy.on_gc_release(mmtk);
        // SAFETY: We're a single threaded GC, so no other thread can access the plan
        let plan_mut: &mut P = unsafe { &mut *(self.plan as *const _ as *mut _) };
        plan_mut.release(worker);

        // ReleaseMutator
        <VM as VMBinding>::VMActivePlan::mutators().for_each(|mutator| mutator.release(worker.tls));

        // ReleaseCollector
        worker.get_copy_context_mut().release();

        // Set GC status
        // mmtk.set_gc_status(GcStatus::NotInGC);
        probe!(mmtk, release_end);
    }
}

pub(crate) struct STStopMutators<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P> STStopMutators<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }

    pub fn execute(
        &self,
        worker: &mut GCWorker<VM>,
        mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, stop_mutators_and_process_thread_roots_start);
        mmtk.state.prepare_for_stack_scanning();
        <VM as VMBinding>::VMCollection::stop_all_mutators(worker.tls, |mutator| {
            STFlushMutatorBuffers::<VM, P>::new(mutator).execute(worker, mmtk);
        });
        mmtk.scheduler.notify_mutators_paused(mmtk);
        probe!(mmtk, stop_mutators_and_process_thread_roots_end);
    }
}

// pub(crate) struct STResumeMutators<VM: VMBinding>(PhantomData<VM>);
//
// impl<VM: VMBinding> STResumeMutators<VM> {
//     pub fn new() -> Self {
//         Self(PhantomData)
//     }
//
//     pub fn execute(&self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
//         <VM as VMBinding>::VMCollection::resume_all_mutators(worker.tls);
//         mmtk.scheduler.notify_mutators_resumed(mmtk);
//     }
// }

pub(crate) struct STObjectGraphTraversalClosure<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    worker: *mut GCWorker<VM>,
}

impl<VM, P, const KIND: TraceKind> STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new(mmtk: &'static MMTK<VM>, worker: &mut GCWorker<VM>) -> Self {
        Self {
            plan: mmtk.get_plan().downcast_ref::<P>().unwrap(),
            worker,
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn is_empty(&self) -> bool {
        self.worker().mark_stack.is_empty()
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        debug_assert!(
            <VM as VMBinding>::VMObjectModel::is_object_sane(object),
            "Object {:?} is not sane!",
            object,
        );
        // self.plan.base().global_state.trace_object_count.fetch_add(1, Ordering::Relaxed);
        self.plan.trace_object::<_, KIND>(self, object, self.worker())
    }

    fn process_slot(&mut self, slot: VM::VMSlot) {
        use crate::policy::space::Space;
        let Some(object) = slot.load() else { return };
        // Re-order cascading if to put VM space check first
        if self.plan.base().vm_space.in_space(object) {
            return;
        }
        let new_object = self.trace_object(object);
        if P::may_move_objects::<KIND>() && new_object != object {
            slot.store(new_object);
        }
    }

    pub fn process_slots(&mut self) {
        while let Some(slot) = self.worker().mark_stack.pop() {
            self.process_slot(slot);
        }
    }
}

impl<VM, P, const KIND: TraceKind> ObjectQueue
    for STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn enqueue(&mut self, object: ObjectReference) {
        let tls = self.worker().tls;
        let mut closure = |slot: VM::VMSlot| {
            #[cfg(feature = "dont_enqueue_vm_space_objects")]
            use crate::policy::space::Space;
            let Some(_obj) = slot.load() else { return };
            // Don't enqueue slots which have objects in the VM space
            // Since we scan all objects in VM space
            #[cfg(feature = "dont_enqueue_vm_space_objects")]
            if self.plan.base().vm_space.in_space(_obj) {
                return;
            }
            self.worker().mark_stack.push(slot);
        };
        <VM as VMBinding>::VMScanning::scan_object(tls, object, &mut closure);
        // self.plan.base().global_state.scan_object_count.fetch_add(1, Ordering::Relaxed);
        self.plan.post_scan_object(object);
    }
}

impl<VM, P, const KIND: TraceKind> ObjectTracer
    for STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        self.trace_object(object)
    }
}

impl<VM, P, const KIND: TraceKind> ObjectGraphTraversal<VM::VMSlot>
    for &mut STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn get_mark_stack(&mut self) -> &mut Vec<VM::VMSlot> {
        &mut self.worker().mark_stack
    }

    fn report_roots(&mut self, len: usize) {
        assert!(self.worker().mark_stack.is_empty());
        // SAFETY: We are the only thread accessing the mark stack so we can
        // set the length
        unsafe {
            self.worker().mark_stack.set_len(len);
        }
        self.process_slots();
    }
}

#[cfg(debug_assertions)]
impl<VM, P, const KIND: TraceKind> Drop for STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn drop(&mut self) {
        assert!(self.worker().mark_stack.is_empty());
    }
}

pub(crate) struct STFlushMutatorBuffers<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
> {
    pub mutator: &'static mut Mutator<VM>,
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P> STFlushMutatorBuffers<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new(mutator: &'static mut Mutator<VM>) -> Self {
        Self { mutator, phantom: PhantomData }
    }

    pub fn execute(
        &mut self,
        worker: &mut GCWorker<VM>,
        mmtk: &'static MMTK<VM>
    ) {
        probe!(mmtk, flush_mutator_buffers_start);
        let num_mutators = <VM as VMBinding>::VMActivePlan::number_of_mutators();
        self.mutator.flush();
        if mmtk.state.inform_stack_scanned(num_mutators) {
            <VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                false, worker.tls,
            );
        }
        probe!(mmtk, flush_mutator_buffers_end);
    }
}

pub(crate) struct STScanMutatorRoots<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STScanMutatorRoots<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { phantom: PhantomData }
    }

    pub fn execute(
        &self,
        closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        _mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, scan_and_process_mutator_roots_start);
        <VM as VMBinding>::VMActivePlan::mutators().for_each(|mutator| {
            <VM as VMBinding>::VMScanning::single_threaded_scan_roots_in_mutator_thread(
                worker.tls,
                mutator,
                &mut *closure,
            );
        });
        probe!(mmtk, scan_and_process_mutator_roots_end);
    }
}

pub(crate) struct STScanVMSpecificRoots<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STScanVMSpecificRoots<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { phantom: PhantomData }
    }

    pub fn execute(
        &self,
        closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        _mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, scan_and_process_vm_roots_start);
        <VM as VMBinding>::VMScanning::single_threaded_scan_vm_specific_roots(worker.tls, closure);
        probe!(mmtk, scan_and_process_vm_roots_end);
    }
}

pub(crate) struct STScanVMSpaceObjects<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STScanVMSpaceObjects<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new() -> Self {
        Self { phantom: PhantomData }
    }

    pub fn execute(
        &self,
        closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        _mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, scan_vm_space_objects_start);
        debug_assert!(closure.is_empty());
        let mut scan_closure = |objects: Vec<ObjectReference>| {
            for object in objects {
                closure.enqueue(object);
            }
            closure.process_slots();
        };
        <VM as VMBinding>::VMScanning::scan_vm_space_objects(worker.tls, scan_closure);
        probe!(mmtk, scan_vm_space_objects_end);
    }
}

pub(crate) struct STTracerContext<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STTracerContext<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    pub fn new() -> Self {
        Self { phantom: PhantomData }
    }
}

impl<VM, P, const KIND: TraceKind> ObjectTracerContext<VM> for STTracerContext<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    type TracerType = STObjectGraphTraversalClosure<VM, P, KIND>;

    fn with_tracer<R, F>(&self, worker: &mut GCWorker<VM>, func: F) -> R
    where
        F: FnOnce(&mut Self::TracerType) -> R,
    {
        let mmtk = worker.mmtk;
        let mut closure = STObjectGraphTraversalClosure::<VM, P, KIND>::new(mmtk, worker);
        let result = func(&mut closure);
        closure.process_slots();
        result
    }
}

impl<VM, P, const KIND: TraceKind> Clone for STTracerContext<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

pub(crate) struct STProcessWeakReferences<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STProcessWeakReferences<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    pub fn new() -> Self {
        Self { phantom: PhantomData }
    }

    pub fn execute(
        &self,
        _closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        _mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, process_weak_references_start);
        debug_assert!(_closure.is_empty());
        let mut need_to_repeat = true;
        while need_to_repeat {
            let tracer_factory = STTracerContext::<VM, P, KIND>::new();
            need_to_repeat = <VM as VMBinding>::VMScanning::process_weak_refs(worker, tracer_factory);
        }
        probe!(mmtk, process_weak_references_end);
    }
}
