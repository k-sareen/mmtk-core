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

pub struct STDoCollection<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P> STDoCollection<VM, P>
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

impl<VM, P> GCWork<VM>
    for STDoCollection<VM, P>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM> + Send,
{
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        STPrepare::<VM, P>::new(mmtk).execute(worker, mmtk);
        let mut closure = STObjectGraphTraversalClosure::<VM, P, DEFAULT_TRACE>::new(mmtk, worker);
        STStopMutators::<VM, P, DEFAULT_TRACE>::new().execute(&mut closure, worker, mmtk);
        STScanVMSpecificRoots::<VM, P, DEFAULT_TRACE>::new().execute(&mut closure, worker, mmtk);
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
        plan_mut.prepare(worker.tls);

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
        plan_mut.release(worker.tls);

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
    const KIND: TraceKind,
> {
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STStopMutators<VM, P, KIND>
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
        closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        mmtk: &'static MMTK<VM>,
    ) {
        probe!(mmtk, stop_mutators_and_process_thread_roots_start);
        mmtk.state.prepare_for_stack_scanning();
        let num_mutators = <VM as VMBinding>::VMActivePlan::number_of_mutators();
        <VM as VMBinding>::VMCollection::stop_all_mutators(worker.tls, |mutator| {
            STScanMutatorRoots::<VM, P, KIND>::new(mutator, num_mutators).execute(closure, worker, mmtk);
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
    slots: Vec<VM::VMSlot>,
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
            slots: Vec::new(),
        }
    }

    pub fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    fn process_slot(&mut self, slot: VM::VMSlot) {
        let Some(object) = slot.load() else { return };
        let new_object = self.plan.trace_object::<_, KIND>(self, object, self.worker());
        if P::may_move_objects::<KIND>() && new_object != object {
            slot.store(new_object);
        }
    }

    fn traverse_from_roots(&mut self) {
        while let Some(slot) = self.slots.pop() {
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
            let Some(_) = slot.load() else { return };
            self.slots.push(slot);
        };
        <VM as VMBinding>::VMScanning::scan_object(tls, object, &mut closure);
        self.plan.post_scan_object(object);
    }
}

impl<VM, P, const KIND: TraceKind> ObjectGraphTraversal<VM::VMSlot>
    for &mut STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn report_roots(&mut self, root_slots: Vec<VM::VMSlot>) {
        assert!(self.slots.is_empty());
        self.slots = Vec::from(root_slots);
        self.traverse_from_roots();
    }
}

#[cfg(debug_assertions)]
impl<VM, P, const KIND: TraceKind> Drop for STObjectGraphTraversalClosure<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    fn drop(&mut self) {
        assert!(self.slots.is_empty());
    }
}

pub(crate) struct STScanMutatorRoots<
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    pub mutator: &'static mut Mutator<VM>,
    num_mutators: usize,
    phantom: PhantomData<(VM, P)>,
}

impl<VM, P, const KIND: TraceKind> STScanMutatorRoots<VM, P, KIND>
where
    VM: VMBinding,
    P: Plan<VM = VM> + PlanTraceObject<VM>,
{
    pub fn new(mutator: &'static mut Mutator<VM>, num_mutators: usize) -> Self {
        Self { mutator, num_mutators, phantom: PhantomData }
    }

    pub fn execute(
        &mut self,
        closure: &mut STObjectGraphTraversalClosure<VM, P, KIND>,
        worker: &mut GCWorker<VM>,
        mmtk: &'static MMTK<VM>
    ) {
        probe!(mmtk, scan_and_process_mutator_roots_start);
        <VM as VMBinding>::VMScanning::single_threaded_scan_roots_in_mutator_thread(
            worker.tls,
            unsafe { &mut *(self.mutator as *mut _) },
            closure,
        );
        if mmtk.state.inform_stack_scanned(self.num_mutators) {
            <VM as VMBinding>::VMScanning::notify_initial_thread_scan_complete(
                false, worker.tls,
            );
        }
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
