use atomic::Ordering;

use crate::plan::PlanTraceObject;
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::{gc_work::*, GCWork, GCWorker, WorkBucketStage};
use crate::util::ObjectReference;
use crate::vm::slot::{MemorySlice, Slot};
use crate::vm::*;
use crate::MMTK;
use crate::ObjectQueue;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use super::global::GenerationalPlanExt;

/// Process edges for a nursery GC. This type is provided if a generational plan does not use
/// [`crate::scheduler::gc_work::SFTProcessEdges`]. If a plan uses `SFTProcessEdges`,
/// it does not need to use this type.
pub struct GenNurseryProcessEdges<
    VM: VMBinding,
    P: GenerationalPlanExt<VM> + PlanTraceObject<VM>,
    const KIND: TraceKind,
> {
    plan: &'static P,
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding, P: GenerationalPlanExt<VM> + PlanTraceObject<VM>, const KIND: TraceKind>
    ProcessEdgesWork for GenNurseryProcessEdges<VM, P, KIND>
{
    type VM = VM;
    type ScanObjectsWorkType = PlanScanObjects<Self, P>;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        let plan = base.plan().downcast_ref().unwrap();
        Self { plan, base }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        // We cannot borrow `self` twice in a call, so we extract `worker` as a local variable.
        let worker = self.worker();
        if cfg!(not(feature = "edge_enqueuing")) {
            self.plan.trace_object_nursery::<VectorObjectQueue, KIND>(
                &mut self.base.nodes,
                object,
                worker,
            )
        } else {
            self.plan.trace_object_nursery::<_, KIND>(self, object, worker)
        }
    }

    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            // Skip slots that are not holding an object reference.
            return;
        };
        let new_object = self.trace_object(object);
        debug_assert!(!self.plan.is_object_in_nursery(new_object));
        // Note: If `object` is a mature object, `trace_object` will not call `space.trace_object`,
        // but will still return `object`.  In that case, we don't need to write it back.
        if new_object != object {
            slot.store(new_object);
        }
    }

    fn create_scan_work(&self, nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        if cfg!(not(feature = "edge_enqueuing")) {
            PlanScanObjects::new(self.plan, nodes, false, self.bucket)
        } else {
            unreachable!()
        }
    }

    #[cfg(feature = "edge_enqueuing")]
    fn process_slots(&mut self) {
        while let Some(slot) = self.slots.pop() {
            self.process_slot(slot)
        }
    }

    #[cfg(feature = "edge_enqueuing")]
    fn flush(&mut self) {
        if !self.slots.is_empty() {
            let slots = std::mem::take(&mut self.slots);
            let w = Self::new(slots, false, self.mmtk(), self.bucket);
            self.worker().add_work(self.bucket, w);
        }
        self.pushes = 0;
    }
}

impl<VM: VMBinding, P: GenerationalPlanExt<VM> + PlanTraceObject<VM>, const KIND: TraceKind> ObjectQueue
    for GenNurseryProcessEdges<VM, P, KIND>
{
    fn enqueue(&mut self, object: ObjectReference) {
        #[cfg(feature = "dont_enqueue_vm_space_objects")]
        use crate::policy::space::Space;
        let tls = self.worker().tls;
        let mut closure = |slot: VM::VMSlot| {
            let Some(_obj) = slot.load() else { return };
            // Don't enqueue slots which have objects in the VM space
            // since for generational GCs we will scan all VM space objects
            // who have been modified (i.e. tripped the write barrier)
            #[cfg(feature = "dont_enqueue_vm_space_objects")]
            if self.plan.base().vm_space.in_space(_obj) {
                return;
            }
            self.slots.push(slot);
            self.pushes += 1;
            if self.slots.len() >= Self::CAPACITY
                || self.pushes >= (Self::CAPACITY / 2) as u32
            {
                self.flush_half();
            }
        };
        <VM as VMBinding>::VMScanning::scan_object(tls, object, &mut closure);
        self.plan.post_scan_object(object);
    }
}

impl<VM: VMBinding, P: GenerationalPlanExt<VM> + PlanTraceObject<VM>, const KIND: TraceKind>
    GenNurseryProcessEdges<VM, P, KIND>
{
    fn flush_half(&mut self) {
        let slots = if self.slots.len() > 1 {
            let half = self.slots.len() / 2;
            self.slots.split_off(half)
        } else {
            return;
        };

        self.pushes = self.slots.len() as u32;
        if slots.is_empty() {
            return;
        }

        let w = Self::new(slots, false, self.mmtk(), self.bucket);
        self.worker().add_work(self.bucket, w);
    }
}

impl<VM: VMBinding, P: GenerationalPlanExt<VM> + PlanTraceObject<VM>, const KIND: TraceKind> Deref
    for GenNurseryProcessEdges<VM, P, KIND>
{
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding, P: GenerationalPlanExt<VM> + PlanTraceObject<VM>, const KIND: TraceKind>
    DerefMut for GenNurseryProcessEdges<VM, P, KIND>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

/// The modbuf contains a list of objects in mature space(s) that
/// may contain pointers to the nursery space.
/// This work packet scans the recorded objects and forwards pointers if necessary.
pub struct ProcessModBuf<E: ProcessEdgesWork> {
    modbuf: Vec<ObjectReference>,
    phantom: PhantomData<E>,
}

impl<E: ProcessEdgesWork> ProcessModBuf<E> {
    pub fn new(modbuf: Vec<ObjectReference>) -> Self {
        debug_assert!(!modbuf.is_empty());
        Self {
            modbuf,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessModBuf<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        // Process and scan modbuf only if the current GC is a nursery GC
        let gen = mmtk.get_plan().generational().unwrap();
        if gen.is_current_gc_nursery() {
            // Flip the per-object unlogged bits to "unlogged" state.
            for obj in &self.modbuf {
                debug_assert!(
                    !gen.is_object_in_nursery(*obj),
                    "{} was logged but is not mature. Dumping process memory maps:\n{}",
                    *obj,
                    crate::util::memory::get_process_memory_maps(),
                );
                <E::VM as VMBinding>::VMObjectModel::GLOBAL_LOG_BIT_SPEC.store_atomic::<E::VM, u8>(
                    *obj,
                    1,
                    None,
                    Ordering::SeqCst,
                );
            }
            // Scan objects in the modbuf and forward pointers
            let modbuf = std::mem::take(&mut self.modbuf);
            GCWork::do_work(
                &mut ScanObjects::<E>::new(modbuf, false, WorkBucketStage::Closure),
                worker,
                mmtk,
            )
        }
    }
}

/// The array-copy modbuf contains a list of array slices in mature space(s) that
/// may contain pointers to the nursery space.
/// This work packet forwards and updates each entry in the recorded slices.
pub struct ProcessRegionModBuf<E: ProcessEdgesWork> {
    /// A list of `(start_address, bytes)` tuple.
    modbuf: Vec<<E::VM as VMBinding>::VMMemorySlice>,
    phantom: PhantomData<E>,
}

impl<E: ProcessEdgesWork> ProcessRegionModBuf<E> {
    pub fn new(modbuf: Vec<<E::VM as VMBinding>::VMMemorySlice>) -> Self {
        Self {
            modbuf,
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessRegionModBuf<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        // Scan modbuf only if the current GC is a nursery GC
        if mmtk
            .get_plan()
            .generational()
            .unwrap()
            .is_current_gc_nursery()
        {
            // Collect all the entries in all the slices
            let mut slots = vec![];
            for slice in &self.modbuf {
                for slot in slice.iter_slots() {
                    slots.push(slot);
                }
            }
            // Forward entries
            GCWork::do_work(
                &mut E::new(slots, false, mmtk, WorkBucketStage::Closure),
                worker,
                mmtk,
            )
        }
    }
}
