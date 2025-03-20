use super::gc_work::GenCopyGCWorkContext;
use super::gc_work::GenCopyNurseryGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::generational::global::CommonGenPlan;
use crate::plan::generational::global::GenerationalPlan;
use crate::plan::generational::global::GenerationalPlanExt;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanTraceObject;
use crate::plan::PlanConstraints;
use crate::policy::copyspace::CopySpace;
use crate::policy::gc_work::TraceKind;
use crate::policy::sft_map::SFTMap;
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::util::VMWorkerThread;
use crate::vm::*;
use crate::ObjectQueue;
use enum_map::EnumMap;
use std::sync::atomic::{AtomicBool, Ordering};

use mmtk_macros::HasSpaces;

struct TraceObjectEntry<VM: VMBinding> {
    space_ptr: u128,
    copy_semantics: Option<CopySemantics>,
    phantom: std::marker::PhantomData<VM>,
}

impl<VM: VMBinding> TraceObjectEntry<VM> {
    fn new(space: &dyn Space<VM>, semantics: Option<CopySemantics>) -> Self {
        let space_ptr: u128 = unsafe { std::mem::transmute(space) };
        Self { space_ptr, copy_semantics: semantics, phantom: std::marker::PhantomData }
    }

    pub(crate) fn get_space(&self) -> &dyn Space<VM> {
        unsafe { std::mem::transmute(self.space_ptr) }
    }

    pub(crate) fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.get_space().trace_object::<Q, KIND>(queue, object, self.copy_semantics, worker)
    }
}

#[derive(HasSpaces)]
pub struct GenCopy<VM: VMBinding> {
    pub gen: CommonGenPlan<VM>,
    pub hi: AtomicBool,
    pub copyspace0: CopySpace<VM>,
    pub copyspace1: CopySpace<VM>,
    // pub trace_object_tbl: Vec<(usize, usize)>,
    // pub trace_object_tbl: Vec<u128>,
    // pub trace_object_args_tbl: Vec<Option<CopySemantics>>,
    pub trace_object_tbl: Vec<TraceObjectEntry<VM>>,
}

/// The plan constraints for the generational copying plan.
pub const GENCOPY_CONSTRAINTS: PlanConstraints = crate::plan::generational::GEN_CONSTRAINTS;

impl<VM: VMBinding> PlanTraceObject<VM> for GenCopy<VM> {
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        use crate::util::metadata::side_metadata::spec_defs::SFT_DENSE_CHUNK_MAP_INDEX;

        let chunk = crate::util::conversions::chunk_align_down(object.to_raw_address());
        let idx = unsafe { SFT_DENSE_CHUNK_MAP_INDEX.load::<u8>(chunk) } as usize;

        debug_assert_ne!(idx, 0, "Chunk index can't be 0 for address {}!", object);
        debug_assert!(idx as usize <= self.trace_object_tbl.len(), "Chunk index {} does not exist in trace_object_tbl", idx);

        // let space = self.trace_object_tbl[idx];
        // let copy_semantics = self.trace_object_args_tbl[idx];

        // let trace_object_fn: fn(&dyn Space<VM>, &mut Q, ObjectReference, Option<CopySemantics>, &mut GCWorker<VM>) = unsafe {
        //     std::mem::transmute(space.0)
        // };
        // let space_ptr: &dyn Space<VM> = unsafe { std::mem::transmute(space.1) };
        // trace_object_fn(space_ptr, queue, object, copy_semantics, worker);

        // let space_ptr: &dyn Space<VM> = unsafe { std::mem::transmute(space) };
        // space_ptr.trace_object::<Q, KIND>(queue, object, copy_semantics, worker)
        // use crate::policy::gc_work::PolicyTraceObject;
        // let policy_trace_ptr: &dyn PolicyTraceObject<VM> = unsafe { std::mem::transmute(space) };
        // policy_trace_ptr.trace_object::<Q, KIND>(queue, object, copy_semantics, worker);

        let entry = &self.trace_object_tbl[idx];
        entry.trace_object::<Q, KIND>(queue, object, worker)

        // use crate :: policy :: space :: Space;
        // use crate :: policy :: gc_work :: PolicyTraceObject;
        // use crate :: plan :: PlanTraceObject;
        // if self.copyspace0.in_space(object) {
        //     return <CopySpace<VM> as PolicyTraceObject <VM>>::trace_object::<Q, KIND>(&self.copyspace0, queue, object, Some(CopySemantics::Mature), worker);
        // }
        // if self.copyspace1.in_space(object) {
        //     return <CopySpace<VM> as PolicyTraceObject<VM>>::trace_object::<Q, KIND>(&self.copyspace1, queue, object, Some(CopySemantics::Mature), worker);
        // }
        // <CommonGenPlan<VM> as PlanTraceObject<VM>>::trace_object::<Q, KIND>(&self.gen, queue, object, worker)
    }

    fn post_scan_object(&self, _object: ObjectReference) {}

    fn may_move_objects<const KIND: TraceKind>() -> bool {
        true
    }
}


impl<VM: VMBinding> Plan for GenCopy<VM> {
    fn constraints(&self) -> &'static PlanConstraints {
        &GENCOPY_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::Mature => CopySelector::CopySpace(0),
                CopySemantics::PromoteToMature => CopySelector::CopySpace(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![
                // The tospace argument doesn't matter, we will rebind before a GC anyway.
                (CopySelector::CopySpace(0), self.tospace()),
            ],
            constraints: &GENCOPY_CONSTRAINTS,
        }
    }

    fn collection_required(&self, space_full: bool, space: Option<SpaceStats<Self::VM>>) -> bool
    where
        Self: Sized,
    {
        self.gen.collection_required(self, space_full, space)
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        let is_full_heap = self.requires_full_heap_collection();
        if is_full_heap {
            scheduler.schedule_common_work::<GenCopyGCWorkContext<VM>>(self);
        } else {
            scheduler.schedule_common_work::<GenCopyNurseryGCWorkContext<VM>>(self);
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        let full_heap = !self.gen.is_current_gc_nursery();
        self.gen.prepare(tls);
        if full_heap {
            self.hi
                .store(!self.hi.load(Ordering::SeqCst), Ordering::SeqCst); // flip the semi-spaces
        }
        let hi = self.hi.load(Ordering::SeqCst);
        self.copyspace0.prepare(hi);
        self.copyspace1.prepare(!hi);

        self.fromspace_mut()
            .set_copy_for_sft_trace(Some(CopySemantics::Mature));
        self.tospace_mut().set_copy_for_sft_trace(None);
    }

    fn prepare_worker(&self, worker: &mut GCWorker<Self::VM>) {
        unsafe { worker.get_copy_context_mut().copy[0].assume_init_mut() }.rebind(self.tospace());
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let full_heap = !self.gen.is_current_gc_nursery();
        self.gen.release(tls);
        if full_heap {
            self.fromspace().release();
        }
    }

    fn end_of_gc(&mut self, _tls: VMWorkerThread) {
        self.gen
            .set_next_gc_full_heap(CommonGenPlan::should_next_gc_be_full_heap(self));
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.gen.get_collection_reserved_pages() + self.tospace().reserved_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.gen.get_used_pages() + self.tospace().reserved_pages()
    }

    fn current_gc_may_move_object(&self) -> bool {
        true
    }

    /// Return the number of pages available for allocation. Assuming all future allocations goes to nursery.
    fn get_available_pages(&self) -> usize {
        // super.get_available_pages() / 2 to reserve pages for copying
        (self
            .get_total_pages()
            .saturating_sub(self.get_reserved_pages()))
            >> 1
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.gen.common.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.gen.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.gen.common
    }

    fn generational(&self) -> Option<&dyn GenerationalPlan<VM = Self::VM>> {
        Some(self)
    }

    fn populate_trace_object_tbl(&mut self, sft_map: &mut dyn SFTMap) {
        use crate::plan::global::HasSpaces;

        let mut trace_obj_tbl = vec![];
        // let trace_obj_args_tbl = &mut self.trace_object_args_tbl;
        let closure = &mut |space: &mut dyn Space<VM>| {
            let name = space.name();
            let idx = sft_map.get_space_idx(name);

            assert!(idx as usize == trace_obj_tbl.len());
            let copy_semantics = if name == "copyspace0" || name == "copyspace1" {
                Some(CopySemantics::Mature)
            } else if name == "nursery" {
                Some(CopySemantics::PromoteToMature)
            } else {
                None
            };

            let entry = TraceObjectEntry::new(space, copy_semantics);
            trace_obj_tbl.push(entry);

            // let space_ptr: u128 = unsafe { std::mem::transmute(space) };
            // trace_obj_tbl.push(space_ptr);
            // trace_obj_args_tbl.push(copy_semantics);
        };
        self.for_each_space_mut(closure);
        for entry in trace_obj_tbl {
            self.trace_object_tbl.push(entry);
        }
    }
}

impl<VM: VMBinding> GenerationalPlan for GenCopy<VM> {
    fn is_current_gc_nursery(&self) -> bool {
        self.gen.is_current_gc_nursery()
    }

    fn is_object_in_nursery(&self, object: ObjectReference) -> bool {
        self.gen.nursery.in_space(object)
    }

    fn is_address_in_nursery(&self, addr: Address) -> bool {
        self.gen.nursery.address_in_space(addr)
    }

    fn get_mature_physical_pages_available(&self) -> usize {
        self.tospace().available_physical_pages()
    }

    fn get_mature_reserved_pages(&self) -> usize {
        self.tospace().reserved_pages()
    }

    fn force_full_heap_collection(&self) {
        self.gen.force_full_heap_collection()
    }

    fn last_collection_full_heap(&self) -> bool {
        self.gen.last_collection_full_heap()
    }
}

impl<VM: VMBinding> GenerationalPlanExt<VM> for GenCopy<VM> {
    fn trace_object_nursery<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.gen
            .trace_object_nursery::<Q, KIND>(queue, object, worker)
    }
}

impl<VM: VMBinding> GenCopy<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &GENCOPY_CONSTRAINTS,
            global_side_metadata_specs:
                crate::plan::generational::new_generational_global_metadata_specs::<VM>(),
        };

        let copyspace0 = CopySpace::new(
            plan_args.get_space_args("copyspace0", true, false, VMRequest::discontiguous()),
            false,
        );
        let copyspace1 = CopySpace::new(
            plan_args.get_space_args("copyspace1", true, false, VMRequest::discontiguous()),
            true,
        );

        let res = GenCopy {
            gen: CommonGenPlan::new(plan_args),
            hi: AtomicBool::new(false),
            copyspace0,
            copyspace1,
            trace_object_tbl: vec![TraceObjectEntry::new(
                unsafe { std::mem::transmute(0_u128) },
                None,
            )],
            // trace_object_args_tbl: vec![None]
        };

        res.verify_side_metadata_sanity();

        res
    }

    fn requires_full_heap_collection(&self) -> bool {
        self.gen.requires_full_heap_collection(self)
    }

    pub fn tospace(&self) -> &CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &self.copyspace1
        } else {
            &self.copyspace0
        }
    }

    pub fn tospace_mut(&mut self) -> &mut CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &mut self.copyspace1
        } else {
            &mut self.copyspace0
        }
    }

    pub fn fromspace(&self) -> &CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &self.copyspace0
        } else {
            &self.copyspace1
        }
    }

    pub fn fromspace_mut(&mut self) -> &mut CopySpace<VM> {
        if self.hi.load(Ordering::SeqCst) {
            &mut self.copyspace0
        } else {
            &mut self.copyspace1
        }
    }
}
