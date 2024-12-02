use super::gc_work::ImmixGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::chunk_map::IMMIX_CHUNK_MASK;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::rust_util::{likely, unlikely};
use crate::vm::VMBinding;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::{HasSpaces, PlanTraceObject};

#[derive(HasSpaces, PlanTraceObject)]
pub struct Immix<VM: VMBinding> {
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
}

/// The plan constraints for the immix plan.
pub const IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: crate::policy::immix::DEFRAG,
    // Max immix object size is half of a block.
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    needs_prepare_mutator: false,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for Immix<VM> {
    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        ImmixSpace::<VM>::is_last_gc_exhaustive(self.last_gc_was_defrag.load(Ordering::Relaxed))
    }

    fn constraints(&self) -> &'static PlanConstraints {
        &IMMIX_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::Immix(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![(CopySelector::Immix(0), &self.immix_space)],
            constraints: &IMMIX_CONSTRAINTS,
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        let immix_space = if unlikely(self.common().is_zygote()) {
            self.common().get_zygote().get_immix_space()
        } else {
            &self.immix_space
        };
        Self::schedule_immix_full_heap_collection::<
            Immix<VM>,
            ImmixGCWorkContext<VM, TRACE_KIND_FAST>,
            ImmixGCWorkContext<VM, TRACE_KIND_DEFRAG>,
        >(self, immix_space, scheduler)
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(
            tls,
            true,
            self.get_total_pages(),
            self.get_reserved_pages(),
            self.get_collection_reserved_pages(),
        );
        self.immix_space.prepare(
            true,
            crate::policy::immix::defrag::StatsForDefrag::new(self),
        );
    }

    fn release(&mut self, tls: VMWorkerThread) {
        self.common.release(tls, true);
        // release the collected region
        if likely(!self.common().is_zygote()) {
            self.immix_space.release(true);
        }
    }

    fn end_of_gc(&self, _tls: VMWorkerThread) {
        if unlikely(self.common().is_zygote()) {
            let zygotespace = self.common().get_zygote().get_immix_space();
            self.last_gc_was_defrag
                .store(zygotespace.end_of_gc(), Ordering::Relaxed);
        } else {
            self.last_gc_was_defrag
                .store(self.immix_space.end_of_gc(), Ordering::Relaxed);
        };
    }

    fn current_gc_may_move_object(&self) -> bool {
        if unlikely(self.common().is_zygote()) {
            let zygotespace = self.common().get_zygote().get_immix_space();
            zygotespace.in_defrag()
        } else {
            self.immix_space.in_defrag()
        }
    }

    fn prepare_worker(&self, worker: &mut GCWorker<Self::VM>) {
        if unlikely(self.common().is_zygote()) {
            // We are the Zygote process and we have not created the ZygoteSpace yet so use the
            // ImmixSpace inside the ZygoteSpace
            unsafe {
                worker.get_copy_context_mut().immix[0].assume_init_mut()
            }.rebind(self.common().get_zygote().get_immix_space());
        } else {
            // Either the runtime has a Zygote space or it is a command-line runtime
            debug_assert!(
                self.common().has_zygote_space()
                    || (!self.common().is_zygote_process()
                        && !*self.common().base.options.is_zygote_process)
            );
            unsafe { worker.get_copy_context_mut().immix[0].assume_init_mut() }.rebind(&self.immix_space);
        }
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_used_pages()
    }

    fn can_pin_objects_in_default_space(&self) -> bool {
        cfg!(feature = "object_pinning")
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }
}

impl<VM: VMBinding> Immix<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &IMMIX_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
        };
        Self::new_with_args(
            plan_args,
            ImmixSpaceArgs {
                reset_log_bit_in_major_gc: false,
                unlog_object_when_traced: false,
                mixed_age: false,
            },
        )
    }

    pub fn new_with_args(
        mut plan_args: CreateSpecificPlanArgs<VM>,
        space_args: ImmixSpaceArgs,
    ) -> Self {
        // Add the chunk mark table to the list of global metadata
        plan_args.global_side_metadata_specs.push(crate::util::heap::chunk_map::ChunkMap::ALLOC_TABLE);

        let immix = Immix {
            immix_space: ImmixSpace::new(
                plan_args.get_space_args("immix", true, VMRequest::discontiguous()),
                space_args,
                IMMIX_CHUNK_MASK,
            ),
            common: CommonPlan::new(plan_args),
            last_gc_was_defrag: AtomicBool::new(false),
        };

        immix.verify_side_metadata_sanity();

        immix
    }

    /// Schedule a full heap immix collection. This method is used by immix/genimmix/stickyimmix
    /// to schedule a full heap collection. A plan must call set_collection_kind and set_gc_status before this method.
    pub(crate) fn schedule_immix_full_heap_collection<
        PlanType: Plan<VM = VM>,
        FastContext: GCWorkContext<VM = VM, PlanType = PlanType>,
        DefragContext: GCWorkContext<VM = VM, PlanType = PlanType>,
    >(
        plan: &'static DefragContext::PlanType,
        immix_space: &ImmixSpace<VM>,
        scheduler: &GCWorkScheduler<VM>,
    ) {
        let in_defrag = immix_space.decide_whether_to_defrag(
            plan.base().global_state.is_emergency_collection(),
            true,
            plan.base()
                .global_state
                .cur_collection_attempts
                .load(Ordering::SeqCst),
            plan.base().global_state.is_user_triggered_collection(),
            *plan.base().options.full_heap_system_gc,
        );

        if in_defrag {
            info!("{} defrag collection", immix_space.get_name());
            scheduler.schedule_common_work::<DefragContext>(plan);
        } else {
            info!("{} non-moving collection", immix_space.get_name());
            scheduler.schedule_common_work::<FastContext>(plan);
        }
    }

    pub(in crate::plan) fn set_last_gc_was_defrag(&self, defrag: bool, order: Ordering) {
        self.last_gc_was_defrag.store(defrag, order)
    }
}
