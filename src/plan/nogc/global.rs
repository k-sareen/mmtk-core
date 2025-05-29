use crate::plan::global::BasePlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::nogc::mutator::ALLOCATOR_MAPPING;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immortalspace::ImmortalSpace;
use crate::policy::space::Space;
use crate::scheduler::GCWorker;
use crate::scheduler::GCWorkScheduler;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::heap::gc_trigger::SpaceStats;
#[allow(unused_imports)]
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::*;
use crate::vm::VMBinding;
use enum_map::EnumMap;
use mmtk_macros::HasSpaces;
#[cfg(feature = "nogc_trace")]
use mmtk_macros::PlanTraceObject;

#[cfg(not(feature = "nogc_lock_free"))]
use crate::policy::immortalspace::ImmortalSpace as NoGCImmortalSpace;
#[cfg(feature = "nogc_lock_free")]
use crate::policy::lockfreeimmortalspace::LockFreeImmortalSpace as NoGCImmortalSpace;

#[cfg(not(feature = "nogc_trace"))]
#[derive(HasSpaces)]
pub struct NoGC<VM: VMBinding> {
    #[parent]
    pub base: BasePlan<VM>,
    #[space]
    pub nogc_space: NoGCImmortalSpace<VM>,
    #[space]
    pub immortal: ImmortalSpace<VM>,
    #[space]
    pub los: ImmortalSpace<VM>,
}

#[cfg(feature = "nogc_trace")]
#[derive(HasSpaces, PlanTraceObject)]
pub struct NoGC<VM: VMBinding> {
    #[parent]
    pub base: BasePlan<VM>,
    #[space]
    pub nogc_space: NoGCImmortalSpace<VM>,
    #[space]
    pub immortal: ImmortalSpace<VM>,
    #[space]
    pub los: ImmortalSpace<VM>,
}

/// The plan constraints for the no gc plan.
pub const NOGC_CONSTRAINTS: PlanConstraints = PlanConstraints {
    collects_garbage: false,
    needs_prepare_mutator: false,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for NoGC<VM> {
    fn constraints(&self) -> &'static PlanConstraints {
        &NOGC_CONSTRAINTS
    }

    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.base
    }

    fn prepare(&mut self, _worker: &mut GCWorker<VM>) {
        cfg_if::cfg_if! {
            if #[cfg(not(feature = "nogc_trace"))] {
                unreachable!()
            } else {
                self.base.prepare(_worker.tls, /* full_heap= */ true);
                self.nogc_space.prepare();
                self.immortal.prepare();
                self.los.prepare();
            }
        }
    }

    fn release(&mut self, _worker: &mut GCWorker<VM>) {
        cfg_if::cfg_if! {
            if #[cfg(not(feature = "nogc_trace"))] {
                unreachable!()
            } else {
                self.base.release(_worker.tls, /* full_heap= */ true);
                self.nogc_space.release();
                self.immortal.release();
                self.los.release();
            }
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        cfg_if::cfg_if! {
            if #[cfg(not(feature = "nogc_trace"))] {
                unreachable!("GC triggered in nogc")
            } else {
                use super::gc_work::NoGCWorkContext;
                use crate::policy::gc_work::DEFAULT_TRACE;
                scheduler.schedule_common_work::<NoGCWorkContext<VM, DEFAULT_TRACE>, DEFAULT_TRACE>(self);
            }
        }
    }

    fn current_gc_may_move_object(&self) -> bool {
        false
    }

    fn get_used_pages(&self) -> usize {
        self.nogc_space.reserved_pages()
            + self.immortal.reserved_pages()
            + self.los.reserved_pages()
            + self.base.get_used_pages()
    }
}

impl<VM: VMBinding> NoGC<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &NOGC_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
        };

        let res = NoGC {
            nogc_space: NoGCImmortalSpace::new(plan_args.get_space_args(
                "nogc_space",
                cfg!(not(feature = "nogc_no_zeroing")),
                false,
                VMRequest::discontiguous(),
            )),
            immortal: ImmortalSpace::new(plan_args.get_space_args(
                "immortal",
                true,
                false,
                VMRequest::discontiguous(),
            )),
            los: ImmortalSpace::new(plan_args.get_space_args(
                "los",
                true,
                false,
                VMRequest::discontiguous(),
            )),
            base: BasePlan::new(plan_args),
        };

        res.verify_side_metadata_sanity();

        res
    }
}
