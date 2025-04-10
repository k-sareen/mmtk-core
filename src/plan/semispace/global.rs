use super::gc_work::SSGCWorkContext;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::semispace::mutator::{ALLOCATOR_MAPPING_DEFAULT, ALLOCATOR_MAPPING_ZYGOTE};
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::copyspace::CopySpace;
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::heap::vm_layout::BYTES_IN_CHUNK;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::opaque_pointer::VMWorkerThread;
use crate::util::rust_util::{likely, unlikely};
use crate::{plan::global::BasePlan, vm::VMBinding};
use std::sync::atomic::{AtomicBool, Ordering};

use mmtk_macros::{HasSpaces, PlanTraceObject};

use enum_map::{enum_map, EnumMap};

#[derive(HasSpaces, PlanTraceObject)]
pub struct SemiSpace<VM: VMBinding> {
    pub hi: AtomicBool,
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub copyspace0: CopySpace<VM>,
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub copyspace1: CopySpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
}

/// The plan constraints for the semispace plan.
pub const SS_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: true,
    max_non_los_default_alloc_bytes:
        crate::plan::plan_constraints::MAX_NON_LOS_ALLOC_BYTES_COPYING_PLAN,
    needs_prepare_mutator: false,
    ..PlanConstraints::default()
};

lazy_static! {
    pub(crate) static ref SS_COPY_CONFIG_DEFAULT: EnumMap<CopySemantics, CopySelector> = enum_map! {
        CopySemantics::DefaultCopy => CopySelector::CopySpace(0),
        _ => CopySelector::Unused,
    };

    pub(crate) static ref SS_COPY_CONFIG_ZYGOTE: EnumMap<CopySemantics, CopySelector> = enum_map! {
        CopySemantics::DefaultCopy => CopySelector::Immix(0),
        _ => CopySelector::Unused,
    };
}

impl<VM: VMBinding> Plan for SemiSpace<VM> {
    fn constraints(&self) -> &'static PlanConstraints {
        &SS_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        let zygote = self.common().is_zygote();
        CopyConfig {
            copy_mapping: if likely(!zygote) {
                *SS_COPY_CONFIG_DEFAULT
            } else {
                *SS_COPY_CONFIG_ZYGOTE
            },
            space_mapping: if likely(!zygote) {
                vec![
                    // The tospace argument doesn't matter, we will rebind before a GC anyway.
                    (CopySelector::CopySpace(0), &self.copyspace0),
                ]
            } else {
                vec![
                    (CopySelector::Immix(0), self.common().get_zygote().get_immix_space()),
                ]
            },
            constraints: &SS_CONSTRAINTS,
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        // XXX(kunals): We have to manually say what trace kind we want when we're the Zygote
        if unlikely(self.common().is_zygote()) {
            use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
            crate::plan::immix::Immix::schedule_immix_full_heap_collection::<
                SemiSpace<VM>,
                SSGCWorkContext<VM, TRACE_KIND_FAST>,
                SSGCWorkContext<VM, TRACE_KIND_DEFRAG>,
            >(self, self.common().get_zygote().get_immix_space(), scheduler);
        } else {
            use crate::policy::gc_work::DEFAULT_TRACE;
            scheduler.schedule_common_work::<SSGCWorkContext<VM, DEFAULT_TRACE>>(self);
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        if likely(!self.common().is_zygote()) {
            &ALLOCATOR_MAPPING_DEFAULT
        } else {
            &ALLOCATOR_MAPPING_ZYGOTE
        }
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(
            tls,
            true,
            self.get_total_pages(),
            self.get_reserved_pages(),
            self.get_collection_reserved_pages(),
        );

        if likely(!self.common().is_zygote()) {
            self.hi
                .store(!self.hi.load(Ordering::SeqCst), Ordering::SeqCst); // flip the semi-spaces
                                                                           // prepare each of the collected regions
            let hi = self.hi.load(Ordering::SeqCst);
            self.copyspace0.prepare(hi);
            self.copyspace1.prepare(!hi);
        }
    }

    fn prepare_worker(&self, worker: &mut GCWorker<VM>) {
        if likely(!self.common().is_zygote()) {
            let copy_config = worker.get_copy_context_mut();
            if copy_config.config.copy_mapping != *SS_COPY_CONFIG_DEFAULT {
                copy_config.config.copy_mapping = *SS_COPY_CONFIG_DEFAULT;
            }
            unsafe { worker.get_copy_context_mut().copy[0].assume_init_mut() }.rebind(self.tospace());
        } else {
            assert_eq!(
                worker.get_copy_context_mut().config.copy_mapping,
                *SS_COPY_CONFIG_ZYGOTE,
                "Worker copy config for Zygote is not ImmixAllocator!",
            );
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        self.common.release(tls, true);
        // release the collected region
        if likely(!self.common().is_zygote()) {
            self.fromspace().release();
        }
    }

    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    fn current_gc_may_move_object(&self) -> bool {
        true
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.tospace().reserved_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.tospace().reserved_pages() + self.common.get_used_pages()
    }

    fn get_available_pages(&self) -> usize {
        (self
            .get_total_pages()
            .saturating_sub(self.get_reserved_pages()))
            >> 1
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

impl<VM: VMBinding> SemiSpace<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &SS_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
        };

        // Add the chunk mark table to the list of global metadata
        plan_args.global_side_metadata_specs.push(crate::util::heap::chunk_map::ChunkMap::ALLOC_TABLE);
        let _max_heap_mb = util::conversions::raw_align_up(plan_args.global_args.gc_trigger.policy.get_max_heap_size_in_pages() * 4 * 1024, BYTES_IN_CHUNK) / 2 / 1024 / 1024;

        let res = SemiSpace {
            hi: AtomicBool::new(false),
            copyspace0: CopySpace::new(
                plan_args.get_space_args("copyspace0", true, false, VMRequest::discontiguous()),
                // plan_args.get_space_args("copyspace0", true, false, VMRequest::fixed_size(_max_heap_mb)),
                false,
            ),
            copyspace1: CopySpace::new(
                plan_args.get_space_args("copyspace1", true, false, VMRequest::discontiguous()),
                // plan_args.get_space_args("copyspace1", true, false, VMRequest::fixed_size(_max_heap_mb)),
                true,
            ),
            common: CommonPlan::new(plan_args),
        };

        res.verify_side_metadata_sanity();

        res
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
