use super::Immix;
use crate::plan::global::Plan;
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::create_space_mapping;
use crate::plan::mutator_context::unreachable_prepare_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::{AllocatorSelector, Allocators};
use crate::util::alloc::ImmixAllocator;
use crate::util::rust_util::{likely,unlikely};
use crate::vm::VMBinding;
use crate::MMTK;
use crate::{
    plan::barriers::NoBarrier,
    util::opaque_pointer::{VMMutatorThread, VMWorkerThread},
};
use enum_map::EnumMap;

pub fn immix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    let immix_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();

    let plan = mutator.plan.downcast_ref::<Immix<VM>>().unwrap();
    if unlikely(plan.common().is_zygote()) {
        immix_allocator.rebind(plan.common().get_zygote().get_immix_space());
    } else {
        // Either the runtime has a Zygote space or it is a command-line runtime
        debug_assert!(
            plan.common().has_zygote_space()
                || (!plan.common().is_zygote_process()
                    && !*plan.common().base.options.is_zygote_process)
        );
        immix_allocator.rebind(&plan.immix_space);
    }
}

pub(in crate::plan) const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_immix: 0,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::Immix(0);
        map
    };
}

pub fn create_immix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let immix = mmtk.get_plan().downcast_ref::<Immix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec =
                create_space_mapping(RESERVED_ALLOCATORS, true, immix);
            // Use real ImmixSpace when we either have a Zygote space already or if we are
            // not the Zygote process
            if likely(immix.common().has_zygote_space() || !immix.common().is_zygote_process()) {
                vec.push((AllocatorSelector::Immix(0), &immix.immix_space));
            }
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &immix_mutator_release,
    };

    Mutator {
        allocators: Allocators::<VM>::new(mutator_tls, mmtk, &config.space_mapping),
        barrier: Box::new(NoBarrier),
        mutator_tls,
        config,
        plan: immix,
    }
}
