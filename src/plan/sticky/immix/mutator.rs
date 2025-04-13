use crate::plan::barriers::ObjectBarrier;
use crate::plan::generational::barrier::GenObjectBarrierSemantics;
use crate::plan::global::Plan;
use crate::plan::immix;
use crate::plan::mutator_context::{
    create_allocator_mapping, create_space_mapping, unreachable_prepare_func, MutatorBuilder, MutatorConfig,
};
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::sticky::immix::global::StickyImmix;
use crate::util::alloc::AllocatorSelector;
use crate::util::alloc::ImmixAllocator;
use crate::util::opaque_pointer::VMWorkerThread;
use crate::util::rust_util::{likely, unlikely};
use crate::util::VMMutatorThread;
use crate::vm::VMBinding;
use crate::AllocationSemantics;
use crate::{Mutator, MMTK};
use enum_map::EnumMap;

pub fn stickyimmix_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    let mut immix_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();

    let plan = mutator.plan.downcast_ref::<StickyImmix<VM>>().unwrap();
    if unlikely(plan.common().is_zygote()) {
        immix_allocator.rebind(plan.common().get_zygote().get_immix_space());
    } else {
        // Either the runtime has a Zygote space or it is a command-line runtime
        debug_assert!(
            plan.common().has_zygote_space()
                || (!plan.common().is_zygote_process()
                    && !*plan.common().base.options.is_zygote_process)
        );
        immix_allocator.rebind(plan.get_immix_space());
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

pub fn create_stickyimmix_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let stickyimmix = mmtk.get_plan().downcast_ref::<StickyImmix<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec =
                create_space_mapping(RESERVED_ALLOCATORS, true, mmtk.get_plan());
            // Use real ImmixSpace when we either have a Zygote space already or if we are
            // not the Zygote process
            if likely(stickyimmix.common().has_zygote_space() || !stickyimmix.common().is_zygote_process()) {
                vec.push((AllocatorSelector::Immix(0), stickyimmix.get_immix_space()));
            }
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &stickyimmix_mutator_release,
    };

    let builder = MutatorBuilder::new(mutator_tls, mmtk, config);
    builder
        .barrier(Box::new(ObjectBarrier::new(
            GenObjectBarrierSemantics::new(mmtk, stickyimmix),
        )))
        .build()
}
