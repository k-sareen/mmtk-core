use super::SemiSpace;
use crate::plan::global::Plan;
use crate::plan::mutator_context::unreachable_prepare_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorBuilder;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::{
    create_allocator_mapping, create_space_mapping, ReservedAllocators,
};
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::alloc::BumpAllocator;
use crate::util::rust_util::likely;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;
use enum_map::EnumMap;

pub fn ss_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    let plan = &mutator.plan;
    if likely(!plan.common().is_zygote()) {
        // Either the runtime has a Zygote space or it is a command-line runtime
        debug_assert!(
            plan.common().has_zygote_space()
                || (!plan.common().is_zygote_process()
                    && !*plan.common().base.options.is_zygote_process)
        );

        // Use the default allocator mapping after the first Zygote fork
        if *(mutator.config.allocator_mapping) != *ALLOCATOR_MAPPING_DEFAULT {
            mutator.config.allocator_mapping = &ALLOCATOR_MAPPING_DEFAULT;
        }

        // rebind the allocation bump pointer to the appropriate semispace
        let bump_allocator = unsafe {
            mutator
                .allocators
                .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_mut::<BumpAllocator<VM>>()
        .unwrap();
        bump_allocator.rebind(
            plan
                .downcast_ref::<SemiSpace<VM>>()
                .unwrap()
                .tospace(),
        );
    } else {
        use crate::util::alloc::ImmixAllocator;
        let allocator = unsafe {
            mutator
                .allocators
                .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
        }
        .downcast_mut::<ImmixAllocator<VM>>();
        debug_assert!(allocator.is_some(), "Default allocator for Zygote is not ImmixAllocator!");
        allocator.unwrap().reset();
    }
}

const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_bump_pointer: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING_DEFAULT: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::BumpPointer(0);
        map
    };
    pub static ref ALLOCATOR_MAPPING_ZYGOTE: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::Immix(0);
        map
    };
}

pub fn create_ss_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let ss = mmtk.get_plan().downcast_ref::<SemiSpace<VM>>().unwrap();
    let zygote = ss.common().is_zygote();
    let config = MutatorConfig {
        allocator_mapping: if likely(!zygote) {
            &ALLOCATOR_MAPPING_DEFAULT
        } else {
            &ALLOCATOR_MAPPING_ZYGOTE
        },
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, ss);
            vec.push((AllocatorSelector::BumpPointer(0), ss.tospace()));
            vec
        }),
        prepare_func: &unreachable_prepare_func,
        release_func: &ss_mutator_release,
    };

    let builder = MutatorBuilder::new(mutator_tls, mmtk, config);
    builder.build()
}
