use atomic::Ordering;

use crate::plan::ObjectQueue;
use crate::plan::VectorObjectQueue;
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::heap::{FreeListPageResource, PageResource};
use crate::util::metadata;
use crate::util::metadata::mark_bit::MarkState;
use crate::util::opaque_pointer::*;
use crate::util::treadmill::TreadMill;
use crate::util::{Address, ObjectReference};
use crate::vm::ObjectModel;
use crate::vm::VMBinding;

#[allow(unused)]
const PAGE_MASK: usize = !(BYTES_IN_PAGE - 1);
const MARK_BIT: u8 = 0b01;
const NURSERY_BIT: u8 = 0b10;
const LOS_BIT_MASK: u8 = 0b11;

/// This type implements a policy for large objects. Each instance corresponds
/// to one Treadmill space.
pub struct LargeObjectSpace<VM: VMBinding> {
    common: CommonSpace<VM>,
    pr: FreeListPageResource<VM>,
    mark_state: MarkState,
    in_nursery_gc: bool,
    treadmill: TreadMill,
}

impl<VM: VMBinding> SFT for LargeObjectSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }
    fn is_live(&self, object: ObjectReference) -> bool {
        self.is_marked(object)
    }
    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, _object: ObjectReference) -> bool {
        false
    }
    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, _object: ObjectReference) -> bool {
        false
    }
    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, _object: ObjectReference) -> bool {
        true
    }
    fn is_movable(&self) -> bool {
        false
    }
    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_object_metadata(&self, object: ObjectReference, alloc: bool) {
        self.mark_state.on_object_metadata_initialization::<VM>(object);
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(object);
        self.treadmill.add_to_treadmill(object, alloc);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr).is_some()
    }
    fn sft_trace_object(
        &self,
        queue: &mut VectorObjectQueue,
        object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        self.trace_object(queue, object)
    }
}

impl<VM: VMBinding> Space<VM> for LargeObjectSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }
    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }

    fn initialize_sft(&self, sft_map: &mut dyn crate::policy::sft_map::SFTMap) {
        self.common().initialize_sft(self.as_sft(), sft_map)
    }

    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn release_multiple_pages(&mut self, start: Address) {
        self.pr.release_pages(start);
    }
}

use crate::scheduler::GCWorker;
use crate::util::copy::CopySemantics;

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for LargeObjectSpace<VM> {
    fn trace_object<Q: ObjectQueue, const KIND: crate::policy::gc_work::TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        _copy: Option<CopySemantics>,
        _worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.trace_object(queue, object)
    }
    fn may_move_objects<const KIND: crate::policy::gc_work::TraceKind>() -> bool {
        false
    }
}

impl<VM: VMBinding> LargeObjectSpace<VM> {
    pub fn new(
        args: crate::policy::space::PlanCreateSpaceArgs<VM>,
        protect_memory_on_release: bool,
    ) -> Self {
        let is_discontiguous = args.vmrequest.is_discontiguous();
        let vm_map = args.vm_map;
        let common = CommonSpace::new(args.into_policy_args(
            false,
            false,
            metadata::extract_side_metadata(&[*VM::VMObjectModel::LOCAL_MARK_BIT_SPEC]),
        ));
        let mut pr = if is_discontiguous {
            FreeListPageResource::new_discontiguous(vm_map)
        } else {
            FreeListPageResource::new_contiguous(common.start, common.extent, vm_map)
        };
        pr.protect_memory_on_release = protect_memory_on_release;
        LargeObjectSpace {
            pr,
            common,
            mark_state: MarkState::new(),
            in_nursery_gc: false,
            treadmill: TreadMill::new(),
        }
    }

    pub fn prepare(&mut self, full_heap: bool) {
        self.mark_state.on_global_prepare::<VM>();
        if full_heap {
            debug_assert!(self.treadmill.is_from_space_empty());
            let mut guard = self.treadmill.to_space.lock().unwrap();
            let to_space_objs: Vec<ObjectReference> = guard.iter().copied().collect();
            guard.clear();
            drop(guard);
            debug_assert!(self.treadmill.is_to_space_empty());
            for object in &to_space_objs {
                self.mark_state.clear::<VM>(*object);
                self.treadmill.add_to_treadmill(*object, true);
            }

            if self.has_zygote_space() {
                self.reset_mark_zygote_objects();
            }
        }

        self.treadmill.flip(full_heap);
        self.in_nursery_gc = !full_heap;
    }

    pub fn release(&mut self, full_heap: bool) {
        self.mark_state.on_global_release::<VM>();

        self.sweep_large_objects(full_heap);
        debug_assert!(self.treadmill.is_nursery_empty());
        debug_assert!(self.treadmill.is_from_space_empty());

        if self.common().global_state.is_pre_first_zygote_fork_gc() {
            assert!(
                self.is_zygote_process(),
                "Cannot create large object zygote space for non-Zygote process!",
            );
            self.create_zygote_space();
        }
    }

    // Allow nested-if for this function to make it clear that test_and_mark() is only executed
    // for the outer condition is met.
    #[allow(clippy::collapsible_if)]
    pub fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        debug_assert!(!object.is_null());
        #[cfg(feature = "vo_bit")]
        debug_assert!(
            crate::util::metadata::vo_bit::is_vo_bit_set::<VM>(object),
            "{:x}: VO bit not set",
            object
        );
        let nursery_object = self.is_in_nursery(object);
        let is_zygote_object = self.has_zygote_space()
            && self.treadmill.is_zygote_object(object);
        trace!(
            "LOS object {} {} a nursery object",
            object,
            if nursery_object { "is" } else { "is not" }
        );
        if !self.in_nursery_gc || nursery_object {
            // Note that test_and_mark() has side effects of
            // clearing nursery bit/moving objects out of logical nursery
            if self.mark_state.test_and_mark::<VM>(object) {
                trace!("LOS object {} is being marked now", object);
                if !is_zygote_object {
                    self.treadmill.copy(object, nursery_object);
                }
                // We just moved the object out of the logical nursery, mark it as unlogged.
                // We also set the unlog bit for mature objects to ensure that
                // any modifications to them are logged
                if self.common.needs_log_bit {
                    VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                        .mark_as_unlogged::<VM>(object, Ordering::SeqCst);
                }
                if !is_zygote_object || (!self.in_nursery_gc && is_zygote_object) {
                    queue.enqueue(object);
                }
            } else {
                trace!(
                    "LOS object {} is not being marked now, it was marked before",
                    object
                );
            }
        }
        object
    }

    fn sweep_large_objects(&mut self, full_heap: bool) {
        let sweep = |object: ObjectReference| {
            #[cfg(feature = "vo_bit")]
            crate::util::metadata::vo_bit::unset_vo_bit::<VM>(object);
            if self.common.needs_log_bit {
                VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                    .clear::<VM>(object, Ordering::SeqCst);
            }
            let start = object.to_object_start::<VM>();
            #[cfg(feature = "poison_on_release")]
            crate::util::memory::set(start, 0xab, VM::VMObjectModel::get_current_size(object));
            self.pr
                .release_pages(get_super_page(start));
        };

        for object in self.treadmill.collect_nursery() {
            sweep(object);
        }

        if full_heap {
            for object in self.treadmill.collect() {
                sweep(object)
            }
        }
    }

    pub fn enumerate_large_objects(&self) -> Vec<crate::util::ObjectReference> {
       self.treadmill.enumerate_large_objects()
    }

    /// Allocate an object
    pub fn allocate_pages(&self, tls: VMThread, pages: usize) -> Address {
        self.acquire(tls, pages)
    }

    /// Check if a given object is marked
    fn is_marked(&self, object: ObjectReference) -> bool {
        self.mark_state.is_marked::<VM>(object)
    }

    /// Check if a given object is in nursery
    fn is_in_nursery(&self, object: ObjectReference) -> bool {
        !self.is_marked(object)
    }

    fn is_zygote_process(&self) -> bool {
        self.common().global_state.is_zygote_process()
    }

    fn has_zygote_space(&self) -> bool {
        self.common().global_state.has_zygote_space()
    }

    fn create_zygote_space(&mut self) {
        self.treadmill.create_zygote_space();
    }

    fn reset_mark_zygote_objects(&self) {
        let zygote_space = self.treadmill.zygote_space.lock().unwrap();
        for object in &(*zygote_space) {
            self.mark_state.clear::<VM>(*object);
        }
    }
}

fn get_super_page(cell: Address) -> Address {
    cell.align_down(BYTES_IN_PAGE)
}
