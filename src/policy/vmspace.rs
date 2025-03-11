use crate::mmtk::SFT_MAP;
use crate::plan::{ObjectQueue, VectorObjectQueue};
use crate::policy::gc_work::DEFAULT_TRACE;
use crate::policy::sft::GCWorkerMutRef;
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::scheduler::{self, gc_work::*, GCWork, GCWorkScheduler, GCWorker, WorkBucketStage};
use crate::util::address::Address;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::heap::externalpageresource::{ExternalPageResource, ExternalPages};
use crate::util::heap::layout::vm_layout::BYTES_IN_CHUNK;
use crate::util::heap::PageResource;
use crate::util::metadata::mark_bit::MarkState;
#[cfg(feature = "set_unlog_bits_vm_space")]
use crate::util::metadata::MetadataSpec;
use crate::util::object_enum::ObjectEnumerator;
use crate::util::opaque_pointer::*;
use crate::util::rust_util::{likely, unlikely};
use crate::util::ObjectReference;
use crate::vm::{ObjectModel, VMBinding};
use crate::Plan;
use crate::MMTK;

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
// use std::sync::Mutex;

/// A special space for VM/Runtime managed memory. The implementation is similar to [`crate::policy::immortalspace::ImmortalSpace`],
/// except that VM space does not allocate. Instead, the runtime can add regions that are externally managed
/// and mmapped to the space, and allow objects in those regions to be traced in the same way
/// as other MMTk objects allocated by MMTk.
pub struct VMSpace<VM: VMBinding> {
    pub(crate) initialized: bool,
    mark_state: MarkState,
    common: CommonSpace<VM>,
    pr: ExternalPageResource<VM>,
    // pub(crate) slots: Mutex<Vec<VM::VMSlot>>,
    // pub(crate) objects: Mutex<Vec<ObjectReference>>,
    scheduler: Arc<GCWorkScheduler<VM>>,
    start: Address,
    size: usize,
    // #[cfg(debug_assertions)]
    // pub(crate) slots_set: Mutex<HashSet<VM::VMSlot>>,
}

impl<VM: VMBinding> SFT for VMSpace<VM> {
    fn name(&self) -> &str {
        self.common.name
    }
    fn is_live(&self, _object: ObjectReference) -> bool {
        true
    }
    fn is_reachable(&self, object: ObjectReference) -> bool {
        // self.mark_state.is_marked::<VM>(object)
        true
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
    fn is_sane(&self, object: ObjectReference) -> bool {
        VM::VMObjectModel::is_object_sane(object)
    }
    fn initialize_object_metadata(&self, object: ObjectReference, _alloc: bool) {
        self.mark_state
            .on_object_metadata_initialization::<VM>(object);
        if self.common.needs_log_bit {
            VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.mark_as_unlogged::<VM>(object, Ordering::SeqCst);
        }
        #[cfg(feature = "vo_bit")]
        crate::util::metadata::vo_bit::set_vo_bit::<VM>(object);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> Option<ObjectReference> {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr)
    }
    #[cfg(feature = "is_mmtk_object")]
    fn find_object_from_internal_pointer(
        &self,
        ptr: Address,
        max_search_bytes: usize,
    ) -> Option<ObjectReference> {
        crate::util::metadata::vo_bit::find_object_from_internal_pointer::<VM>(
            ptr,
            max_search_bytes,
        )
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

impl<VM: VMBinding> Space<VM> for VMSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }
    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        &self.pr
    }
    fn maybe_get_page_resource_mut(&mut self) -> Option<&mut dyn PageResource<VM>> {
        Some(&mut self.pr)
    }
    fn common(&self) -> &CommonSpace<VM> {
        &self.common
    }

    fn initialize_sft(&self, sft_map: &mut dyn crate::policy::sft_map::SFTMap) {
        // Initialize sft for current external pages. This method is called at the end of plan creation.
        // So we only set SFT for VM regions that are set by options (we skipped sft initialization for them earlier).
        let vm_regions = self.pr.get_external_pages();
        // We should have at most one region at this point (set by the option). If we allow setting multiple VM spaces through options,
        // we can remove this assertion.
        assert!(vm_regions.len() <= 1);
        for external_pages in vm_regions.iter() {
            // Chunk align things.
            let start = external_pages.start.align_down(BYTES_IN_CHUNK);
            let size = external_pages.end.align_up(BYTES_IN_CHUNK) - start;
            // The region should be empty in SFT map -- if they were set before this point, there could be invalid SFT pointers.
            debug_assert_eq!(
                sft_map.get_checked(start).name(),
                crate::policy::sft::EMPTY_SFT_NAME
            );
            // Set SFT
            assert!(sft_map.has_sft_entry(start), "The VM space start (aligned to {}) does not have a valid SFT entry. Possibly the address range is not in the address range we use.", start);
            unsafe {
                sft_map.eager_initialize(self.as_sft(), start, size);
            }
        }
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }

    fn acquire(&self, _tls: VMThread, _pages: usize) -> Address {
        unreachable!()
    }

    fn address_in_space(&self, start: Address) -> bool {
        // The default implementation checks with vm map. But vm map has some assumptions about
        // the address range for spaces and the VM space may break those assumptions (as the space is
        // mmapped by the runtime rather than us). So we we use SFT here.
        self.start <= start && start < self.start + self.size
        // unsafe { SFT_MAP.get_checked_nonatomic(start).name() == self.name() }
        // SFT_MAP.get_checked(start).name() == self.name()
        // self.pr.get_external_pages().iter().any(|ep| ep.start <= start && start < ep.end)
    }

    fn enumerate_objects(&self, enumerator: &mut dyn ObjectEnumerator) {
        let external_pages = self.pr.get_external_pages();
        for ep in external_pages.iter() {
            enumerator.visit_address_range(ep.start, ep.end);
        }
    }
}

use crate::util::copy::CopySemantics;

impl<VM: VMBinding> crate::policy::gc_work::PolicyTraceObject<VM> for VMSpace<VM> {
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

impl<VM: VMBinding> VMSpace<VM> {
    pub fn new(args: crate::policy::space::PlanCreateSpaceArgs<VM>) -> Self {
        let (vm_space_start, vm_space_size) =
            (*args.options.vm_space_start, *args.options.vm_space_size);
        let scheduler = args.scheduler.clone();
        let space = Self {
            initialized: false,
            mark_state: MarkState::new(),
            pr: ExternalPageResource::new(args.vm_map),
            common: CommonSpace::new(args.into_policy_args(
                false,
                true,
                crate::util::metadata::extract_side_metadata(&[
                    *VM::VMObjectModel::LOCAL_MARK_BIT_SPEC,
                ]),
            )),
            // slots: Mutex::new(vec![]),
            // objects: Mutex::new(vec![]),
            scheduler,
            // #[cfg(debug_assertions)]
            // slots_set: Mutex::new(HashSet::new()),
            start: vm_space_start,
            size: vm_space_size,
        };

        if !vm_space_start.is_zero() {
            // Do not set sft here, as the space may be moved. We do so for those regions in `initialize_sft`.
            space.set_vm_region_inner(vm_space_start, vm_space_size, false);
        }

        space
    }

    pub fn set_vm_region(&mut self, start: Address, size: usize) {
        self.set_vm_region_inner(start, size, true);
        self.start = start;
        self.size = size;
    }

    pub fn scheduler(&self) -> &GCWorkScheduler<VM> {
        &self.scheduler
    }

    fn set_vm_region_inner(&self, start: Address, size: usize, set_sft: bool) {
        assert!(size > 0);
        assert!(!start.is_zero());

        let end = start + size;

        let chunk_start = start.align_down(BYTES_IN_CHUNK);
        let chunk_end = end.align_up(BYTES_IN_CHUNK);
        let chunk_size = chunk_end - chunk_start;

        // For simplicity, VMSpace has to be outside our available heap range.
        // TODO: Allow VMSpace in our available heap range.
        assert!(Address::range_intersection(
            &(chunk_start..chunk_end),
            &crate::util::heap::layout::available_range()
        )
        .is_empty());

        debug!(
            "Align VM space ({}, {}) to chunk ({}, {})",
            start, end, chunk_start, chunk_end
        );

        // Mark as mapped in mmapper
        self.common.mmapper.mark_as_mapped(chunk_start, chunk_size);
        // Map side metadata
        self.common
            .metadata
            .try_map_metadata_space(chunk_start, chunk_size)
            .unwrap();
        // Insert to vm map: it would be good if we can make VM map aware of the region. However, the region may be outside what we can map in our VM map implementation.
        // self.common.vm_map.insert(chunk_start, chunk_size, self.common.descriptor);
        // Set SFT if we should
        if set_sft {
            assert!(SFT_MAP.has_sft_entry(chunk_start), "The VM space start (aligned to {}) does not have a valid SFT entry. Possibly the address range is not in the address range we use.", chunk_start);
            unsafe {
                SFT_MAP.update(self.as_sft(), chunk_start, chunk_size);
            }
        }

        self.pr.add_new_external_pages(ExternalPages {
            start: start.align_down(BYTES_IN_PAGE),
            end: end.align_up(BYTES_IN_PAGE),
        });

        #[cfg(feature = "set_unlog_bits_vm_space")]
        if self.common.needs_log_bit {
            // Bulk set unlog bits for all addresses in the VM space. This ensures that any
            // modification to the bootimage is logged
            if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC {
                side.bset_metadata(start, size);
            }
        }
    }

    pub fn prepare(&mut self, _major_gc: bool) {}

    pub fn release(&mut self) {}

    pub fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "vo_bit")]
        debug_assert!(
            crate::util::metadata::vo_bit::is_vo_bit_set::<VM>(object),
            "{:x}: VO bit not set",
            object
        );
        debug_assert!(self.in_space(object));
        // TODO(kunals): Fix this because it's most likely broken for generational GC
        // if self.mark_state.test_and_mark::<VM>(object) {
        //     if self.common.needs_log_bit {
        //         VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
        //             .mark_byte_as_unlogged::<VM>(object, Ordering::SeqCst);
        //     }
        // }
        object
    }
}

pub struct ProcessVmSpaceObjects<E: ProcessEdgesWork> {
    phantom: PhantomData<E>,
}

impl<E: ProcessEdgesWork> ProcessVmSpaceObjects<E> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<E: ProcessEdgesWork> GCWork<E::VM> for ProcessVmSpaceObjects<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        use crate::vm::Scanning;

        let tls = worker.tls;
        let mut closure = |objects: Vec<ObjectReference>| {
            let mut work_packet = ScanObjects::<E>::new(objects, false, WorkBucketStage::Closure);
            worker.add_work(WorkBucketStage::Closure, work_packet);
        };

        <E::VM as VMBinding>::VMScanning::scan_vm_space_objects(tls, closure);

        // if likely(self.vmspace.initialized) {
        //     // use crate::vm::slot::Slot;
        //     // use crate::vm::Scanning;

        //     let objects = self.vmspace.objects.lock().unwrap();
        //     // let slots = self.vmspace.slots.lock().unwrap();

        //     // println!("Processing {} VM space objects", objects.len());
        //     // println!("Processing {} VM space slots", slots.len());

        //     // let mut object_slots = vec![];
        //     // let mut outside_vmspace_object_slots = HashSet::new();
        //     // for object in objects.iter() {
        //     //     let slots = <E::VM as VMBinding>::VMScanning::scan_object(
        //     //         worker.tls,
        //     //         *object,
        //     //         &mut |slot: <E::VM as VMBinding>::VMSlot| {
        //     //             object_slots.push(slot);
        //     //             if !self.vmspace.address_in_space(slot.as_address()) {
        //     //                 outside_vmspace_object_slots.insert(*object);
        //     //             }
        //     //         },
        //     //     );
        //     // }

        //     // println!("Processing {} VM space object slots", object_slots.len());
        //     // println!(
        //     //     "{} VM space object with slots outside VM space",
        //     //     outside_vmspace_object_slots.len()
        //     // );

        //     // let slot_set: HashSet<<E::VM as VMBinding>::VMSlot> =
        //     //     HashSet::from_iter(slots.iter().cloned());
        //     // let object_slots_set = HashSet::from_iter(object_slots.iter().cloned());

        //     // let mut diff = object_slots_set.difference(&slot_set).collect::<Vec<_>>();
        //     // diff.sort();

        //     // for object in outside_vmspace_object_slots.iter().take(5) {
        //     //     <E::VM as VMBinding>::VMObjectModel::dump_object(*object);
        //     // }

        //     // println!("{} difference slots", diff.len());
        //     // println!("Difference: {:?}", diff);

        //     // iterate through the first five elements of the difference
        //     // for slot in diff.iter().take(5) {
        //     //     let Some(object) = slot.load() else { continue };
        //     //     <E::VM as VMBinding>::VMObjectModel::dump_object(object);
        //     // }

        //     // assert_eq!(object_slots.len(), slots.len());

        //     GCWork::do_work(
        //         &mut ScanObjects::<E>::new(objects.clone(), false, WorkBucketStage::Closure),
        //         worker,
        //         mmtk,
        //     )

        //     // let slots = self.vmspace.slots.lock().unwrap();
        //     // println!("Processing {} VM space slots", slots.len());
        //     // GCWork::do_work(
        //     //     &mut E::new(slots.clone(), false, mmtk, WorkBucketStage::Closure),
        //     //     worker,
        //     //     mmtk,
        //     // )
        // }
    }
}
