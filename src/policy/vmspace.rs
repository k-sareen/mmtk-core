use crate::mmtk::SFT_MAP;
use crate::plan::{ObjectQueue, VectorObjectQueue};
use crate::policy::sft::{GCWorkerMutRef, EmptySpaceSFT, EMPTY_SPACE_SFT};
use crate::policy::sft::SFT;
use crate::policy::space::{CommonSpace, Space};
use crate::scheduler::{self, gc_work::*, GCWork, GCWorker, WorkBucketStage};
use crate::util::address::Address;
use crate::util::alloc::allocator::AllocationOptions;
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

/// A special space for VM/Runtime managed memory. The implementation is similar to [`crate::policy::immortalspace::ImmortalSpace`],
/// except that VM space does not allocate. Instead, the runtime can add regions that are externally managed
/// and mmapped to the space, and allow objects in those regions to be traced in the same way
/// as other MMTk objects allocated by MMTk.
pub struct VMSpace<VM: VMBinding> {
    pub(crate) initialized: bool,
    pub(crate) object_cache: Vec<ObjectReference>,
    mark_state: MarkState,
    common: CommonSpace<VM>,
    pr: ExternalPageResource<VM>,
    ranges: Vec<ExternalPages>,
}

impl<VM: VMBinding> SFT for VMSpace<VM> {
    fn name(&self) -> &'static str {
        self.common.name
    }
    fn is_live(&self, _object: ObjectReference) -> bool {
        true
    }
    fn is_reachable(&self, object: ObjectReference) -> bool {
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
        crate::util::metadata::vo_bit::set_vo_bit(object);
    }
    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> Option<ObjectReference> {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr(addr)
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
            assert!(
                sft_map.has_sft_entry(start),
                "The VM space start (aligned to {}) does not have a valid SFT entry. Possibly the address range is not in the address range we use.",
                start,
            );
            unsafe {
                sft_map.eager_initialize(self.as_sft(), start, size);
            }
        }
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }

    fn acquire(&self, _tls: VMThread, _pages: usize, _alloc_options: AllocationOptions) -> Address {
        unreachable!()
    }

    fn address_in_space(&self, start: Address) -> bool {
        self.ranges.iter().any(|region| {
            region.start <= start && start < region.end
        })
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
        let space = Self {
            initialized: false,
            object_cache: vec![],
            mark_state: MarkState::new(),
            pr: ExternalPageResource::new(args.vm_map),
            common: CommonSpace::new(args.into_policy_args(
                false,
                true,
                vec![],
            )),
            ranges: vec![],
        };

        if !vm_space_start.is_zero() {
            // Do not set sft here, as the space may be moved. We do so for those regions in `initialize_sft`.
            space.set_vm_region_inner(vm_space_start, vm_space_size, false);
        }

        space
    }

    pub fn set_vm_region(&mut self, start: Address, size: usize) {
        self.set_vm_region_inner(start, size, true);

        self.ranges.push(ExternalPages {
            start: start.align_down(BYTES_IN_PAGE),
            end: (start + size).align_up(BYTES_IN_PAGE),
        });

        // Reset the initialized flag, so that we re-initialize the object cache
        self.initialized = false;
    }

    pub fn remove_vm_region(&mut self, start: Address, size: usize) {
        assert!(size > 0);
        assert!(!start.is_zero());

        let end = start + size;
        let extern_page = ExternalPages {
            start: start.align_down(BYTES_IN_PAGE),
            end: end.align_up(BYTES_IN_PAGE),
        };
        let chunk_start = start.align_down(BYTES_IN_CHUNK);
        let chunk_end = end.align_up(BYTES_IN_CHUNK);
        let chunk_size = chunk_end - chunk_start;

        debug!(
            "Removing VM space ({}, {}) chunk ({}, {})",
            start, end, chunk_start, chunk_end
        );

        if !self.pr.remove_external_pages(extern_page) {
            warn!("Failed to remove external pages ({}, {}) at chunks ({}, {})", start, end, chunk_start, chunk_end);
            return;
        }

        // We've checked that the region exists in `remove_external_pages`
        let index = self.ranges.iter()
                        .position(|&p| p == extern_page)
                        .expect(format!("External pages {:?} not found in ranges", extern_page).as_str());
        self.ranges.remove(index);

        // Mark VM space as unmapped. Note that we don't unmap the metadata since it may be used by other spaces,
        // for example global metadata like the chunk mark metadata.
        let mut can_unmap_region = true;
        for region in self.pr.get_external_pages().iter() {
            // If the chunk we are trying to unmap intersects with any other region, we cannot unmap it
            let region_chunk_start = region.start.align_down(BYTES_IN_CHUNK);
            let region_chunk_end = region.end.align_up(BYTES_IN_CHUNK);
            if !Address::range_intersection(&(chunk_start..chunk_end), &(region_chunk_start..region_chunk_end))
                .is_empty()
            {
                can_unmap_region = false;
                break;
            }
        }

        if can_unmap_region {
            self.common.mmapper.mark_as_unmapped(chunk_start, chunk_size);
        }

        assert!(
            SFT_MAP.has_sft_entry(chunk_start),
            "The VM space start (aligned to {}) does not have a valid SFT entry. Possibly the address range is not in the address range we use.",
            chunk_start,
        );
        assert!(
            SFT_MAP.get_checked(chunk_start).name() == self.name(),
            "The VM space region ({}, {}) to be cleared does not belong to us: {}",
            chunk_start, chunk_end, SFT_MAP.get_checked(chunk_start).name(),
        );

        // Clear the SFT entry for the removed region
        if can_unmap_region {
            unsafe {
                SFT_MAP.clear(chunk_start);
            }
        }

        // Reset the initialized flag, so that we re-initialize the object cache
        self.initialized = false;

        debug!(
            "Removed VM space ({}, {}) from chunk ({}, {})",
            start, end, chunk_start, chunk_end
        );
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
            "Adding VM space ({}, {}) chunk ({}, {})",
            start, end, chunk_start, chunk_end
        );

        // Mark as mapped in mmapper
        self.common.mmapper.mark_as_mapped(chunk_start, chunk_size);
        // Map side metadata
        self.common
            .metadata
            .try_map_metadata_space(chunk_start, chunk_size, self.get_name())
            .unwrap();
        // Insert to vm map: it would be good if we can make VM map aware of the region. However, the region may be outside what we can map in our VM map implementation.
        // self.common.vm_map.insert(chunk_start, chunk_size, self.common.descriptor);

        // Set SFT if we should
        if set_sft {
            assert!(
                SFT_MAP.has_sft_entry(chunk_start),
                "The VM space start (aligned to {}) does not have a valid SFT entry. Possibly the address range is not in the address range we use.",
                chunk_start,
            );
            assert!(
                SFT_MAP.get_checked(chunk_start).name() == crate::policy::sft::EMPTY_SFT_NAME || SFT_MAP.get_checked(chunk_start).name() == self.get_name(),
                "The VM space region ({}, {}) to be set already has a non-empty SFT: {}",
                chunk_start, chunk_end, SFT_MAP.get_checked(chunk_start).name(),
            );
            unsafe {
                SFT_MAP.update(self.as_sft(), chunk_start, chunk_size);
            }
        }

        self.pr.add_external_pages(ExternalPages {
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

        debug!(
            "Dumping process maps after adding VM space ({}, {}) chunks ({}, {})\n{}",
            start,
            end,
            chunk_start,
            chunk_end,
            crate::util::memory::get_process_memory_maps(),
        );
    }

    pub fn prepare(&mut self, major_gc: bool) {
        if major_gc {
            if self.common.needs_log_bit {
                // XXX(kunals): Have to set the log bit for the entire VM space for major GCs since
                // we don't trace and hence mark objects as unlogged anymore. This might be a bit
                // inefficient. We could potentially set the bits for objects by checking in the
                // ProcessVmSpaceObjects work packet.
                self.pr.get_external_pages().iter().for_each(|region| {
                    if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC {
                        side.bset_metadata(region.start, region.end - region.start);
                    }
                });
            }
        }
    }

    pub fn release(&mut self) {}

    pub fn trace_object<Q: ObjectQueue>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
    ) -> ObjectReference {
        #[cfg(feature = "vo_bit")]
        debug_assert!(
            crate::util::metadata::vo_bit::is_vo_bit_set(object),
            "{:x}: VO bit not set",
            object
        );
        debug_assert!(self.in_space(object));
        object
    }

    /// Initialize the object cache by scanning the VM space for objects. The VM space must
    /// not be initialized before this call. If we need to re-initialize the VM space (for
    /// example, if we have to add an application image at run-time), we have to unset the
    /// `initialized` field and then call this function again.
    pub fn initialize_object_cache(&mut self, tls: VMWorkerThread) {
        use crate::vm::Scanning;

        assert!(!self.initialized);
        // Clear the object cache in case we have to re-initialize the VM space
        // For example, if we have to add an application image at run-time
        self.object_cache.clear();
        let mut push_closure = |objects: Vec<ObjectReference>| {
            self.object_cache.extend(objects)
        };
        <VM as VMBinding>::VMScanning::scan_vm_space_objects(tls, push_closure);
        self.initialized = true;
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
        let tls = worker.tls;
        // SAFETY: There is only one work packet of this type in the prepare stage
        let mut vm_space = &mut unsafe { mmtk.get_plan_mut() }.base_mut().vm_space;
        let mut scan_closure = |objects: &Vec<ObjectReference>| {
            // If there are too many objects, split them into multiple work packets
            let chunk_size = crate::scheduler::EDGES_WORK_BUFFER_SIZE;
            for chunk in objects.chunks(chunk_size) {
                let mut work_packet =
                    ScanObjects::<E>::new(chunk.to_vec(), false, WorkBucketStage::Closure);
                worker.add_work(WorkBucketStage::Closure, work_packet);
            }
            return;
        };
        if crate::util::rust_util::unlikely(!vm_space.initialized) {
            vm_space.initialize_object_cache(tls);
        }
        scan_closure(&vm_space.object_cache);
    }
}
