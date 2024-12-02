use crate::plan::{ObjectQueue, VectorObjectQueue};
use crate::policy::gc_work::{PolicyTraceObject, TraceKind};
use crate::policy::immix::block::{Block, BlockState};
use crate::policy::immix::defrag::StatsForDefrag;
use crate::policy::immix::{ImmixSpace, ImmixSpaceArgs};
use crate::policy::sft::{GCWorkerMutRef, SFT};
use crate::policy::sft_map::SFTMap;
use crate::policy::space::{CommonSpace, Space};
use crate::util::copy::CopySemantics;
use crate::util::heap::chunk_map::{*, self};
use crate::util::heap::{PageResource, VMRequest};
use crate::util::linear_scan::Region;
use crate::util::metadata::side_metadata::SideMetadataSanity;
use crate::util::metadata::MetadataSpec;
use crate::util::rust_util::{likely, unlikely};
use crate::util::{Address, ObjectReference};
use crate::scheduler::{GCWork, GCWorker, WorkBucketStage};
use crate::vm::object_model::ObjectModel;
use crate::vm::VMBinding;
use crate::MMTK;
use atomic::Ordering;

/// A Zygote space for use when running Android applications. It wraps around an ImmixSpace as this
/// allows us to allocate non-moving objects into it (via pinning) and so we can compact directly
/// into the holes between the non-moving objects.
///
/// The idea of the Zygote space is to generate a bootimage at run-time essentially. The benefits of
/// the Zygote are twofold: (i) we get to share seldom written-to data between different app
/// processes, reducing memory consumption; and (ii) reduces app startup time since most of the common
/// libraries and classes are preloaded into the Zygote.
///
/// How the Zygote space works is we allocate into it if the runtime was started with the `-Xzygote`
/// option and once the runtime is ready to fork for the first time, we do a full defrag GC (i.e.
/// try to copy as much as possible) and then freeze the Zygote space. From here-on we will never
/// allocate or free objects from the Zygote space.
///
/// After the Zygote space has been generated for the first time, we want to avoid touching its pages
/// as much as possible as this would lead to copying the pages across to our memory address space,
/// defeating its purpose. To this end, we place object metadata such as mark bits and log bits
/// outside of the object header.
pub struct ZygoteSpace<VM: VMBinding> {
    /// Zygote space internally uses an ImmixSpace
    pub immix_space: ImmixSpace<VM>,
    /// Has the Zygote space been created?
    pub created_zygote_space: bool,
}

unsafe impl<VM: VMBinding> Sync for ZygoteSpace<VM> {}

impl<VM: VMBinding> SFT for ZygoteSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        self.immix_space.get_forwarded_object(object)
    }

    fn get_potential_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        self.immix_space.get_potential_forwarded_object(object)
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        if likely(self.created_zygote_space) {
            // If the object is inside the Zygote address space and we have created it,
            // then we just always return true here
            true
        } else {
            self.immix_space.is_live(object)
        }
    }

    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, object: ObjectReference) -> bool {
        self.immix_space.pin_object(object)
    }

    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, object: ObjectReference) -> bool {
        self.immix_space.unpin_object(object)
    }

    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, object: ObjectReference) -> bool {
        self.immix_space.is_object_pinned(object)
    }

    fn is_movable(&self) -> bool {
        if likely(self.created_zygote_space) {
            // We will never move objects once the Zygote space has been created
            false
        } else {
            self.immix_space.is_movable()
        }
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self, object: ObjectReference) -> bool {
        self.is_live(object)
    }

    fn initialize_object_metadata(&self, object: ObjectReference, alloc: bool) {
        self.immix_space.initialize_object_metadata(object, alloc);
    }

    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        crate::util::metadata::vo_bit::is_vo_bit_set_for_addr::<VM>(addr).is_some()
    }

    fn sft_trace_object(
        &self,
        _queue: &mut VectorObjectQueue,
        _object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        panic!("We do not use SFT to trace objects for Immix-like spaces. sft_trace_object() cannot be used.")
    }
}

impl<VM: VMBinding> Space<VM> for ZygoteSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }

    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }

    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        self.immix_space.get_page_resource()
    }

    fn maybe_get_page_resource_mut(&mut self) -> Option<&mut dyn PageResource<VM>> {
        self.immix_space.maybe_get_page_resource_mut()
    }

    fn common(&self) -> &CommonSpace<VM> {
        self.immix_space.common()
    }

    fn initialize_sft(&self, sft_map: &mut dyn SFTMap) {
        self.common().initialize_sft(self.as_sft(), sft_map)
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("ZygoteSpace only releases pages enmasse")
    }

    fn set_copy_for_sft_trace(&mut self, _semantics: Option<CopySemantics>) {
        panic!("We do not use SFT to trace objects for Immix-like spaces. set_copy_context() cannot be used.")
    }

    fn iterate_allocated_regions(&self) -> Vec<(Address, usize)> {
        let mut blocks = vec![];
        let chunk_map = &self.immix_space.chunk_map;
        for chunk in chunk_map.all_chunks() {
            if chunk_map.get(chunk) == ChunkState::Allocated {
                for block in chunk.iter_region::<Block>() {
                    if block.get_state() != BlockState::Unallocated {
                        blocks.push((block.start(), block.end() - block.start()));
                    }
                }
            }
        }
        blocks
    }
}

impl<VM: VMBinding> ZygoteSpace<VM> {
    pub fn new(
        args: crate::policy::space::PlanCreateSpaceArgs<VM>,
    ) -> Self {
        let immix_space_args = if args.constraints.needs_log_bit {
            ImmixSpaceArgs {
                unlog_object_when_traced: true,
                reset_log_bit_in_major_gc: true,
                mixed_age: false,
            }
        } else {
            ImmixSpaceArgs {
                unlog_object_when_traced: false,
                reset_log_bit_in_major_gc: false,
                mixed_age: false,
            }
        };

        // XXX(kunals): We set the defrag headroom percent to 50% for the Zygote
        let mut immix_space = ImmixSpace::new(args, immix_space_args, ZYGOTE_CHUNK_MASK);
        immix_space.set_defrag_headroom_percent(50);

        ZygoteSpace {
            immix_space,
            created_zygote_space: false,
        }
    }

    pub fn prepare(&mut self, major_gc: bool, plan_stats: StatsForDefrag) {
        if likely(self.created_zygote_space) {
            if major_gc {
                // For major GCs we reset the mark and log bits for the Zygote space
                // and then do a transitive closure
                // SAFETY: ImmixSpace reference is always valid within this collection cycle.
                let space = unsafe { &*(self.get_immix_space() as *const ImmixSpace<VM>) };
                let work_packets = self.immix_space.chunk_map.generate_tasks(|chunk| {
                    Box::new(ClearZygoteMetadata {
                        space,
                        chunk,
                    })
                });
                self.immix_space.scheduler().work_buckets[WorkBucketStage::Prepare].bulk_add(work_packets);
            }
        } else {
            self.immix_space.prepare(major_gc, plan_stats);
        }
    }

    pub fn release(&mut self, major_gc: bool, is_pre_first_zygote_fork_gc: bool) {
        if likely(self.created_zygote_space) {
            // The Zygote space has been created -- nothing to be done
            return;
        } else {
            self.immix_space.release(major_gc);
            if is_pre_first_zygote_fork_gc {
                self.created_zygote_space = true;
            }
        }
    }

    pub fn is_marked(&self, object: ObjectReference) -> bool {
        self.immix_space.is_marked(object)
    }

    pub fn get_immix_space(&self) -> &ImmixSpace<VM> {
        &self.immix_space
    }
}

impl<VM: VMBinding> PolicyTraceObject<VM> for ZygoteSpace<VM> {
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        if likely(self.created_zygote_space) {
            // The Zygote space has been created -- mark and enqueue the object for scanning
            if self.immix_space.mark_state.test_and_mark::<VM>(object) {
                if self.common().needs_log_bit {
                    VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC
                        .mark_byte_as_unlogged::<VM>(object, Ordering::SeqCst);
                }
                queue.enqueue(object);
            }
            object
        } else {
            self.immix_space.trace_object::<Q, KIND>(queue, object, copy, worker)
        }
    }

    fn may_move_objects<const KIND: TraceKind>() -> bool {
        ImmixSpace::<VM>::may_move_objects::<KIND>()
    }

    fn post_scan_object(&self, object: ObjectReference) {
        if unlikely(!self.created_zygote_space) {
            self.immix_space.post_scan_object(object)
        }
    }
}

impl<VM: VMBinding> Space<VM> for Option<ZygoteSpace<VM>> {
    fn as_space(&self) -> &dyn Space<VM> {
        panic!("Cannot call as_space on Option<ZygoteSpace<VM>>")
    }

    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }

    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        panic!("Cannot call get_page_resource on Option<ZygoteSpace<VM>>")
    }

    fn maybe_get_page_resource_mut(&mut self) -> Option<&mut dyn PageResource<VM>> {
        if let Some(space) = self.as_mut() {
            space.maybe_get_page_resource_mut()
        } else {
            None
        }
    }

    fn common(&self) -> &CommonSpace<VM> {
        panic!("Cannot call common on Option<ZygoteSpace<VM>>")
    }

    fn initialize_sft(&self, sft_map: &mut dyn SFTMap) {
        if let Some(space) = self.as_ref() {
            space.initialize_sft(sft_map);
        }
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        panic!("Cannot call release_multiple_pages on Option<ZygoteSpace<VM>>")
    }

    fn set_copy_for_sft_trace(&mut self, _semantics: Option<CopySemantics>) {
        panic!("Cannot call set_copy_for_sft_trace on Option<ZygoteSpace<VM>>")
    }

    fn verify_side_metadata_sanity(&self, side_metadata_sanity_checker: &mut SideMetadataSanity) {
        if let Some(space) = self.as_ref() {
            space.verify_side_metadata_sanity(side_metadata_sanity_checker);
        }
    }

    fn in_space(&self, object: ObjectReference) -> bool {
        if let Some(space) = self.as_ref() {
            space.in_space(object)
        } else {
            false
        }
    }

    fn iterate_allocated_regions(&self) -> Vec<(Address, usize)> {
        if let Some(space) = self.as_ref() {
            space.iterate_allocated_regions()
        } else {
            vec![]
        }
    }
}

impl<VM: VMBinding> SFT for Option<ZygoteSpace<VM>> {
    fn name(&self) -> &str {
        self.as_ref().unwrap().get_name()
    }

    fn get_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        self.as_ref().unwrap().get_forwarded_object(object)
    }

    fn get_potential_forwarded_object(&self, object: ObjectReference) -> Option<ObjectReference> {
        self.as_ref().unwrap().get_potential_forwarded_object(object)
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        self.as_ref().unwrap().is_live(object)
    }

    #[cfg(feature = "object_pinning")]
    fn pin_object(&self, object: ObjectReference) -> bool {
        self.as_ref().unwrap().pin_object(object)
    }

    #[cfg(feature = "object_pinning")]
    fn unpin_object(&self, object: ObjectReference) -> bool {
        self.as_ref().unwrap().unpin_object(object)
    }

    #[cfg(feature = "object_pinning")]
    fn is_object_pinned(&self, object: ObjectReference) -> bool {
        self.as_ref().unwrap().is_object_pinned(object)
    }

    fn is_movable(&self) -> bool {
        self.as_ref().unwrap().is_movable()
    }

    #[cfg(feature = "sanity")]
    fn is_sane(&self, object: ObjectReference) -> bool {
        self.as_ref().unwrap().is_sane(object)
    }

    fn initialize_object_metadata(&self, object: ObjectReference, alloc: bool) {
        self.as_ref().unwrap().initialize_object_metadata(object, alloc);
    }

    #[cfg(feature = "is_mmtk_object")]
    fn is_mmtk_object(&self, addr: Address) -> bool {
        self.as_ref().unwrap().is_mmtk_object(addr)
    }

    fn sft_trace_object(
        &self,
        _queue: &mut VectorObjectQueue,
        _object: ObjectReference,
        _worker: GCWorkerMutRef,
    ) -> ObjectReference {
        panic!("We do not use SFT to trace objects for Immix-like spaces. sft_trace_object() cannot be used.")
    }
}

impl<VM: VMBinding> PolicyTraceObject<VM> for Option<ZygoteSpace<VM>> {
    fn trace_object<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        copy: Option<CopySemantics>,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.as_ref()
            .unwrap()
            .trace_object::<Q, KIND>(queue, object, copy, worker)
    }

    fn may_move_objects<const KIND: TraceKind>() -> bool {
        ImmixSpace::<VM>::may_move_objects::<KIND>()
    }

    fn post_scan_object(&self, object: ObjectReference) {
        if let Some(space) = self.as_ref() {
            space.post_scan_object(object)
        }
    }
}

/// A work packet to prepare each block for a major GC.
/// Performs the action on a range of chunks.
pub struct ClearZygoteMetadata<VM: VMBinding> {
    pub space: &'static ImmixSpace<VM>,
    pub chunk: Chunk,
}

impl<VM: VMBinding> ClearZygoteMetadata<VM> {
    /// Clear object mark and log tables for allocated blocks in the chunk
    fn reset_object_metadata(&self) {
        // We reset the mark bits if they are on the side
        for block in self.chunk.iter_region::<Block>() {
            let state = block.get_state();
            if state == BlockState::Unallocated {
                continue;
            } else {
                self.space.mark_state.on_block_reset::<VM>(block.start(), Block::BYTES);
                if self.space.space_args.reset_log_bit_in_major_gc {
                    if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC {
                        // We zero all the log bits in major GC, and for every object we trace, we will mark the log bit again.
                        side.bzero_metadata(block.start(), Block::BYTES);
                    } else {
                        // If the log bit is not in side metadata, we cannot bulk zero. We can either
                        // clear the bit for dead objects in major GC, or clear the log bit for new
                        // objects. In either cases, we do not need to set log bit at tracing.
                        unimplemented!("We cannot bulk zero unlogged bit.")
                    }
                }
                // If the forwarding bits are on the side, we need to clear them, too.
                if let MetadataSpec::OnSide(side) = *VM::VMObjectModel::LOCAL_FORWARDING_BITS_SPEC {
                    side.bzero_metadata(block.start(), Block::BYTES);
                }
            }
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ClearZygoteMetadata<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        // Clear object mark and log tables for allocated blocks in this chunk
        self.reset_object_metadata();
    }
}
