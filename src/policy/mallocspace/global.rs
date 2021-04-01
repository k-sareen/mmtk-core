use crate::MMTK;
use super::metadata::*;
use crate::plan::TransitiveClosure;
use crate::policy::space::CommonSpace;
use crate::policy::space::SFT;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::conversions;
use crate::util::heap::layout::heap_layout::VMMap;
use crate::util::heap::PageResource;
use crate::util::malloc::*;
use crate::scheduler::*;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::util::OpaquePointer;
use crate::vm::VMBinding;
use crate::vm::{ActivePlan, Collection, ObjectModel};
use crate::{policy::space::Space, util::heap::layout::vm_layout_constants::BYTES_IN_CHUNK};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(feature = "chunk_hashset")]
use std::collections::HashSet;
use std::marker::PhantomData;
// only used for debugging
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(any(debug_assertions, not(feature = "chunk_hashset")))]
use std::sync::Mutex;

// If true, we will use a hashmap to store all the allocated memory from malloc, and use it
// to make sure our allocation is correct.
#[cfg(debug_assertions)]
const ASSERT_ALLOCATION: bool = false;

pub struct MallocSpace<VM: VMBinding> {
    phantom: PhantomData<VM>,
    active_bytes: AtomicUsize,
    // Mapping between allocated address and its size - this is used to check correctness.
    // Size will be set to zero when the memory is freed.
    #[cfg(debug_assertions)]
    active_mem: Mutex<HashMap<Address, usize>>,
    #[cfg(not(feature = "chunk_hashset"))]
    alloc_addr_min: Mutex<Address>,
    #[cfg(not(feature = "chunk_hashset"))]
    alloc_addr_max: Mutex<Address>,
}

impl<VM: VMBinding> SFT for MallocSpace<VM> {
    fn name(&self) -> &str {
        self.get_name()
    }

    fn is_live(&self, object: ObjectReference) -> bool {
        is_marked(object)
    }
    fn is_movable(&self) -> bool {
        false
    }
    #[cfg(feature = "sanity")]
    fn is_sane(&self) -> bool {
        true
    }
    fn initialize_header(&self, object: ObjectReference, _alloc: bool) {
        trace!("initialize_header for object {}", object);
        // let page_addr = conversions::page_align_down(object.to_address());
        // set_page_mark_bit(page_addr);
        set_alloc_bit(object);
    }
}

impl<VM: VMBinding> Space<VM> for MallocSpace<VM> {
    fn as_space(&self) -> &dyn Space<VM> {
        self
    }
    fn as_sft(&self) -> &(dyn SFT + Sync + 'static) {
        self
    }
    fn get_page_resource(&self) -> &dyn PageResource<VM> {
        unreachable!()
    }
    fn common(&self) -> &CommonSpace<VM> {
        unreachable!()
    }
    unsafe fn unsafe_common_mut(&self) -> &mut CommonSpace<VM> {
        unreachable!()
    }

    fn init(&mut self, _vm_map: &'static VMMap) {
        // Do nothing
    }

    fn release_multiple_pages(&mut self, _start: Address) {
        unreachable!()
    }

    fn in_space(&self, object: ObjectReference) -> bool {
        let ret = is_alloced_by_malloc(object);

        #[cfg(debug_assertions)]
        if ASSERT_ALLOCATION {
            let addr = VM::VMObjectModel::object_start_ref(object);
            let active_mem = self.active_mem.lock().unwrap();
            if ret {
                // The alloc bit tells that the object is in space.
                debug_assert!(
                    *active_mem.get(&addr).unwrap() != 0,
                    "active mem check failed for {} (object {}) - was freed",
                    addr,
                    object
                );
            } else {
                // The alloc bit tells that the object is not in space. It could never be allocated, or have been freed.
                debug_assert!(
                    (!active_mem.contains_key(&addr))
                        || (active_mem.contains_key(&addr) && *active_mem.get(&addr).unwrap() == 0),
                    "mem check failed for {} (object {}): allocated = {}, size = {:?}",
                    addr,
                    object,
                    active_mem.contains_key(&addr),
                    if active_mem.contains_key(&addr) {
                        active_mem.get(&addr)
                    } else {
                        None
                    }
                );
            }
        }
        ret
    }

    fn address_in_space(&self, _start: Address) -> bool {
        unreachable!("We do not know if an address is in malloc space. Use in_space() to check if an object is in malloc space.")
    }

    fn get_name(&self) -> &'static str {
        "MallocSpace"
    }

    fn reserved_pages(&self) -> usize {
        conversions::bytes_to_pages_up(self.active_bytes.load(Ordering::SeqCst))
    }

    unsafe fn release_all_chunks(&self) {
        #[cfg(feature = "chunk_hashset")]
        let mut released_chunks: HashSet<Address> = HashSet::new();

        // To sum up the total size of live objects. We check this against the active_bytes we maintain.
        #[cfg(debug_assertions)]
        let mut live_bytes = 0;

        debug!(
            "Used bytes before releasing: {}",
            self.active_bytes.load(Ordering::Relaxed)
        );

        #[cfg(feature = "chunk_hashset")]
        for chunk_start in ACTIVE_CHUNKS.read().unwrap().iter() {
            debug!("Check active chunk {:?}", chunk_start);
            let mut chunk_is_empty = true;
            let mut address = *chunk_start;
            let chunk_end = chunk_start.add(BYTES_IN_CHUNK);
            // let mut page = conversions::page_align_down(address);
            // let mut page_is_empty = true;

            // Linear scan through the chunk
            while address < chunk_end {
                trace!("Check address {}", address);

                // // we've crossed to the next page
                // if address - page >= BYTES_IN_PAGE {
                //     if page_is_empty {
                //         unset_page_mark_bit(page);
                //     }
                //     page = conversions::page_align_down(address);
                //     page_is_empty = true;
                // }

                // if is_page_marked(address) {
                    if is_alloced_object(address) {
                        // We know it is an object
                        let object = address.to_object_reference();
                        let obj_start = VM::VMObjectModel::object_start_ref(object);
                        let bytes = malloc_usable_size(obj_start.to_mut_ptr());

                        #[cfg(debug_assertions)]
                        if ASSERT_ALLOCATION {
                            debug_assert!(
                                self.active_mem.lock().unwrap().contains_key(&obj_start),
                                "Address {} with alloc bit is not in active_mem",
                                obj_start
                            );
                            debug_assert_eq!(
                                self.active_mem.lock().unwrap().get(&obj_start),
                                Some(&bytes),
                                "Address {} size in active_mem does not match the size from malloc_usable_size",
                                obj_start
                            );
                        }

                        if !is_marked(object) {
                            // Dead object
                            trace!(
                                "Object {} has alloc bit but no mark bit, it is dead. ",
                                object
                            );

                            // Free object
                            self.free(obj_start);
                            trace!("free object {}", object);
                            unset_alloc_bit(object);
                        } else {
                            // Live object. Unset mark bit
                            unset_mark_bit(object);
                            // This chunk and page are still active.
                            chunk_is_empty = false;
                            // page_is_empty = false;

                            #[cfg(debug_assertions)]
                            {
                                // Accumulate live bytes
                                live_bytes += bytes;
                            }
                        }

                        // Skip to next object
                        address += bytes;
                    } else { // not an object
                        address += VM::MIN_ALIGNMENT;
                    }
                // } else { // page is not marked; skip to next page
                //     address = page.add(BYTES_IN_PAGE);
                // }
            }

            if chunk_is_empty {
                debug!(
                    "Release malloc chunk {} to {}",
                    chunk_start,
                    *chunk_start + BYTES_IN_CHUNK
                );
                released_chunks.insert(*chunk_start);
            }
        }

        #[cfg(not(feature = "chunk_hashset"))]
        {
            let start = conversions::chunk_align_down(*self.alloc_addr_min.lock().unwrap());
            let end = conversions::chunk_align_up(*self.alloc_addr_max.lock().unwrap());
            let mut address = start;
            let mut chunk = start;
            let mut chunk_is_empty = true;
            // let mut page = conversions::page_align_down(address);
            // let mut page_is_empty = true;

            while address < end {
                // // we've crossed to the next page
                // if address - page >= BYTES_IN_PAGE {
                //     if is_chunk_marked(chunk) && page_is_empty {
                //         unset_page_mark_bit(page);
                //     }
                //     page = conversions::page_align_down(address);
                //     page_is_empty = true;
                // }

                // we've crossed to the next chunk
                if address - chunk >= BYTES_IN_CHUNK {
                    if chunk_is_empty {
                        unset_chunk_mark_bit(chunk);
                    }
                    chunk = conversions::chunk_align_down(address);
                    chunk_is_empty = true;
                }

                // only perform the linear scan if page and chunk are marked active
                if is_chunk_marked(chunk) {
                    // if is_page_marked(page) {
                        if is_alloced_object(address) {
                            // We know it is an object
                            let object = address.to_object_reference();
                            let obj_start = VM::VMObjectModel::object_start_ref(object);
                            let bytes = malloc_usable_size(obj_start.to_mut_ptr());

                            #[cfg(debug_assertions)]
                            if ASSERT_ALLOCATION {
                                debug_assert!(
                                    self.active_mem.lock().unwrap().contains_key(&obj_start),
                                    "Address {} with alloc bit is not in active_mem",
                                    obj_start
                                );
                                debug_assert_eq!(
                                    self.active_mem.lock().unwrap().get(&obj_start),
                                    Some(&bytes),
                                    "Address {} size in active_mem does not match the size from malloc_usable_size",
                                    obj_start
                                );
                            }

                            if !is_marked(object) {
                                // Dead object
                                trace!(
                                    "Object {} has alloc bit but no mark bit, it is dead. ",
                                    object
                                );

                                // Free object
                                self.free(obj_start);
                                trace!("free object {}", object);
                                unset_alloc_bit(object);
                            } else {
                                // Live object. Unset mark bit
                                unset_mark_bit(object);
                                // This chunk and page are still active.
                                chunk_is_empty = false;
                                // page_is_empty = false;

                                #[cfg(debug_assertions)]
                                {
                                    // Accumulate live bytes
                                    live_bytes += bytes;
                                }
                            }

                            // Skip to next object
                            address += bytes;
                        } else { // not an object
                            address += VM::MIN_ALIGNMENT;
                        }
                    // } else { // page is not marked; skip to next page
                    //     address = page.add(BYTES_IN_PAGE);
                    // }
                } else { // chunk is not marked; skip to next chunk
                    address = chunk.add(BYTES_IN_CHUNK);
                }
            }
        }

        debug!(
            "Used bytes after releasing: {}",
            self.active_bytes.load(Ordering::SeqCst)
        );
        #[cfg(debug_assertions)]
        debug_assert_eq!(live_bytes, self.active_bytes.load(Ordering::SeqCst));

        #[cfg(feature = "chunk_hashset")]
        ACTIVE_CHUNKS.write().unwrap().retain(|c| {
            debug!("Release malloc chunk {} to {}", *c, *c + BYTES_IN_CHUNK);
            !released_chunks.contains(&*c)
        });
    }
}

// Address is used for a chunk
#[cfg(not(feature = "sweep_seq"))]
pub struct MSSweepChunk<VM: VMBinding>(pub &'static MallocSpace<VM>, pub Address);

#[cfg(not(feature = "sweep_seq"))]
impl<VM: VMBinding> GCWork<VM> for MSSweepChunk<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, _mmtk: &'static MMTK<VM>) {
        &self.0.sweep_chunk(self.1);
    }
}

#[cfg(not(feature = "sweep_seq"))]
pub struct MSSweepChunks<VM: VMBinding>(pub &'static MallocSpace<VM>);

#[cfg(not(feature = "sweep_seq"))]
impl<VM: VMBinding> MSSweepChunks<VM> {
    pub fn new(ms: &'static MallocSpace<VM>) -> Self {
        Self(ms)
    }
}

#[cfg(not(feature = "sweep_seq"))]
impl<VM: VMBinding> GCWork<VM> for MSSweepChunks<VM> {
    #[inline]
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let mut work_packets: Vec<Box<dyn Work<MMTK<VM>>>> = vec![];
        #[cfg(feature = "chunk_hashset")]
        {
            for chunk_start in ACTIVE_CHUNKS.read().unwrap().iter() {
                work_packets.push(box MSSweepChunk(self.0, *chunk_start));
            }

        }

        #[cfg(not(feature = "chunk_hashset"))]
        {
            let mut address = conversions::chunk_align_down(*self.0.alloc_addr_min.lock().unwrap());
            let end = conversions::chunk_align_up(*self.0.alloc_addr_max.lock().unwrap());
            let mut chunk = address;

            while address < end {
                if is_chunk_marked(chunk) {
                    work_packets.push(box MSSweepChunk(self.0, chunk));
                }

                address += BYTES_IN_CHUNK;
                chunk = conversions::chunk_align_down(address);
            }
        }

        info!("Generated {} work packets", work_packets.len());
        mmtk.scheduler.work_buckets[WorkBucketStage::Release].bulk_add(GCWorkBucket::<VM>::DEFAULT_PRIORITY, work_packets);
    }
}

impl<VM: VMBinding> MallocSpace<VM> {
    pub fn new() -> Self {
        MallocSpace {
            phantom: PhantomData,
            active_bytes: AtomicUsize::new(0),
            #[cfg(debug_assertions)]
            active_mem: Mutex::new(HashMap::new()),
            #[cfg(not(feature = "chunk_hashset"))]
            alloc_addr_min: Mutex::new(unsafe { Address::max() }),
            #[cfg(not(feature = "chunk_hashset"))]
            alloc_addr_max: Mutex::new(unsafe { Address::zero() }),
        }
    }

    #[cfg(not(feature = "sweep_seq"))]
    pub fn release(&'static self, mmtk: &'static MMTK<VM>) {
        let mut work_packets: Vec<Box<dyn Work<MMTK<VM>>>> = vec![];
        #[cfg(feature = "chunk_hashset")]
        {
            for chunk_start in ACTIVE_CHUNKS.read().unwrap().iter() {
                work_packets.push(box MSSweepChunk(self, *chunk_start));
            }

        }

        #[cfg(not(feature = "chunk_hashset"))]
        {
            let mut address = conversions::chunk_align_down(*self.alloc_addr_min.lock().unwrap());
            let end = conversions::chunk_align_up(*self.alloc_addr_max.lock().unwrap());
            let mut chunk = address;

            while address < end {
                if is_chunk_marked(chunk) {
                    work_packets.push(box MSSweepChunk(self, chunk));
                }

                address += BYTES_IN_CHUNK;
                chunk = conversions::chunk_align_down(address);
            }
        }

        info!("Generated {} work packets", work_packets.len());
        mmtk.scheduler.work_buckets[WorkBucketStage::Release].bulk_add(GCWorkBucket::<VM>::DEFAULT_PRIORITY, work_packets);
    }

    #[cfg(not(feature = "sweep_seq"))]
    pub fn sweep_chunk(&self, chunk_start: Address) {
        #[cfg(debug_assertions)]
        let mut live_bytes = 0;

        #[cfg(feature = "chunk_hashset")]
        {
            debug!("Check active chunk {:?}", chunk_start);
            let mut chunk_is_empty = true;
            let mut address = chunk_start;
            let chunk_end = chunk_start.add(BYTES_IN_CHUNK);
            // let mut page = conversions::page_align_down(address);
            // let mut page_is_empty = true;

            // Linear scan through the chunk
            while address < chunk_end {
                trace!("Check address {}", address);

                // // we've crossed to the next page
                // if address - page >= BYTES_IN_PAGE {
                //     if page_is_empty {
                //         unset_page_mark_bit(page);
                //     }
                //     page = conversions::page_align_down(address);
                //     page_is_empty = true;
                // }

                // if is_page_marked(address) {
                    if is_alloced_object(address) {
                        // We know it is an object
                        let object = unsafe { address.to_object_reference() };
                        let obj_start = VM::VMObjectModel::object_start_ref(object);
                        let bytes = unsafe { malloc_usable_size(obj_start.to_mut_ptr()) };

                        #[cfg(debug_assertions)]
                        if ASSERT_ALLOCATION {
                            debug_assert!(
                                self.active_mem.lock().unwrap().contains_key(&obj_start),
                                "Address {} with alloc bit is not in active_mem",
                                obj_start
                            );
                            debug_assert_eq!(
                                self.active_mem.lock().unwrap().get(&obj_start),
                                Some(&bytes),
                                "Address {} size in active_mem does not match the size from malloc_usable_size",
                                obj_start
                            );
                        }

                        if !is_marked(object) {
                            // Dead object
                            trace!(
                                "Object {} has alloc bit but no mark bit, it is dead. ",
                                object
                            );

                            // Free object
                            self.free(obj_start);
                            trace!("free object {}", object);
                            unset_alloc_bit(object);
                        } else {
                            // Live object. Unset mark bit
                            unset_mark_bit(object);
                            // This chunk and page are still active.
                            chunk_is_empty = false;
                            // page_is_empty = false;

                            #[cfg(debug_assertions)]
                            {
                                // Accumulate live bytes
                                live_bytes += bytes;
                            }
                        }

                        // Skip to next object
                        address += bytes;
                    } else { // not an object
                        address += VM::MIN_ALIGNMENT;
                    }
                // } else { // page is not marked; skip to next page
                //     address = page.add(BYTES_IN_PAGE);
                // }
            }

            if chunk_is_empty {
                ACTIVE_CHUNKS.write().unwrap().remove(&chunk_start);
            }
        }

        #[cfg(not(feature = "chunk_hashset"))]
        {
            let mut address = chunk_start;
            let chunk_end = chunk_start + BYTES_IN_CHUNK;
            let mut chunk_is_empty = true;
            // let mut page = conversions::page_align_down(address);
            // let mut page_is_empty = true;

            while address < chunk_end {
                // // we've crossed to the next page
                // if address - page >= BYTES_IN_PAGE {
                //     if page_is_empty {
                //         unset_page_mark_bit(page);
                //     }
                //     page = conversions::page_align_down(address);
                //     page_is_empty = true;
                // }

                // only perform the linear scan if page and chunk are marked active
                // if is_page_marked(page) {
                    if is_alloced_object(address) {
                        // We know it is an object
                        let object = unsafe { address.to_object_reference() };
                        let obj_start = VM::VMObjectModel::object_start_ref(object);
                        let bytes = unsafe { malloc_usable_size(obj_start.to_mut_ptr()) };

                        #[cfg(debug_assertions)]
                        if ASSERT_ALLOCATION {
                            debug_assert!(
                                self.active_mem.lock().unwrap().contains_key(&obj_start),
                                "Address {} with alloc bit is not in active_mem",
                                obj_start
                            );
                            debug_assert_eq!(
                                self.active_mem.lock().unwrap().get(&obj_start),
                                Some(&bytes),
                                "Address {} size in active_mem does not match the size from malloc_usable_size",
                                obj_start
                            );
                        }

                        if !is_marked(object) {
                            // Dead object
                            trace!(
                                "Object {} has alloc bit but no mark bit, it is dead. ",
                                object
                            );

                            // Free object
                            self.free(obj_start);
                            trace!("free object {}", object);
                            unset_alloc_bit(object);
                        } else {
                            // Live object. Unset mark bit
                            unset_mark_bit(object);
                            // This chunk and page are still active.
                            chunk_is_empty = false;
                            // page_is_empty = false;

                            #[cfg(debug_assertions)]
                            {
                                // Accumulate live bytes
                                live_bytes += bytes;
                            }
                        }

                        // Skip to next object
                        address += bytes;
                    } else { // not an object
                        address += VM::MIN_ALIGNMENT;
                    }
                // } else { // page is not marked; skip to next page
                //     address = page.add(BYTES_IN_PAGE);
                // }
            }

            if chunk_is_empty {
                unset_chunk_mark_bit(chunk_start);
            }
        }

        debug!(
            "Used bytes after releasing: {}",
            self.active_bytes.load(Ordering::SeqCst)
        );
    }

    pub fn alloc(&self, tls: OpaquePointer, size: usize) -> Address {
        // TODO: Should refactor this and Space.acquire()
        if VM::VMActivePlan::global().poll(false, self) {
            VM::VMCollection::block_for_gc(tls);
            return unsafe { Address::zero() };
        }

        let raw = unsafe { calloc(1, size) };
        let address = Address::from_mut_ptr(raw);

        if !address.is_zero() {
            let actual_size = unsafe { malloc_usable_size(raw) };
            if !is_meta_space_mapped(address) {
                let chunk_start = conversions::chunk_align_down(address);
                debug!(
                    "Add malloc chunk {} to {}",
                    chunk_start,
                    chunk_start + BYTES_IN_CHUNK
                );
                map_meta_space_for_chunk(chunk_start);
            }
            self.active_bytes.fetch_add(actual_size, Ordering::SeqCst);

            #[cfg(debug_assertions)]
            if ASSERT_ALLOCATION {
                debug_assert!(actual_size != 0);
                self.active_mem.lock().unwrap().insert(address, actual_size);
            }
        }

        #[cfg(not(feature = "chunk_hashset"))]
        {
            let mut min = self.alloc_addr_min.lock().unwrap();
            let mut max = self.alloc_addr_max.lock().unwrap();

            if address < *min {
                *min = address;
            } else if address > *max {
                *max = address;
            }
        }

        address
    }

    pub fn free(&self, addr: Address) {
        let ptr = addr.to_mut_ptr();
        let bytes = unsafe { malloc_usable_size(ptr) };
        trace!("Free memory {:?}", ptr);
        unsafe {
            free(ptr);
        }
        self.active_bytes.fetch_sub(bytes, Ordering::SeqCst);

        #[cfg(debug_assertions)]
        if ASSERT_ALLOCATION {
            self.active_mem.lock().unwrap().insert(addr, 0).unwrap();
        }
    }

    #[inline]
    pub fn trace_object<T: TransitiveClosure>(
        &self,
        trace: &mut T,
        object: ObjectReference,
    ) -> ObjectReference {
        if object.is_null() {
            return object;
        }
        let address = object.to_address();
        // let page_addr = conversions::page_align_down(address);
        #[cfg(not(feature = "chunk_hashset"))]
        let chunk_start = conversions::chunk_align_down(address);
        assert!(
            self.in_space(object),
            "Cannot mark an object {} that was not alloced by malloc.",
            address,
        );
        if !is_marked(object) {
            set_mark_bit(object);
            trace.process_node(object);
        }

        // set_page_mark_bit(page_addr);
        #[cfg(not(feature = "chunk_hashset"))]
        set_chunk_mark_bit(chunk_start);
        object
    }
}

impl<VM: VMBinding> Default for MallocSpace<VM> {
    fn default() -> Self {
        Self::new()
    }
}
