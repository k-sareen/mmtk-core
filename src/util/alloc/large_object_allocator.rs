use std::sync::Arc;

use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::util::alloc::{allocator, Allocator};
use crate::util::opaque_pointer::*;
use crate::util::Address;
use crate::vm::VMBinding;

use super::allocator::AllocatorContext;

/// An allocator that only allocates at page granularity.
/// This is intended for large objects.
#[repr(C)]
pub struct LargeObjectAllocator<VM: VMBinding> {
    /// [`VMThread`] associated with this allocator instance
    pub tls: VMThread,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static LargeObjectSpace<VM>,
    context: Arc<AllocatorContext<VM>>,
}

impl<VM: VMBinding> Allocator<VM> for LargeObjectAllocator<VM> {
    fn get_tls(&self) -> VMThread {
        self.tls
    }

    fn get_context(&self) -> &AllocatorContext<VM> {
        &self.context
    }

    fn get_space(&self) -> &'static dyn Space<VM> {
        // Casting the interior of the Option: from &LargeObjectSpace to &dyn Space
        self.space as &'static dyn Space<VM>
    }

    fn does_thread_local_allocation(&self) -> bool {
        false
    }

    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        let cell: Address = self.alloc_slow(size, align, offset);
        // We may get a null ptr from alloc due to the VM being OOM
        if !cell.is_zero() {
            #[cfg(feature = "measure_large_object_alloc")]
            self.get_context().state.num_large_object_alloc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            allocator::align_allocation::<VM>(cell, align, offset)
        } else {
            cell
        }
    }

    fn alloc_slow_once(&mut self, size: usize, align: usize, _offset: usize) -> Address {
        #[cfg(feature = "measure_large_object_alloc")]
        let start_time = std::time::Instant::now();
        if self.space.will_oom_on_acquire(self.tls, size) {
            return Address::ZERO;
        }

        let maxbytes = allocator::get_maximum_aligned_size::<VM>(size, align);
        let pages = crate::util::conversions::bytes_to_pages_up(maxbytes);
        #[cfg(feature = "measure_large_object_alloc")]
        {
            let rtn = self.space.allocate_pages(self.tls, pages);
            let end_time = std::time::Instant::now();
            if !rtn.is_zero() {
                let alloc_time = end_time.duration_since(start_time).as_nanos() as u64;
                self.get_context().state.time_large_object_alloc_ns.fetch_add(alloc_time, std::sync::atomic::Ordering::SeqCst);
            }
            return rtn;
        }
        self.space.allocate_pages(self.tls, pages)
    }
}

impl<VM: VMBinding> LargeObjectAllocator<VM> {
    pub(crate) fn new(
        tls: VMThread,
        space: &'static LargeObjectSpace<VM>,
        context: Arc<AllocatorContext<VM>>,
    ) -> Self {
        LargeObjectAllocator {
            tls,
            space,
            context,
        }
    }
}
