use super::layout::VMMap;
use super::pageresource::{PRAllocFail, PRAllocResult};
use super::PageResource;
use crate::policy::space::Space;
use crate::util::address::Address;
use crate::util::constants::BYTES_IN_PAGE;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::heap::pageresource::CommonPageResource;
use crate::util::heap::space_descriptor::SpaceDescriptor;
use crate::util::opaque_pointer::*;
use crate::vm::VMBinding;

use std::marker::PhantomData;
use std::sync::{RwLock, RwLockReadGuard};

/// A special page resource that records some external pages that are not mmapped by us,
/// but are used by our space (namely VM space). Unlike other page resources, we cannot
/// allocate from this page resource.
pub struct ExternalPageResource<VM: VMBinding> {
    common: CommonPageResource,
    ranges: RwLock<Vec<ExternalPages>>,
    _p: PhantomData<VM>,
}

#[derive(Copy, Clone, Debug)]
pub struct ExternalPages {
    pub start: Address,
    pub end: Address,
}

impl<VM: VMBinding> PageResource<VM> for ExternalPageResource<VM> {
    fn common(&self) -> &CommonPageResource {
        &self.common
    }

    fn common_mut(&mut self) -> &mut CommonPageResource {
        &mut self.common
    }

    fn reserve_pages(&self, _pages: usize) -> usize {
        unreachable!()
    }
    fn commit_pages(&self, _reserved_pages: usize, _actual_pages: usize, _tls: VMThread) {
        unreachable!()
    }

    fn get_available_physical_pages(&self) -> usize {
        0
    }

    fn alloc_pages(
        &self,
        _space: &dyn Space<VM>,
        _reserved_pages: usize,
        _required_pages: usize,
        _tls: VMThread,
    ) -> Result<PRAllocResult, PRAllocFail> {
        panic!("Cannot allocate from ExternalPageResource")
    }
}

impl<VM: VMBinding> ExternalPageResource<VM> {
    pub fn new(vm_map: &'static dyn VMMap) -> Self {
        Self {
            common: CommonPageResource::new(false, false, vm_map),
            ranges: RwLock::new(vec![]),
            _p: PhantomData,
        }
    }

    pub fn add_external_pages(&self, pages: ExternalPages) {
        assert!(pages.start.is_aligned_to(BYTES_IN_PAGE));
        assert!(pages.end.is_aligned_to(BYTES_IN_PAGE));

        let mut lock = self.ranges.write().unwrap();
        let n_pages = (pages.end - pages.start) >> LOG_BYTES_IN_PAGE;
        self.common.accounting.reserve_and_commit(n_pages);
        lock.push(pages);
    }

    /// Remove the record of external pages. This does not unmap the pages, as they are not
    /// mapped by us. Returns true if the record is found and removed, false otherwise.
    pub fn remove_external_pages(&self, pages: ExternalPages) -> bool {
        assert!(pages.start.is_aligned_to(BYTES_IN_PAGE));
        assert!(pages.end.is_aligned_to(BYTES_IN_PAGE));

        let mut lock = self.ranges.write().unwrap();
        let n_pages = (pages.end - pages.start) >> LOG_BYTES_IN_PAGE;
        self.common.accounting.release(n_pages);
        let index = lock
            .iter()
            .position(|&p| p.start == pages.start && p.end == pages.end);
        if let Some(idx) = index {
            lock.remove(idx);
            true
        } else {
            warn!("Failed in trying to remove external pages: {:?}", pages);
            false
        }
    }

    pub fn get_external_pages(&self) -> RwLockReadGuard<Vec<ExternalPages>> {
        self.ranges.read().unwrap()
    }
}
