// Export one of the malloc libraries.

#[cfg(feature = "malloc_hoard")]
pub use self::hoard::*;
#[cfg(feature = "malloc_jemalloc")]
pub use self::jemalloc::*;
#[cfg(not(any(
    feature = "malloc_jemalloc",
    feature = "malloc_mimalloc",
    feature = "malloc_tcmalloc",
    feature = "malloc_hoard",
)))]
pub use self::libc_malloc::*;
#[cfg(feature = "malloc_mimalloc")]
pub use self::mimalloc::*;
#[cfg(feature = "malloc_tcmalloc")]
pub use self::tcmalloc::*;

// Different malloc libraries

// TODO: We should conditinally include some methods in the module, such as posix extension and GNU extension.

#[cfg(feature = "malloc_jemalloc")]
mod jemalloc {
    // ANSI C
    pub use jemalloc_sys::{calloc, free, malloc, realloc};
    // Posix
    pub use jemalloc_sys::posix_memalign;
    // GNU
    pub use jemalloc_sys::malloc_usable_size;
}

#[cfg(feature = "malloc_mimalloc")]
mod mimalloc {
    // ANSI C
    pub use mimalloc_sys::{
        mi_calloc as calloc, mi_free as free, mi_malloc as malloc, mi_realloc as realloc,
    };
    // Posix
    pub use mimalloc_sys::mi_posix_memalign as posix_memalign;
    // GNU
    pub use mimalloc_sys::mi_malloc_usable_size as malloc_usable_size;
}

#[cfg(feature = "malloc_hoard")]
mod hoard {
    // ANSI C
    pub use hoard_sys::{calloc, free, malloc, realloc};
    // Posix
    pub use hoard_sys::posix_memalign;
    // GNU
    pub use hoard_sys::malloc_usable_size;
}

#[cfg(feature = "malloc_tcmalloc")]
mod tcmalloc {
    mod internal {
        use std::os::raw::{c_int, c_void};
        #[link(name = "tcmalloc")]
        extern "C" {
            pub fn tc_malloc(size: usize) -> *mut c_void;
            pub fn tc_calloc(nmemb: usize, size: usize) -> *mut c_void;
            pub fn tc_realloc(ptr: *mut c_void, newsize: usize) -> *mut c_void;
            pub fn tc_posix_memalign(ptr: *mut *mut c_void, alignment: usize, size: usize) -> c_int;
            pub fn tc_free(ptr: *mut c_void);
            pub fn tc_malloc_size(ptr: *mut c_void) -> usize;
        }
    }

    // ANSI C
    pub use self::internal::{
        tc_calloc as calloc, tc_free as free, tc_malloc as malloc, tc_realloc as realloc,
    };
    // Posix
    pub use self::internal::tc_posix_memalign as posix_memalign;
    // GNU
    pub use self::internal::tc_malloc_size as malloc_usable_size;
}

/// If no malloc lib is specified, use the libc implementation
#[cfg(not(any(
    feature = "malloc_jemalloc",
    feature = "malloc_mimalloc",
    feature = "malloc_tcmalloc",
    feature = "malloc_hoard",
)))]
mod libc_malloc {
    // ANSI C
    pub use libc::{calloc, free, malloc, realloc};
    // Posix
    pub use libc::posix_memalign;
    // GNU
    pub use libc::malloc_usable_size;
}
