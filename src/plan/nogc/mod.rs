//! Plan: nogc (allocation-only)

#[cfg(feature = "nogc_trace")]
pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::NoGC;
pub use self::global::NOGC_CONSTRAINTS;
