//! GC algorithms from the MMTk suite.
//!
//! This module provides various GC plans, each of which implements a GC algorithm.
//! Generally a plan consists of a few parts:
//! * A plan type that implements the [`Plan`] trait, which defines
//!   spaces used in the plan, and their behaviors in GC and page accounting.
//! * A mutator definition, which describes the mapping between allocators and allocation semantics,
//!   and the mapping between allocators and spaces. If the plan needs barrier, the barrier definition is
//!   also included here.
//! * A constant for [`PlanConstraints`], which defines
//!   plan-specific constants.
//! * Plan-specific [`GCWork`](crate::scheduler::GCWork), which is scheduled during GC.
//!
//! For more about implementing a plan, it is recommended to read the [MMTk tutorial](/docs/tutorial/Tutorial.md).

mod barriers;
pub use barriers::BarrierSelector;

pub(crate) mod gc_requester;

mod global;
pub(crate) use global::create_gc_worker_context;
pub(crate) use global::create_mutator;
pub(crate) use global::create_plan;
pub use global::AllocationSemantics;
pub(crate) use global::CreateGeneralPlanArgs;
pub(crate) use global::HasSpaces;
pub use global::Plan;
pub(crate) use global::PlanTraceObject;

mod mutator_context;
pub use mutator_context::Mutator;
pub use mutator_context::MutatorContext;

mod plan_constraints;
pub use plan_constraints::PlanConstraints;
pub(crate) use plan_constraints::DEFAULT_PLAN_CONSTRAINTS;

mod tracing;
pub use tracing::{ObjectQueue, ObjectsClosure, VectorObjectQueue, VectorQueue};

/// Generational plans (with a copying nursery)
mod generational;
/// Sticky plans (using sticky marks for generational behaviors without a copying nursery)
#[cfg(not(feature = "specialization"))]
mod sticky;

#[cfg(not(feature = "specialization"))]
mod immix;
#[cfg(not(feature = "specialization"))]
mod markcompact;
#[cfg(not(feature = "specialization"))]
mod marksweep;
#[cfg(not(feature = "specialization"))]
mod nogc;
#[cfg(not(feature = "specialization"))]
mod pageprotect;
#[cfg(not(feature = "specialization"))]
mod semispace;

pub(crate) use generational::global::is_nursery_gc;
pub(crate) use generational::global::GenerationalPlan;

// Expose plan constraints as public. Though a binding can get them from plan.constraints(),
// it is possible for performance reasons that they want the constraints as constants.

pub use generational::copying::GENCOPY_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use generational::immix::GENIMMIX_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use immix::IMMIX_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use markcompact::MARKCOMPACT_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use marksweep::MS_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use nogc::NOGC_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use pageprotect::PP_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use semispace::SS_CONSTRAINTS;
#[cfg(not(feature = "specialization"))]
pub use sticky::immix::STICKY_IMMIX_CONSTRAINTS;
