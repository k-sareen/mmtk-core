use super::global::NoGC;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::{PlanProcessEdges, UnsupportedProcessEdges};
use crate::vm::VMBinding;

pub struct NoGCWorkContext<VM: VMBinding, const KIND: TraceKind>(std::marker::PhantomData<VM>);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext for NoGCWorkContext<VM, KIND> {
    type VM = VM;
    type PlanType = NoGC<VM>;
    #[cfg(feature = "single_worker")]
    type STPlanType = NoGC<VM>;
    type DefaultProcessEdges = PlanProcessEdges<Self::VM, NoGC<VM>, KIND>;
    type PinningProcessEdges = UnsupportedProcessEdges<VM>;
}
