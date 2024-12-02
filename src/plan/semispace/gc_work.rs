use super::global::SemiSpace;
use crate::policy::gc_work::TraceKind;
use crate::scheduler::gc_work::{PlanProcessEdges, UnsupportedProcessEdges};
use crate::vm::VMBinding;

pub struct SSGCWorkContext<VM: VMBinding, const KIND: TraceKind>(std::marker::PhantomData<VM>);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext for SSGCWorkContext<VM, KIND> {
    type VM = VM;
    type PlanType = SemiSpace<VM>;
    type DefaultProcessEdges = PlanProcessEdges<Self::VM, SemiSpace<VM>, KIND>;
    type PinningProcessEdges = UnsupportedProcessEdges<VM>;
}
