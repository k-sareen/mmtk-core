use crate::plan::Plan;
use crate::scheduler::gc_works::*;
use crate::scheduler::*;
use crate::util::{Address, ObjectReference};
use crate::vm::*;
use crate::MMTK;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

#[allow(dead_code)]
pub struct SanityChecker {
    refs: HashSet<ObjectReference>,
    ref_count: usize,
    null_ref_count: usize,
    live_object_count: usize,
}

impl Default for SanityChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl SanityChecker {
    pub fn new() -> Self {
        Self {
            refs: HashSet::new(),
            ref_count: 0,
            null_ref_count: 0,
            live_object_count: 0,
        }
    }
}

#[derive(Default)]
pub struct ScheduleSanityGC;

impl<VM: VMBinding> GCWork<VM> for ScheduleSanityGC {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        worker.scheduler().reset_state();
        mmtk.plan.schedule_sanity_collection(worker.scheduler());
    }
}

pub struct SanityPrepare<P: Plan> {
    pub plan: &'static P,
}

unsafe impl<P: Plan> Sync for SanityPrepare<P> {}

impl<P: Plan> SanityPrepare<P> {
    pub fn new(plan: &'static P) -> Self {
        Self { plan }
    }
}

impl<P: Plan> GCWork<P::VM> for SanityPrepare<P> {
    fn do_work(&mut self, _worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        {
            let mut sanity_checker = mmtk.sanity_checker.lock().unwrap();
            sanity_checker.refs.clear();
            sanity_checker.ref_count = 0;
            sanity_checker.null_ref_count = 0;
            sanity_checker.live_object_count = 0;
        }
        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler
                .prepare_stage
                .add(PrepareMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_works.add(PrepareCollector::default());
        }
    }
}

pub struct SanityCheck<P: Plan> {
    pub plan: &'static P,
}

unsafe impl<P: Plan> Sync for SanityCheck<P> {}

impl<P: Plan> SanityCheck<P> {
    pub fn new(plan: &'static P) -> Self {
        Self { plan }
    }
}

impl<P: Plan> GCWork<P::VM> for SanityCheck<P> {
    fn do_work(&mut self, _worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        let mut err_found = false;
        let sanity_checker = mmtk.sanity_checker.lock().unwrap();
        for object in &sanity_checker.refs {
            if !object.is_sane() {
                err_found = true;
                error!("Invalid reference: object {:?}", object);
            }
        }

        if err_found {
            panic!("Invalid references found!");
        }
    }
}

pub struct SanityRelease<P: Plan> {
    pub plan: &'static P,
}

unsafe impl<P: Plan> Sync for SanityRelease<P> {}

impl<P: Plan> SanityRelease<P> {
    pub fn new(plan: &'static P) -> Self {
        Self { plan }
    }
}

impl<P: Plan> GCWork<P::VM> for SanityRelease<P> {
    fn do_work(&mut self, _worker: &mut GCWorker<P::VM>, mmtk: &'static MMTK<P::VM>) {
        {
            let sanity_checker = mmtk.sanity_checker.lock().unwrap();
            info!(
                "sanity_checker: live_object_count = {}, ref_count = {}, null_ref_count = {}",
                sanity_checker.live_object_count,
                sanity_checker.ref_count,
                sanity_checker.null_ref_count
            );
        }
        for mutator in <P::VM as VMBinding>::VMActivePlan::mutators() {
            mmtk.scheduler
                .release_stage
                .add(ReleaseMutator::<P::VM>::new(mutator));
        }
        for w in &mmtk.scheduler.worker_group().workers {
            w.local_works.add(ReleaseCollector::default());
        }
    }
}

#[derive(Default)]
pub struct SanityGCProcessEdges<VM: VMBinding> {
    base: ProcessEdgesBase<SanityGCProcessEdges<VM>>,
    phantom: PhantomData<VM>,
}

impl<VM: VMBinding> Deref for SanityGCProcessEdges<VM> {
    type Target = ProcessEdgesBase<Self>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for SanityGCProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<VM: VMBinding> ProcessEdgesWork for SanityGCProcessEdges<VM> {
    type VM = VM;
    const OVERWRITE_REFERENCE: bool = false;
    fn new(edges: Vec<Address>, _roots: bool) -> Self {
        Self {
            base: ProcessEdgesBase::new(edges),
            ..Default::default()
        }
    }

    #[inline]
    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        let mut sanity_checker = self.mmtk().sanity_checker.lock().unwrap();
        sanity_checker.ref_count += 1;
        if object.is_null() {
            sanity_checker.null_ref_count += 1;
            return object;
        }
        if !sanity_checker.refs.contains(&object) {
            // FIXME steveb consider VM-specific integrity check on reference.
            // if !object.is_sane() {
            //     panic!("Invalid reference {:?}", object);
            // }
            sanity_checker.live_object_count += 1;
            // Object is not "marked"
            sanity_checker.refs.insert(object); // "Mark" it
            ProcessEdgesWork::process_node(self, object);
        }
        object
    }
}
