use crate::plan::generational::global::GenerationalPlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::immix;
use crate::plan::PlanConstraints;
use crate::policy::gc_work::PolicyTraceObject;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::DEFAULT_TRACE;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::policy::immix::ImmixSpace;
use crate::policy::immix::PREFER_COPY_ON_NURSERY_GC;
use crate::policy::immix::TRACE_KIND_FAST;
use crate::policy::sft::SFT;
use crate::policy::space::Space;
use crate::scheduler::GCWorker;
use crate::util::copy::CopyConfig;
use crate::util::copy::CopySelector;
use crate::util::copy::CopySemantics;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::rust_util::{likely, unlikely};
use crate::util::statistics::counter::EventCounter;
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::Plan;

use atomic::Ordering;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use mmtk_macros::{HasSpaces, PlanTraceObject};

use super::gc_work::StickyImmixMatureGCWorkContext;
use super::gc_work::StickyImmixNurseryGCWorkContext;

#[derive(HasSpaces, PlanTraceObject)]
pub struct StickyImmix<VM: VMBinding> {
    #[parent]
    immix: immix::Immix<VM>,
    gc_full_heap: AtomicBool,
    next_gc_full_heap: AtomicBool,
    full_heap_gc_count: Arc<Mutex<EventCounter>>,
}

/// The plan constraints for the sticky immix plan.
pub const STICKY_IMMIX_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: crate::policy::immix::DEFRAG || crate::policy::immix::PREFER_COPY_ON_NURSERY_GC,
    needs_log_bit: true,
    barrier: crate::plan::BarrierSelector::ObjectBarrier,
    // We may trace duplicate edges in sticky immix (or any plan that uses object remembering barrier). See https://github.com/mmtk/mmtk-core/issues/743.
    may_trace_duplicate_edges: true,
    ..immix::IMMIX_CONSTRAINTS
};

impl<VM: VMBinding> Plan for StickyImmix<VM> {
    fn constraints(&self) -> &'static crate::plan::PlanConstraints {
        &STICKY_IMMIX_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::Immix(0),
                _ => CopySelector::Unused,
            },
            space_mapping: {
                let mut vec: Vec<(CopySelector, &'static (dyn Space<VM> + 'static))> = vec![];
                if likely(self.common().has_zygote_space() || !self.common().is_zygote_process()) {
                    vec.push((CopySelector::Immix(0), self.get_immix_space()));
                } else {
                    // We are the Zygote process and we have not created the ZygoteSpace yet so use
                    // the ImmixSpace inside the ZygoteSpace
                    vec.push((
                        CopySelector::Immix(0),
                        self.common().get_zygote().get_immix_space(),
                    ));
                }
                vec
            },
            constraints: &STICKY_IMMIX_CONSTRAINTS,
        }
    }

    fn base(&self) -> &crate::plan::global::BasePlan<Self::VM> {
        self.immix.base()
    }

    fn base_mut(&mut self) -> &mut crate::plan::global::BasePlan<Self::VM> {
        self.immix.base_mut()
    }

    fn prepare_worker(&self, worker: &mut GCWorker<Self::VM>) {
        if unlikely(self.common().is_zygote()) {
            // We are the Zygote process and we have not created the ZygoteSpace yet so use the
            // ImmixSpace inside the ZygoteSpace
            unsafe { worker.get_copy_context_mut().immix[0].assume_init_mut() }
                .rebind(self.common().get_zygote().get_immix_space());
        } else {
            // Either the runtime has a Zygote space or it is a command-line runtime
            debug_assert!(
                self.common().has_zygote_space()
                    || (!self.common().is_zygote_process()
                        && !*self.common().base.options.is_zygote_process)
            );
            unsafe { worker.get_copy_context_mut().immix[0].assume_init_mut() }
                .rebind(self.get_immix_space());
        }
    }

    fn generational(
        &self,
    ) -> Option<&dyn crate::plan::generational::global::GenerationalPlan<VM = Self::VM>> {
        Some(self)
    }

    fn common(&self) -> &CommonPlan<Self::VM> {
        self.immix.common()
    }

    fn schedule_collection(&'static self, scheduler: &crate::scheduler::GCWorkScheduler<Self::VM>) {
        let is_full_heap = self.requires_full_heap_collection();
        self.gc_full_heap.store(is_full_heap, Ordering::Relaxed);
        probe!(mmtk, gen_full_heap, is_full_heap);

        if !is_full_heap {
            info!("Nursery GC");
            // nursery GC -- we schedule it
            scheduler
                .schedule_common_work::<StickyImmixNurseryGCWorkContext<VM, DEFAULT_TRACE>>(self);
        } else {
            info!("Full heap GC");
            use crate::plan::immix::Immix;
            use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
            let immix_space = if unlikely(self.common().is_zygote()) {
                self.common().get_zygote().get_immix_space()
            } else {
                &self.immix.immix_space
            };
            Immix::schedule_immix_full_heap_collection::<
                StickyImmix<VM>,
                StickyImmixMatureGCWorkContext<VM, TRACE_KIND_FAST>,
                StickyImmixMatureGCWorkContext<VM, TRACE_KIND_DEFRAG>,
            >(self, immix_space, scheduler);
        }
    }

    fn get_allocator_mapping(
        &self,
    ) -> &'static enum_map::EnumMap<crate::AllocationSemantics, crate::util::alloc::AllocatorSelector>
    {
        &super::mutator::ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: crate::util::VMWorkerThread) {
        if self.is_current_gc_nursery() {
            // Prepare both large object space and immix space
            self.immix.immix_space.prepare(
                false,
                crate::policy::immix::defrag::StatsForDefrag::new(self),
            );
            self.immix.common.los.prepare(false);
            // self.immix.common.nonmoving.prepare(false);
        } else {
            self.full_heap_gc_count.lock().unwrap().inc();
            self.immix.prepare(tls);
        }
    }

    fn release(&mut self, tls: crate::util::VMWorkerThread) {
        if self.is_current_gc_nursery() {
            self.immix.immix_space.release(false);
            self.immix.common.los.release(false);
        } else {
            self.immix.release(tls);
        }
    }

    fn end_of_gc(&self, _tls: crate::util::opaque_pointer::VMWorkerThread) {
        let next_gc_full_heap =
            crate::plan::generational::global::CommonGenPlan::should_next_gc_be_full_heap(self);
        self.next_gc_full_heap
            .store(next_gc_full_heap, Ordering::Relaxed);

        let was_defrag = self.immix.immix_space.end_of_gc();
        self.immix
            .set_last_gc_was_defrag(was_defrag, Ordering::Relaxed);
    }

    fn collection_required(&self, space_full: bool, space: Option<SpaceStats<Self::VM>>) -> bool {
        let nursery_full = self.immix.immix_space.get_pages_allocated()
            > self.base().gc_trigger.get_max_nursery_pages();
        if space_full
            && space.is_some()
            && space.as_ref().unwrap().0.name() != self.immix.immix_space.name()
        {
            self.next_gc_full_heap.store(true, Ordering::Relaxed);
        }
        self.immix.collection_required(space_full, space) || nursery_full
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        self.gc_full_heap.load(Ordering::Relaxed) && self.immix.last_collection_was_exhaustive()
    }

    fn current_gc_may_move_object(&self) -> bool {
        if self.is_current_gc_nursery() {
            PREFER_COPY_ON_NURSERY_GC
        } else {
            self.get_immix_space().in_defrag()
        }
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.immix.get_collection_reserved_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.immix.get_used_pages()
    }

    fn sanity_check_object(&self, object: crate::util::ObjectReference) -> bool {
        if self.is_current_gc_nursery() {
            // Every reachable object should be logged
            if !VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC.is_unlogged::<VM>(object, Ordering::Relaxed)
            {
                error!("Object {} is not unlogged (all objects that have been traced should be unlogged/mature)", object);
                return false;
            }

            // Every reachable object should be marked
            if self.immix.immix_space.in_space(object) && !self.immix.immix_space.is_marked(object)
            {
                error!(
                    "Object {} is not marked (all objects that have been traced should be marked)",
                    object
                );
                return false;
            } else if self.immix.common.los.in_space(object)
                && !self.immix.common.los.is_live(object)
            {
                error!("LOS Object {} is not marked", object);
                return false;
            }
        }
        true
    }

    fn can_pin_objects_in_default_space(&self) -> bool {
        cfg!(feature = "object_pinning")
    }
}

impl<VM: VMBinding> GenerationalPlan for StickyImmix<VM> {
    fn is_current_gc_nursery(&self) -> bool {
        !self.gc_full_heap.load(Ordering::Relaxed)
    }

    fn is_object_in_nursery(&self, object: crate::util::ObjectReference) -> bool {
        self.immix.immix_space.in_space(object) && !self.immix.immix_space.is_marked(object)
    }

    // This check is used for memory slice copying barrier, where we only know addresses instead of objects.
    // As sticky immix needs object metadata to know if an object is an nursery object or not, we cannot really tell
    // whether an address is in nursery or not. In this case, we just return false -- this is a conservative return value
    // for the memory slice copying barrier. It means we will treat the object as if it is in mature space, and will
    // push it to the remembered set.
    // FIXME: this will remember excessive objects, and can cause serious slowdown in some cases.
    fn is_address_in_nursery(&self, _addr: crate::util::Address) -> bool {
        false
    }

    fn get_mature_physical_pages_available(&self) -> usize {
        self.immix.immix_space.available_physical_pages()
    }

    fn get_mature_reserved_pages(&self) -> usize {
        self.immix.immix_space.reserved_pages()
    }

    fn force_full_heap_collection(&self) {
        self.next_gc_full_heap.store(true, Ordering::Relaxed);
    }

    fn last_collection_full_heap(&self) -> bool {
        self.gc_full_heap.load(Ordering::Relaxed)
    }
}

impl<VM: VMBinding> crate::plan::generational::global::GenerationalPlanExt<VM> for StickyImmix<VM> {
    fn trace_object_nursery<Q: crate::ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: crate::util::ObjectReference,
        worker: &mut crate::scheduler::GCWorker<VM>,
    ) -> crate::util::ObjectReference {
        if self.immix.immix_space.in_space(object) {
            if !self.is_object_in_nursery(object) {
                // Mature object
                trace!("Immix mature object {}, skip", object);
                return object;
            } else {
                // Nursery object
                let object = if KIND == TRACE_KIND_TRANSITIVE_PIN || KIND == TRACE_KIND_FAST {
                    trace!(
                        "Immix nursery object {} is being traced without moving",
                        object
                    );
                    self.immix
                        .immix_space
                        .trace_object_without_moving(queue, object)
                } else if crate::policy::immix::PREFER_COPY_ON_NURSERY_GC {
                    let ret = self.immix.immix_space.trace_object_with_opportunistic_copy(
                        queue,
                        object,
                        // We just use default copy here. We have set args for ImmixSpace to deal with unlogged bit,
                        // and we do not need to use CopySemantics::PromoteToMature.
                        CopySemantics::DefaultCopy,
                        worker,
                        true,
                    );
                    trace!(
                        "Immix nursery object {} is being traced with opportunistic copy {}",
                        object,
                        if ret == object {
                            "".to_string()
                        } else {
                            format!("-> new object {}", ret)
                        }
                    );
                    ret
                } else {
                    trace!(
                        "Immix nursery object {} is being traced without moving",
                        object
                    );
                    self.immix
                        .immix_space
                        .trace_object_without_moving(queue, object)
                };

                return object;
            }
        }

        if self.immix.common().get_los().in_space(object) {
            trace!("LOS object {} is being traced", object);
            return self
                .immix
                .common()
                .get_los()
                .trace_object::<Q>(queue, object);
        }

        object
    }
}

impl<VM: VMBinding> StickyImmix<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let full_heap_gc_count = args.stats.new_event_counter("majorGC", true, true);
        let plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &STICKY_IMMIX_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(
                &crate::plan::generational::new_generational_global_metadata_specs::<VM>(),
            ),
        };

        let immix = immix::Immix::new_with_args(
            plan_args,
            crate::policy::immix::ImmixSpaceArgs {
                // Every object we trace in nursery GC becomes a mature object.
                // Every object we trace in full heap GC is a mature object. Thus in both cases,
                // they should be unlogged.
                unlog_object_when_traced: true,
                // In StickyImmix, both young and old objects are allocated in the ImmixSpace.
                #[cfg(feature = "vo_bit")]
                mixed_age: true,
            },
        );
        Self {
            immix,
            gc_full_heap: AtomicBool::new(false),
            next_gc_full_heap: AtomicBool::new(false),
            full_heap_gc_count,
        }
    }

    fn requires_full_heap_collection(&self) -> bool {
        // Separate each condition so the code is clear
        #[allow(clippy::if_same_then_else, clippy::needless_bool)]
        if crate::plan::generational::FULL_NURSERY_GC {
            trace!("full heap: forced full heap");
            // For barrier overhead measurements, we always do full gc in nursery collections.
            true
        } else if self
            .immix
            .common
            .base
            .global_state
            .user_triggered_collection
            .load(Ordering::Relaxed)
            && *self.immix.common.base.options.full_heap_system_gc
        {
            // User triggered collection, and we force full heap for user triggered collection
            true
        } else if self.next_gc_full_heap.load(Ordering::Relaxed)
            || self
                .immix
                .common
                .base
                .global_state
                .cur_collection_attempts
                .load(Ordering::Relaxed)
                > 1
        {
            // Forces full heap collection
            true
        } else if self.common().is_zygote() {
            // Always do full-heap GCs if we are the Zygote process and don't have a Zygote space yet
            true
        } else {
            false
        }
    }

    pub fn get_immix_space(&self) -> &ImmixSpace<VM> {
        &self.immix.immix_space
    }
}
