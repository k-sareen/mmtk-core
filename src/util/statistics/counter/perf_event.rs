use super::Diffable;
use perf_event::Builder as PerfEventBuilder;
use perf_event::Counter as PerfEvent;
use perf_event::events::Event as PerfEventKind;

/// A [`Diffable`] helper type for measuring overall perf events for mutators
/// and GC
/// This is the process-wide counterpart of [`crate::scheduler::work_counter::WorkPerfEvent`].
pub struct PerfEventDiffable {
    pe: perf_event::Counter,
}

impl PerfEventDiffable {
    pub fn new(kind: PerfEventKind, _exclude_kernel: bool) -> std::io::Result<Self> {
        let pe = PerfEventBuilder::new()
            .kind(kind)
            .any_cpu()
            .observe_self()
            .inherit(true)
            .include_hv()
            .include_kernel()
            .build()?;
        Ok(PerfEventDiffable { pe })
    }
}

impl Diffable for PerfEventDiffable {
    type Val = perf_event::CountAndTime;

    fn start(&mut self) {
        self.pe.reset().expect("Failed to reset perf evet");
        self.pe.enable().expect("Failed to enable perf evet");
    }

    fn stop(&mut self) {
        self.pe.disable().expect("Failed to disable perf evet");
    }

    fn current_value(&mut self) -> Self::Val {
        let val = self.pe.read_count_and_time().expect("Failed to read perf evet");
        assert_eq!(val.time_enabled, val.time_running, "perf event multiplexed");
        val
    }

    fn diff(current: &Self::Val, earlier: &Self::Val) -> u64 {
        assert!(current.count >= earlier.count, "perf event overflowed");
        current.count as u64 - earlier.count as u64
    }

    fn print_diff(val: u64, output: &mut String) {
        output.push_str(format!("{}", val).as_str());
    }
}
