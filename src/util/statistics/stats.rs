use crate::mmtk::MMTK;
use crate::util::options::Options;
use crate::util::statistics::counter::*;
use crate::util::statistics::Timer;
use crate::vm::VMBinding;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

// TODO: Increasing this number would cause JikesRVM die at boot time. I don't really know why.
// E.g. using 1 << 14 will cause JikesRVM segfault at boot time.
pub const MAX_PHASES: usize = 1 << 12;
pub const MAX_COUNTERS: usize = 100;

/// GC stats shared among counters
pub struct SharedStats {
    phase: AtomicUsize,
    gathering_stats: AtomicBool,
}

impl SharedStats {
    fn increment_phase(&self) {
        self.phase.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get_phase(&self) -> usize {
        self.phase.load(Ordering::SeqCst)
    }

    pub fn get_gathering_stats(&self) -> bool {
        self.gathering_stats.load(Ordering::SeqCst)
    }

    fn set_gathering_stats(&self, val: bool) {
        self.gathering_stats.store(val, Ordering::SeqCst);
    }
}

/// GC statistics
///
/// The struct holds basic GC statistics, like the GC count,
/// and an array of counters.
pub struct Stats {
    gc_count: AtomicUsize,
    total_time: Arc<Mutex<Timer>>,
    pub shared: Arc<SharedStats>,
    counters: Mutex<Vec<Arc<Mutex<dyn Counter + Send>>>>,
    exceeded_phase_limit: AtomicBool,
    pub power_stats: Arc<Mutex<PowerStats>>,
    pub prev_power_stats: Arc<Mutex<Vec<PowerStatsEnergyMeasurement>>>,
}

impl Stats {
    #[allow(unused)]
    pub fn new() -> Self {
        let shared = Arc::new(SharedStats {
            phase: AtomicUsize::new(0),
            gathering_stats: AtomicBool::new(false),
        });
        let mut counters: Vec<Arc<Mutex<dyn Counter + Send>>> = vec![];
        // We always have a time counter enabled
        let t = Arc::new(Mutex::new(LongCounter::new(
            "time".to_string(),
            shared.clone(),
            true,
            false,
            MonotoneNanoTime {},
        )));
        counters.push(t.clone());
        Stats {
            gc_count: AtomicUsize::new(0),
            total_time: t,
            shared,
            counters: Mutex::new(counters),
            exceeded_phase_limit: AtomicBool::new(false),
            power_stats: Arc::new(Mutex::new(PowerStats::new())),
            prev_power_stats: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn new_event_counter(
        &self,
        name: &str,
        implicit_start: bool,
        merge_phases: bool,
    ) -> Arc<Mutex<EventCounter>> {
        let mut guard = self.counters.lock().unwrap();
        let counter = Arc::new(Mutex::new(EventCounter::new(
            name.to_string(),
            self.shared.clone(),
            implicit_start,
            merge_phases,
        )));
        guard.push(counter.clone());
        counter
    }

    pub fn new_size_counter(
        &self,
        name: &str,
        implicit_start: bool,
        merge_phases: bool,
    ) -> Mutex<SizeCounter> {
        let u = self.new_event_counter(name, implicit_start, merge_phases);
        let v = self.new_event_counter(&format!("{}.volume", name), implicit_start, merge_phases);
        Mutex::new(SizeCounter::new(u, v))
    }

    pub fn new_timer(
        &self,
        name: &str,
        implicit_start: bool,
        merge_phases: bool,
    ) -> Arc<Mutex<Timer>> {
        let mut guard = self.counters.lock().unwrap();
        let counter = Arc::new(Mutex::new(Timer::new(
            name.to_string(),
            self.shared.clone(),
            implicit_start,
            merge_phases,
            MonotoneNanoTime {},
        )));
        guard.push(counter.clone());
        counter
    }

    /// Create perf counters specified in the Options
    #[cfg(feature = "perf_counter")]
    pub fn create_perf_counters(&self, options: &Options) {
        let mut guard = self.counters.lock().unwrap();
        // Read from the MMTK option for a list of perf events we want to
        // measure, and create corresponding counters
        for e in options.phase_perf_events.events {
            if let Ok(pe) = PerfEventDiffable::new(e.0, *options.perf_exclude_kernel) {
                info!("Created performance counter {}", e.1);
                guard.push(Arc::new(Mutex::new(LongCounter::new(
                    e.1.to_string(),
                    self.shared.clone(),
                    true,
                    false,
                    pe,
                ))));
            } else {
                warn!("Error opening event {}", e.1);
            }
        }
        let mut power_stats = self.power_stats.lock().unwrap();
        power_stats.init(vec!["s2mpg12-odpm", "s2mpg13-odpm"].as_slice());
    }

    pub fn start_gc(&self) {
        self.gc_count.fetch_add(1, Ordering::SeqCst);
        if !self.get_gathering_stats() {
            return;
        }
        if self.get_phase() < MAX_PHASES - 1 {
            let counters = self.counters.lock().unwrap();
            for counter in &(*counters) {
                counter.lock().unwrap().phase_change(self.get_phase());
            }
            self.shared.increment_phase();
        } else if !self.exceeded_phase_limit.load(Ordering::SeqCst) {
            warn!("Number of GC phases exceeds MAX_PHASES");
            self.exceeded_phase_limit.store(true, Ordering::SeqCst);
        }
    }

    pub fn end_gc(&self) {
        if !self.get_gathering_stats() {
            return;
        }
        if self.get_phase() < MAX_PHASES - 1 {
            let counters = self.counters.lock().unwrap();
            for counter in &(*counters) {
                counter.lock().unwrap().phase_change(self.get_phase());
            }
            self.shared.increment_phase();
        } else if !self.exceeded_phase_limit.load(Ordering::SeqCst) {
            warn!("Number of GC phases exceeds MAX_PHASES");
            self.exceeded_phase_limit.store(true, Ordering::SeqCst);
        }
    }

    pub fn print_stats<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        let mut output_string = String::new();
        output_string.push_str(
            "============================ MMTk Statistics Totals ============================\n",
        );
        let scheduler_stat = mmtk.scheduler.statistics();
        self.print_column_names(&scheduler_stat, &mut output_string);
        output_string.push_str(format!("{}\t", self.get_phase() / 2).as_str());
        self.total_time
            .lock()
            .unwrap()
            .print_total(None, &mut output_string);
        output_string.push('\t');
        let counter = self.counters.lock().unwrap();
        for iter in &(*counter) {
            let c = iter.lock().unwrap();
            if c.merge_phases() {
                c.print_total(None, &mut output_string);
            } else {
                c.print_total(Some(true), &mut output_string);
                output_string.push('\t');
                c.print_total(Some(false), &mut output_string)
            }
            output_string.push('\t');
        }
        for value in scheduler_stat.values() {
            output_string.push_str(format!("{}\t", value).as_str());
        }
        output_string.push_str(
            "\n------------------------------ End MMTk Statistics -----------------------------\n",
        );
        warn!("{}", output_string);
    }

    pub fn print_column_names(
        &self,
        scheduler_stat: &HashMap<String, String>,
        output_string: &mut String,
    ) {
        output_string.push_str("GC\ttime\t");
        let counter = self.counters.lock().unwrap();
        for iter in &(*counter) {
            let c = iter.lock().unwrap();
            if c.merge_phases() {
                output_string.push_str(format!("{}\t", c.name()).as_str());
            } else {
                output_string.push_str(format!("{}.other\t{}.stw\t", c.name(), c.name()).as_str());
            }
        }
        for name in scheduler_stat.keys() {
            output_string.push_str(format!("{}\t", name).as_str());
        }
        output_string.push('\n');
    }

    pub fn start_all(&self) {
        let counters = self.counters.lock().unwrap();
        if self.get_gathering_stats() {
            panic!("Calling Stats.start_all() while stats running");
        }
        self.shared.set_gathering_stats(true);

        for c in &(*counters) {
            let mut ctr = c.lock().unwrap();
            if ctr.implicitly_start() {
                ctr.start();
            }
        }
        let mut power_stats = self.power_stats.lock().unwrap();
        let mut prev_power_stats = self.prev_power_stats.lock().unwrap();
        let readings = power_stats.read_energy_meter(vec![].as_slice());
        *prev_power_stats = readings;
    }

    pub fn stop_all<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        self.stop_all_counters();
        let mut power_stats = self.power_stats.lock().unwrap();
        let prev_power_stats = self.prev_power_stats.lock().unwrap();
        let channel_infos = power_stats.get_energy_meter_info().clone();
        let readings = power_stats.read_energy_meter(vec![].as_slice());
        self.print_stats(mmtk);
        let mut total_energy = 0;
        let mut total_power = 0;
        for energy_stat in readings {
            let channel_info = &channel_infos[energy_stat.id as usize];
            let prev_energy_stat = &prev_power_stats[energy_stat.id as usize];
            let energy = energy_stat.energy_uW_s - prev_energy_stat.energy_uW_s;
            let duration = energy_stat.duration_ms - prev_energy_stat.duration_ms;
            let power = energy / (duration / 1000);
            total_energy += energy;
            total_power += power;
            warn!(
                "kunals: Energy consumption for {} (subsystem: {}) is {} uWs, power is {} uW",
                channel_info.name, channel_info.subsystem, energy, power,
            );
        }
        warn!(
            "kunals: Total energy consumption is {} uWs, total power is {} uW",
            total_energy, total_power
        );
    }

    fn stop_all_counters(&self) {
        let counters = self.counters.lock().unwrap();
        for c in &(*counters) {
            c.lock().unwrap().stop();
        }
        self.shared.set_gathering_stats(false);
    }

    fn get_phase(&self) -> usize {
        self.shared.get_phase()
    }

    pub fn get_gathering_stats(&self) -> bool {
        self.shared.get_gathering_stats()
    }
}
