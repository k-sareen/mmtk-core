use std::sync::Arc;
use std::sync::Mutex;
use super::*;

pub struct SizeCounter {
    units: Arc<Mutex<EventCounter>>,
    volume: Arc<Mutex<EventCounter>>,
}

impl SizeCounter {
    pub fn new(
        units: Arc<Mutex<EventCounter>>,
        volume: Arc<Mutex<EventCounter>>
    ) -> Self {
        SizeCounter {
            units,
            volume,
        }
    }

    pub fn inc(&mut self, size: u64) {
        self.units.lock().unwrap().inc();
        self.volume.lock().unwrap().inc_by(size);
    }

    pub fn start(&mut self) {
        self.units.lock().unwrap().start();
        self.volume.lock().unwrap().start();
    }

    pub fn stop(&mut self) {
        self.units.lock().unwrap().stop();
        self.volume.lock().unwrap().stop();
    }

    pub fn print_current_units(&self) {
        self.units.lock().unwrap().print_current();
    }

    pub fn print_current_volume(&self) {
        self.volume.lock().unwrap().print_current();
    }

    pub fn print_units(&self) {
        self.units.lock().unwrap().print_total(None);
    }

    pub fn print_volume(&self) {
        self.volume.lock().unwrap().print_total(None);
    }
}
