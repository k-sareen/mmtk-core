use crate::util::analysis::RtAnalysis;
use crate::util::statistics::counter::EventCounter;
use std::sync::Arc;
use std::sync::Mutex;

pub struct ObjNum {
    counter: Arc<Mutex<EventCounter>>,
}

impl ObjNum {
    pub fn new(counter: Arc<Mutex<EventCounter>>) -> Self {
        Self {
            counter,
        }
    }
}

impl RtAnalysis<()> for ObjNum {
    fn alloc_hook(&mut self, _args: ()) {
        self.counter.lock().unwrap().inc();
    }
}
