use crate::util::analysis::RtAnalysis;
use crate::util::statistics::counter::EventCounter;
use crate::util::statistics::stats::Stats;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

pub struct ObjSize {
    size_classes: Mutex<HashMap<String, Arc<Mutex<EventCounter>>>>,
}

pub struct ObjSizeArgs<'a> {
    stats: &'a Stats,
    size: usize,
}

impl<'a> ObjSizeArgs<'a> {
    pub fn new(stats: &'a Stats, size: usize) -> Self {
        Self {
            stats,
            size,
        }
    }
}

macro_rules! new_ctr {
    ( $stats:expr, $map:expr, $size:expr ) => {
        {
            let name = format!("size{}", $size);
            let ctr = $stats.new_event_counter(&name, true, true);
            $map.insert(name, ctr.clone());
            ctr
        }
    };
}

impl ObjSize {
    pub fn new() -> Self {
        Self {
            size_classes: Mutex::new(HashMap::new()),
        }
    }

    fn size_class(&self, size: usize) -> usize {
        2_usize.pow(63_u32 - (size - 1).leading_zeros() + 1)
    }
}

impl RtAnalysis<ObjSizeArgs<'_>> for ObjSize {
    fn alloc_hook(&mut self, args: ObjSizeArgs) {
        let stats = args.stats;
        let size = args.size;
        let size_class = format!("size{}", self.size_class(size));
        let mut size_classes = self.size_classes.lock().unwrap();

        let c = size_classes.get_mut(&size_class);
        match c {
            None => {
                let ctr = new_ctr!(stats, size_classes, self.size_class(size));
                ctr.lock().unwrap().inc();
            }
            Some(ctr) => {
                ctr.lock().unwrap().inc();
            }
        }
    }
}
