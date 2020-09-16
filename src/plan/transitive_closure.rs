use crate::util::{Address, ObjectReference};
use crate::scheduler::gc_works::ProcessEdgesWork;

pub trait TransitiveClosure {
    // The signature of this function changes during the port
    // because the argument `ObjectReference source` is never used in the original version
    // See issue #5
    fn process_edge(&mut self, slot: Address);
    fn process_node(&mut self, object: ObjectReference);
}

impl <T: ProcessEdgesWork> TransitiveClosure for T {
    fn process_edge(&mut self, _slot: Address) {
        unreachable!();
    }
    fn process_node(&mut self, object: ObjectReference) {
        ProcessEdgesWork::process_node(self, object);
    }
}