pub mod obj_size;
pub mod obj_num;

pub trait RtAnalysis<T> {
    fn alloc_hook(&mut self, _args: T) {}
}
