use crate::KernelResult;

pub(crate) mod level_iter;
pub(crate) mod merging_iter;
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum Seek<'s> {
    // 第一个元素
    First,
    // 最后一个元素
    Last,
    // 与key相等或稍大的元素
    Backward(&'s [u8]),
}

/// 硬盘迭代器
pub trait Iter<'a> {
    type Item;

    fn try_next(&mut self) -> KernelResult<Option<Self::Item>>;

    fn is_valid(&self) -> bool;
}

pub trait SeekIter<'a>: Iter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> KernelResult<()>;
}

