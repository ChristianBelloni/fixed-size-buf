use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

pub enum BackingBuffer<Inner, const BLOCKS: usize, const SIZE: usize> {
    BoxedSlice(UnsafeCell<Inner>),
}

unsafe impl<Inner, const BLOCKS: usize, const SIZE: usize> Send
    for BackingBuffer<Inner, BLOCKS, SIZE>
{
}

unsafe impl<Inner, const BLOCKS: usize, const SIZE: usize> Sync
    for BackingBuffer<Inner, BLOCKS, SIZE>
{
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> BackingBuffer<Inner, BLOCKS, SIZE> {
    pub(crate) unsafe fn new(inner: Inner) -> Self {
        Self::BoxedSlice(UnsafeCell::new(inner))
    }
}

impl<const BLOCKS: usize, const SIZE: usize> BackingBuffer<Box<[u8]>, BLOCKS, SIZE> {
    pub fn new_vec() -> Self {
        let mut v = Vec::with_capacity(BLOCKS * SIZE);
        unsafe {
            v.set_len(BLOCKS * SIZE);
        }
        v.fill(0);

        Self::BoxedSlice(UnsafeCell::new(v.into_boxed_slice()))
    }
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> BackingBuffer<Inner, BLOCKS, SIZE>
where
    Inner: Deref<Target = [u8]> + DerefMut,
{
    pub(crate) unsafe fn get_block(&self, index: usize) -> &mut [u8] {
        match self {
            Self::BoxedSlice(slice) => unsafe {
                let slice = (*slice.get()).as_mut_ptr();
                let start_ptr = slice.byte_offset((index * SIZE) as _);
                std::slice::from_raw_parts_mut(start_ptr, SIZE)
            },
        }
    }
}
