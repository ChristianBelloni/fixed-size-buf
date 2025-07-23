use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

pub struct BackingBuffer<Inner, const BLOCKS: usize, const SIZE: usize>(UnsafeCell<Inner>);

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
        Self(UnsafeCell::new(inner))
    }
}

impl<const BLOCKS: usize, const SIZE: usize> BackingBuffer<Box<[u8]>, BLOCKS, SIZE> {
    pub fn new_vec() -> Self {
        let mut v = Vec::with_capacity(BLOCKS * SIZE);
        v.fill(0);
        unsafe {
            v.set_len(BLOCKS * SIZE);
        }

        Self(UnsafeCell::new(v.into_boxed_slice()))
    }
}

#[cfg(feature = "memmap")]
impl<const BLOCKS: usize, const SIZE: usize> BackingBuffer<memmap2::MmapMut, BLOCKS, SIZE> {
    pub fn new_memmap(file: &std::path::Path) -> std::io::Result<Self> {
        use std::io::Write;

        let mut backing_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(true)
            .write(true)
            .open(file)
            .unwrap();

        backing_file.set_len((BLOCKS * SIZE).try_into().unwrap())?;
        backing_file.flush()?;
        let map = unsafe { memmap2::MmapMut::map_mut(&backing_file)? };
        Ok(Self(UnsafeCell::new(map)))
    }
}

#[cfg(feature = "bytes")]
impl<const BLOCKS: usize, const SIZE: usize> BackingBuffer<bytes::BytesMut, BLOCKS, SIZE> {
    pub fn new_bytes() -> Self {
        let bytes = bytes::BytesMut::with_capacity(BLOCKS * SIZE);
        Self(UnsafeCell::new(bytes))
    }
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> BackingBuffer<Inner, BLOCKS, SIZE>
where
    Inner: Deref<Target = [u8]> + DerefMut,
{
    pub(crate) unsafe fn get_block(&self, index: usize) -> &'static mut [u8] {
        unsafe {
            let slice = &self.0;
            let slice = (*slice.get()).as_mut_ptr();
            let start_ptr = slice.byte_offset((index * SIZE) as _);
            std::slice::from_raw_parts_mut(start_ptr, SIZE)
        }
    }
}
