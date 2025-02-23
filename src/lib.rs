use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Waker,
};

use backing_buffer::BackingBuffer;
use futures::BufGuardFuture;

mod backing_buffer;
mod futures;

pub struct Buffer<Inner, const BLOCKS: usize, const SIZE: usize> {
    storage: Arc<BackingBuffer<Inner, BLOCKS, SIZE>>,
    /// TRUE available FALSE occupied
    registry: Arc<[AtomicBool; BLOCKS]>,
    waiters: Arc<crossbeam_queue::SegQueue<Waker>>,
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> Clone for Buffer<Inner, BLOCKS, SIZE> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            registry: self.registry.clone(),
            waiters: self.waiters.clone(),
        }
    }
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> Buffer<Inner, BLOCKS, SIZE> {
    pub unsafe fn new(inner: Inner) -> Self {
        let mut registry = Vec::with_capacity(BLOCKS);
        for _ in 0..BLOCKS {
            registry.push(AtomicBool::new(true));
        }
        Self {
            storage: unsafe { Arc::new(BackingBuffer::new(inner)) },
            registry: Arc::new(registry.try_into().unwrap()),
            waiters: Default::default(),
        }
    }

    fn free_block(&self, guard: &BufGuard<'_, Inner, BLOCKS, SIZE>) {
        let idx = guard.idx;
        self.registry[idx].store(true, Ordering::SeqCst);
        if let Some(waker) = self.waiters.pop() {
            waker.wake();
        }
    }

    fn push_waker(&self, waker: Waker) {
        self.waiters.push(waker);
    }
}

impl<Inner, const BLOCKS: usize, const SIZE: usize> Buffer<Inner, BLOCKS, SIZE>
where
    Inner: Deref<Target = [u8]> + DerefMut,
{
    pub async fn acquire_block<'a>(&'a self) -> BufGuard<'a, Inner, BLOCKS, SIZE> {
        let block = self.try_acquire_block();

        if let Some(block) = block {
            block
        } else {
            BufGuardFuture::new(self).await
        }
    }

    fn try_acquire_block<'a>(&'a self) -> Option<BufGuard<'a, Inner, BLOCKS, SIZE>> {
        let mut idx = None;
        for (i, e) in self.registry.iter().enumerate() {
            if e.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                idx = Some(i);
                break;
            }
        }
        let idx = idx?;

        let block = unsafe { self.storage.get_block(idx) };
        BufGuard {
            block,
            buffer: self,
            idx,
        }
        .into()
    }
}

impl<const BLOCKS: usize, const SIZE: usize> Buffer<Box<[u8]>, BLOCKS, SIZE> {
    pub fn new_vec() -> Self {
        let mut registry = Vec::with_capacity(BLOCKS);
        for _ in 0..BLOCKS {
            registry.push(AtomicBool::new(true));
        }
        Self {
            storage: Arc::new(BackingBuffer::new_vec()),
            registry: Arc::new(registry.try_into().unwrap()),
            waiters: Default::default(),
        }
    }
}

#[cfg(feature = "memmap")]
impl<const BLOCKS: usize, const SIZE: usize> Buffer<memmap2::MmapMut, BLOCKS, SIZE> {
    pub unsafe fn new_memmap(backing_file: &std::path::Path) -> std::io::Result<Self> {
        use std::io::Write;

        let mut backing_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(backing_file)
            .unwrap();

        backing_file.set_len((BLOCKS * SIZE).try_into().unwrap())?;
        backing_file.flush()?;
        let map = unsafe { memmap2::MmapMut::map_mut(&backing_file)? };
        unsafe { Ok(Self::new(map)) }
    }
}

pub struct BufGuard<'a, Inner, const BLOCKS: usize, const SIZE: usize> {
    buffer: &'a Buffer<Inner, BLOCKS, SIZE>,
    block: &'a mut [u8],
    idx: usize,
}

impl<'a, Inner, const BLOCKS: usize, const SIZE: usize> Deref
    for BufGuard<'a, Inner, BLOCKS, SIZE>
{
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl<'a, Inner, const BLOCKS: usize, const SIZE: usize> DerefMut
    for BufGuard<'a, Inner, BLOCKS, SIZE>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

impl<'a, Inner, const BLOCKS: usize, const SIZE: usize> Drop for BufGuard<'a, Inner, BLOCKS, SIZE> {
    fn drop(&mut self) {
        self.buffer.free_block(self);
    }
}

#[cfg(test)]
mod tests {

    const SIZE: usize = 1048576 * 2;
    use std::{io::Write, thread, time::Duration};

    use tempdir::TempDir;

    use super::*;
    #[test]
    fn basic_test() {
        let (tx, rx) = oneshot::channel();
        thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            tx.send(())
        });
        ::futures::executor::block_on(async move {
            let mut v = Vec::with_capacity(10 * SIZE);
            v.fill(0);

            let buffer = unsafe { Buffer::<_, 10, SIZE>::new(v) };
            let buf = buffer.acquire_block().await;
            assert_eq!(buf.len(), SIZE);
            assert!(buf.iter().all(|&a| a == 0));

            let mut blocks = Vec::new();
            for _ in 0..9 {
                let b = buffer.acquire_block().await;
                blocks.push(b);
            }
            let (_, _) = ::futures::join!(buffer.acquire_block(), async move {
                _ = rx.await;
                drop(buf);
            });
        });
    }

    #[cfg(feature = "memmap")]
    #[test]
    fn test_memmap() {
        let dir = TempDir::new("dir").unwrap();

        let (tx, rx) = oneshot::channel();

        let buffer =
            unsafe { Buffer::<_, 10, SIZE>::new_memmap(&dir.path().join("file.txt")).unwrap() };
        let sent_buffer = buffer.clone();
        thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            _ = tx.send(());
            ::futures::executor::block_on(sent_buffer.acquire_block());
        });
        ::futures::executor::block_on(async move {
            let mut v = Vec::with_capacity(10 * SIZE);
            v.fill(0);

            let buf = buffer.acquire_block().await;
            assert_eq!(buf.len(), SIZE);
            assert!(buf.iter().all(|&a| a == 0));

            let mut blocks = Vec::new();
            for _ in 0..9 {
                let b = buffer.acquire_block().await;
                blocks.push(b);
            }
            let (_, _) = ::futures::join!(buffer.acquire_block(), async move {
                _ = rx.await;
                drop(buf);
            });

            buffer.acquire_block().await;
        });
    }

    #[test]
    fn test_race() {
        let dir = TempDir::new("dir").unwrap();
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir.path().join("file.txt"))
            .unwrap();

        f.set_len((10 * SIZE) as _).unwrap();
        f.flush().unwrap();
        let map = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
        let buffer = unsafe { Buffer::<_, 10, SIZE>::new(map) };
        let mut hs = Vec::new();
        for _ in 0..50 {
            let buffer = buffer.clone();
            let h = std::thread::spawn(move || {
                ::futures::executor::block_on(async move {
                    for _ in 0..500 {
                        buffer.acquire_block().await;
                    }
                });
            });
            hs.push(h);
        }

        for h in hs {
            h.join().unwrap();
        }
    }
}
