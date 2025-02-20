use std::{
    cell::UnsafeCell,
    future::Future,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Waker,
};

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
    fn free_block(&self, guard: &BufGuard<'_, Inner, BLOCKS, SIZE>) {
        let idx = guard.idx;
        self.registry[idx].store(true, Ordering::SeqCst);
        if let Some(waker) = self.waiters.pop() {
            waker.wake();
        }
    }

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
}

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
    pub unsafe fn new(inner: Inner) -> Self {
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
    unsafe fn get_block(&self, index: usize) -> &mut [u8] {
        match self {
            Self::BoxedSlice(slice) => unsafe {
                let slice = (*slice.get()).as_mut_ptr();
                let start_ptr = slice.byte_offset((index * SIZE) as _);
                std::slice::from_raw_parts_mut(start_ptr, SIZE)
            },
        }
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

impl<Inner, const BLOCKS: usize, const SIZE: usize> Buffer<Inner, BLOCKS, SIZE>
where
    Inner: Deref<Target = [u8]> + DerefMut,
{
    pub async fn acquire_block<'a>(&'a self) -> BufGuard<'a, Inner, BLOCKS, SIZE> {
        let block = self.try_acquire_block();

        if let Some(block) = block {
            block
        } else {
            BufGuardFuture { buffer: self }.await
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

    fn push_waker(&self, waker: Waker) {
        self.waiters.push(waker);
    }
}

pub struct BufGuardFuture<'a, Inner, const BLOCKS: usize, const SIZE: usize> {
    buffer: &'a Buffer<Inner, BLOCKS, SIZE>,
}

impl<'a, Inner, const BLOCKS: usize, const SIZE: usize> Future
    for BufGuardFuture<'a, Inner, BLOCKS, SIZE>
where
    Inner: DerefMut<Target = [u8]>,
{
    type Output = BufGuard<'a, Inner, BLOCKS, SIZE>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(block) = self.buffer.try_acquire_block() {
            std::task::Poll::Ready(block)
        } else {
            self.buffer.push_waker(cx.waker().clone());
            std::task::Poll::Pending
        }
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
        futures::executor::block_on(async move {
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
            let (_, _) = futures::join!(buffer.acquire_block(), async move {
                _ = rx.await;
                drop(buf);
            });
        });
    }

    #[test]
    fn test_memmap() {
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
        let (tx, rx) = oneshot::channel();

        let buffer = unsafe { Buffer::<_, 10, SIZE>::new(map) };
        let sent_buffer = buffer.clone();
        thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            _ = tx.send(());
            futures::executor::block_on(sent_buffer.acquire_block());
        });
        futures::executor::block_on(async move {
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
            let (_, _) = futures::join!(buffer.acquire_block(), async move {
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
                futures::executor::block_on(async move {
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
