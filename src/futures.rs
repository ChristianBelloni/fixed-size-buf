use std::{future::Future, ops::DerefMut};

use crate::{BufGuard, Buffer};

pub struct BufGuardFuture<'a, Inner, const BLOCKS: usize, const SIZE: usize> {
    buffer: &'a Buffer<Inner, BLOCKS, SIZE>,
}

impl<'a, Inner, const BLOCKS: usize, const SIZE: usize> BufGuardFuture<'a, Inner, BLOCKS, SIZE> {
    pub(crate) fn new(buffer: &'a Buffer<Inner, BLOCKS, SIZE>) -> Self {
        Self { buffer }
    }
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
