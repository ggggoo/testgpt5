//! A lock-free bounded MPMC queue.
//!
//! The queue is based on Dmitry Vyukov's bounded MPMC ring-buffer algorithm:
//! each slot has a monotonically increasing sequence number, while producers
//! and consumers reserve positions with compare-and-swap on independent atomic
//! counters.
//!
//! No `Mutex`, `RwLock`, condition variable, or parking primitive is used.
//! Operations are non-blocking `try_*` calls; callers that want blocking
//! behavior should add their own backoff policy outside the queue.
//!
//! # NUMA notes
//!
//! This design is lock-free, but it is not NUMA-local. The enqueue counter,
//! dequeue counter, and each hot slot sequence bounce cache lines between
//! sockets under cross-NUMA producer/consumer traffic. Padding keeps the most
//! contended atomics on separate cache lines, which reduces false sharing, but
//! it cannot remove remote-cache invalidation. For best NUMA throughput, pin
//! related producers and consumers to the same socket, shard work into one
//! queue per NUMA node, and steal between shards only when needed. A single
//! global MPMC queue is usually latency-stable but not bandwidth-optimal across
//! multiple sockets.

use core::cell::UnsafeCell;
use core::fmt;
use core::hint;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const MAX_CAPACITY: usize = isize::MAX as usize;

#[repr(align(64))]
struct CachePadded<T>(T);

struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

/// A bounded multi-producer multi-consumer queue.
///
/// Capacity is chosen at runtime. It may be any positive value; it does not
/// need to be a power of two.
pub struct BoundedQueue<T> {
    buffer: Box<[Slot<T>]>,
    capacity: usize,
    enqueue: CachePadded<AtomicUsize>,
    dequeue: CachePadded<AtomicUsize>,
    closed: CachePadded<AtomicBool>,
}

unsafe impl<T: Send> Send for BoundedQueue<T> {}
unsafe impl<T: Send> Sync for BoundedQueue<T> {}

/// Error returned by [`BoundedQueue::try_push`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushError<T> {
    /// The queue is full and the value was not inserted.
    Full(T),
    /// The queue is closed and the value was not inserted.
    Closed(T),
}

/// Error returned by [`BoundedQueue::try_pop`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PopError {
    /// The queue is currently empty, but it may receive more values.
    Empty,
    /// The queue is closed and fully drained.
    Closed,
}

/// Iterator returned by [`BoundedQueue::drain`].
pub struct Drain<'a, T> {
    queue: &'a BoundedQueue<T>,
}

impl<T> BoundedQueue<T> {
    /// Creates a queue with the requested runtime capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0` or if it is too large for the sequence-number
    /// arithmetic used by the ring buffer.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "bounded queue capacity must be positive");
        assert!(capacity <= MAX_CAPACITY, "bounded queue capacity is too large");

        let mut slots = Vec::with_capacity(capacity);
        for i in 0..capacity {
            slots.push(Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Self {
            buffer: slots.into_boxed_slice(),
            capacity,
            enqueue: CachePadded(AtomicUsize::new(0)),
            dequeue: CachePadded(AtomicUsize::new(0)),
            closed: CachePadded(AtomicBool::new(false)),
        }
    }

    /// Returns the configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns whether the queue has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.0.load(Ordering::Acquire)
    }

    /// Closes the queue.
    ///
    /// A producer racing with `close` may already have reserved a slot and may
    /// still publish that value; consumers and [`Drain`] handle that by
    /// draining until the reserved tail is reached.
    pub fn close(&self) {
        self.closed.0.store(true, Ordering::Release);
    }

    /// Attempts to push `value` into the queue.
    ///
    /// This method uses only atomic operations. It never waits for capacity to
    /// become available; full queues return [`PushError::Full`].
    pub fn try_push(&self, value: T) -> Result<(), PushError<T>> {
        let mut value = Some(value);
        let mut state = self.enqueue.0.load(Ordering::Acquire);

        loop {
            if self.is_closed() {
                return Err(PushError::Closed(value.take().unwrap()));
            }

            let pos = state;
            let slot = &self.buffer[pos % self.capacity];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;

            if diff == 0 {
                let next = pos.wrapping_add(1);
                match self.enqueue.0.compare_exchange_weak(
                    state,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        unsafe {
                            (*slot.value.get()).write(value.take().unwrap());
                        }
                        slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(actual) => state = actual,
                }
            } else if diff < 0 {
                if self.is_closed() {
                    return Err(PushError::Closed(value.take().unwrap()));
                }
                return Err(PushError::Full(value.take().unwrap()));
            } else {
                state = self.enqueue.0.load(Ordering::Acquire);
            }
        }
    }

    /// Attempts to pop a value from the queue.
    ///
    /// If this returns [`PopError::Closed`], the queue is closed and no queued
    /// or pre-reserved values remain.
    pub fn try_pop(&self) -> Result<T, PopError> {
        let mut pos = self.dequeue.0.load(Ordering::Acquire);

        loop {
            let slot = &self.buffer[pos % self.capacity];
            let seq = slot.sequence.load(Ordering::Acquire);
            let ready = pos.wrapping_add(1);
            let diff = seq.wrapping_sub(ready) as isize;

            if diff == 0 {
                match self.dequeue.0.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let value = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.sequence
                            .store(pos.wrapping_add(self.capacity), Ordering::Release);
                        return Ok(value);
                    }
                    Err(actual) => pos = actual,
                }
            } else if diff < 0 {
                if self.is_fully_drained(pos) {
                    return Err(PopError::Closed);
                }
                return Err(PopError::Empty);
            } else {
                pos = self.dequeue.0.load(Ordering::Acquire);
            }
        }
    }

    /// Closes the queue and returns an iterator that drains all remaining
    /// values, including values reserved by producers before `close`.
    pub fn drain(&self) -> Drain<'_, T> {
        self.close();
        Drain { queue: self }
    }

    fn is_fully_drained(&self, observed_dequeue: usize) -> bool {
        let enqueue = self.enqueue.0.load(Ordering::Acquire);
        self.is_closed() && observed_dequeue == enqueue
    }
}

impl<T> Drop for BoundedQueue<T> {
    fn drop(&mut self) {
        self.close();
        while self.try_pop().is_ok() {}
    }
}

impl<'a, T> Iterator for Drain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.queue.try_pop() {
                Ok(value) => return Some(value),
                Err(PopError::Closed) => return None,
                Err(PopError::Empty) => {
                    hint::spin_loop();
                    std::thread::yield_now();
                }
            }
        }
    }
}

impl<T> fmt::Debug for BoundedQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedQueue")
            .field("capacity", &self.capacity)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn capacity_is_runtime_configured() {
        let q = BoundedQueue::<u32>::new(3);
        assert_eq!(q.capacity(), 3);
        assert_eq!(q.try_push(1), Ok(()));
        assert_eq!(q.try_push(2), Ok(()));
        assert_eq!(q.try_push(3), Ok(()));
        assert_eq!(q.try_push(4), Err(PushError::Full(4)));
    }

    #[test]
    fn fifo_for_single_producer_single_consumer() {
        let q = BoundedQueue::new(2);
        q.try_push("a").unwrap();
        q.try_push("b").unwrap();
        assert_eq!(q.try_pop(), Ok("a"));
        q.try_push("c").unwrap();
        assert_eq!(q.try_pop(), Ok("b"));
        assert_eq!(q.try_pop(), Ok("c"));
        assert_eq!(q.try_pop(), Err(PopError::Empty));
    }

    #[test]
    fn close_rejects_push_and_allows_drain() {
        let q = BoundedQueue::new(4);
        q.try_push(10).unwrap();
        q.try_push(20).unwrap();

        q.close();

        assert_eq!(q.try_push(30), Err(PushError::Closed(30)));
        assert_eq!(q.try_pop(), Ok(10));
        assert_eq!(q.try_pop(), Ok(20));
        assert_eq!(q.try_pop(), Err(PopError::Closed));
    }

    #[test]
    fn drain_closes_and_empties_queue() {
        let q = BoundedQueue::new(8);
        for n in 0..5 {
            q.try_push(n).unwrap();
        }

        let drained: Vec<_> = q.drain().collect();
        assert_eq!(drained, vec![0, 1, 2, 3, 4]);
        assert_eq!(q.try_pop(), Err(PopError::Closed));
        assert_eq!(q.try_push(99), Err(PushError::Closed(99)));
    }

    #[test]
    fn mpmc_transfers_every_value_once() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 10_000;

        let q = Arc::new(BoundedQueue::new(257));
        let mut handles = Vec::new();

        for producer in 0..PRODUCERS {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    let value = producer * PER_PRODUCER + i;
                    loop {
                        match q.try_push(value) {
                            Ok(()) => break,
                            Err(PushError::Full(v)) => {
                                assert_eq!(v, value);
                                hint::spin_loop();
                            }
                            Err(PushError::Closed(_)) => panic!("queue closed too early"),
                        }
                    }
                }
            }));
        }

        let mut consumers = Vec::new();
        for _ in 0..CONSUMERS {
            let q = Arc::clone(&q);
            consumers.push(thread::spawn(move || {
                let mut values = Vec::new();
                loop {
                    match q.try_pop() {
                        Ok(value) => values.push(value),
                        Err(PopError::Empty) => {
                            hint::spin_loop();
                        }
                        Err(PopError::Closed) => return values,
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        q.close();

        let mut received = Vec::new();
        for consumer in consumers {
            received.extend(consumer.join().unwrap());
        }

        assert_eq!(received.len(), PRODUCERS * PER_PRODUCER);
        let unique: HashSet<_> = received.into_iter().collect();
        assert_eq!(unique.len(), PRODUCERS * PER_PRODUCER);
        for value in 0..PRODUCERS * PER_PRODUCER {
            assert!(unique.contains(&value), "missing value {value}");
        }
    }
}
