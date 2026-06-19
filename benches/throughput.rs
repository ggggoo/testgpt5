use lockfree_mpmc_queue::{BoundedQueue, PopError, PushError};
use std::hint;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const PRODUCERS: usize = 4;
const CONSUMERS: usize = 4;
const PER_PRODUCER: usize = 250_000;
const CAPACITY: usize = 1024;

fn main() {
    let queue = Arc::new(BoundedQueue::new(CAPACITY));
    let start = Instant::now();

    let producers = (0..PRODUCERS)
        .map(|producer| {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    let value = producer * PER_PRODUCER + i;
                    loop {
                        match queue.try_push(value) {
                            Ok(()) => break,
                            Err(PushError::Full(v)) => {
                                debug_assert_eq!(v, value);
                                hint::spin_loop();
                            }
                            Err(PushError::Closed(_)) => panic!("queue closed before benchmark ended"),
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let consumers = (0..CONSUMERS)
        .map(|_| {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let mut count = 0usize;
                loop {
                    match queue.try_pop() {
                        Ok(_) => count += 1,
                        Err(PopError::Empty) => hint::spin_loop(),
                        Err(PopError::Closed) => return count,
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for producer in producers {
        producer.join().unwrap();
    }
    queue.close();

    let total = consumers
        .into_iter()
        .map(|consumer| consumer.join().unwrap())
        .sum::<usize>();

    let elapsed = start.elapsed();
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    println!("producers: {PRODUCERS}");
    println!("consumers: {CONSUMERS}");
    println!("capacity: {CAPACITY}");
    println!("messages: {total}");
    println!("elapsed: {:.3?}", elapsed);
    println!("throughput: {:.0} msg/s", ops_per_sec);
}
