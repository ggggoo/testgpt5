# lockfree_mpmc_queue

A bounded MPMC queue implemented in Rust with atomic operations only.

Features:

- runtime-selected capacity
- multi-producer, multi-consumer `try_push` / `try_pop`
- `close()` to reject future pushes
- `drain()` to close and consume all remaining values
- unit tests and a no-dependency benchmark
- notes in the crate docs about NUMA behavior

The public queue operations are non-blocking. `drain()` closes the queue and
iterates until every value that was reserved before `close()` has been
published and consumed, using spin/yield while waiting for those in-flight
producers.

Run:

```sh
cargo test
cargo bench --bench throughput
```

The implementation follows the classic bounded ring-buffer algorithm where
each slot carries a sequence number. Producers reserve enqueue positions with a
CAS, publish the value, then release-store the next sequence. Consumers reserve
dequeue positions with a CAS, read the value, then release-store the recycled
sequence for the next lap.
