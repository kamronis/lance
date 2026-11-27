# PR #11159: stock SortExec ownership prerequisite

This supplements the earlier custom `ResourceSortStream` / `sort_batch` proof.
That earlier proof remains valid for its stated consumer and is not evidence
for stock DataFusion `SortExec`.

## Pinned boundary

DataFusion 55.2.0 has no public reservation handoff API on `SortExec`:

- `ExternalSorter` and its `reservation` are private in
  `datafusion-physical-plan/src/sorts/sort.rs`.
- `reserve_memory_for_batch_and_maybe_spill` reserves
  `get_record_batch_memory_size(input) + input.get_sliced_size()`. The first
  term accounts for retained input already covered by the Lance backing lease.
  The second is the native sort/merge allowance. The terms must remain distinct;
  renaming their sum as additional workspace would conceal the overlap.
- `sort_batch_stream` passes its resized output reservation to the private
  `ReservationStream`. That stream shrinks the reservation before returning each
  output batch. Plain Arrow slices do not receive that reservation.
- Public `SortExec` configuration exposes ordering, input, fetch and partitioning
  controls, but no adoption or output-reservation transfer hook.

The stock tests in `rust/lance/tests/read_budget_sort_handoff/sortexec.rs` use a
real Lance scan, the real read-policy overdraft path, actual `SortExec`, and one
finite `GreedyMemoryPool`. A transparent observer delegates every native and
adapter allocation to that same pool and records all growth/shrink operations.
No consumer is suppressed, credited, renamed or given an independent cap.

The input gate forwards its batch directly to `SortExec`. Only when the sorter
requests another item does the gate drain the scan's EOF, then withhold EOF
from the sorter. No test-side collection or clone retains the input. This
separates legitimately retained sorter input from the scan's soft ticket.

`sortexec_stock_scan_overdraft_refusal_and_retry` characterizes hard refusal and
identical retry with native reservations included. It does not assert that the
native input overlap is solved.

`sortexec_last_slice_requires_upstream_reservation_handoff` is an intentionally
failing contract regression. It verifies sorted values and derives the expected
output backing from Arrow's allocation accounting. After dropping the pipeline,
it requires the final slice to retain that charge. This assertion must remain
strict; an expected failure is not a successful ownership proof.

## Proposed upstream interface

A supported operator-level handoff is required; pool callbacks alone lack the
batch/allocation identity and cannot safely adopt an existing backing charge.
One concrete API shape for upstream discussion is:

```rust
struct AccountedBacking {
    owner: Arc<dyn Send + Sync>,
    unique_backing_bytes: usize,
    pool: Arc<dyn MemoryPool>,
}

trait SortMemoryHandoff: Send + Sync {
    fn input_backing(&self, batch: &RecordBatch) -> Result<Option<AccountedBacking>>;
    fn output_backing(
        &self,
        batch: RecordBatch,
        reservation: MemoryReservation,
    ) -> Result<RecordBatch>;
}

// Optional configuration on SortExec::new(ordering, input).
// pub fn with_memory_handoff(self, handoff: Arc<dyn SortMemoryHandoff>) -> Self;
```

This is a proposal, not an implemented or supported DataFusion API. Its producer
protocol must expose actual backing identity/ownership, not infer credit from a
consumer name. Required implementation points are:

1. Validate that accounted inputs belong to the same authoritative pool. Keep
   their owner alive with every retained input and alias. Reserve native sort
   workspace separately, with audited allowances for the selected layout.
   Unaccounted inputs keep the normal DataFusion admission path.
2. Pre-admit all additional copied output/workspace before sorting. A callback
   after allocation is too late to introduce a fallible reservation.
3. In `sort_batch_stream` / `ReservationStream`, split/transfer the existing
   output reservation to `output_backing` instead of freeing that portion before
   emission. Lance can then attach that owner through `retain_batch_owner`.
4. Release input and temporary workspace on completion/cancellation, while an
   emitted output's reservation survives all Arrow clones/slices. Test refusal,
   aliasing, errors, cancellation, and the final slice on one finite pool.

No local wrapper is claimed to replace these missing operator hooks. The
reservation interface remains experimental, and the human review's SortExec
merge gate remains open until the prerequisite is implemented and the strict
contract test passes. General decoders, external spill sorting and complete
index builds are outside this focused case.
