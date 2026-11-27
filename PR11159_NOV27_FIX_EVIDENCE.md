# PR #11159 — November 27 follow-up

## Status

The two main-PR bugs are fixed and regression-tested. **Stock SortExec ownership
is not proven and remains a merge prerequisite.** The real operator passes the
hard-refusal/retry case, but its final-slice contract fails: a live 131,168-byte
output has zero retained reservation after emission and pipeline teardown.
The earlier custom-sorter results are historical evidence for that different
consumer; they are not SortExec results.

## Revisions

- Reviewed main PR: `fbeeea7c6f5f86e456994ab7a130ced4b95fadd8`.
- Main PR base: `1686234925dde0289409fa31ff615d724943ccb6`.
- Completion fix: `d1616fcea7725695c4335c0aaa3e0f25370fea3e`.
- Initial-directory admission fix / final main-PR code head:
  `65c530a6597129594a289942d8e8eeffe6642ac3`.
- Previous published proof: `9885d42daeea9fb92617fd941c03be20c1a2610b`.
- Both fixes were cherry-picked onto `test/pr11159-read-sort-handoff` as
  `b1e24a5404a49c3c026db620fcb131945ee75258` and
  `c5ebd554a92aaf5cb0abdc21b6fb8954c0023841`.
- Proof-stack lint cleanup: `e2026df106dbe22a73db18367c6ab08309b2737b`.
- Actual SortExec prerequisite regression and API proposal:
  `d3d6c3e34eeabbca0578b1f4f46be9ad5ea69601`.
- Read policy: PR #10802 at `c908caf6f17e7cca8f26027c6581f83983d9a6d6`.
- DataFusion: 55.2.0 in both stacks. No dependency changes were introduced.

## Main fixes and failure-before controls

The I/O and CPU drivers now retain the task token in the outer completion
owner. Native work and unclaimed results are disposed first; completion control
is released next; the task token is released last, including on unwind.
`wait()` still permits caller-owned output buffers to remain charged.

The deterministic core observer identifies `I/O completion future` and
`CPU completion future` by stage, and records whether the task is active when
that reservation is dropped. All 12 success/error/panic and cancelled-await
cases fail on the old ordering and pass on the fix. No 64-byte layout is pinned.
The existing local queued-read test retains its immediate zero assertion;
it passed 20 repetitions normally and 20 with `LANCE_CPU_THREADS=1`, without
adding sleeps, yields, or polling-until-zero.

First-time spill directory initialization, canonicalization, file/backing/path
construction, and file creation now run in the admitted blocking closure.
Shared initialization remains serialized inside blocking work. Configured and
unconfigured creation use the same native closure. Refused dispatch cannot
initialize the scratch slot or create its previously absent parent.

The extended create/open refusal matrix covers all three I/O control stages
and group cancellation. With old production code, six fresh-create cases fail
the absent-parent assertion; six open cases pass. With the fix, all 12 pass,
including first-spill retry after transient refusal. The full suite also covers
shared directories, quota/error cleanup, and queued/already-started create/open
cancellation.

## Actual SortExec observations

The two-fragment V2.1 fixture contains 32,768 descending Int32 values per
fragment. `SortExec::new(ordering, scan_input)` consumes fragment 0. A forwarding
stream moves the batch directly into the operator, drains the real scan's EOF,
then asynchronously withholds EOF from SortExec. The test never collects or
clones the retained batch, and no CPU worker waits on a test signal.

One finite GreedyMemoryPool is shared by the scan adapter and TaskContext.
The observer delegates every native and adapter grow/shrink operation to that
pool. It neither suppresses native reservations nor subtracts consumer names.
Its peaks include actual SortExec reservations.

| Hard-refusal/retry measurement | Bytes |
| --- | ---: |
| Hard limit | 524,287 |
| Soft read-policy limit | 1 |
| Input backing, measured before forwarding | 131,072 |
| Producer charge, including array/owner control | 132,192 |
| Native ExternalSorter reservation | 262,144 |
| Total retained while paused | 394,336 |
| Baseline at decoded-admission refusal | 402,832 |
| Requested decoded allowance | 135,168 |
| Baseline plus request | 538,000 |
| Observed peak including native reservations | 412,224 |
| Soft-policy peak | 197,280 |

The failed read releases its soft ticket. Aborting and awaiting the paused
pipeline releases native and producer reservations. The identical fragment-1
read succeeds on the same pool, session and limits; returned values are checked,
and both counters return to zero.

Native accounting is not relabelled as arbitrary extra workspace. The pinned
operator reserves input backing plus sliced-size allowance: 131,072 + 131,072.
Its first term overlaps the already-owned producer backing. No supported API
exposes adoption of that ownership.

In the separate output case, actual SortExec returns correctly sorted values.
The expected full output allocation/control is independently derived from Arrow
as 131,168 bytes; stock SortExec adds no independent output-owner control.
After retaining one slice and dropping output roots, plans, streams, session,
runtime and pending work, the pool reports **0**, not 131,168. The strict test
therefore fails normally and in one-CPU mode. The observed native-inclusive
output-case peak is 394,336 bytes.

A temporary negative control added a duplicate 262,336-byte charge. The same
independent assertion rejected it against 131,168. That control was removed
before committing; no artificial reservation remains in the test.

`PR11159_SORTEXEC_HANDOFF_PREREQUISITE.md` documents the private ExternalSorter /
ReservationStream boundary and proposes explicit input-adoption and output-lease
transfer hooks. ReservationStream currently shrinks before emission. A wrapper
alone is not claimed to solve this. The interface remains experimental; general
decoders, external spill sorting and complete index builds remain outside scope.

## Validation and limits

All builds ran serially with `CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0`, Rust
1.98.1 and repository profiles. No benchmarks or large datasets were run.

- `cargo fmt --all`: passed.
- Main: `cargo test -p lance-core -p lance-io --lib --locked --profile ci`:
  678 core and 893 I/O cases passed.
- Main, `LANCE_CPU_THREADS=1 ... resource_`: 26 core and 76 I/O cases passed.
- Main core/I/O doctests: 50 and 38 passed; three and one pre-existing ignored
  cases respectively.
- Main `cargo clippy -p lance-core -p lance-io --tests --benches --locked -- -D warnings`:
  passed.
- Main production workspace check and Clippy (`--workspace/--all --lib --bins`):
  passed.
- Proof core/I/O suites: 678 and 912 cases passed, including all 19 existing
  read-budget policy cases.
- Proof `lance-datafusion-exec --lib`: all 80 cases passed.
- Proof handoff target, normal and `LANCE_CPU_THREADS=1`: **3 passed, 1 failed**.
  Two passes are the older custom-sorter cases; one is actual SortExec refusal
  and retry. The failure is the intentionally strict upstream prerequisite.
- Proof handoff-target Clippy and core/I/O/DataFusion component test/bench Clippy:
  passed.
- Proof production-workspace check and Clippy (`--workspace/--all --lib --bins`):
  passed. An unused import inherited from the earlier read-budget test port
  was removed after the full check identified it.

The required full workspace test/bench check and Clippy were attempted with a
7 GiB task-RSS guard to protect this 15 GiB workstation. Main attempts stopped
at 7,516,807,168 and 7,605,829,632 bytes respectively. These are resource stops,
not passing checks. Proof-stack attempts similarly stopped at 7,560,036,352
and 7,629,062,144 bytes. Selected/production passes do not establish a
full-workspace pass. Windows runtime coverage remains for CI; portable tests and file/drop-order
coverage are retained.

Local logs/results: `/tmp/pr11159-nov27-*`. Native accounting snapshots:
`/tmp/pr11159-nov27-sortexec-evidence/`. Historical November 26 evidence was
preserved. No independent PR or alternate pool was introduced. Review comments
have not been posted; the SortExec merge gate remains open.

## Publication and reproduction

The main-PR branch was pushed at
[`65c530a65`](https://github.com/kamronis/lance/commit/65c530a6597129594a289942d8e8eeffe6642ac3).
The actual SortExec prerequisite test is
[`d3d6c3e34`](https://github.com/kamronis/lance/commit/d3d6c3e34eeabbca0578b1f4f46be9ad5ea69601)
on the existing `test/pr11159-read-sort-handoff` branch. It deliberately leaves
the strict failing contract visible. No merge or review-thread resolution is
claimed.

From the proof worktree, with the build environment described above:

```sh
cargo test -p lance --test read_budget_sort_handoff --locked --profile ci
LANCE_CPU_THREADS=1 cargo test -p lance --test read_budget_sort_handoff --locked --profile ci
```

Both commands return 101: three passes and the same single final-slice failure.
For only the green actual-operator refusal/retry characterization:

```sh
cargo test -p lance --test read_budget_sort_handoff --locked --profile ci sortexec_stock_scan_overdraft_refusal_and_retry
```

Set `LANCE_HANDOFF_EVIDENCE_DIR` to export the native-inclusive accounting JSON.
The earlier `read_budget_sort_handoff_respects_build_pool` cases use the custom
consumer and remain labelled accordingly.
