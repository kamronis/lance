// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Stock SortExec characterization and the outstanding ownership contract.
//! These tests do not replace the older custom-sorter proof.

use super::*;
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    execution::{
        config::SessionConfig,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{
            GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
        },
    },
    physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
        sorts::sort::SortExec, stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, stream};
use std::{collections::BTreeMap, fmt, sync::Mutex};
use tokio::sync::Notify;

const ROWS: usize = 32 * 1024;

#[derive(Debug, Default)]
struct Observations {
    live: BTreeMap<String, usize>,
    peak: usize,
    denied: Vec<(String, usize, usize)>,
}

/// Observes every native and adapter operation; GreedyMemoryPool remains the
/// sole authority. No reservation is suppressed, credited or renamed.
#[derive(Debug)]
struct ObservedPool {
    inner: GreedyMemoryPool,
    observations: Mutex<Observations>,
}
impl fmt::Display for ObservedPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObservedGreedyPool")
    }
}
impl MemoryPool for ObservedPool {
    fn name(&self) -> &str {
        "ObservedGreedyPool"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        let mut state = self.observations.lock().unwrap();
        self.inner.grow(reservation, bytes);
        *state
            .live
            .entry(reservation.consumer().name().to_owned())
            .or_default() += bytes;
        state.peak = state.peak.max(self.inner.reserved());
    }
    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        bytes: usize,
    ) -> datafusion::error::Result<()> {
        let mut state = self.observations.lock().unwrap();
        let baseline = self.inner.reserved();
        let result = self.inner.try_grow(reservation, bytes);
        if result.is_ok() {
            *state
                .live
                .entry(reservation.consumer().name().to_owned())
                .or_default() += bytes;
            state.peak = state.peak.max(self.inner.reserved());
        } else {
            state
                .denied
                .push((reservation.consumer().name().to_owned(), bytes, baseline));
        }
        result
    }
    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        let mut state = self.observations.lock().unwrap();
        self.inner.shrink(reservation, bytes);
        *state
            .live
            .entry(reservation.consumer().name().to_owned())
            .or_default() -= bytes;
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[derive(Debug, Default)]
struct InputSizes {
    backing: std::sync::atomic::AtomicUsize,
    array: std::sync::atomic::AtomicUsize,
}

/// After forwarding one batch, signal only when SortExec asks for its next
/// input. At that point insertion/reservation has completed inside SortExec.
/// The test owns no retained batch and never parks a CPU worker.
#[derive(Debug)]
struct PausedScan {
    input: Arc<dyn ExecutionPlan>,
    consumed: Arc<Notify>,
    sizes: Arc<InputSizes>,
}
impl DisplayAs for PausedScan {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PausedScan")
    }
}
impl ExecutionPlan for PausedScan {
    fn name(&self) -> &str {
        "PausedScan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: children.remove(0),
            consumed: self.consumed.clone(),
            sizes: self.sizes.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let consumed = self.consumed.clone();
        let sizes = self.sizes.clone();
        let forwarded = stream::unfold((input, false), move |(mut input, forwarded)| {
            let consumed = consumed.clone();
            let sizes = sizes.clone();
            async move {
                if forwarded {
                    // Drain the real scan's EOF, releasing its soft ticket,
                    // but withhold EOF from SortExec, which still owns the batch.
                    assert!(input.next().await.is_none());
                    drop(input);
                    consumed.notify_one();
                    return std::future::pending().await;
                }
                let next = input.next().await;
                if let Some(Ok(batch)) = &next {
                    sizes.backing.store(
                        datafusion::common::utils::memory::get_record_batch_memory_size(batch),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    sizes.array.store(
                        batch.get_array_memory_size(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                next.map(|batch| (batch, (input, true)))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            forwarded,
        )))
    }
}

struct StockFixture {
    dataset: Arc<Dataset>,
    session: Arc<Session>,
    context: Arc<TaskContext>,
    pool: Arc<ObservedPool>,
    tasks: Arc<lance_core::utils::tasks::ResourceTaskGroup>,
    _directory: TempStrDir,
}
impl StockFixture {
    async fn new(limit: usize) -> Self {
        let directory = TempStrDir::default();
        let batches = (0..2)
            .map(|fragment| {
                let values = (fragment * ROWS..(fragment + 1) * ROWS)
                    .rev()
                    .map(|n| n as i32)
                    .collect::<Vec<_>>();
                arrow_array::record_batch!(("id", Int32, values))
            })
            .collect::<Vec<_>>();
        let schema = batches[0].as_ref().unwrap().schema();
        Dataset::write(
            arrow_array::RecordBatchIterator::new(batches, schema),
            directory.as_str(),
            Some(WriteParams {
                max_rows_per_file: ROWS,
                data_storage_version: Some(LanceFileVersion::V2_1),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let pool = Arc::new(ObservedPool {
            inner: GreedyMemoryPool::new(limit),
            observations: Mutex::default(),
        });
        let tasks = Arc::new(lance_core::utils::tasks::ResourceTaskGroup::default());
        let adapter = Arc::new(lance_datafusion::resources::DataFusionMemoryReserver::new(
            pool.clone(),
            tasks.clone(),
        ));
        let policy = lance_io::read_budget::ReadMemoryBudget::new(1).with_memory_reserver(adapter);
        let session = Arc::new(Session::default().with_read_memory_budget(policy));
        let dataset = Arc::new(
            DatasetBuilder::from_uri(directory.as_str())
                .with_session(session.clone())
                .load()
                .await
                .unwrap(),
        );
        let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .unwrap();
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(runtime)
                .with_session_config(
                    SessionConfig::new()
                        .with_batch_size(ROWS)
                        .with_sort_spill_reservation_bytes(0),
                ),
        );
        Self {
            _directory: directory,
            dataset,
            session,
            context,
            pool,
            tasks,
        }
    }
    fn scan(&self, fragment: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(LanceScanExec::new(
            self.dataset.clone(),
            Arc::new(vec![self.dataset.fragments()[fragment].clone()]),
            None,
            Arc::new(self.dataset.schema().project(&["id"]).unwrap()),
            LanceScanConfig {
                batch_size: ROWS,
                batch_readahead: 1,
                fragment_readahead: Some(1),
                ..Default::default()
            },
        ))
    }
    fn sort(input: Arc<dyn ExecutionPlan>) -> Arc<SortExec> {
        Arc::new(SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("id", 0)),
                options: Default::default(),
            }])
            .unwrap(),
            input,
        ))
    }
}

fn evidence(name: &str, value: serde_json::Value) {
    if let Ok(directory) = std::env::var("LANCE_HANDOFF_EVIDENCE_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            std::path::Path::new(&directory).join(name),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn sortexec_stock_scan_overdraft_refusal_and_retry() {
    let limit = 4 * ROWS * size_of::<i32>() - 1;
    let fixture = StockFixture::new(limit).await;
    let policy = fixture.session.read_budget().unwrap().clone();
    let consumed = Arc::new(Notify::new());
    let sizes = Arc::new(InputSizes::default());
    let input = Arc::new(PausedScan {
        input: fixture.scan(0),
        consumed: consumed.clone(),
        sizes: sizes.clone(),
    });
    let sort = StockFixture::sort(input);
    let mut stream = sort.execute(0, fixture.context.clone()).unwrap();
    let mut pending = tokio::spawn(async move { stream.try_next().await });
    tokio::select! {
        () = consumed.notified() => {},
        result = &mut pending => panic!("SortExec ended before retaining input: {result:?}"),
    }
    fixture.tasks.wait().await;
    let retained = fixture.pool.reserved();
    let native = fixture
        .pool
        .observations
        .lock()
        .unwrap()
        .live
        .get("ExternalSorter[0]")
        .copied()
        .unwrap();
    let input_backing = sizes.backing.load(std::sync::atomic::Ordering::Relaxed);
    let producer_charge = sizes.array.load(std::sync::atomic::Ordering::Relaxed) + 1024;
    assert_eq!(input_backing, ROWS * size_of::<i32>());
    // Pin the stock algorithm's two terms; do not relabel their sum as workspace.
    assert_eq!(native, input_backing + ROWS * size_of::<i32>());
    assert_eq!(retained, producer_charge + native);
    let attempted_plan = fixture.scan(1);
    let mut attempted = attempted_plan.execute(0, fixture.context.clone()).unwrap();
    let error = attempted.try_next().await.unwrap_err();
    assert!(
        matches!(error, datafusion::error::DataFusionError::External(_)),
        "{error}"
    );
    assert!(
        error.to_string().contains("strict scan decoded backing"),
        "{error}"
    );
    drop(attempted);
    drop(attempted_plan);
    fixture.tasks.wait().await;
    assert_eq!(policy.reserved_bytes(), 0);
    let (stage, bytes, baseline) = fixture
        .pool
        .observations
        .lock()
        .unwrap()
        .denied
        .last()
        .cloned()
        .unwrap();
    assert_eq!(stage, "strict scan decoded backing");
    assert!(baseline + bytes > limit);
    assert!(policy.stats().peak_reserved_bytes > policy.limit());
    assert_eq!(fixture.pool.reserved(), retained);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    drop(sort);
    fixture.tasks.wait().await;
    assert_eq!(fixture.pool.reserved(), 0);
    let retry_plan = fixture.scan(1);
    let mut retry = retry_plan.execute(0, fixture.context.clone()).unwrap();
    let batch = retry.try_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), ROWS);
    assert_eq!(
        batch["id"].as_primitive::<Int32Type>().value(0),
        (2 * ROWS - 1) as i32
    );
    assert!(retry.try_next().await.unwrap().is_none());
    drop(batch);
    drop(retry);
    drop(retry_plan);
    fixture.tasks.wait().await;
    assert_eq!(policy.reserved_bytes(), 0);
    assert_eq!(fixture.pool.reserved(), 0);
    let state = fixture.pool.observations.lock().unwrap();
    assert!(state.peak <= limit);
    evidence(
        "sortexec-hard-limit.json",
        serde_json::json!({"limit":limit,"retained_with_native_sorter":retained,"native_sorter_reservation":native,"input_backing":input_backing,"producer_charge":producer_charge,"refused_stage":stage,"attempted_bytes":bytes,"baseline":baseline,"peak_including_native":state.peak,"soft_peak":policy.stats().peak_reserved_bytes}),
    );
}

/// Intentionally failing prerequisite: stock SortExec releases its reservation
/// at emission; no supported API transfers it into the Arrow backing owner.
/// Keep this assertion strict until the upstream ownership hook exists.
#[tokio::test]
async fn sortexec_last_slice_requires_upstream_reservation_handoff() {
    let fixture = StockFixture::new(32 * 1024 * 1024).await;
    let plan = StockFixture::sort(fixture.scan(0));
    let mut output = plan.execute(0, fixture.context.clone()).unwrap();
    let batch = output.try_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), ROWS);
    assert!(
        batch["id"]
            .as_primitive::<Int32Type>()
            .values()
            .iter()
            .copied()
            .eq(0..ROWS as i32)
    );
    let backing = batch.get_array_memory_size();
    // Stock SortExec attaches no independent output owner/control reservation.
    let output_control = 0;
    let slice = batch.slice(1, 1);
    drop(batch);
    assert!(output.try_next().await.unwrap().is_none());
    drop(output);
    drop(plan);
    let pool = fixture.pool.clone();
    fixture.tasks.wait().await;
    drop(fixture);
    assert_eq!(slice["id"].as_primitive::<Int32Type>().value(0), 1);
    let retained = pool.reserved();
    let state = pool.observations.lock().unwrap();
    evidence(
        "sortexec-last-slice-prerequisite.json",
        serde_json::json!({"expected_output_backing":backing,"output_control":output_control,"actual_retained_charge":retained,"peak_including_native":state.peak,"native_consumers":state.live,"prerequisite":"SortExec must transfer its output reservation into an Arrow backing owner before emission"}),
    );
    drop(state);
    drop(slice);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(
        retained,
        backing + output_control,
        "missing SortExec output ownership handoff: the live slice must retain one full backing charge"
    );
}
