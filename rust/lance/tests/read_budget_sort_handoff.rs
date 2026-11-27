// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{RecordBatch, cast::AsArray, types::Int32Type};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use lance::{
    Dataset,
    dataset::{WriteParams, builder::DatasetBuilder},
    io::exec::{LanceScanConfig, LanceScanExec},
    session::Session,
};
use lance_core::utils::tempfile::TempStrDir;
use lance_file::version::LanceFileVersion;
use rstest::rstest;
use std::sync::Arc;

#[derive(Debug)]
struct StrictBuildPool {
    adapter: lance_datafusion::resources::DataFusionMemoryReserver,
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    denied: std::sync::Mutex<Option<(String, usize, usize)>>,
    peak: std::sync::atomic::AtomicUsize,
    output_control_bytes: std::sync::atomic::AtomicUsize,
    output_control_reservations: std::sync::atomic::AtomicUsize,
}
impl lance_core::utils::memory::MemoryReserver for StrictBuildPool {
    fn task_group(&self) -> Option<Arc<lance_core::utils::tasks::ResourceTaskGroup>> {
        self.adapter.task_group()
    }
    fn reserve(&self, stage: &str, bytes: usize) -> lance_core::Result<Box<dyn Send + Sync>> {
        let result = self.adapter.reserve(stage, bytes);
        if result.is_err() {
            *self.denied.lock().unwrap() = Some((stage.to_owned(), bytes, self.pool.reserved()));
        } else if stage == "inline allocation control" {
            // Record the sort output's control admission independently of the
            // pool total, so duplicate backing charges cannot become expected.
            self.output_control_bytes
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
            self.output_control_reservations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.peak
            .fetch_max(self.pool.reserved(), std::sync::atomic::Ordering::Relaxed);
        result
    }
    fn reserve_lease(
        &self,
        stage: &str,
        bytes: usize,
    ) -> lance_core::Result<Box<dyn lance_core::utils::memory::MemoryLease>> {
        let result = self.adapter.reserve_lease(stage, bytes);
        if result.is_err() {
            *self.denied.lock().unwrap() = Some((stage.to_owned(), bytes, self.pool.reserved()));
        }
        self.peak
            .fetch_max(self.pool.reserved(), std::sync::atomic::Ordering::Relaxed);
        result
    }
}

/// Custom ResourceSortStream proof; this is not the stock SortExec proof.
#[rstest]
#[case::hard_limit(false)]
#[case::last_slice(true)]
#[tokio::test]
async fn read_budget_sort_handoff_respects_build_pool(#[case] last_slice: bool) {
    const ROWS: usize = 32 * 1024;
    let directory = TempStrDir::default();
    let batches = (0..2)
        .map(|fragment| {
            let values = (fragment * ROWS..(fragment + 1) * ROWS)
                .rev()
                .map(|value| value as i32)
                .collect::<Vec<_>>();
            Ok(RecordBatch::try_from_iter([(
                "id",
                Arc::new(arrow_array::Int32Array::from(values)) as arrow_array::ArrayRef,
            )])
            .unwrap())
        })
        .collect::<Vec<_>>();
    let schema = batches[0].as_ref().unwrap().schema();
    let source = arrow_array::RecordBatchIterator::new(batches, schema);
    Dataset::write(
        source,
        directory.as_str(),
        Some(WriteParams {
            max_rows_per_file: ROWS,
            data_storage_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    // The refusal case fits one decoded Int32 allocation and the actual local
    // file/task baseline, but cannot hold two such decoded allocations.
    let limit = if last_slice {
        32 * 1024 * 1024
    } else {
        2 * ROWS * size_of::<i32>() - 1
    };
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
        datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
    );
    let tasks = Arc::new(lance_core::utils::tasks::ResourceTaskGroup::default());
    let adapter = Arc::new(StrictBuildPool {
        adapter: lance_datafusion::resources::DataFusionMemoryReserver::new(
            pool.clone(),
            tasks.clone(),
        ),
        pool: pool.clone(),
        denied: Default::default(),
        peak: Default::default(),
        output_control_bytes: Default::default(),
        output_control_reservations: Default::default(),
    });
    let read_policy =
        lance_io::read_budget::ReadMemoryBudget::new(1).with_memory_reserver(adapter.clone());
    let session = Arc::new(Session::default().with_read_memory_budget(read_policy));
    let dataset = Arc::new(
        DatasetBuilder::from_uri(directory.as_str())
            .with_session(session.clone())
            .load()
            .await
            .unwrap(),
    );
    let policy = session.read_budget().unwrap().clone();
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let scan = |fragment: usize| {
        LanceScanExec::new(
            dataset.clone(),
            Arc::new(vec![dataset.fragments()[fragment].clone()]),
            None,
            Arc::new(dataset.schema().project(&["id"]).unwrap()),
            LanceScanConfig {
                batch_size: ROWS,
                batch_readahead: 1,
                fragment_readahead: Some(1),
                ..Default::default()
            },
        )
    };
    let first_plan = scan(0);
    let mut first = first_plan.execute(0, context.clone()).unwrap();
    let batch = first.try_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), ROWS);
    let scan_backing = batch.get_array_memory_size();
    assert!(first.try_next().await.unwrap().is_none());
    drop(first);
    drop(first_plan);
    tasks.wait().await;
    assert_eq!(policy.reserved_bytes(), 0);
    let retained = pool.reserved();
    assert_eq!(
        retained,
        scan_backing + 1024,
        "scan backing must retain exactly one hard charge"
    );
    let mut sorter =
        lance_datafusion::resource_sort::ResourceSortStream::try_new(adapter.clone()).unwrap();
    let directory_charge = pool.reserved() - retained;
    let mut evidence = serde_json::json!({
        "hard_limit_bytes": limit, "soft_limit_bytes": policy.limit(),
        "retained_scan_bytes": retained, "sort_directory_bytes": directory_charge,
    });
    sorter.push(batch).unwrap();
    assert_eq!(
        pool.reserved(),
        retained + directory_charge,
        "sort input must not be charged twice"
    );
    if last_slice {
        let sorted = sorter.finish().await.unwrap();
        assert_eq!(sorted.num_rows(), ROWS);
        let ids = sorted["id"].as_primitive::<Int32Type>();
        assert!(ids.values().iter().copied().eq(0..ROWS as i32));
        let output_backing = sorted.get_array_memory_size();
        let output_control = adapter
            .output_control_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            adapter
                .output_control_reservations
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the sorted output must have one control reservation"
        );
        assert!(output_control > 0);
        let expected_charge = output_backing.checked_add(output_control).unwrap();
        assert_eq!(
            pool.reserved(),
            expected_charge,
            "sorted backing must be charged exactly once, plus admitted control"
        );
        let slice = sorted.slice(1, 1);
        evidence["sorted_output_backing_bytes"] = serde_json::json!(output_backing);
        evidence["sorted_output_control_bytes"] = serde_json::json!(output_control);
        evidence["sorted_output_and_control_bytes"] = serde_json::json!(expected_charge);
        drop(sorted);
        drop(dataset);
        drop(session);
        drop(context);
        tasks.wait().await;
        assert_eq!(
            pool.reserved(),
            expected_charge,
            "the final slice must retain that same single backing charge"
        );
        assert_eq!(slice["id"].as_primitive::<Int32Type>().value(0), 1);
        drop(slice);
        assert_eq!(pool.reserved(), 0);
    } else {
        let attempted_plan = scan(1);
        let mut attempted = attempted_plan.execute(0, context.clone()).unwrap();
        let error = attempted.try_next().await.unwrap_err();
        assert!(
            error.to_string().contains("strict scan decoded backing"),
            "{error}"
        );
        drop(attempted);
        drop(attempted_plan);
        tasks.wait().await;
        let (stage, bytes, baseline) = adapter.denied.lock().unwrap().clone().unwrap();
        assert_eq!(stage, "strict scan decoded backing");
        evidence["refused_stage"] = serde_json::json!(stage);
        evidence["attempted_decoded_allowance_bytes"] = serde_json::json!(bytes);
        evidence["baseline_at_refusal_bytes"] = serde_json::json!(baseline);
        assert!(baseline.checked_add(bytes).unwrap() > limit);
        assert!(
            policy.stats().peak_reserved_bytes > policy.limit(),
            "the real read policy must have admitted through overdraft"
        );
        assert_eq!(
            policy.reserved_bytes(),
            0,
            "hard denial must release its soft ticket and overdraft"
        );
        assert_eq!(pool.reserved(), retained + directory_charge);
        drop(sorter);
        assert_eq!(pool.reserved(), 0);
        let retry_plan = scan(1);
        let mut retry = retry_plan.execute(0, context.clone()).unwrap();
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
        tasks.wait().await;
        assert_eq!(policy.reserved_bytes(), 0);
        assert_eq!(pool.reserved(), 0);
    }
    assert!(adapter.peak.load(std::sync::atomic::Ordering::Relaxed) <= limit);
    evidence["hard_peak_bytes"] =
        serde_json::json!(adapter.peak.load(std::sync::atomic::Ordering::Relaxed));
    evidence["soft_peak_bytes"] = serde_json::json!(policy.stats().peak_reserved_bytes);
    // Optional local evidence output records the actual allowances without
    // printing or pinning implementation layout constants in assertions.
    if let Ok(directory) = std::env::var("LANCE_HANDOFF_EVIDENCE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(if last_slice {
                "last-slice.json"
            } else {
                "hard-limit.json"
            }),
            serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
    }
}

#[path = "read_budget_sort_handoff/sortexec.rs"]
mod sortexec;
