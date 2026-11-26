// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Experimental in-memory run of the planned resource-aware scalar sorter.
//! Extracted from prototype 34c96d09f scalar/sort/{stream,batch}.rs: retain
//! input, admit consolidation/sort workspace, and run DataFusion sort_batch.
//! External spill runs and complete index trainers are outside this proof stack.

use crate::resources::retain_batch_owner;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion_physical_plan::sorts::sort::sort_batch;
use lance_core::{
    Error, Result,
    utils::{memory::MemoryReserver, tokio::MemoryOwned},
};
use std::sync::{Arc, Mutex};

/// Bounded retained input for the in-memory sort run; inputs already own backing.
/// `push` retains those same owners instead of charging their buffers again.
#[doc(hidden)]
pub struct ResourceSortStream {
    batches: Vec<RecordBatch>,
    pool: Arc<dyn MemoryReserver>,
    control: Box<dyn Send + Sync>,
}
impl ResourceSortStream {
    /// Admit the fixed run directory before allocating it. Large runs must spill
    /// in the complete trainer; this focused consumer refuses them explicitly.
    pub fn try_new(pool: Arc<dyn MemoryReserver>) -> Result<Self> {
        // Include the retained-input Arc and directory ownership metadata.
        let control = pool.reserve("sort run directory", 16 * size_of::<RecordBatch>() + 1024)?;
        Ok(Self {
            batches: Vec::with_capacity(16),
            pool,
            control,
        })
    }
    /// Retain already-accounted scan backing without copying or reserving it twice.
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.batches.len() == self.batches.capacity() {
            return Err(Error::execution(
                "in-memory sort run requires spilling: batch_limit=16",
            ));
        }
        self.batches.push(batch);
        Ok(())
    }
    /// Sort the admitted run by its first column. Temporary work and copied
    /// output use the same pool, and independent output carries its own lease.
    pub async fn finish(self) -> Result<RecordBatch> {
        let schema = self
            .batches
            .first()
            .ok_or_else(|| Error::invalid_input("cannot sort an empty run"))?
            .schema();
        let bytes = self.batches.iter().try_fold(0usize, |n, batch| {
            n.checked_add(batch.get_array_memory_size())
                .ok_or_else(|| Error::invalid_input("sort backing size overflow"))
        })?;
        let rows = self.batches.iter().try_fold(0usize, |n, batch| {
            n.checked_add(batch.num_rows())
                .ok_or_else(|| Error::invalid_input("sort row count overflow"))
        })?;
        u32::try_from(rows)
            .map_err(|_| Error::invalid_input("sort row count exceeds UInt32 indices"))?;
        // Preserve the prototype's conservative flat-batch sort allowance.
        let workspace = bytes
            .checked_mul(16)
            .and_then(|n| rows.checked_mul(256).and_then(|rows| n.checked_add(rows)))
            .and_then(|n| {
                schema
                    .fields()
                    .len()
                    .checked_mul(32768)
                    .and_then(|columns| n.checked_add(columns))
            })
            .and_then(|n| n.checked_add(131072))
            .ok_or_else(|| Error::invalid_input("sort workspace size overflow"))?;
        // The actual CPU task must retain the directory charge as well as its
        // batches when the public finish future is cancelled.
        let inputs = Arc::new((self.batches, self.control));
        let cpu_inputs = inputs.clone();
        let prepared = MemoryOwned::<Option<RecordBatch>>::try_new(
            self.pool.as_ref(),
            "sort output",
            workspace,
            || Ok(None),
        )?;
        let mut result = prepared
            .into_cpu_task(self.pool, "sort run", 4096, inputs, move |_| {
                let combined = concat_batches(&schema, cpu_inputs.0.iter())?;
                let ordering = LexOrdering::new(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new(schema.field(0).name(), 0)),
                    options: Default::default(),
                }])
                .ok_or_else(|| Error::internal("sort ordering unexpectedly empty"))?;
                Ok(sort_batch(&combined, &ordering, None)?)
            })?
            .await?;
        result.try_shrink_workspace(result.value().get_array_memory_size())?;
        let (batch, resources) = result.into_parts();
        retain_batch_owner(batch, Arc::new(Mutex::new(resources)))
    }
}
