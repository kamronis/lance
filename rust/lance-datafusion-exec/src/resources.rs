// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Experimental bridge to one existing DataFusion pool, without another budget.

use arrow::array::{ArrayData, make_array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use std::{panic::AssertUnwindSafe, sync::Arc};

use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use lance_core::{
    Error, Result,
    utils::{
        memory::{MemoryLease, MemoryReserver},
        tasks::ResourceTaskGroup,
    },
};

/// Experimental adapter. Every returned owner uses the supplied pool directly.
#[derive(Debug)]
#[doc(hidden)]
pub struct DataFusionMemoryReserver {
    pool: Arc<dyn MemoryPool>,
    tasks: Arc<ResourceTaskGroup>,
}

impl DataFusionMemoryReserver {
    /// Reuse the operation's pool; this constructor creates no independent cap.
    pub fn new(pool: Arc<dyn MemoryPool>, tasks: Arc<ResourceTaskGroup>) -> Self {
        Self { pool, tasks }
    }
}

struct DataFusionLease(MemoryReservation);
impl MemoryLease for DataFusionLease {
    fn try_shrink(&mut self, bytes: usize) -> Result<bool> {
        let reserved = self.0.size();
        if bytes > reserved {
            return Err(Error::invalid_input(format!(
                "memory lease cannot grow: requested_bytes={bytes}, reserved_bytes={reserved}"
            )));
        }
        self.0.shrink(reserved - bytes);
        Ok(true)
    }
}
impl MemoryReserver for DataFusionMemoryReserver {
    fn task_group(&self) -> Option<Arc<ResourceTaskGroup>> {
        Some(self.tasks.clone())
    }
    fn reserve_lease(&self, stage: &str, bytes: usize) -> Result<Box<dyn MemoryLease>> {
        let reservation = MemoryConsumer::new(stage).register(&self.pool);
        reservation
            .try_grow(bytes)
            .map_err(|source| Error::External {
                source: Box::new(source),
            })?;
        Ok(Box::new(DataFusionLease(reservation)))
    }
    fn reserve(&self, stage: &str, bytes: usize) -> Result<Box<dyn Send + Sync>> {
        let reservation = MemoryConsumer::new(stage).register(&self.pool);
        reservation
            .try_grow(bytes)
            .map_err(|source| Error::External {
                source: Box::new(source),
            })?;
        Ok(Box::new(reservation))
    }
}

/// Retain one existing backing owner through every Arrow buffer and slice.
/// This adds no reservation and does not copy the data. The caller must have
/// admitted the sidecar/control allowance before calling this function. The
/// integration proof covers flat Int32 backing, not arbitrary decoder layouts.
#[doc(hidden)]
pub fn retain_batch_owner(
    batch: arrow_array::RecordBatch,
    owner: Arc<dyn std::any::Any + Send + Sync>,
) -> Result<arrow_array::RecordBatch> {
    struct OwnedBuffer {
        _buffer: Buffer,
        _owner: AssertUnwindSafe<Arc<dyn std::any::Any + Send + Sync>>,
    }
    fn wrap(buffer: &Buffer, owner: &Arc<dyn std::any::Any + Send + Sync>) -> Result<Buffer> {
        let ptr = std::ptr::NonNull::new(buffer.as_ptr().cast_mut())
            .ok_or_else(|| Error::internal("Arrow backing pointer is null"))?;
        // SAFETY: the owner retains the immutable original buffer, including its
        // allocation and offset, for the entire lifetime of this view.
        Ok(unsafe {
            Buffer::from_custom_allocation(
                ptr,
                buffer.len(),
                Arc::new(OwnedBuffer {
                    _buffer: buffer.clone(),
                    _owner: AssertUnwindSafe(owner.clone()),
                }),
            )
        })
    }
    fn data(value: ArrayData, owner: &Arc<dyn std::any::Any + Send + Sync>) -> Result<ArrayData> {
        let buffers = value
            .buffers()
            .iter()
            .map(|b| wrap(b, owner))
            .collect::<Result<Vec<_>>>()?;
        let children = value
            .child_data()
            .iter()
            .map(|child| data(child.clone(), owner))
            .collect::<Result<Vec<_>>>()?;
        let nulls = value
            .nulls()
            .map(|nulls| {
                wrap(nulls.inner().inner(), owner).map(|buffer| {
                    NullBuffer::new(BooleanBuffer::new(buffer, nulls.offset(), nulls.len()))
                })
            })
            .transpose()?;
        Ok(value
            .into_builder()
            .buffers(buffers)
            .child_data(children)
            .nulls(nulls)
            .build()?)
    }
    let columns = batch
        .columns()
        .iter()
        .map(|array| data(array.to_data(), &owner).map(make_array))
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow_array::RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    #[test]
    fn scratch_serialization_shares_the_stage_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(80));
        let adapter = DataFusionMemoryReserver::new(pool.clone(), Arc::default());
        let external = MemoryConsumer::new("DataFusion operator").register(&pool);
        external.try_grow(32).unwrap();
        let error = adapter.reserve("scratch serialization", 64).err().unwrap();
        assert!(matches!(error, Error::External { .. }));
        assert!(error.to_string().contains("scratch serialization"));
        assert_eq!(pool.reserved(), 32);
        drop(external);
        let mut lease = adapter.reserve_lease("scratch serialization", 64).unwrap();
        assert_eq!(pool.reserved(), 64);
        assert!(lease.try_shrink(8).unwrap());
        assert_eq!(pool.reserved(), 8);
        drop(lease);
        assert_eq!(pool.reserved(), 0);
    }
}
