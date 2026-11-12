use std::future::Future;

use crate::persist::event_stream::ReplayStream;
use crate::persist::{PersistenceError, SerializedEvent, SerializedSnapshot};
use crate::Aggregate;
use serde_json::Value;

/// Handles the database access needed for operation of a PersistedSnapshotStore.
pub trait PersistedEventRepository: Send + Sync {
    /// Returns all events for a single aggregate instance.
    fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send;

    /// Returns the last events for a single aggregate instance.
    fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send;

    /// Returns the current snapshot for an aggregate instance.
    fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, PersistenceError>> + Send;

    /// Commits the updated aggregate and accompanying events.
    fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send;

    /// Commits events with the snapshot's explicit covered sequence and version.
    ///
    /// Snapshot-aware repositories should override this method to persist
    /// `current_sequence` independently of the command batch's final sequence.
    /// The default adapter preserves compatibility with existing repositories
    /// by forwarding the legacy snapshot tuple to `persist`.
    fn persist_with_snapshot<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send {
        self.persist::<A>(
            events,
            snapshot_update.map(|snapshot| {
                (
                    snapshot.aggregate_id,
                    snapshot.aggregate,
                    snapshot.current_snapshot,
                )
            }),
        )
    }

    /// Streams all events for an aggregate instance.
    fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send;

    /// Streams all events for an aggregate type.
    fn stream_all_events<A: Aggregate>(
        &self,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send;
}
