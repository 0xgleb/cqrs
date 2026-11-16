use sqlx::{AssertSqlSafe, SqlSafeStr, SqlStr};

pub(crate) struct SqlQueryFactory {
    event_table: SqlStr,
    select_events: SqlStr,
    insert_event: SqlStr,
    all_events: SqlStr,
    insert_snapshot: SqlStr,
    update_snapshot: SqlStr,
    select_snapshot: SqlStr,
}

impl SqlQueryFactory {
    pub fn new(
        event_table: impl SqlSafeStr,
        snapshot_table: impl SqlSafeStr,
        snapshot_counter: impl SqlSafeStr,
    ) -> Self {
        let event_table = event_table.into_sql_str();
        let snapshot_table = snapshot_table.into_sql_str();
        let snapshot_counter = snapshot_counter.into_sql_str();
        let select_events = AssertSqlSafe(format!(
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {}
  WHERE aggregate_type = $1 AND aggregate_id = $2
  ORDER BY sequence",
            event_table.as_str()
        ))
        .into_sql_str();
        let insert_event = AssertSqlSafe(format!(
            "
INSERT INTO {} (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            event_table.as_str()
        ))
        .into_sql_str();
        let all_events = AssertSqlSafe(format!(
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {}
  WHERE aggregate_type = $1
  ORDER BY sequence",
            event_table.as_str()
        ))
        .into_sql_str();
        let insert_snapshot = AssertSqlSafe(format!(
            "
INSERT INTO {} (aggregate_type, aggregate_id, last_sequence, {}, payload)
VALUES ($1, $2, $3, $4, $5)",
            snapshot_table.as_str(),
            snapshot_counter.as_str()
        ))
        .into_sql_str();
        let update_snapshot = AssertSqlSafe(format!(
            "
UPDATE {}
  SET last_sequence= $3 , payload= $6, {}= $4
  WHERE aggregate_type= $1 AND aggregate_id= $2 AND {}= $5",
            snapshot_table.as_str(),
            snapshot_counter.as_str(),
            snapshot_counter.as_str()
        ))
        .into_sql_str();
        let select_snapshot = AssertSqlSafe(format!(
            "
SELECT aggregate_type, aggregate_id, last_sequence, {} AS current_snapshot, payload
  FROM {}
  WHERE aggregate_type = $1 AND aggregate_id = $2",
            snapshot_counter.as_str(),
            snapshot_table.as_str()
        ))
        .into_sql_str();
        Self {
            event_table,
            select_events,
            insert_event,
            all_events,
            insert_snapshot,
            update_snapshot,
            select_snapshot,
        }
    }
    pub fn select_events(&self) -> SqlStr {
        self.select_events.clone()
    }
    pub fn insert_event(&self) -> SqlStr {
        self.insert_event.clone()
    }
    pub fn insert_snapshot(&self) -> SqlStr {
        self.insert_snapshot.clone()
    }
    pub fn update_snapshot(&self) -> SqlStr {
        self.update_snapshot.clone()
    }
    pub fn select_snapshot(&self) -> SqlStr {
        self.select_snapshot.clone()
    }
    pub fn all_events(&self) -> SqlStr {
        self.all_events.clone()
    }
    pub fn get_last_events(
        &self,
        last_sequence: usize,
    ) -> Result<SqlStr, crate::error::PostgresAggregateError> {
        let last_sequence = i64::try_from(last_sequence)?;
        Ok(AssertSqlSafe(format!(
            "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM {}
  WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > {}
  ORDER BY sequence",
            self.event_table.as_str(),
            last_sequence
        ))
        .into_sql_str())
    }
}

#[test]
#[cfg(target_pointer_width = "64")]
fn suffix_checkpoint_must_fit_the_stored_sequence_range() {
    let factory = SqlQueryFactory::new("events", "snapshots", "current_snapshot");
    let error = factory.get_last_events(usize::MAX).unwrap_err();
    assert!(matches!(
        error,
        crate::error::PostgresAggregateError::Integer(_)
    ));
    assert!(factory
        .get_last_events(0)
        .unwrap()
        .as_str()
        .contains("sequence > 0"));
}

#[test]
fn test_queries() {
    let query_factory = SqlQueryFactory::new("my_events", "my_snapshots", "current_snapshot");
    assert_eq!(
        query_factory.select_events().as_str(),
        "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1 AND aggregate_id = $2
  ORDER BY sequence"
    );
    assert_eq!(query_factory.insert_event().as_str(), "
INSERT INTO my_events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7)");
    assert_eq!(
        query_factory.all_events().as_str(),
        "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1
  ORDER BY sequence"
    );
    assert_eq!(
        query_factory.insert_snapshot().as_str(),
        "
INSERT INTO my_snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload)
VALUES ($1, $2, $3, $4, $5)"
    );
    assert_eq!(
        query_factory.update_snapshot().as_str(),
        "
UPDATE my_snapshots
  SET last_sequence= $3 , payload= $6, current_snapshot= $4
  WHERE aggregate_type= $1 AND aggregate_id= $2 AND current_snapshot= $5"
    );
    assert_eq!(
        query_factory.select_snapshot().as_str(),
        "
SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot AS current_snapshot, payload
  FROM my_snapshots
  WHERE aggregate_type = $1 AND aggregate_id = $2"
    );
    assert_eq!(
        query_factory.get_last_events(20).unwrap().as_str(),
        "
SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata
  FROM my_events
  WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > 20
  ORDER BY sequence"
    );
}
