use cqrs_es::persist::PersistenceError;
use cqrs_es::AggregateError;
use sqlx::Error;

#[derive(Debug, thiserror::Error)]
pub enum PostgresAggregateError {
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    #[error("stored index must be positive: {value}")]
    NonPositiveIndex { value: i64 },
    #[error("optimistic lock error")]
    OptimisticLock,
    #[error(transparent)]
    ConnectionError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(transparent)]
    DeserializationError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(transparent)]
    UnknownError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sqlx::Error> for PostgresAggregateError {
    fn from(err: sqlx::Error) -> Self {
        // TODO: improve error handling
        match &err {
            Error::Database(database_error) => {
                if let Some(code) = database_error.code() {
                    if code.as_ref() == "23505" {
                        return Self::OptimisticLock;
                    }
                }
                Self::UnknownError(Box::new(err))
            }
            Error::Io(_) | Error::Tls(_) | Error::PoolClosed => {
                Self::ConnectionError(Box::new(err))
            }
            _ => Self::UnknownError(Box::new(err)),
        }
    }
}

impl<T: std::error::Error> From<PostgresAggregateError> for AggregateError<T> {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            invalid @ (PostgresAggregateError::Integer(_)
            | PostgresAggregateError::NonPositiveIndex { .. }) => {
                Self::UnexpectedError(Box::new(invalid))
            }
            PostgresAggregateError::OptimisticLock => Self::AggregateConflict,
            PostgresAggregateError::ConnectionError(error) => Self::DatabaseConnectionError(error),
            PostgresAggregateError::DeserializationError(error) => {
                Self::DeserializationError(error)
            }
            PostgresAggregateError::UnknownError(error) => Self::UnexpectedError(error),
        }
    }
}

impl From<serde_json::Error> for PostgresAggregateError {
    fn from(err: serde_json::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => {
                Self::DeserializationError(Box::new(err))
            }
            serde_json::error::Category::Io | serde_json::error::Category::Eof => {
                Self::UnknownError(Box::new(err))
            }
        }
    }
}

impl From<PostgresAggregateError> for PersistenceError {
    fn from(err: PostgresAggregateError) -> Self {
        match err {
            invalid @ (PostgresAggregateError::Integer(_)
            | PostgresAggregateError::NonPositiveIndex { .. }) => {
                Self::UnknownError(Box::new(invalid))
            }
            PostgresAggregateError::OptimisticLock => Self::OptimisticLockError,
            PostgresAggregateError::ConnectionError(error) => Self::ConnectionError(error),
            PostgresAggregateError::DeserializationError(error) => Self::UnknownError(error),
            PostgresAggregateError::UnknownError(error) => Self::UnknownError(error),
        }
    }
}
