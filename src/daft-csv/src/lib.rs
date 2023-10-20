#![feature(async_closure)]
#![feature(let_chains)]
use common_error::DaftError;
use snafu::Snafu;

mod compression;
pub mod metadata;
pub mod options;
#[cfg(feature = "python")]
pub mod python;
pub mod read;

pub use options::{CsvConvertOptions, CsvParseOptions, CsvReadOptions};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    IOError { source: daft_io::Error },
    #[snafu(display("{source}"))]
    CSVError { source: csv_async::Error },
    #[snafu(display("{source}"))]
    ArrowError { source: arrow2::error::Error },
    #[snafu(display("Error joining spawned task: {}", source))]
    JoinError { source: tokio::task::JoinError },
    #[snafu(display(
        "Sender of OneShot Channel Dropped before sending data over: {}",
        source
    ))]
    OneShotRecvError {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        match err {
            Error::IOError { source } => source.into(),
            _ => DaftError::External(err.into()),
        }
    }
}

impl From<daft_io::Error> for Error {
    fn from(err: daft_io::Error) -> Self {
        Error::IOError { source: err }
    }
}

#[cfg(feature = "python")]
pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<CsvConvertOptions>()?;
    parent.add_class::<CsvParseOptions>()?;
    parent.add_class::<CsvReadOptions>()?;
    Ok(())
}
