#![allow(unused)] // MAKE SURE TO REMOVE THIS

use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use daft_core::{
    ffi,
    python::{datatype::PyTimeUnit, schema::PySchema, PySeries},
    schema::Schema,
    Series,
};
use daft_dsl::python::PyExpr;
use daft_io::{get_io_client, python::IOConfig, IOStatsContext};
use daft_parquet::read::ParquetSchemaInferenceOptions;
use daft_stats::TableStatistics;
use daft_table::{python::PyTable, Table};
use indexmap::IndexMap;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict, PyList, PyTuple},
    Python,
};

use crate::micropartition::{DeferredLoadingParams, MicroPartition, TableState};

use daft_stats::TableMetadata;
use pyo3::PyTypeInfo;

#[pyclass(module = "daft.daft", frozen)]
#[derive(Clone)]
struct PyMicroPartition {
    inner: Arc<MicroPartition>,
}

#[pymethods]
impl PyMicroPartition {
    pub fn schema(&self) -> PyResult<PySchema> {
        Ok(PySchema {
            schema: self.inner.schema.clone(),
        })
    }

    pub fn column_names(&self) -> PyResult<Vec<String>> {
        Ok(self.inner.column_names())
    }

    pub fn get_column(&self, name: &str) -> PyResult<PySeries> {
        let tables = self.inner.concat_or_get()?;
        let columns = tables
            .iter()
            .map(|t| t.get_column(name))
            .collect::<DaftResult<Vec<_>>>()?;
        match columns.as_slice() {
            [] => Ok(Series::empty(name, &self.inner.schema.get_field(name)?.dtype).into()),
            columns => Ok(Series::concat(columns)?.into()),
        }
    }

    pub fn size_bytes(&self) -> PyResult<usize> {
        Ok(self.inner.size_bytes()?)
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        Ok(format!("{}", self.inner))
    }

    pub fn __repr_html__(&self) -> PyResult<String> {
        todo!("[MICROPARTITION_INT] __repr_html__")
    }

    // Creation Methods
    #[staticmethod]
    pub fn from_tables(tables: Vec<PyTable>) -> PyResult<Self> {
        match &tables[..] {
            [] => Ok(MicroPartition::empty(None).into()),
            [first, ..] => {
                let tables = Arc::new(tables.iter().map(|t| t.table.clone()).collect::<Vec<_>>());
                Ok(MicroPartition::new(
                    first.table.schema.clone(),
                    TableState::Loaded(tables.clone()),
                    TableMetadata {
                        length: tables.iter().map(|t| t.len()).sum(),
                    },
                    // Don't compute statistics if data is already materialized
                    None,
                )
                .into())
            }
        }
    }

    #[staticmethod]
    pub fn empty(schema: Option<PySchema>) -> PyResult<Self> {
        Ok(MicroPartition::empty(match schema {
            Some(s) => Some(s.schema),
            None => None,
        })
        .into())
    }

    #[staticmethod]
    pub fn from_arrow_record_batches(
        py: Python,
        record_batches: Vec<&PyAny>,
        schema: &PySchema,
    ) -> PyResult<Self> {
        // TODO: Cleanup and refactor code for sharing with Table
        let tables = record_batches
            .iter()
            .map(|rb| daft_table::ffi::record_batches_to_table(py, &[rb], schema.schema.clone()))
            .collect::<PyResult<Vec<_>>>()?;

        let total_len = tables.iter().map(|tbl| tbl.len()).sum();
        Ok(MicroPartition::new(
            schema.schema.clone(),
            TableState::Loaded(Arc::new(tables)),
            TableMetadata { length: total_len },
            None,
        )
        .into())
    }

    // Export Methods
    pub fn to_table(&self, py: Python) -> PyResult<PyTable> {
        let concatted = self.inner.concat_or_get()?;
        match &concatted.as_ref()[..] {
            [] => PyTable::empty(Some(self.schema()?)),
            [table] => Ok(PyTable {
                table: table.clone(),
            }),
            [..] => unreachable!("concat_or_get should return one or none"),
        }
    }

    // Compute Methods

    #[staticmethod]
    pub fn concat(py: Python, to_concat: Vec<Self>) -> PyResult<Self> {
        let mps: Vec<_> = to_concat.iter().map(|t| t.inner.as_ref()).collect();
        py.allow_threads(|| Ok(MicroPartition::concat(mps.as_slice())?.into()))
    }

    pub fn slice(&self, py: Python, start: i64, end: i64) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.slice(start as usize, end as usize)?.into()))
    }

    pub fn cast_to_schema(&self, py: Python, schema: PySchema) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.cast_to_schema(schema.schema)?.into()))
    }

    pub fn eval_expression_list(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .eval_expression_list(converted_exprs.as_slice())?
                .into())
        })
    }

    pub fn take(&self, py: Python, idx: &PySeries) -> PyResult<Self> {
        py.allow_threads(|| Ok(self.inner.take(&idx.series)?.into()))
    }

    pub fn filter(&self, py: Python, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| Ok(self.inner.filter(converted_exprs.as_slice())?.into()))
    }

    pub fn sort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<Self> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .sort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn argsort(
        &self,
        py: Python,
        sort_keys: Vec<PyExpr>,
        descending: Vec<bool>,
    ) -> PyResult<PySeries> {
        let converted_exprs: Vec<daft_dsl::Expr> =
            sort_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .argsort(converted_exprs.as_slice(), descending.as_slice())?
                .into())
        })
    }

    pub fn agg(&self, py: Python, to_agg: Vec<PyExpr>, group_by: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_agg: Vec<daft_dsl::Expr> = to_agg.into_iter().map(|e| e.into()).collect();
        let converted_group_by: Vec<daft_dsl::Expr> =
            group_by.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .agg(converted_to_agg.as_slice(), converted_group_by.as_slice())?
                .into())
        })
    }

    pub fn join(
        &self,
        py: Python,
        right: &Self,
        left_on: Vec<PyExpr>,
        right_on: Vec<PyExpr>,
    ) -> PyResult<Self> {
        let left_exprs: Vec<daft_dsl::Expr> = left_on.into_iter().map(|e| e.into()).collect();
        let right_exprs: Vec<daft_dsl::Expr> = right_on.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .join(&right.inner, left_exprs.as_slice(), right_exprs.as_slice())?
                .into())
        })
    }

    pub fn explode(&self, py: Python, to_explode: Vec<PyExpr>) -> PyResult<Self> {
        let converted_to_explode: Vec<daft_dsl::Expr> =
            to_explode.into_iter().map(|e| e.expr).collect();

        py.allow_threads(|| Ok(self.inner.explode(converted_to_explode.as_slice())?.into()))
    }

    pub fn head(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not head MicroPartition with negative number: {num}"
                )));
            }
            Ok(self.inner.head(num as usize)?.into())
        })
    }

    pub fn sample(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not sample table with negative number: {num}"
                )));
            }
            Ok(self.inner.sample(num as usize)?.into())
        })
    }

    pub fn quantiles(&self, py: Python, num: i64) -> PyResult<Self> {
        py.allow_threads(|| {
            if num < 0 {
                return Err(PyValueError::new_err(format!(
                    "Can not fetch quantile from table with negative number: {num}"
                )));
            }
            Ok(self.inner.quantiles(num as usize)?.into())
        })
    }

    pub fn partition_by_hash(
        &self,
        py: Python,
        exprs: Vec<PyExpr>,
        num_partitions: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }
        let exprs: Vec<daft_dsl::Expr> = exprs.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_hash(exprs.as_slice(), num_partitions as usize)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_random(
        &self,
        py: Python,
        num_partitions: i64,
        seed: i64,
    ) -> PyResult<Vec<Self>> {
        if num_partitions < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not partition into negative number of partitions: {num_partitions}"
            )));
        }

        if seed < 0 {
            return Err(PyValueError::new_err(format!(
                "Can not have seed has negative number: {seed}"
            )));
        }
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_random(num_partitions as usize, seed as u64)?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    pub fn partition_by_range(
        &self,
        py: Python,
        partition_keys: Vec<PyExpr>,
        boundaries: &PyTable,
        descending: Vec<bool>,
    ) -> PyResult<Vec<Self>> {
        let exprs: Vec<daft_dsl::Expr> = partition_keys.into_iter().map(|e| e.into()).collect();
        py.allow_threads(|| {
            Ok(self
                .inner
                .partition_by_range(exprs.as_slice(), &boundaries.table, descending.as_slice())?
                .into_iter()
                .map(|t| t.into())
                .collect::<Vec<Self>>())
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    pub fn read_csv(
        py: Python,
        uri: &str,
        column_names: Option<Vec<&str>>,
        include_columns: Option<Vec<&str>>,
        num_rows: Option<usize>,
        has_header: Option<bool>,
        delimiter: Option<&str>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        schema: Option<PySchema>,
        buffer_size: Option<usize>,
        chunk_size: Option<usize>,
    ) -> PyResult<Self> {
        let delimiter = delimiter
            .map(|delimiter| match delimiter.as_bytes() {
                [c] => Ok(*c),
                _ => Err(PyValueError::new_err(
                    "Provided CSV delimiter must be a 1-byte character",
                )),
            })
            .transpose()?;

        let mp = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_csv: for uri {uri}"));
            let io_config = io_config.unwrap_or_default().config.into();

            crate::micropartition::read_csv_into_micropartition(
                [uri].as_ref(),
                column_names,
                include_columns,
                num_rows,
                has_header.unwrap_or(true),
                delimiter,
                io_config,
                multithreaded_io.unwrap_or(true),
                Some(io_stats),
                schema.map(|s| s.schema),
                buffer_size,
                chunk_size,
            )
        })?;
        Ok(mp.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    pub fn read_parquet(
        py: Python,
        uri: &str,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<i64>>,
        io_config: Option<IOConfig>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uri}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::micropartition::read_parquet_into_micropartition(
                [uri].as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups.map(|rg| vec![rg]),
                io_config,
                Some(io_stats),
                1,
                multithreaded_io.unwrap_or(true),
                &schema_infer_options,
            )
        })?;
        Ok(mp.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    pub fn read_parquet_bulk(
        py: Python,
        uris: Vec<&str>,
        columns: Option<Vec<&str>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        row_groups: Option<Vec<Vec<i64>>>,
        io_config: Option<IOConfig>,
        num_parallel_tasks: Option<i64>,
        multithreaded_io: Option<bool>,
        coerce_int96_timestamp_unit: Option<PyTimeUnit>,
    ) -> PyResult<Self> {
        let mp = py.allow_threads(|| {
            let io_stats = IOStatsContext::new(format!("read_parquet: for uri {uris:?}"));

            let io_config = io_config.unwrap_or_default().config.into();
            let schema_infer_options = ParquetSchemaInferenceOptions::new(
                coerce_int96_timestamp_unit.map(|tu| tu.timeunit),
            );

            crate::micropartition::read_parquet_into_micropartition(
                uris.as_ref(),
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups,
                io_config,
                Some(io_stats),
                num_parallel_tasks.unwrap_or(128) as usize,
                multithreaded_io.unwrap_or(true),
                &schema_infer_options,
            )
        })?;
        Ok(mp.into())
    }

    #[staticmethod]
    pub fn _from_unloaded_table_state(
        py: Python,
        schema_bytes: &PyBytes,
        loading_params_bytes: &PyBytes,
        metadata_bytes: &PyBytes,
        statistics_bytes: &PyBytes,
    ) -> PyResult<Self> {
        let schema = bincode::deserialize::<Schema>(schema_bytes.as_bytes()).unwrap();
        let params =
            bincode::deserialize::<DeferredLoadingParams>(loading_params_bytes.as_bytes()).unwrap();
        let metadata = bincode::deserialize::<TableMetadata>(metadata_bytes.as_bytes()).unwrap();
        let statistics =
            bincode::deserialize::<Option<TableStatistics>>(statistics_bytes.as_bytes()).unwrap();

        Ok(MicroPartition::new(
            schema.into(),
            TableState::Unloaded(params),
            metadata,
            statistics,
        )
        .into())
    }

    #[staticmethod]
    pub fn _from_loaded_table_state(
        py: Python,
        schema_bytes: &PyBytes,
        table_objs: Vec<PyObject>,
        metadata_bytes: &PyBytes,
        statistics_bytes: &PyBytes,
    ) -> PyResult<Self> {
        let schema = bincode::deserialize::<Schema>(schema_bytes.as_bytes()).unwrap();
        let metadata = bincode::deserialize::<TableMetadata>(metadata_bytes.as_bytes()).unwrap();
        let statistics =
            bincode::deserialize::<Option<TableStatistics>>(statistics_bytes.as_bytes()).unwrap();

        let tables = table_objs
            .into_iter()
            .map(|p| {
                Ok(p.getattr(py, pyo3::intern!(py, "_table"))?
                    .extract::<PyTable>(py)?
                    .table)
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(MicroPartition::new(
            schema.into(),
            TableState::Loaded(tables.into()),
            metadata,
            statistics,
        )
        .into())
    }

    pub fn __reduce__(&self, py: Python) -> PyResult<(PyObject, PyObject)> {
        let schema_bytes = PyBytes::new(py, &bincode::serialize(&self.inner.schema).unwrap());

        let py_metadata_bytes =
            PyBytes::new(py, &bincode::serialize(&self.inner.metadata).unwrap());
        let py_stats_bytes = PyBytes::new(py, &bincode::serialize(&self.inner.statistics).unwrap());

        let guard = self.inner.state.lock().unwrap();
        if let TableState::Loaded(tables) = guard.deref() {
            let _from_pytable = py
                .import(pyo3::intern!(py, "daft.table"))?
                .getattr(pyo3::intern!(py, "Table"))?
                .getattr(pyo3::intern!(py, "_from_pytable"))?;

            let pytables = tables.iter().map(|t| PyTable { table: t.clone() });
            let pyobjs = pytables
                .map(|pt| _from_pytable.call1((pt,)))
                .collect::<PyResult<Vec<_>>>()?;
            Ok((
                Self::type_object(py)
                    .getattr(pyo3::intern!(py, "_from_loaded_table_state"))?
                    .to_object(py),
                (schema_bytes, pyobjs, py_metadata_bytes, py_stats_bytes).to_object(py),
            ))
        } else if let TableState::Unloaded(params) = guard.deref() {
            let py_params_bytes = PyBytes::new(py, &bincode::serialize(params).unwrap());
            Ok((
                Self::type_object(py)
                    .getattr(pyo3::intern!(py, "_from_unloaded_table_state"))?
                    .to_object(py),
                (
                    schema_bytes,
                    py_params_bytes,
                    py_metadata_bytes,
                    py_stats_bytes,
                )
                    .to_object(py),
            ))
        } else {
            unreachable!()
        }
    }
}

impl From<MicroPartition> for PyMicroPartition {
    fn from(value: MicroPartition) -> Self {
        PyMicroPartition {
            inner: Arc::new(value),
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<PyMicroPartition>()?;
    Ok(())
}
