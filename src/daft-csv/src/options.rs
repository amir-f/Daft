use common_error::{DaftError, DaftResult};
use daft_core::{impl_bincode_py_state_serialization, schema::SchemaRef};
use serde::{Deserialize, Serialize};
#[cfg(feature = "python")]
use {
    daft_core::python::schema::PySchema,
    pyo3::{
        pyclass, pyclass::CompareOp, pymethods, types::PyBytes, PyObject, PyResult, PyTypeInfo,
        Python, ToPyObject,
    },
};

/// Options for converting CSV data to Daft data.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft"))]
pub struct CsvConvertOptions {
    pub limit: Option<usize>,
    pub include_columns: Option<Vec<String>>,
    pub column_names: Option<Vec<String>>,
    pub schema: Option<SchemaRef>,
}

impl CsvConvertOptions {
    pub fn new_internal(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        column_names: Option<Vec<String>>,
        schema: Option<SchemaRef>,
    ) -> Self {
        Self {
            limit,
            include_columns,
            column_names,
            schema,
        }
    }

    pub fn with_limit(self, limit: Option<usize>) -> Self {
        Self {
            limit,
            include_columns: self.include_columns,
            column_names: self.column_names,
            schema: self.schema,
        }
    }

    pub fn with_include_columns(self, include_columns: Option<Vec<String>>) -> Self {
        Self {
            limit: self.limit,
            include_columns,
            column_names: self.column_names,
            schema: self.schema,
        }
    }

    pub fn with_column_names(self, column_names: Option<Vec<String>>) -> Self {
        Self {
            limit: self.limit,
            include_columns: self.include_columns,
            column_names,
            schema: self.schema,
        }
    }

    pub fn with_schema(self, schema: Option<SchemaRef>) -> Self {
        Self {
            limit: self.limit,
            include_columns: self.include_columns,
            column_names: self.column_names,
            schema,
        }
    }
}

impl Default for CsvConvertOptions {
    fn default() -> Self {
        Self::new_internal(None, None, None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvConvertOptions {
    /// Create conversion options for the CSV reader.
    ///
    /// # Arguments:
    ///
    /// * `limit` - Only read this many rows.
    /// * `include_columns` - The names of the columns that should be kept, e.g. via a projection.
    /// * `column_names` - The names for the CSV columns.
    /// * `schema` - The names and dtypes for the CSV columns.
    #[new]
    #[pyo3(signature = (limit=None, include_columns=None, column_names=None, schema=None))]
    pub fn new(
        limit: Option<usize>,
        include_columns: Option<Vec<String>>,
        column_names: Option<Vec<String>>,
        schema: Option<PySchema>,
    ) -> Self {
        Self::new_internal(
            limit,
            include_columns,
            column_names,
            schema.map(|s| s.into()),
        )
    }

    #[getter]
    pub fn get_limit(&self) -> PyResult<Option<usize>> {
        Ok(self.limit)
    }

    #[getter]
    pub fn get_include_columns(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.include_columns.clone())
    }

    #[getter]
    pub fn get_column_names(&self) -> PyResult<Option<Vec<String>>> {
        Ok(self.column_names.clone())
    }

    #[getter]
    pub fn get_schema(&self) -> PyResult<Option<PySchema>> {
        Ok(self.schema.as_ref().map(|s| s.clone().into()))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(CsvConvertOptions);

/// Options for parsing CSV files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvParseOptions {
    pub has_header: bool,
    pub delimiter: u8,
}

impl CsvParseOptions {
    pub fn new_internal(has_header: bool, delimiter: u8) -> Self {
        Self {
            has_header,
            delimiter,
        }
    }

    pub fn with_has_header(self, has_header: bool) -> Self {
        Self {
            has_header,
            delimiter: self.delimiter,
        }
    }

    pub fn with_delimiter(self, delimiter: u8) -> Self {
        Self {
            has_header: self.has_header,
            delimiter,
        }
    }
}

impl Default for CsvParseOptions {
    fn default() -> Self {
        Self::new_internal(true, b',')
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvParseOptions {
    /// Create parsing options for the CSV reader.
    ///
    /// # Arguments:
    ///
    /// * `has_headers` - Whether the CSV has a header row; if so, it will be skipped during data parsing.
    /// * `delimiter` - The character delmiting individual cells in the CSV data.
    #[new]
    #[pyo3(signature = (has_header=true, delimiter=","))]
    pub fn new(has_header: bool, delimiter: &str) -> PyResult<Self> {
        let delimiter = str_delimiter_to_byte(delimiter)?;
        Ok(Self::new_internal(has_header, delimiter))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

fn str_delimiter_to_byte(delimiter: &str) -> DaftResult<u8> {
    match delimiter.as_bytes() {
        &[c] => Ok(c),
        _ => Err(DaftError::ValueError(format!(
            "Delimiter must be a single-character string, but got {}",
            delimiter
        ))),
    }
}

impl_bincode_py_state_serialization!(CsvParseOptions);

/// Options for reading CSV files.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "python", pyclass(module = "daft.daft", get_all))]
pub struct CsvReadOptions {
    pub buffer_size: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl CsvReadOptions {
    pub fn new_internal(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size,
        }
    }

    pub fn with_buffer_size(self, buffer_size: Option<usize>) -> Self {
        Self {
            buffer_size,
            chunk_size: self.chunk_size,
        }
    }

    pub fn with_chunk_size(self, chunk_size: Option<usize>) -> Self {
        Self {
            buffer_size: self.buffer_size,
            chunk_size,
        }
    }
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self::new_internal(None, None)
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl CsvReadOptions {
    /// Create reading options for the CSV reader.
    ///
    /// # Arguments:
    ///
    /// * `buffer_size` - Size of the buffer (in bytes) used by the streaming reader.
    /// * `chunk_size` - Size of the chunks (in bytes) deserialized in parallel by the streaming reader.
    #[new]
    #[pyo3(signature = (buffer_size=None, chunk_size=None))]
    pub fn new(buffer_size: Option<usize>, chunk_size: Option<usize>) -> Self {
        Self::new_internal(buffer_size, chunk_size)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
        match op {
            CompareOp::Eq => self == other,
            CompareOp::Ne => !self.__richcmp__(other, CompareOp::Eq),
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl_bincode_py_state_serialization!(CsvReadOptions);
