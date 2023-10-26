use pyo3::prelude::*;

pub mod pylib {
    use daft_dsl::col;
    use daft_dsl::python::PyExpr;
    use pyo3::exceptions::PyNotADirectoryError;
    use pyo3::exceptions::PyNotImplementedError;
    use pyo3::prelude::*;
    use std::borrow::BorrowMut;
    use std::fmt::Display;
    use std::str::FromStr;

    use daft_core::python::schema::PySchema;

    use pyo3::pyclass;

    use crate::anonymous::AnonymousScanOperator;
    use crate::FileType;
    use crate::ScanOperator;
    use crate::ScanOperatorRef;

    #[pyclass(module = "daft.daft", frozen)]
    pub(crate) struct ScanOperatorHandle {
        scan_op: ScanOperatorRef,
    }

    #[pymethods]
    impl ScanOperatorHandle {
        pub fn __repr__(&self) -> PyResult<String> {
            Ok(format!("{}", self.scan_op))
        }

        #[staticmethod]
        pub fn anonymous_scan(
            schema: PySchema,
            file_type: &str,
            files: Vec<String>,
        ) -> PyResult<Self> {
            let schema = schema.schema;
            let operator = Box::new(AnonymousScanOperator::new(
                schema,
                FileType::from_str(file_type)?,
                files,
            ));
            Ok(ScanOperatorHandle { scan_op: operator })
        }

        #[staticmethod]
        pub fn from_python_abc(py_scan: PyObject) -> PyResult<Self> {
            let scan_op: ScanOperatorRef =
                Box::new(PythonScanOperatorBridge::from_python_abc(py_scan)?);
            Ok(ScanOperatorHandle { scan_op })
        }
    }
    #[pyclass(module = "daft.daft")]
    #[derive(Debug)]
    pub(self) struct PythonScanOperatorBridge {
        operator: PyObject,
    }
    #[pymethods]
    impl PythonScanOperatorBridge {
        #[staticmethod]
        pub fn from_python_abc(abc: PyObject) -> PyResult<Self> {
            Ok(Self { operator: abc })
        }

        pub fn _filter(&self, py: Python, predicate: PyExpr) -> PyResult<(bool, Self)> {
            let _from_pyexpr = py
                .import(pyo3::intern!(py, "daft.expressions"))?
                .getattr(pyo3::intern!(py, "Expression"))?
                .getattr(pyo3::intern!(py, "_from_pyexpr"))?;
            let expr = _from_pyexpr.call1((predicate,))?;
            let result = self.operator.call_method(py, "filter", (expr,), None)?;
            let (absorb, new_op) = result.extract::<(bool, PyObject)>(py)?;
            Ok((absorb, Self { operator: new_op }))
        }
    }

    impl Display for PythonScanOperatorBridge {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:#?}", self)
        }
    }

    impl ScanOperator for PythonScanOperatorBridge {
        fn filter(
            self: Box<Self>,
            predicate: &daft_dsl::Expr,
        ) -> common_error::DaftResult<(bool, ScanOperatorRef)> {
            Python::with_gil(|py| {
                let (can, new_op) = self._filter(
                    py,
                    PyExpr {
                        expr: predicate.clone(),
                    },
                )?;
                Ok((can, Box::new(new_op) as ScanOperatorRef))
            })
        }
        fn limit(self: Box<Self>, num: usize) -> common_error::DaftResult<ScanOperatorRef> {
            todo!()
        }
        fn num_partitions(&self) -> common_error::DaftResult<usize> {
            todo!()
        }
        fn partitioning_keys(&self) -> &[crate::PartitionField] {
            todo!()
        }
        fn schema(&self) -> daft_core::schema::SchemaRef {
            todo!()
        }
        fn select(self: Box<Self>, columns: &[&str]) -> common_error::DaftResult<ScanOperatorRef> {
            todo!()
        }
        fn to_scan_tasks(
            self: Box<Self>,
        ) -> common_error::DaftResult<
            Box<dyn Iterator<Item = common_error::DaftResult<crate::ScanTask>>>,
        > {
            todo!()
        }
    }
}

pub fn register_modules(_py: Python, parent: &PyModule) -> PyResult<()> {
    parent.add_class::<pylib::ScanOperatorHandle>()?;
    Ok(())
}
