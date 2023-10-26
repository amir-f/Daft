from __future__ import annotations

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from daft.io.scan import ScanOperator
from daft.logical.schema import Schema


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table) -> None:
        super().__init__()
        self._table = iceberg_table
        arrow_schema = schema_to_pyarrow(iceberg_table.schema())
        self._schema = Schema.from_pyarrow_schema(arrow_schema)

    def schema(self) -> Schema:
        return self._schema


def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


cat = catalog()
tab = cat.load_table("default.test_partitioned_by_years")
ice = IcebergScanOperator(tab)
