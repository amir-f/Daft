from __future__ import annotations

import pyarrow as pa
import pytest

from daft.expressions import col
from daft.series import Series
from daft.table import MicroPartition, Table

TEST_DATA = [
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64()))),
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64()))),
    Series.from_arrow(pa.array([[1, 2], [3, 4], None, None], type=pa.list_(pa.int64(), list_size=2))),
]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode(TableCls, data):
    table = TableCls.from_pydict({"nested": data, "sidecar": ["a", "b", "c", "d"]})
    table = table.explode([col("nested")._explode()])
    assert table.column_names() == ["nested", "sidecar"]
    assert table.to_pydict() == {"nested": [1, 2, 3, 4, None, None], "sidecar": ["a", "a", "b", "b", "c", "d"]}


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_flipped(TableCls, data):
    table = TableCls.from_pydict({"sidecar": ["a", "b", "c", "d"], "nested": data})
    table = table.explode([col("nested")._explode()])
    assert table.column_names() == ["sidecar", "nested"]
    assert table.to_pydict() == {"nested": [1, 2, 3, 4, None, None], "sidecar": ["a", "a", "b", "b", "c", "d"]}


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_multiple_cols(TableCls, data):
    table = TableCls.from_pydict({"nested": data, "nested2": data, "sidecar": ["a", "b", "c", "d"]})
    table = table.explode([col("nested")._explode(), col("nested2")._explode()])
    assert table.column_names() == ["nested", "nested2", "sidecar"]
    assert table.to_pydict() == {
        "nested": [1, 2, 3, 4, None, None],
        "nested2": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_explode_multiple_cols_mixed_types(TableCls):
    data1 = pa.array([[1, 2], [3, 4], None, None], type=pa.list_(pa.int64()))
    data2 = pa.array([[1, 2], [3, 4], None, None], type=pa.list_(pa.int64(), list_size=2))
    table = TableCls.from_pydict({"nested": data1, "nested2": data2, "sidecar": ["a", "b", "c", "d"]})
    table = table.explode([col("nested")._explode(), col("nested2")._explode()])
    assert table.to_pydict() == {
        "nested": [1, 2, 3, 4, None, None],
        "nested2": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_explode_bad_multiple_cols(TableCls):
    table = TableCls.from_pydict(
        {
            "nested": [[1, 2, 3], [4], None, None],
            "nested2": [[1, 2], [3, 4], None, None],
            "sidecar": ["a", "b", "c", "d"],
        }
    )
    with pytest.raises(ValueError, match="In multicolumn explode, list length did not match"):
        table.explode([col("nested")._explode(), col("nested2")._explode()])


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_multiple_cols_with_alias(TableCls, data):
    table = TableCls.from_pydict({"nested": data, "nested2": data, "sidecar": ["a", "b", "c", "d"]})
    table = table.explode([col("nested").alias("nested3")._explode(), col("nested2")._explode()])
    assert table.column_names() == ["nested", "nested2", "sidecar", "nested3"]
    data_py = data.to_pylist()
    assert table.to_pydict() == {
        "nested": [data_py[0], data_py[0], data_py[1], data_py[1], data_py[2], data_py[3]],
        "nested2": [1, 2, 3, 4, None, None],
        "nested3": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
@pytest.mark.parametrize(
    "data",
    TEST_DATA,
)
def test_explode_eval_expr(TableCls, data):
    table = TableCls.from_pydict({"nested": data})
    table = table.eval_expression_list([col("nested")._explode()])
    assert table.to_pydict() == {"nested": [1, 2, 3, 4, None, None]}


def test_explode_bad_col_type():
    table = Table.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="to be a List Type, but is"):
        table = table.explode([col("a")._explode()])
