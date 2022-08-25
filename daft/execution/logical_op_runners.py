from bisect import bisect_right
from functools import partial
from itertools import accumulate
from typing import Callable, Dict, List

from pyarrow import csv, parquet

from daft.datasources import (
    CSVSourceInfo,
    InMemorySourceInfo,
    ParquetSourceInfo,
    ScanType,
)
from daft.filesystem import get_filesystem_from_path
from daft.logical.logical_plan import (
    Coalesce,
    Filter,
    GlobalLimit,
    Join,
    LocalAggregate,
    LocalLimit,
    LogicalPlan,
    PartitionScheme,
    Projection,
    Repartition,
    Scan,
    Sort,
)
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionSet, vPartition
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    SortOp,
)


class LogicalPartitionOpRunner:
    def run_node_list(self, inputs: Dict[int, vPartition], nodes: List[LogicalPlan], partition_id: int) -> vPartition:
        part_set = inputs.copy()
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node, partition_id=partition_id)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: Dict[int, vPartition], node: LogicalPlan, partition_id: int) -> vPartition:
        if isinstance(node, Scan):
            return self._handle_scan(inputs, node, partition_id=partition_id)
        elif isinstance(node, Projection):
            return self._handle_projection(inputs, node, partition_id=partition_id)
        elif isinstance(node, Filter):
            return self._handle_filter(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalLimit):
            return self._handle_local_limit(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalAggregate):
            return self._handle_local_aggregate(inputs, node, partition_id=partition_id)
        elif isinstance(node, Join):
            return self._handle_join(inputs, node, partition_id=partition_id)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    def _handle_scan(self, inputs: Dict[int, vPartition], scan: Scan, partition_id: int) -> vPartition:
        schema = scan.schema()
        column_ids = [col.get_id() for col in schema.to_column_expressions()]
        if scan._source_info.scan_type() == ScanType.IN_MEMORY:
            assert isinstance(scan._source_info, InMemorySourceInfo)
            table_len = [len(scan._source_info.data[key]) for key in scan._source_info.data][0]
            partition_size = table_len // scan._source_info.num_partitions
            start, end = (partition_size * partition_id, partition_size * (partition_id + 1))
            data = {key: scan._source_info.data[key][start:end] for key in scan._source_info.data}
            vpart = vPartition.from_pydict(data, schema=schema, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            path = scan._source_info.filepaths[partition_id]
            fs = get_filesystem_from_path(path)
            table = csv.read_csv(
                fs.open(path),
                parse_options=csv.ParseOptions(
                    delimiter=scan._source_info.delimiter,
                ),
                read_options=csv.ReadOptions(
                    column_names=[expr.name() for expr in schema],
                    skip_rows_after_names=1 if scan._source_info.has_headers else 0,
                ),
            )
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            table = parquet.read_table(scan._source_info.filepaths[partition_id])
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

    def _handle_projection(self, inputs: Dict[int, vPartition], proj: Projection, partition_id: int) -> vPartition:
        child_id = proj._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.eval_expression_list(proj._projection)

    def _handle_filter(self, inputs: Dict[int, vPartition], filter: Filter, partition_id: int) -> vPartition:
        predicate = filter._predicate
        child_id = filter._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.filter(predicate)

    def _handle_local_limit(self, inputs: Dict[int, vPartition], limit: LocalLimit, partition_id: int) -> vPartition:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.head(num)

    def _handle_local_aggregate(
        self, inputs: Dict[int, vPartition], agg: LocalAggregate, partition_id: int
    ) -> vPartition:
        child_id = agg._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.agg(agg._agg, group_by=agg._group_by)

    def _handle_join(self, inputs: Dict[int, vPartition], join: Join, partition_id: int) -> vPartition:
        left_id = join._children()[0].id()
        right_id = join._children()[1].id()
        left_partition = inputs[left_id]
        right_partition = inputs[right_id]
        return left_partition.join(
            right_partition,
            left_on=join._left_on,
            right_on=join._right_on,
            output_schema=join.schema(),
            how=join._how.value,
        )


class PyRunnerSimpleShuffler(ShuffleOp):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}

        source_partitions = input.num_partitions()
        map_results = [
            self.map_fn(input=input.partitions[i], output_partitions=num_target_partitions, **map_args)
            for i in range(source_partitions)
        ]
        reduced_results = []
        for t in range(num_target_partitions):
            reduced_part = self.reduce_fn(
                [map_results[i][t] for i in range(source_partitions) if t in map_results[i]], **reduce_args
            )
            reduced_results.append(reduced_part)

        return PartitionSet({i: part for i, part in enumerate(reduced_results)})


class PyRunnerRepartitionRandom(PyRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class PyRunnerRepartitionHash(PyRunnerSimpleShuffler, RepartitionHashOp):
    ...


class PyRunnerCoalesceOp(PyRunnerSimpleShuffler, CoalesceOp):
    ...


class PyRunnerSortOp(PyRunnerSimpleShuffler, SortOp):
    ...


class LogicalGlobalOpRunner:
    def run_node_list(self, inputs: Dict[int, PartitionSet], nodes: List[LogicalPlan]) -> PartitionSet:
        part_set = inputs.copy()
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: Dict[int, PartitionSet], node: LogicalPlan) -> PartitionSet:
        if isinstance(node, GlobalLimit):
            return self._handle_global_limit(inputs, node)
        elif isinstance(node, Repartition):
            return self._handle_repartition(inputs, node)
        elif isinstance(node, Sort):
            return self._handle_sort(inputs, node)
        elif isinstance(node, Coalesce):
            return self._handle_coalesce(inputs, node)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    def map_partitions(self, pset: PartitionSet, func: Callable[[vPartition], vPartition]) -> PartitionSet:
        return PartitionSet({i: func(part) for i, part in pset.partitions.items()})

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[List[vPartition]], vPartition]) -> vPartition:
        data = list(pset.partitions.values())
        return func(data)

    def _handle_global_limit(self, inputs: Dict[int, PartitionSet], limit: GlobalLimit) -> PartitionSet:
        child_id = limit._children()[0].id()
        prev_part = inputs[child_id]

        num = limit._num
        size_per_partition = prev_part.len_of_partitions()
        total_size = sum(size_per_partition)
        if total_size <= num:
            return prev_part

        cum_sum = list(accumulate(size_per_partition))
        where_to_cut_idx = bisect_right(cum_sum, num)
        count_so_far = cum_sum[where_to_cut_idx - 1]
        remainder = num - count_so_far
        assert remainder >= 0

        def limit_map_func(part: vPartition) -> vPartition:
            if part.partition_id < where_to_cut_idx:
                return part
            elif part.partition_id == where_to_cut_idx:
                return part.head(remainder)
            else:
                return part.head(0)

        return self.map_partitions(prev_part, limit_map_func)

    def _handle_repartition(self, inputs: Dict[int, PartitionSet], repartition: Repartition) -> PartitionSet:

        child_id = repartition._children()[0].id()

        repartitioner: PyRunnerSimpleShuffler
        if repartition._scheme == PartitionScheme.RANDOM:
            repartitioner = PyRunnerRepartitionRandom()
        elif repartition._scheme == PartitionScheme.HASH:
            repartitioner = PyRunnerRepartitionHash(map_args={"exprs": repartition._partition_by.exprs})
        else:
            raise NotImplementedError()
        prev_part = inputs[child_id]
        return repartitioner.run(input=prev_part, num_target_partitions=repartition.num_partitions())

    def _handle_sort(self, inputs: Dict[int, PartitionSet], sort: Sort) -> PartitionSet:

        child_id = sort._children()[0].id()

        SAMPLES_PER_PARTITION = 20
        num_partitions = sort.num_partitions()
        exprs: ExpressionList = sort._sort_by

        def sample_map_func(part: vPartition) -> vPartition:
            return part.sample(SAMPLES_PER_PARTITION).eval_expression_list(exprs)

        prev_part = inputs[child_id]
        sampled_partitions = self.map_partitions(prev_part, sample_map_func)
        merged_samples = self.reduce_partitions(
            sampled_partitions, partial(vPartition.merge_partitions, verify_partition_id=False)
        )
        assert len(sort._sort_by.exprs) == 1
        assert len(merged_samples.columns) == 1
        first_column = list(merged_samples.columns.values())[0]
        boundaries = first_column.block.quantiles(num_partitions)
        expr = exprs.exprs[0]
        sort_op = PyRunnerSortOp(
            map_args={"expr": expr, "boundaries": boundaries, "desc": sort._desc},
            reduce_args={"expr": expr, "desc": sort._desc},
        )

        return sort_op.run(input=prev_part, num_target_partitions=num_partitions)

    def _handle_coalesce(self, inputs: Dict[int, PartitionSet], coal: Coalesce) -> PartitionSet:
        child_id = coal._children()[0].id()
        num_partitions = coal.num_partitions()
        prev_part = inputs[child_id]
        coalesce_op = PyRunnerCoalesceOp(
            map_args={"num_input_partitions": prev_part.num_partitions()},
        )
        return coalesce_op.run(input=prev_part, num_target_partitions=num_partitions)
