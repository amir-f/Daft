use std::sync::Arc;
use std::{ops::Deref, sync::Mutex};

use arrow2::io::parquet::read::schema::infer_schema_with_options;
use common_error::DaftResult;
use daft_core::schema::{Schema, SchemaRef};
use daft_csv::{
    read_csv_bulk, read_csv_schema_bulk, CsvConvertOptions, CsvParseOptions, CsvReadOptions,
};
use daft_dsl::Expr;
use daft_parquet::read::{read_parquet_metadata_bulk, ParquetSchemaInferenceOptions};
use daft_table::Table;

use snafu::ResultExt;

use crate::DaftCoreComputeSnafu;

use crate::{column_stats::TruthValue, table_stats::TableStatistics};
use daft_io::{IOConfig, IOStatsRef};

#[derive(Clone)]
enum FormatParams {
    Parquet(ParquetSchemaInferenceOptions),
    Csv(
        Option<CsvParseOptions>,
        Option<CsvReadOptions>,
        Option<Vec<String>>,
    ),
}

#[derive(Clone)]
struct DeferredLoadingParams {
    format_params: FormatParams,
    urls: Vec<String>,
    io_config: Arc<IOConfig>,
    multithreaded_io: bool,
    filters: Vec<Expr>,
    limit: Option<usize>,
    columns: Option<Vec<String>>,
}

enum TableState {
    Unloaded(DeferredLoadingParams),
    Loaded(Arc<Vec<Table>>),
}

struct MicroPartition {
    schema: SchemaRef,
    state: Mutex<TableState>,
    statistics: Option<TableStatistics>,
}

impl MicroPartition {
    pub fn new(schema: SchemaRef, state: TableState, statistics: Option<TableStatistics>) -> Self {
        MicroPartition {
            schema,
            state: Mutex::new(state),
            statistics,
        }
    }

    pub fn empty() -> Self {
        Self::new(
            Schema::empty().into(),
            TableState::Loaded(Arc::new(vec![])),
            None,
        )
    }

    fn tables_or_read(&self, io_stats: Option<IOStatsRef>) -> crate::Result<Arc<Vec<Table>>> {
        let mut guard = self.state.lock().unwrap();

        match guard.deref() {
            TableState::Loaded(tables) => Ok(tables.clone()),
            TableState::Unloaded(params) => {
                let table_values: Vec<_> = match &params.format_params {
                    FormatParams::Parquet(parquet_schema_inference) => {
                        let io_client = daft_io::get_io_client(
                            params.multithreaded_io,
                            params.io_config.clone(),
                        )
                        .unwrap();
                        let column_names = params
                            .columns
                            .as_ref()
                            .map(|v| v.iter().map(|s| s.as_ref()).collect::<Vec<_>>());
                        let urls = params.urls.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                        daft_parquet::read::read_parquet_bulk(
                            urls.as_slice(),
                            column_names.as_deref(),
                            None,
                            params.limit,
                            None,
                            io_client.clone(),
                            io_stats,
                            8,
                            params.multithreaded_io,
                            parquet_schema_inference,
                        )
                        .context(DaftCoreComputeSnafu)?
                    }
                    FormatParams::Csv(parse_options, read_options, column_names) => {
                        let io_client = daft_io::get_io_client(
                            params.multithreaded_io,
                            params.io_config.clone(),
                        )
                        .unwrap();
                        let uris = params.urls.iter().map(|s| s.as_str()).collect::<Vec<_>>();
                        let convert_options = CsvConvertOptions::new_internal(
                            params.limit,
                            params.columns.clone(),
                            column_names.clone(),
                            Some(self.schema.clone()),
                        );
                        read_csv_bulk(
                            uris.as_slice(),
                            Some(convert_options),
                            parse_options.clone(),
                            read_options.clone(),
                            io_client.clone(),
                            io_stats,
                            params.multithreaded_io,
                            None,
                            8usize,
                        )
                        .context(DaftCoreComputeSnafu)?
                    }
                };

                let filter_preds = &params.filters;
                let table_values = if !filter_preds.is_empty() {
                    table_values
                        .into_iter()
                        .map(|t| t.filter(filter_preds.as_slice()))
                        .collect::<Result<Vec<Table>, _>>()
                        .context(DaftCoreComputeSnafu)?
                } else {
                    table_values
                };

                let table_values = Arc::new(table_values);
                *guard = TableState::Loaded(table_values.clone());
                Ok(table_values)
            }
        }
    }

    pub fn filter(&self, predicate: &[Expr]) -> super::Result<Self> {
        if predicate.is_empty() {
            return Ok(Self::new(
                self.schema.clone(),
                TableState::Loaded(vec![].into()),
                None,
            ));
        }
        if let Some(statistics) = &self.statistics {
            let folded_expr = predicate
                .iter()
                .cloned()
                .reduce(|a, b| a.and(&b))
                .expect("should have at least 1 expr");
            let eval_result = statistics.eval_expression(&folded_expr)?;
            let tv = eval_result.to_truth_value();

            if matches!(tv, TruthValue::False) {
                return Ok(Self::new(
                    self.schema.clone(),
                    TableState::Loaded(vec![].into()),
                    None,
                ));
            }
        }

        let guard = self.state.lock().unwrap();
        let new_state = match guard.deref() {
            TableState::Unloaded(params) => {
                let mut params = params.clone();
                params.filters.extend(predicate.iter().cloned());
                TableState::Unloaded(params)
            }
            TableState::Loaded(tables) => TableState::Loaded(Arc::new(
                tables
                    .iter()
                    .map(|t| t.filter(predicate))
                    .collect::<DaftResult<Vec<_>>>()
                    .context(DaftCoreComputeSnafu)?,
            )),
        };

        // TODO: We should also "filter" the TableStatistics so it's more accurate for downstream tasks
        Ok(Self::new(
            self.schema.clone(),
            new_state,
            self.statistics.clone(),
        ))
    }
}

fn read_parquet_into_micropartition(
    uris: &[&str],
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    // thread in columns and limit
    let runtime_handle = daft_io::get_runtime(true)?;
    let io_client = daft_io::get_io_client(true, io_config.clone())?;
    let metadata = runtime_handle
        .block_on(async move { read_parquet_metadata_bulk(uris, io_client, io_stats).await })?;

    let vals = metadata
        .iter()
        .flat_map(|fm| fm.row_groups.iter().map(|rg| rg.try_into()))
        .collect::<crate::Result<Vec<TableStatistics>>>()?;

    let folded_stats = vals.into_iter().try_reduce(|a, b| a.union(&b))?;

    let first_metadata = metadata.first().expect("we need at least 1 metadata");
    let schema = infer_schema_with_options(first_metadata, &None)?;

    let daft_schema = daft_core::schema::Schema::try_from(&schema)?;
    let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let params = DeferredLoadingParams {
        format_params: FormatParams::Parquet(ParquetSchemaInferenceOptions::default()),
        urls: owned_urls,
        io_config: io_config.clone(),
        multithreaded_io: true,
        filters: vec![],
        limit: None,
        columns: None,
    };

    Ok(MicroPartition::new(
        Arc::new(daft_schema),
        TableState::Unloaded(params),
        folded_stats,
    ))
}

fn read_csv_into_micropartition(
    uris: &[&str],
    parse_options: Option<CsvParseOptions>,
    read_options: Option<CsvReadOptions>,
    schema: Option<SchemaRef>,
    column_names: Option<Vec<String>>,
    io_config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<MicroPartition> {
    // thread in columns and limit
    let schema = match schema {
        Some(schema) => schema,
        None => {
            let runtime_handle = daft_io::get_runtime(true)?;
            let io_client = daft_io::get_io_client(true, io_config.clone())?;
            let owned_parse_options = parse_options.clone();
            let schemas_and_stats = runtime_handle.block_on(async move {
                read_csv_schema_bulk(
                    uris,
                    owned_parse_options,
                    // Default to 1 MiB.
                    Some(1024 * 1024),
                    io_client,
                    io_stats,
                )
                .await
            })?;
            let mut schemas = schemas_and_stats
                .into_iter()
                .map(|pair| pair.0)
                .collect::<Vec<Schema>>();
            debug_assert!(!schemas.is_empty());
            Arc::new(std::mem::take(&mut schemas[0]))
        }
    };

    let owned_urls = uris.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let params = DeferredLoadingParams {
        format_params: FormatParams::Csv(parse_options, read_options, column_names),
        urls: owned_urls,
        io_config: io_config.clone(),
        multithreaded_io: true,
        filters: vec![],
        limit: None,
        columns: None,
    };

    Ok(MicroPartition::new(
        schema,
        TableState::Unloaded(params),
        None,
    ))
}

#[cfg(test)]
mod test {}
