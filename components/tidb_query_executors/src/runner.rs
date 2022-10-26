// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, sync::Arc, collections::HashSet, collections::HashMap,};
use codec::{number::NumberCodec, prelude::NumberDecoder};
use fail::fail_point;
use kvproto::coprocessor::KeyRange;
use kvproto::kvrpcpb::{GetRequest, GetResponse};
use protobuf::Message;
use protobuf::CodedInputStream;
use tidb_query_common::{
    execute_stats::ExecSummary,
    metrics::*,
    storage::{IntervalRange, Storage},
    Result,
};
use itertools::izip;
use tidb_query_datatype::{
    codec::{
        batch::{LazyBatchColumn, LazyBatchColumnVec},
        collation::collator::PADDING_SPACE,
        datum,
        datum::DatumDecoder,
        row, table,
        row::v2::{RowSlice, V1CompatibleEncoder, decode_v2_u64},
        table::{check_index_key, INDEX_VALUE_VERSION_FLAG, MAX_OLD_ENCODED_VALUE_LEN},
        Datum,
    },
    expr::{EvalConfig, EvalContext, EvalWarnings},
    EvalType, FieldTypeAccessor,
};

use tikv_util::{
    deadline::Deadline,
    metrics::{ThrottleType, NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC},
    quota_limiter::QuotaLimiter,
};
use tipb::{
    self, Chunk, ColumnInfo, DagRequest, EncodeType, ExecType, ExecutorExecutionSummary, FieldType,
    SelectResponse, StreamResponse, PointGet, TableInfoDetail, IndexInfoDetail,
};

use super::{
    interface::{BatchExecutor, ExecuteStats, BatchExecuteResult},
    index_scan_executor::{DecodeHandleOp, DecodePartitionIdOp, RestoreData},
    *,
};

use crate::table_scan_executor::HandleIndicesVec;
use crate::util::scan_executor::field_type_from_column_info;
use crate::index_scan_executor::{
    DecodeHandleStrategy, DecodeHandleStrategy::DecodeCommonHandle, DecodeHandleStrategy::DecodeIntHandle, DecodeHandleStrategy::NoDecode,
};

use tikv::storage::errors::extract_key_error;

// TODO: The value is chosen according to some very subjective experience, which
// is not tuned carefully. We need to benchmark to find a best value. Also we
// may consider accepting this value from TiDB side.
const BATCH_INITIAL_SIZE: usize = 32;

// TODO: This value is chosen based on MonetDB/X100's research without our own
// benchmarks.
pub use tidb_query_expr::types::BATCH_MAX_SIZE;

// TODO: Maybe there can be some better strategy. Needs benchmarks and tunes.
const BATCH_GROW_FACTOR: usize = 2;

pub struct BatchExecutorsRunner<SS> {
    /// The deadline of this handler. For each check point (e.g. each iteration)
    /// we need to check whether or not the deadline is exceeded and break
    /// the process if so.
    // TODO: Deprecate it using a better deadline mechanism.
    deadline: Deadline,

    out_most_executor: Box<dyn BatchExecutor<StorageStats = SS>>,

    /// The offset of the columns need to be outputted. For example, TiDB may
    /// only needs a subset of the columns in the result so that unrelated
    /// columns don't need to be encoded and returned back.
    output_offsets: Vec<u32>,

    config: Arc<EvalConfig>,

    /// Whether or not execution summary need to be collected.
    collect_exec_summary: bool,

    exec_stats: ExecuteStats,

    /// Maximum rows to return in batch stream mode.
    stream_row_limit: usize,

    /// The encoding method for the response.
    /// Possible encoding methods are:
    /// 1. default: result is encoded row by row using datum format.
    /// 2. chunk: result is encoded column by column using chunk format.
    encode_type: EncodeType,

    /// If it's a paging request, paging_size indicates to the required size for
    /// current page.
    paging_size: Option<u64>,

    quota_limiter: Arc<QuotaLimiter>,
}

// We assign a dummy type `()` so that we can omit the type when calling
// `check_supported`.
impl BatchExecutorsRunner<()> {
    /// Given a list of executor descriptors and checks whether all executor
    /// descriptors can be used to build batch executors.
    pub fn check_supported(exec_descriptors: &[tipb::Executor]) -> Result<()> {
        for ed in exec_descriptors {
            match ed.get_tp() {
                ExecType::TypeTableScan => {
                    let descriptor = ed.get_tbl_scan();
                    BatchTableScanExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchTableScanExecutor: {}", e))?;
                }
                ExecType::TypeIndexScan => {
                    let descriptor = ed.get_idx_scan();
                    BatchIndexScanExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchIndexScanExecutor: {}", e))?;
                }
                ExecType::TypeSelection => {
                    let descriptor = ed.get_selection();
                    BatchSelectionExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchSelectionExecutor: {}", e))?;
                }
                ExecType::TypeAggregation | ExecType::TypeStreamAgg
                    if ed.get_aggregation().get_group_by().is_empty() =>
                {
                    let descriptor = ed.get_aggregation();
                    BatchSimpleAggregationExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchSimpleAggregationExecutor: {}", e))?;
                }
                ExecType::TypeAggregation => {
                    let descriptor = ed.get_aggregation();
                    if BatchFastHashAggregationExecutor::check_supported(descriptor).is_err() {
                        BatchSlowHashAggregationExecutor::check_supported(descriptor)
                            .map_err(|e| other_err!("BatchSlowHashAggregationExecutor: {}", e))?;
                    }
                }
                ExecType::TypeStreamAgg => {
                    // Note: We won't check whether the source of stream aggregation is in order.
                    //       It is undefined behavior if the source is unordered.
                    let descriptor = ed.get_aggregation();
                    BatchStreamAggregationExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchStreamAggregationExecutor: {}", e))?;
                }
                ExecType::TypeLimit => {}
                ExecType::TypeTopN => {
                    let descriptor = ed.get_top_n();
                    BatchTopNExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchTopNExecutor: {}", e))?;
                }
                ExecType::TypeProjection => {
                    let descriptor = ed.get_projection();
                    BatchProjectionExecutor::check_supported(descriptor)
                        .map_err(|e| other_err!("BatchProjectionExecutor: {}", e))?;
                }
                ExecType::TypeJoin => {
                    other_err!("Join executor not implemented");
                }
                ExecType::TypeKill => {
                    other_err!("Kill executor not implemented");
                }
                ExecType::TypeExchangeSender => {
                    other_err!("ExchangeSender executor not implemented");
                }
                ExecType::TypeExchangeReceiver => {
                    other_err!("ExchangeReceiver executor not implemented");
                }
                ExecType::TypePartitionTableScan => {
                    other_err!("PartitionTableScan executor not implemented");
                }
                ExecType::TypeSort => {
                    other_err!("TypeSort executor not implemented");
                }
                ExecType::TypeWindow => {
                    other_err!("TypeWindow executor not implemented");
                }
            }
        }

        Ok(())
    }
}

#[inline]
fn is_arrow_encodable(schema: &[FieldType]) -> bool {
    schema
        .iter()
        .all(|schema| EvalType::try_from(schema.as_accessor().tp()).is_ok())
}

#[allow(clippy::explicit_counter_loop)]
pub fn build_executors<S: Storage + 'static>(
    executor_descriptors: Vec<tipb::Executor>,
    storage: S,
    ranges: Vec<KeyRange>,
    config: Arc<EvalConfig>,
    is_scanned_range_aware: bool,
) -> Result<Box<dyn BatchExecutor<StorageStats = S::Statistics>>> {
    let mut executor_descriptors = executor_descriptors.into_iter();
    let mut first_ed = executor_descriptors
        .next()
        .ok_or_else(|| other_err!("No executors"))?;

    let mut summary_slot_index = 0;
    // Limit executor use this flag to check if its src is table/index scan.
    // Performance enhancement for plan like: limit 1 -> table/index scan.
    let mut is_src_scan_executor = true;

    let mut executor: Box<dyn BatchExecutor<StorageStats = S::Statistics>> = match first_ed.get_tp()
    {
        ExecType::TypeTableScan => {
            EXECUTOR_COUNT_METRICS.batch_table_scan.inc();

            let mut descriptor = first_ed.take_tbl_scan();
            let columns_info = descriptor.take_columns().into();
            let primary_column_ids = descriptor.take_primary_column_ids();
            let primary_prefix_column_ids = descriptor.take_primary_prefix_column_ids();

            Box::new(
                BatchTableScanExecutor::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    primary_column_ids,
                    descriptor.get_desc(),
                    is_scanned_range_aware,
                    primary_prefix_column_ids,
                )?
                .collect_summary(summary_slot_index),
            )
        }
        ExecType::TypeIndexScan => {
            EXECUTOR_COUNT_METRICS.batch_index_scan.inc();

            let mut descriptor = first_ed.take_idx_scan();
            let columns_info = descriptor.take_columns().into();
            let primary_column_ids_len = descriptor.take_primary_column_ids().len();
            Box::new(
                BatchIndexScanExecutor::new(
                    storage,
                    config.clone(),
                    columns_info,
                    ranges,
                    primary_column_ids_len,
                    descriptor.get_desc(),
                    descriptor.get_unique(),
                    is_scanned_range_aware,
                )?
                .collect_summary(summary_slot_index),
            )
        }
        _ => {
            return Err(other_err!(
                "Unexpected first executor {:?}",
                first_ed.get_tp()
            ));
        }
    };

    for mut ed in executor_descriptors {
        summary_slot_index += 1;

        executor = match ed.get_tp() {
            ExecType::TypeSelection => {
                EXECUTOR_COUNT_METRICS.batch_selection.inc();

                Box::new(
                    BatchSelectionExecutor::new(
                        config.clone(),
                        executor,
                        ed.take_selection().take_conditions().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeProjection => {
                EXECUTOR_COUNT_METRICS.batch_projection.inc();

                Box::new(
                    BatchProjectionExecutor::new(
                        config.clone(),
                        executor,
                        ed.take_projection().take_exprs().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation | ExecType::TypeStreamAgg
                if ed.get_aggregation().get_group_by().is_empty() =>
            {
                EXECUTOR_COUNT_METRICS.batch_simple_aggr.inc();

                Box::new(
                    BatchSimpleAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeAggregation => {
                if BatchFastHashAggregationExecutor::check_supported(ed.get_aggregation()).is_ok() {
                    EXECUTOR_COUNT_METRICS.batch_fast_hash_aggr.inc();

                    Box::new(
                        BatchFastHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                } else {
                    EXECUTOR_COUNT_METRICS.batch_slow_hash_aggr.inc();

                    Box::new(
                        BatchSlowHashAggregationExecutor::new(
                            config.clone(),
                            executor,
                            ed.mut_aggregation().take_group_by().into(),
                            ed.mut_aggregation().take_agg_func().into(),
                        )?
                        .collect_summary(summary_slot_index),
                    )
                }
            }
            ExecType::TypeStreamAgg => {
                EXECUTOR_COUNT_METRICS.batch_stream_aggr.inc();

                Box::new(
                    BatchStreamAggregationExecutor::new(
                        config.clone(),
                        executor,
                        ed.mut_aggregation().take_group_by().into(),
                        ed.mut_aggregation().take_agg_func().into(),
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeLimit => {
                EXECUTOR_COUNT_METRICS.batch_limit.inc();

                Box::new(
                    BatchLimitExecutor::new(
                        executor,
                        ed.get_limit().get_limit() as usize,
                        is_src_scan_executor,
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            ExecType::TypeTopN => {
                EXECUTOR_COUNT_METRICS.batch_top_n.inc();

                let mut d = ed.take_top_n();
                let order_bys = d.get_order_by().len();
                let mut order_exprs_def = Vec::with_capacity(order_bys);
                let mut order_is_desc = Vec::with_capacity(order_bys);
                for mut item in d.take_order_by().into_iter() {
                    order_exprs_def.push(item.take_expr());
                    order_is_desc.push(item.get_desc());
                }

                Box::new(
                    BatchTopNExecutor::new(
                        config.clone(),
                        executor,
                        order_exprs_def,
                        order_is_desc,
                        d.get_limit() as usize,
                    )?
                    .collect_summary(summary_slot_index),
                )
            }
            _ => {
                return Err(other_err!(
                    "Unexpected non-first executor {:?}",
                    ed.get_tp()
                ));
            }
        };
        is_src_scan_executor = false;
    }

    Ok(executor)
}

impl<SS: 'static> BatchExecutorsRunner<SS> {
    pub fn from_request<S: Storage<Statistics = SS> + 'static>(
        mut req: DagRequest,
        ranges: Vec<KeyRange>,
        storage: S,
        deadline: Deadline,
        stream_row_limit: usize,
        is_streaming: bool,
        paging_size: Option<u64>,
        quota_limiter: Arc<QuotaLimiter>,
    ) -> Result<Self> {
        let executors_len = req.get_executors().len();
        let collect_exec_summary = req.get_collect_execution_summaries();
        let mut config = EvalConfig::from_request(&req)?;
        config.paging_size = paging_size;
        let config = Arc::new(config);

        let out_most_executor = build_executors(
            req.take_executors().into(),
            storage,
            ranges,
            config.clone(),
            is_streaming || paging_size.is_some(), /* For streaming and paging request,
                                                    * executors will continue scan from range
                                                    * end where last scan is finished */
        )?;

        let encode_type = if !is_arrow_encodable(out_most_executor.schema()) {
            EncodeType::TypeDefault
        } else {
            req.get_encode_type()
        };

        // Check output offsets
        let output_offsets = req.take_output_offsets();
        let schema_len = out_most_executor.schema().len();
        for offset in &output_offsets {
            if (*offset as usize) >= schema_len {
                return Err(other_err!(
                    "Invalid output offset (schema has {} columns, access index {})",
                    schema_len,
                    offset
                ));
            }
        }

        let exec_stats = ExecuteStats::new(executors_len);

        Ok(Self {
            deadline,
            out_most_executor,
            output_offsets,
            config,
            collect_exec_summary,
            exec_stats,
            stream_row_limit,
            encode_type,
            paging_size,
            quota_limiter,
        })
    }

    fn batch_initial_size() -> usize {
        fail_point!("copr_batch_initial_size", |r| r
            .map_or(1, |e| e.parse().unwrap()));
        BATCH_INITIAL_SIZE
    }

    /// handle_request returns the response of selection and an optional range,
    /// only paging request will return Some(IntervalRange),
    /// this should be used when calculating ranges of the next batch.
    /// IntervalRange records whole range scanned though there are gaps in multi
    /// ranges. e.g.: [(k1 -> k2), (k4 -> k5)] may got response (k1, k2, k4)
    /// with IntervalRange like (k1, k4).
    pub async fn handle_request(&mut self) -> Result<(SelectResponse, Option<IntervalRange>)> {
        let mut chunks = vec![];
        let mut batch_size = Self::batch_initial_size();
        let mut warnings = self.config.new_eval_warnings();
        let mut ctx = EvalContext::new(self.config.clone());
        let mut record_all = 0;

        loop {
            let mut chunk = Chunk::default();
            let mut sample = self.quota_limiter.new_sample(true);
            let (drained, record_len) = {
                let (cpu_time, res) = sample
                    .observe_cpu_async(self.internal_handle_request(
                        false,
                        batch_size,
                        &mut chunk,
                        &mut warnings,
                        &mut ctx,
                    ))
                    .await;
                sample.add_cpu_time(cpu_time);
                res?
            };
            if chunk.has_rows_data() {
                sample.add_read_bytes(chunk.get_rows_data().len());
            }

            let quota_delay = self.quota_limiter.consume_sample(sample, true).await;
            if !quota_delay.is_zero() {
                NON_TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                    .get(ThrottleType::dag)
                    .inc_by(quota_delay.as_micros() as u64);
            }

            if record_len > 0 {
                chunks.push(chunk);
                record_all += record_len;
            }

            if drained || self.paging_size.map_or(false, |p| record_all >= p as usize) {
                self.out_most_executor
                    .collect_exec_stats(&mut self.exec_stats);

                let range = self
                    .paging_size
                    .map(|_| self.out_most_executor.take_scanned_range());

                let mut sel_resp = SelectResponse::default();
                sel_resp.set_chunks(chunks.into());
                sel_resp.set_encode_type(self.encode_type);

                // TODO: output_counts should not be i64. Let's fix it in Coprocessor DAG V2.
                sel_resp.set_output_counts(
                    self.exec_stats
                        .scanned_rows_per_range
                        .iter()
                        .map(|v| *v as i64)
                        .collect(),
                );

                if self.collect_exec_summary {
                    let summaries = self
                        .exec_stats
                        .summary_per_executor
                        .iter()
                        .map(|summary| {
                            let mut ret = ExecutorExecutionSummary::default();
                            ret.set_num_iterations(summary.num_iterations as u64);
                            ret.set_num_produced_rows(summary.num_produced_rows as u64);
                            ret.set_time_processed_ns(summary.time_processed_ns as u64);
                            ret
                        })
                        .collect::<Vec<_>>();
                    sel_resp.set_execution_summaries(summaries.into());
                }

                sel_resp.set_warnings(warnings.warnings.into());
                sel_resp.set_warning_count(warnings.warning_cnt as i64);
                return Ok((sel_resp, range));
            }

            // Grow batch size
            grow_batch_size(&mut batch_size);
        }
    }

    pub async fn handle_streaming_request(
        &mut self,
    ) -> Result<(Option<(StreamResponse, IntervalRange)>, bool)> {
        let mut warnings = self.config.new_eval_warnings();

        let (mut record_len, mut is_drained) = (0, false);
        let mut chunk = Chunk::default();
        let mut ctx = EvalContext::new(self.config.clone());
        let batch_size = self.stream_row_limit.min(BATCH_MAX_SIZE);

        // record count less than batch size and is not drained
        while record_len < self.stream_row_limit && !is_drained {
            let mut current_chunk = Chunk::default();
            // TODO: Streaming coprocessor on TiKV is just not enabled in TiDB now.
            let (drained, len) = self
                .internal_handle_request(
                    true,
                    batch_size.min(self.stream_row_limit - record_len),
                    &mut current_chunk,
                    &mut warnings,
                    &mut ctx,
                )
                .await?;
            chunk
                .mut_rows_data()
                .extend_from_slice(current_chunk.get_rows_data());
            record_len += len;
            is_drained = drained;
        }

        if !is_drained || record_len > 0 {
            let range = self.out_most_executor.take_scanned_range();
            return self
                .make_stream_response(chunk, warnings)
                .map(|r| (Some((r, range)), is_drained));
        }
        Ok((None, true))
    }

    pub fn collect_storage_stats(&mut self, dest: &mut SS) {
        self.out_most_executor.collect_storage_stats(dest);
    }

    pub fn can_be_cached(&self) -> bool {
        self.out_most_executor.can_be_cached()
    }

    pub fn collect_scan_summary(&mut self, dest: &mut ExecSummary) {
        // Get the first executor which is always the scan executor
        if let Some(exec_stat) = self.exec_stats.summary_per_executor.first() {
            dest.clone_from(exec_stat);
        }
    }

    async fn internal_handle_request(
        &mut self,
        is_streaming: bool,
        batch_size: usize,
        chunk: &mut Chunk,
        warnings: &mut EvalWarnings,
        ctx: &mut EvalContext,
    ) -> Result<(bool, usize)> {
        let mut record_len = 0;

        self.deadline.check()?;

        let mut result = self.out_most_executor.next_batch(batch_size).await;

        let is_drained = result.is_drained?;

        if !result.logical_rows.is_empty() {
            assert_eq!(
                result.physical_columns.columns_len(),
                self.out_most_executor.schema().len()
            );
            {
                let data = chunk.mut_rows_data();
                // Although `schema()` can be deeply nested, it is ok since we process data in
                // batch.
                if is_streaming || self.encode_type == EncodeType::TypeDefault {
                    data.reserve(
                        result
                            .physical_columns
                            .maximum_encoded_size(&result.logical_rows, &self.output_offsets),
                    );
                    result.physical_columns.encode(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schema(),
                        data,
                        ctx,
                    )?;
                } else {
                    data.reserve(
                        result
                            .physical_columns
                            .maximum_encoded_size_chunk(&result.logical_rows, &self.output_offsets),
                    );
                    result.physical_columns.encode_chunk(
                        &result.logical_rows,
                        &self.output_offsets,
                        self.out_most_executor.schema(),
                        data,
                        ctx,
                    )?;
                }
            }
            record_len += result.logical_rows.len();
        }

        warnings.merge(&mut result.warnings);
        Ok((is_drained, record_len))
    }

    fn make_stream_response(
        &mut self,
        chunk: Chunk,
        warnings: EvalWarnings,
    ) -> Result<StreamResponse> {
        self.out_most_executor
            .collect_exec_stats(&mut self.exec_stats);

        let mut s_resp = StreamResponse::default();
        s_resp.set_data(box_try!(chunk.write_to_bytes()));

        s_resp.set_output_counts(
            self.exec_stats
                .scanned_rows_per_range
                .iter()
                .map(|v| *v as i64)
                .collect(),
        );

        s_resp.set_warnings(warnings.warnings.into());
        s_resp.set_warning_count(warnings.warning_cnt as i64);

        self.exec_stats.clear();

        Ok(s_resp)
    }
}

#[inline]
fn batch_grow_factor() -> usize {
    fail_point!("copr_batch_grow_size", |r| r
        .map_or(1, |e| e.parse().unwrap()));
    BATCH_GROW_FACTOR
}

#[inline]
fn grow_batch_size(batch_size: &mut usize) {
    if *batch_size < BATCH_MAX_SIZE {
        *batch_size *= batch_grow_factor();
        if *batch_size > BATCH_MAX_SIZE {
            *batch_size = BATCH_MAX_SIZE
        }
    }
}

pub struct PointGetExecutorsRunner {

    table_point_get_executor: Option<TablePointGetExecutorImpl>,

    index_point_get_executor: Option<IndexPointGetExecutorImpl>,

    /// The offset of the columns need to be outputted. For example, TiDB may
    /// only needs a subset of the columns in the result so that unrelated
    /// columns don't need to be encoded and returned back.
    output_offsets: Vec<u32>,

    encode_type: EncodeType,

    context: EvalContext,
}

impl PointGetExecutorsRunner {
    pub fn new(
        point_get: PointGet,
    ) -> Result<Self> {
        let mut table_point_get_executor;
        let mut index_point_get_executor;
        let mut config = EvalConfig::default();
        let config = Arc::new(config);
        let context = EvalContext::new(config);
        let mut is_arrow_enable = false;

        if point_get.has_read_table() {
            index_point_get_executor = None;

            let table = point_get.take_read_table();
            let columns_info: Vec<ColumnInfo> = table.take_columns().into();
            let primary_column_ids = table.take_primary_column_ids();
            let primary_prefix_column_ids = table.take_primary_prefix_column_ids();
            let is_column_filled = vec![false; columns_info.len()];
            let mut handle_indices = HandleIndicesVec::new();
            let mut schema = Vec::with_capacity(columns_info.len());
            let mut columns_default_value = Vec::with_capacity(columns_info.len());
            let mut column_id_index = HashMap::default();
            let mut is_key_only = true;

            let primary_column_ids_set = primary_column_ids.iter().collect::<HashSet<_>>();
            let primary_prefix_column_ids_set =
            primary_prefix_column_ids.iter().collect::<HashSet<_>>();

            for (index, mut ci) in columns_info.into_iter().enumerate() {
                // For each column info, we need to extract the following info:
                // - Corresponding field type (push into `schema`).
                schema.push(field_type_from_column_info(&ci));
    
                // - Prepare column default value (will be used to fill missing column later).
                columns_default_value.push(ci.take_default_val());
    
                // - Store the index of the PK handles.
                // - Check whether or not we don't need KV values (iff PK handle is given).
                if ci.get_pk_handle() {
                    handle_indices.push(index);
                } else {
                    if !primary_column_ids_set.contains(&ci.get_column_id())
                        || primary_prefix_column_ids_set.contains(&ci.get_column_id())
                        || ci.need_restored_data()
                    {
                        is_key_only = false;
                    }
                    column_id_index.insert(ci.get_column_id(), index);
                }
    
                // Note: if two PK handles are given, we will only preserve the
                // *last* one. Also if two columns with the same column
                // id are given, we will only preserve the *last* one.
            }
            is_arrow_enable = is_arrow_encodable(&schema);
            let imp = TablePointGetExecutorImpl {
                context: EvalContext::new(config),
                schema,
                columns_default_value,
                column_id_index,
                handle_indices,
                primary_column_ids,
                is_column_filled,
            };
            table_point_get_executor = Some(imp);            
        }
        else {
            table_point_get_executor = None;
            let index_info = point_get.take_read_index();
            let columns_info: Vec<ColumnInfo> = index_info.take_columns().into();
            let primary_column_ids_len = index_info.take_primary_column_ids().len();

            let physical_table_id_column_cnt = columns_info.last().map_or(0, |ci| {
                (ci.get_column_id() == table::EXTRA_PHYSICAL_TABLE_ID_COL_ID) as usize
            });
            let pid_column_cnt = columns_info
                .get(columns_info.len() - 1 - physical_table_id_column_cnt)
                .map_or(0, |ci| {
                    (ci.get_column_id() == table::EXTRA_PARTITION_ID_COL_ID) as usize
                });
            let is_int_handle = columns_info
                .get(columns_info.len() - 1 - pid_column_cnt - physical_table_id_column_cnt)
                .map_or(false, |ci| ci.get_pk_handle());
            let is_common_handle = primary_column_ids_len > 0;
            let (decode_handle_strategy, handle_column_cnt) = match (is_int_handle, is_common_handle) {
                (false, false) => (NoDecode, 0),
                (false, true) => (DecodeCommonHandle, primary_column_ids_len),
                (true, false) => (DecodeIntHandle, 1),
                // TiDB may accidentally push down both int handle or common handle.
                // However, we still try to decode int handle.
                _ => {
                    return Err(other_err!(
                        "Both int handle and common handle are push downed"
                    ));
                }
            };
    
            if handle_column_cnt + pid_column_cnt + physical_table_id_column_cnt > columns_info.len() {
                return Err(other_err!(
                    "The number of handle columns exceeds the length of `columns_info`"
                ));
            }
    
            let schema: Vec<_> = columns_info
                .iter()
                .map(|ci| field_type_from_column_info(ci))
                .collect();
    
            let columns_id_without_handle: Vec<_> = columns_info[..columns_info.len()
                - handle_column_cnt
                - pid_column_cnt
                - physical_table_id_column_cnt]
                .iter()
                .map(|ci| ci.get_column_id())
                .collect();
    
            let columns_id_for_common_handle = columns_info[columns_id_without_handle.len()
                ..columns_info.len() - pid_column_cnt - physical_table_id_column_cnt]
                .iter()
                .map(|ci| ci.get_column_id())
                .collect();
            is_arrow_enable = is_arrow_encodable(&schema);
            let imp = IndexPointGetExecutorImpl {
                context: EvalContext::new(config),
                schema,
                columns_id_without_handle,
                columns_id_for_common_handle,
                decode_handle_strategy,
                pid_column_cnt,
                physical_table_id_column_cnt,
                index_version: -1,
            };
        } 

        let encode_type = if !is_arrow_enable {
            warn!("the schema doesn't support arrow encoding");
            return Err(other_err!(
                "the schema doesn't support arrow encoding"
            ));           
        } else {
            EncodeType::TypeChunk
        };
        let output_offsets = point_get.take_output_offsets();
        Ok(Self {
            table_point_get_executor,
            index_point_get_executor,
            output_offsets,
            encode_type,
            context,
        })
    }

    fn schema(&self) -> &[FieldType] {
        if let Some(table) = self.table_point_get_executor {
            table.schema()
        } else {
            let index = self.index_point_get_executor.unwrap();
            index.schema()
        }
    }

    fn internal_handle_request(&mut self,
        key: &[u8],
        val: &[u8],
        chunk: &mut Chunk,
    ) -> Result<()>{
        let mut result;
        if let Some(table_point_executor) = self.table_point_get_executor {
            result = table_point_executor.convert_kv_to_batch_execute_result(key, &val)?;

        } else if let Some(index_point_executor) = self.index_point_get_executor {
            result = index_point_executor.convert_kv_to_batch_execute_result(key, &val)?;
        } else {
            unimplemented!()
        }

        assert!(!result.logical_rows.is_empty());
        assert_eq!(
            result.physical_columns.columns_len(),
            self.schema().len()
        );

        let data = chunk.mut_rows_data();

        if self.encode_type == EncodeType::TypeDefault {
            unimplemented!()
        } else {
            data.reserve(
                result
                    .physical_columns
                    .maximum_encoded_size_chunk(&result.logical_rows, &self.output_offsets),
            );
            result.physical_columns.encode_chunk(
                &result.logical_rows,
                &self.output_offsets,
                self.schema(),
                data,
                &mut self.context,
            )?;
        }
        return Ok(());        
    }
}

struct TablePointGetExecutorImpl {
    /// Note: Although called `EvalContext`, it is some kind of execution
    /// context instead.
    // TODO: Rename EvalContext to ExecContext.
    context: EvalContext,

    /// The schema of the output. All of the output come from specific columns
    /// in the underlying storage.
    schema: Vec<FieldType>,

    columns_default_value: Vec<Vec<u8>>,


    /// The output position in the schema giving the column id.
    column_id_index: HashMap<i64, usize>,

    /// Vec of indices in output row to put the handle. The indices must be
    /// sorted in the vec.
    handle_indices: HandleIndicesVec,

    /// Vec of Primary key column's IDs.
    primary_column_ids: Vec<i64>,

    /// A vector of flags indicating whether corresponding column is filled in
    /// `next_batch`. It is a struct level field in order to prevent repeated
    /// memory allocations since its length is fixed for each `next_batch` call.
    is_column_filled: Vec<bool>,
}

impl TablePointGetExecutorImpl {
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }
    fn build_column_vec(&self) -> LazyBatchColumnVec {
        let scan_rows = 1;
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        // If there are any PK columns, for each of them, fill non-PK columns before it
        // and push the PK column.
        // For example, consider:
        //                  non-pk non-pk non-pk pk non-pk non-pk pk pk non-pk non-pk
        // handle_indices:                       ^3               ^6 ^7
        // Each turn of the following loop will push this to `columns`:
        // 1st turn: [non-pk, non-pk, non-pk, pk]
        // 2nd turn: [non-pk, non-pk, pk]
        // 3rd turn: [pk]
        let physical_table_id_column_idx = self
            .column_id_index
            .get(&table::EXTRA_PHYSICAL_TABLE_ID_COL_ID)
            .copied();
        let mut last_index = 0usize;
        for handle_index in &self.handle_indices {
            // `handle_indices` is expected to be sorted.
            assert!(*handle_index >= last_index);

            // Fill last `handle_index - 1` columns.
            for i in last_index..*handle_index {
                if Some(i) == physical_table_id_column_idx {
                    columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                        scan_rows,
                        EvalType::Int,
                    ));
                } else {
                    columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
                }
            }

            // For PK handles, we construct a decoded `VectorValue` because it is directly
            // stored as i64, without a datum flag, at the end of key.
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));

            last_index = *handle_index + 1;
        }

        // Then fill remaining columns after the last handle column. If there are no PK
        // columns, the previous loop will be skipped and this loop will be run
        // on 0..columns_len. For the example above, this loop will push:
        // [non-pk, non-pk]
        for i in last_index..columns_len {
            if Some(i) == physical_table_id_column_idx {
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            } else {
                columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
            }
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)

    }


    fn fill_column_vec(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {

        if let Err(e) = self.process_kv_pair(&key, &value, columns) {
            // When there are errors in `process_kv_pair`, columns' length may not be
            // identical. For example, the filling process may be partially done so that
            // first several columns have N rows while the rest have N-1 rows. Since we do
            // not immediately fail when there are errors, these irregular columns may
            // further cause future executors to panic. So let's truncate these columns to
            // make they all have N-1 rows in that case.
            columns.truncate_into_equal_length();
            return Err(e);
        }       

        Ok(())
    }

    fn process_kv_pair(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let columns_len = self.schema.len();
        let mut decoded_columns = 0;

        if value.is_empty() || (value.len() == 1 && value[0] == datum::NIL_FLAG) {
            // Do nothing
        } else {
            match value[0] {
                row::v2::CODEC_VERSION => self.process_v2(value, columns, &mut decoded_columns)?,
                _ => self.process_v1(key, value, columns, &mut decoded_columns)?,
            }
        }

        if !self.handle_indices.is_empty() {
            // In this case, An int handle is expected.
            let handle = table::decode_int_handle(key)?;

            for handle_index in &self.handle_indices {
                // TODO: We should avoid calling `push_int` repeatedly. Instead we should
                // specialize a `&mut Vec` first. However it is hard to program
                // due to lifetime restriction.
                if !self.is_column_filled[*handle_index] {
                    columns[*handle_index].mut_decoded().push_int(Some(handle));
                    decoded_columns += 1;
                    self.is_column_filled[*handle_index] = true;
                }
            }
        } else if !self.primary_column_ids.is_empty() {
            // Otherwise, if `primary_column_ids` is not empty, we try to extract the values
            // of the columns from the common handle.
            let mut handle = table::decode_common_handle(key)?;
            for primary_id in self.primary_column_ids.iter() {
                let index = self.column_id_index.get(primary_id);
                let (datum, remain) = datum::split_datum(handle, false)?;
                handle = remain;

                // If the column info of the corresponding primary column id is missing, we
                // ignore this slice of the datum.
                if let Some(&index) = index {
                    if !self.is_column_filled[index] {
                        columns[index].mut_raw().push(datum);
                        decoded_columns += 1;
                        self.is_column_filled[index] = true;
                    }
                }
            }
        } else {
            table::check_record_key(key)?;
        }

        let some_physical_table_id_column_index = self
            .column_id_index
            .get(&table::EXTRA_PHYSICAL_TABLE_ID_COL_ID);
        if let Some(idx) = some_physical_table_id_column_index {
            let table_id = table::decode_table_id(key)?;
            columns[*idx].mut_decoded().push_int(Some(table_id));
            self.is_column_filled[*idx] = true;
        }

        // Some fields may be missing in the row, we push corresponding default value to
        // make all columns in same length.
        for i in 0..columns_len {
            if !self.is_column_filled[i] {
                // Missing fields must not be a primary key, so it must be
                // `LazyBatchColumn::raw`.

                let default_value = if !self.columns_default_value[i].is_empty() {
                    // default value is provided, use the default value
                    self.columns_default_value[i].as_slice()
                } else if !self.schema[i]
                    .as_accessor()
                    .flag()
                    .contains(tidb_query_datatype::FieldTypeFlag::NOT_NULL)
                {
                    // NULL is allowed, use NULL
                    datum::DATUM_DATA_NULL
                } else {
                    return Err(other_err!(
                        "Data is corrupted, missing data for NOT NULL column (offset = {})",
                        i
                    ));
                };

                columns[i].mut_raw().push(default_value);
            } else {
                // Reset to not-filled, prepare for next function call.
                self.is_column_filled[i] = false;
            }
        }

        Ok(())
    }

    pub fn convert_kv_to_batch_execute_result(&mut self,
        key: &[u8],
        value: &[u8]) -> Result<BatchExecuteResult>{
        let mut logical_columns = self.build_column_vec();
        self.fill_column_vec(key, value, &mut logical_columns)?;
        logical_columns.assert_columns_equal_length();
        let logical_rows = (0..logical_columns.rows_len()).collect();        

        Ok(BatchExecuteResult {
            physical_columns: logical_columns,
            logical_rows,
            is_drained: Ok(true),
            warnings: self.context.take_warnings(),
        })
    }

    fn process_v1(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
        decoded_columns: &mut usize,
    ) -> Result<()> {
        // The layout of value is: [col_id_1, value_1, col_id_2, value_2, ...]
        // where each element is datum encoded.
        // The column id datum must be in var i64 type.
        let columns_len = columns.columns_len();
        let mut remaining = value;
        while !remaining.is_empty() && *decoded_columns < columns_len {
            if remaining[0] != datum::VAR_INT_FLAG {
                return Err(other_err!(
                    "Unable to decode row: column id must be VAR_INT"
                ));
            }
            remaining = &remaining[1..];
            let column_id = box_try!(remaining.read_var_i64());
            let (val, new_remaining) = datum::split_datum(remaining, false)?;
            // Note: The produced columns may be not in the same length if there is error
            // due to corrupted data. It will be handled in `ScanExecutor`.
            let some_index = self.column_id_index.get(&column_id);
            if let Some(index) = some_index {
                let index = *index;
                if !self.is_column_filled[index] {
                    columns[index].mut_raw().push(val);
                    *decoded_columns += 1;
                    self.is_column_filled[index] = true;
                } else {
                    // This indicates that there are duplicated elements in the row, which is
                    // unexpected. We won't abort the request or overwrite the previous element,
                    // but will output a log anyway.
                    warn!(
                        "Ignored duplicated row datum in table scan";
                        "key" => log_wrappers::Value::key(key),
                        "value" => log_wrappers::Value::value(value),
                        "dup_column_id" => column_id,
                    );
                }
            }
            remaining = new_remaining;
        }
        Ok(())
    }

    fn process_v2(
        &mut self,
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
        decoded_columns: &mut usize,
    ) -> Result<()> {
        let row = RowSlice::from_bytes(value)?;
        for (col_id, idx) in &self.column_id_index {
            if self.is_column_filled[*idx] {
                continue;
            }
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[*idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[*idx])?;
                *decoded_columns += 1;
                self.is_column_filled[*idx] = true;
            } else if row.search_in_null_ids(*col_id) {
                columns[*idx].mut_raw().push(datum::DATUM_DATA_NULL);
                *decoded_columns += 1;
                self.is_column_filled[*idx] = true;
            } else {
                // This column is missing. It will be filled with default values
                // later.
            }
        }
        Ok(())
    }    
}

struct IndexPointGetExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// ID of interested columns (exclude PK handle column).
    columns_id_without_handle: Vec<i64>,

    columns_id_for_common_handle: Vec<i64>,

    /// The strategy to decode handles.
    /// Handle will be always placed in the last column.
    decode_handle_strategy: DecodeHandleStrategy,

    /// Number of partition ID columns, now it can only be 0 or 1.
    /// Must be after all normal columns and handle, but before
    /// physical_table_id_column
    pid_column_cnt: usize,

    /// Number of Physical Table ID columns, can only be 0 or 1.
    /// Must be last, after pid_column
    physical_table_id_column_cnt: usize,

    index_version: i64,
}

impl IndexPointGetExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    pub fn convert_kv_to_batch_execute_result(&mut self,
        key: &[u8],
        value: &[u8]) -> Result<BatchExecuteResult>{
        let mut logical_columns = self.build_column_vec(1);
        self.fill_column_vec(key, value, &mut logical_columns)?;
        logical_columns.assert_columns_equal_length();
        let logical_rows = (0..logical_columns.rows_len()).collect();        

        Ok(BatchExecuteResult {
            physical_columns: logical_columns,
            logical_rows,
            is_drained: Ok(true),
            warnings: self.context.take_warnings(),
        })
    }

    fn fill_column_vec(
        &mut self,
        key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {

        if let Err(e) = self.process_kv_pair(&key, &value, columns) {
            // When there are errors in `process_kv_pair`, columns' length may not be
            // identical. For example, the filling process may be partially done so that
            // first several columns have N rows while the rest have N-1 rows. Since we do
            // not immediately fail when there are errors, these irregular columns may
            // further cause future executors to panic. So let's truncate these columns to
            // make they all have N-1 rows in that case.
            columns.truncate_into_equal_length();
            return Err(e);
        }       

        Ok(())
    }

    /// Constructs empty columns, with PK containing int handle in decoded
    /// format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan
    /// executor but due to different reasons.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        for _ in 0..self.columns_id_without_handle.len() {
            columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
        }

        match self.decode_handle_strategy {
            NoDecode => {}
            DecodeIntHandle => {
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            }
            DecodeCommonHandle => {
                for _ in self.columns_id_without_handle.len()
                    ..columns_len - self.pid_column_cnt - self.physical_table_id_column_cnt
                {
                    columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
                }
            }
        }

        if self.pid_column_cnt > 0 {
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
        }

        if self.physical_table_id_column_cnt > 0 {
            columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                scan_rows,
                EvalType::Int,
            ));
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    // Value layout: (see https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing)
    // ```text
    // 		+-- IndexValueVersion0  (with restore data, or common handle, or index is global)
    // 		|
    // 		|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
    // 		|  Length:   1     | len(options) | len(padding) |    8        |     1
    // 		|
    // 		|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
    // 		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    // 		|                 See below for more information.
    // 		|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
    // 		|  IntHandle:     Only exists when table use int handles and index is unique.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		+-- Old Encoding (without restore data, integer handle, local)
    // 		|
    // 		|  Layout: [Handle] | [UntouchedFlag]
    // 		|  Length:   8      |     1
    // 		|
    // 		|  Handle:        Only exists in unique index.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		|  If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
    // 		|  Length of value <= 9, use to distinguish from the new encoding.
    // 		|
    // 		+-- IndexValueForClusteredIndexVersion1
    // 		|
    // 		|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options      |   [UntouchedFlag]
    // 		|  Length:   1     |        1        |      1         |  len(options) |         1
    // 		|
    // 		|  TailLen:       len(UntouchedFlag)
    // 		|  Options:       Encode some value for new features, such as common handle, new collations or global index.
    // 		|                 See below for more information.
    // 		|  UntouchedFlag: Only exists when index is untouched.
    // 		|
    // 		|  Layout of Options:
    // 		|
    // 		|     Segment:             Common Handle                 |     Global Index      |   New Collation
    // 		|     Layout:  CHandle Flag | CHandle Len | CHandle      | PidFlag | PartitionID |    restoreData
    // 		|     Length:     1         | 2           | len(CHandle) |    1    |    8        |   len(restoreData)
    // 		|
    // 		|     Common Handle Segment: Exists when unique index used common handles.
    // 		|     Global Index Segment:  Exists when index is global.
    // 		|     New Collation Segment: Exists when new collation is used and index or handle contains non-binary string.
    // 		|     In v4.0, restored data contains all the index values. For example, (a int, b char(10)) and index (a, b).
    // 		|     The restored data contains both the values of a and b.
    // 		|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the above example, the restored data contains only the value of b.
    // 		|     Besides, if the collation of b is _bin, then restored data is an integer indicate the spaces are truncated. Then we use sortKey
    // 		|     and the restored data together to restore original data.
    // ```
    #[inline]
    fn process_kv_pair(
        &mut self,
        mut key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        check_index_key(key)?;
        if self.physical_table_id_column_cnt > 0 {
            self.process_physical_table_id_column(key, columns)?;
        }
        key = &key[table::PREFIX_LEN + table::ID_LEN..];
        if self.index_version == -1 {
            self.index_version = Self::get_index_version(value)?
        }
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            self.process_kv_general(key, value, columns)
        } else {
            self.process_old_collation_kv(key, value, columns)
        }
    }

    #[inline]
    fn decode_int_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    #[inline]
    fn decode_int_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn extract_columns_from_row_format(
        &mut self,
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let row = RowSlice::from_bytes(value)?;
        for (idx, col_id) in self.columns_id_without_handle.iter().enumerate() {
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[idx])?;
            } else if row.search_in_null_ids(*col_id) {
                columns[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing column {}", col_id));
            }
        }
        Ok(())
    }

    fn extract_columns_from_datum_format(
        datum: &mut &[u8],
        columns: &mut [LazyBatchColumn],
    ) -> Result<()> {
        for (i, column) in columns.iter_mut().enumerate() {
            if datum.is_empty() {
                return Err(other_err!("{}th column is missing value", i));
            }
            let (value, remaining) = datum::split_datum(datum, false)?;
            column.mut_raw().push(value);
            *datum = remaining;
        }
        Ok(())
    }

    // Process index values that are in old collation.
    // NOTE: We should extract the index columns from the key first, and extract the
    // handles from value if there is no handle in the key. Otherwise, extract the
    // handles from the key.
    fn process_old_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        Self::extract_columns_from_datum_format(
            &mut key_payload,
            &mut columns[..self.columns_id_without_handle.len()],
        )?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            DecodeIntHandle if key_payload.is_empty() => {
                // This is a unique index, and we should look up PK int handle in the value.
                let handle_val = self.decode_int_handle_from_value(value)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeIntHandle => {
                // This is a normal index, and we should look up PK handle in the key.
                let handle_val = self.decode_int_handle_from_key(key_payload)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeCommonHandle => {
                // Otherwise, if the handle is common handle, we extract it from the key.
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[self.columns_id_without_handle.len()..],
                )?;
            }
        }

        Ok(())
    }

    // restore_original_data restores the index values whose format is introduced in
    // TiDB 5.0. Unlike the format in TiDB 4.0, the new format is optimized for
    // storage space:
    // - If the index is a composed index, only the non-binary string column's value
    //   need to write to value, not all.
    // - If a string column's collation is _bin, then we only write the number of
    //   the truncated spaces to value.
    // - If a string column is char, not varchar, then we use the sortKey directly.
    //
    // The whole logic of this function is:
    // - For each column pass in, check if it needs the restored data to get to
    //   original data. If not, check the next column.
    // - Skip if the `sort key` is NULL, because the original data must be NULL.
    // - Depend on the collation if `_bin` or not. Process them differently to get
    //   the correct original data.
    // - Write the original data into the column, we need to make sure pop() is
    //   called.
    fn restore_original_data<'a>(
        &self,
        restored_values: &[u8],
        column_iter: impl Iterator<Item = (&'a FieldType, &'a i64, &'a mut LazyBatchColumn)>,
    ) -> Result<()> {
        let row = RowSlice::from_bytes(restored_values)?;
        for (field_type, column_id, column) in column_iter {
            if !field_type.need_restored_data() {
                continue;
            }
            let is_bin_collation = field_type
                .collation()
                .map(|col| col.is_bin_collation())
                .unwrap_or(false);

            assert!(!column.is_empty());
            let mut last_value = column.raw().last().unwrap();
            let decoded_value = last_value.read_datum()?;
            if !last_value.is_empty() {
                return Err(other_err!(
                    "Unexpected extra bytes: {}",
                    log_wrappers::Value(last_value)
                ));
            }
            if decoded_value == Datum::Null {
                continue;
            }
            column.mut_raw().pop();

            let original_data = if is_bin_collation {
                // _bin collation, we need to combine data from key and value to form the
                // original data.

                // Unwrap as checked by `decoded_value.read_datum() == Datum::Null`
                let truncate_str = decoded_value.as_string()?.unwrap();

                let space_num_data = row
                    .get(*column_id)?
                    .ok_or_else(|| other_err!("Unexpected missing column {}", column_id))?;
                let space_num = decode_v2_u64(space_num_data)?;

                // Form the original data.
                truncate_str
                    .iter()
                    .cloned()
                    .chain(std::iter::repeat(PADDING_SPACE as _).take(space_num as _))
                    .collect::<Vec<_>>()
            } else {
                let original_data = row
                    .get(*column_id)?
                    .ok_or_else(|| other_err!("Unexpected missing column {}", column_id))?;
                original_data.to_vec()
            };

            let mut buffer_to_write = column.mut_raw().begin_concat_extend();
            buffer_to_write.write_v2_as_datum(&original_data, field_type)?;
        }

        Ok(())
    }

    // get_index_version is the same as getIndexVersion() in the TiDB repo.
    fn get_index_version(value: &[u8]) -> Result<i64> {
        if value.len() == 3 || value.len() == 4 {
            // For the unique index with null value or non-unique index, the length can be 3
            // or 4 if <= 9.
            return Ok(1);
        }
        if value.len() <= MAX_OLD_ENCODED_VALUE_LEN {
            return Ok(0);
        }
        let tail_len = value[0] as usize;
        if tail_len >= value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }
        if (tail_len == 0 || tail_len == 1) && value[1] == INDEX_VALUE_VERSION_FLAG {
            return Ok(value[2] as i64);
        }

        Ok(0)
    }

    // Process new layout index values in an extensible way,
    // see https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing
    fn process_kv_general(
        &mut self,
        key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let (decode_handle, decode_pid, restore_data) =
            self.build_operations(key_payload, value)?;

        self.decode_index_columns(key_payload, columns, restore_data)?;
        self.decode_handle_columns(decode_handle, columns, restore_data)?;
        self.decode_pid_columns(columns, decode_pid)?;

        Ok(())
    }

    #[inline]
    fn build_operations<'a, 'b>(
        &'b self,
        mut key_payload: &'a [u8],
        index_value: &'a [u8],
    ) -> Result<(DecodeHandleOp<'a>, DecodePartitionIdOp<'a>, RestoreData<'a>)> {
        let tail_len = index_value[0] as usize;
        if tail_len >= index_value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }

        let (remaining, tail) = if self.index_version == 1 {
            // Skip the version segment.
            index_value[3..].split_at(index_value.len() - 3 - tail_len)
        } else {
            index_value[1..].split_at(index_value.len() - 1 - tail_len)
        };

        let (common_handle_bytes, remaining) = Self::split_common_handle(remaining)?;
        let (decode_handle_op, remaining) = {
            if !common_handle_bytes.is_empty() && self.decode_handle_strategy != DecodeCommonHandle
            {
                return Err(other_err!(
                    "Expect to decode index values with common handles in `DecodeCommonHandle` mode."
                ));
            }

            let dispatcher = match self.decode_handle_strategy {
                NoDecode => DecodeHandleOp::Nop,
                DecodeIntHandle if tail_len < 8 => {
                    // This is a non-unique index, we should extract the int handle from the key.
                    datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
                    DecodeHandleOp::IntFromKey(key_payload)
                }
                DecodeIntHandle => {
                    // This is a unique index, we should extract the int handle from the value.
                    DecodeHandleOp::IntFromValue(tail)
                }
                DecodeCommonHandle if common_handle_bytes.is_empty() => {
                    // This is a non-unique index, we should extract the common handle from the key.
                    datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
                    DecodeHandleOp::CommonHandle(key_payload)
                }
                DecodeCommonHandle => {
                    // This is a unique index, we should extract the common handle from the value.
                    DecodeHandleOp::CommonHandle(common_handle_bytes)
                }
            };

            (dispatcher, remaining)
        };

        let (partition_id_bytes, remaining) = Self::split_partition_id(remaining)?;
        let decode_pid_op = {
            if self.pid_column_cnt > 0 && partition_id_bytes.is_empty() {
                return Err(other_err!(
                    "Expect to decode partition id but payload is empty"
                ));
            } else if partition_id_bytes.is_empty() {
                DecodePartitionIdOp::Nop
            } else {
                DecodePartitionIdOp::Pid(partition_id_bytes)
            }
        };

        let (restore_data, remaining) = Self::split_restore_data(remaining)?;
        let restore_data = {
            if restore_data.is_empty() {
                RestoreData::NotExists
            } else if self.index_version == 1 {
                RestoreData::V5(restore_data)
            } else {
                RestoreData::V4(restore_data)
            }
        };

        if !remaining.is_empty() {
            return Err(other_err!(
                "Unexpected corrupted extra bytes: {}",
                log_wrappers::Value(remaining)
            ));
        }

        Ok((decode_handle_op, decode_pid_op, restore_data))
    }

    #[inline]
    fn decode_index_columns(
        &mut self,
        mut key_payload: &[u8],
        columns: &mut LazyBatchColumnVec,
        restore_data: RestoreData<'_>,
    ) -> Result<()> {
        match restore_data {
            RestoreData::NotExists => {
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[..self.columns_id_without_handle.len()],
                )?;
            }

            // If there are some restore data, we need to process them to get the original data.
            RestoreData::V4(rst) => {
                // 4.0 version format, use the restore data directly. The restore data contain
                // all the indexed values.
                self.extract_columns_from_row_format(rst, columns)?;
            }
            RestoreData::V5(rst) => {
                // Extract the data from key, then use the restore data to get the original
                // data.
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[..self.columns_id_without_handle.len()],
                )?;
                let limit = self.columns_id_without_handle.len();
                self.restore_original_data(
                    rst,
                    izip!(
                        &self.schema[..limit],
                        &self.columns_id_without_handle,
                        &mut columns[..limit],
                    ),
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn decode_handle_columns(
        &mut self,
        decode_handle: DecodeHandleOp<'_>,
        columns: &mut LazyBatchColumnVec,
        restore_data: RestoreData<'_>,
    ) -> Result<()> {
        match decode_handle {
            DecodeHandleOp::Nop => {}
            DecodeHandleOp::IntFromKey(handle) => {
                let handle = self.decode_int_handle_from_key(handle)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            DecodeHandleOp::IntFromValue(handle) => {
                let handle = self.decode_int_handle_from_value(handle)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            DecodeHandleOp::CommonHandle(mut handle) => {
                let end_index =
                    columns.columns_len() - self.pid_column_cnt - self.physical_table_id_column_cnt;
                Self::extract_columns_from_datum_format(
                    &mut handle,
                    &mut columns[self.columns_id_without_handle.len()..end_index],
                )?;
            }
        }

        let restore_data_bytes = match restore_data {
            RestoreData::V5(value) => value,
            _ => return Ok(()),
        };

        if let DecodeHandleOp::CommonHandle(_) = decode_handle {
            let skip = self.columns_id_without_handle.len();
            let end_index =
                columns.columns_len() - self.pid_column_cnt - self.physical_table_id_column_cnt;
            self.restore_original_data(
                restore_data_bytes,
                izip!(
                    &self.schema[skip..end_index],
                    &self.columns_id_for_common_handle,
                    &mut columns[skip..end_index],
                ),
            )?;
        }

        Ok(())
    }

    #[inline]
    fn process_physical_table_id_column(
        &mut self,
        key: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let table_id = table::decode_table_id(key)?;
        let col_index = columns.columns_len() - 1;
        columns[col_index].mut_decoded().push_int(Some(table_id));
        Ok(())
    }

    #[inline]
    fn decode_pid_columns(
        &mut self,
        columns: &mut LazyBatchColumnVec,
        decode_pid: DecodePartitionIdOp<'_>,
    ) -> Result<()> {
        match decode_pid {
            DecodePartitionIdOp::Nop => {}
            DecodePartitionIdOp::Pid(pid) => {
                // If need partition id, append partition id to the last column
                // before physical table id column if exists.
                let pid = NumberCodec::decode_i64(pid);
                let idx = columns.columns_len() - self.physical_table_id_column_cnt - 1;
                columns[idx].mut_decoded().push_int(Some(pid))
            }
        }
        Ok(())
    }

    #[inline]
    fn split_common_handle(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .first()
            .map_or(false, |c| *c == table::INDEX_VALUE_COMMON_HANDLE_FLAG)
        {
            let handle_len = (&value[1..]).read_u16().map_err(|_| {
                other_err!(
                    "Fail to read common handle's length from value: {}",
                    log_wrappers::Value::value(value)
                )
            })? as usize;
            let handle_end_offset = 3 + handle_len;
            if handle_end_offset > value.len() {
                return Err(other_err!("`handle_len` is corrupted: {}", handle_len));
            }
            Ok(value[3..].split_at(handle_len))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_partition_id(value: &[u8]) -> Result<(&[u8], &[u8])> {
        if value
            .first()
            .map_or(false, |c| *c == table::INDEX_VALUE_PARTITION_ID_FLAG)
        {
            if value.len() < 9 {
                return Err(other_err!(
                    "Remaining len {} is too short to decode partition ID",
                    value.len()
                ));
            }
            Ok(value[1..].split_at(8))
        } else {
            Ok(value.split_at(0))
        }
    }

    #[inline]
    fn split_restore_data(value: &[u8]) -> Result<(&[u8], &[u8])> {
        Ok(
            if value
                .first()
                .map_or(false, |c| *c == table::INDEX_VALUE_RESTORED_DATA_FLAG)
            {
                (value, &value[value.len()..])
            } else {
                (&value[..0], value)
            },
        )
    }    

}

fn convert_raw_value_to_chunk(req: &GetRequest, 
    resp: &mut GetResponse, val: Vec<u8>) -> Result<()>{
    let meta = req.take_meta();
    let mut input = CodedInputStream::from_bytes(&meta);
    input.set_recursion_limit(1000);
    let mut point_get = PointGet::default();
    box_try!(point_get.merge_from(&mut input));

    if point_get.get_encode_type() != EncodeType::TypeChunk {
        resp.set_value(val);
        return Ok(());
    }

    let runner = PointGetExecutorsRunner::new(point_get)?;

    let mut chunk = Chunk::default();

    runner.internal_handle_request(req.get_key(), &val, &mut chunk)?;

    resp.set_value(chunk.take_rows_data());

    return Ok(());
}
