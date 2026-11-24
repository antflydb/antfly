// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Static column/row worker schedule lowering. Native signatures, planning,
//! field ordering and slice expressions are explicit policies; chunk dispatch,
//! context construction and completion have one owner. Expansion adds no work
//! to worker execution and preserves each qualified compiler token sequence.
const std = @import("std");
const cpu = @import("quant_kernel_cpu_source.zig");
// Worker context fields have one binding contract. Exceptional acquisition
// layouts use explicit tags; field order remains a property of each schedule.
const Field = enum {
    input,
    prepared_blocks,
    output,
    rows,
    in_dim,
    out_dim,
    out_start,
    out_end,
    err,
    prepared_blocks_a,
    prepared_blocks_b,
    prepared_blocks_c,
    bias_a,
    bias_b,
    bias_c,
    output_a,
    output_b,
    output_c,
    weight_raw,
    block_size,
    values_per_block,
    row_stride,
    dot_fn,
    weight_raw_a,
    weight_raw_b,
    dot_fn_a,
    dot_fn_b,
    prepared_panel_blocks,
    q_input_legacy,
    input_scales,
    prepared_panel8_blocks,
    bias,
    q_input_legacy_values,
    prepared_panel_blocks_a,
    prepared_panel_blocks_b,
    prepared_panel8_blocks_a,
    prepared_panel8_blocks_b,
    weight_raw_c,
    prepared_panel_blocks_c,
    prepared_panel8_blocks_c,
    q_input,
    panel_blocks,
    panel8_blocks,
    panel16_blocks,
    panel_blocks_a,
    panel_blocks_b,
    panel8_blocks_a,
    panel8_blocks_b,
    panel16_blocks_a,
    panel16_blocks_b,
    panel_blocks_c,
    panel8_blocks_c,
    panel16_blocks_c,
    known,
    q6,
    packed_qkv_panel16_blocks,
    packed_qkv_panel16_mr2,
    packed_qkv_panel16_mr8,
    q_values_legacy,
    input_sums,
    use_mr4,
    q_values,
    input_scales_128,
    has_min,
    q3_panel_blocks,
    q3_panel_blocks_a,
    q3_panel_blocks_b,
    q3_panel_blocks_c,

    fn binding(self: Field) struct { name: []const u8, source: ?[]const u8 = null, stride: ?[]const u8 = null } {
        return switch (self) {
            .input => .{ .name = "input", .stride = "in_dim" },
            .q_input_legacy => .{ .name = "q_input", .stride = "row_blocks * values_per_block" },
            .input_scales => .{ .name = "input_scales", .stride = "row_blocks" },
            .q_input_legacy_values => .{ .name = "q_input", .source = "q_values", .stride = "row_blocks * values_per_block" },
            .q_input => .{ .name = "q_input", .stride = "row_blocks * q8_block_size" },
            .packed_qkv_panel16_mr8 => .{ .name = "packed_qkv_panel16_mr8", .source = "use_packed_qkv_panel16_mr8" },
            .q_values_legacy => .{ .name = "q_values", .stride = "row_blocks * values_per_block" },
            .input_sums => .{ .name = "input_sums", .stride = "row_blocks" },
            .q_values => .{ .name = "q_values", .stride = "in_dim" },
            .input_scales_128 => .{ .name = "input_scales", .stride = "input_blocks" },
            else => .{ .name = @tagName(self) },
        };
    }
};
const Schedule = struct {
    name: []const u8,
    parameters: []const Parameter,
    initial: []const u8 = "",
    column_plan: ColumnPlan,
    column_debug: Debug,
    column_chunks: []const u8 = "chunkColsForWorkers(out_dim, col_plan, &chunks)",
    row_plan_multiline: bool = false,
    row_debug: ?Debug = null,
    row_setup: []const u8 = "",
    row_chunks: []const u8 = "chunkRowsForWorkers(rows, plan, &chunks)",
    context: []const u8,
    job: []const u8,
    fields: []const Field,
};
const Parameter = cpu.Parameter;

pub const owners = .{
    Owner(0),
    Owner(1),
    Owner(2),
    Owner(3),
    Owner(4),
    Owner(5),
    Owner(6),
    Owner(7),
    Owner(8),
    Owner(9),
    Owner(10),
    Owner(11),
    Owner(12),
    Owner(13),
    Owner(14),
    Owner(15),
    Owner(16),
    Owner(17),
    Owner(18),
    Owner(19),
    Owner(20),
    Owner(21),
    Owner(22),
    Owner(23),
    Owner(24),
    Owner(25),
    Owner(26),
};

const schedules = [_]Schedule{
    .{
        .name = "linearNoBiasKPreparedParallel",
        .parameters = &.{ .kind_comptime, .io, .input, .prepared_blocks_required, .output, .rows, .in_dim, .out_dim },
        .initial =
        \\const tensor_type: gguf_tensor_types.TensorType = .{ .known = kind };
        \\const values_per_block = gguf_tensor_types.valuesPerBlock(tensor_type).?;
        \\if (in_dim % values_per_block != 0) return error.InvalidQuantizedLinearShape;
        \\const row_blocks = in_dim / values_per_block;
        ,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "_parallel_cols", .select_kind = true },
        .row_debug = .{ .label = "_parallel", .select_kind = true },
        .row_plan_multiline = true,
        .context = "KPreparedWorkerCtx",
        .job = "preparedWorkerJob(kind)",
        .fields = &.{
            .input,
            .prepared_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .err,
        },
    },
    .{
        .name = "linearQ4KPreparedTripleParallel",
        .parameters = &.{ .io, .input, .prepared_blocks_a_required, .prepared_blocks_b_required, .prepared_blocks_c_required, .bias_a, .bias_b, .bias_c, .output_a, .output_b, .output_c, .rows, .in_dim, .out_dim },
        .initial =
        \\const tensor_type: gguf_tensor_types.TensorType = .{ .known = .Q4_K };
        \\const values_per_block = gguf_tensor_types.valuesPerBlock(tensor_type).?;
        \\if (in_dim % values_per_block != 0) return error.InvalidQuantizedLinearShape;
        \\const row_blocks = in_dim / values_per_block;
        ,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q4k_triple_parallel_cols" },
        .row_debug = .{ .label = "q4k_triple_parallel" },
        .row_plan_multiline = true,
        .context = "Q4KPreparedTripleWorkerCtx",
        .job = "q4KPreparedTripleWorkerJob",
        .fields = &.{
            .input,
            .prepared_blocks_a,
            .prepared_blocks_b,
            .prepared_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .err,
        },
    },
    .{
        .name = "linearNoBiasQuantizedGenericParallel",
        .parameters = &.{ .io, .input, .weight_raw, .output, .rows, .in_dim, .out_dim, .row_blocks, .block_size, .values_per_block, .row_stride, .dot_fn },
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "generic_quant_parallel_cols" },
        .row_debug = .{ .label = "generic_quant_parallel" },
        .row_plan_multiline = true,
        .context = "QuantizedLinearWorkerCtx",
        .job = "quantizedLinearWorkerJob",
        .fields = &.{
            .input,
            .weight_raw,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .block_size,
            .values_per_block,
            .row_stride,
            .dot_fn,
            .err,
        },
    },
    .{
        .name = "linearNoBiasQuantizedGenericPairParallel",
        .parameters = &.{ .io, .input, .weight_raw_a, .dot_fn_a, .weight_raw_b, .dot_fn_b, .output_a, .output_b, .rows, .in_dim, .out_dim, .row_blocks, .block_size, .values_per_block, .row_stride },
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "generic_quant_pair_parallel_cols" },
        .row_debug = .{ .label = "generic_quant_pair_parallel" },
        .row_plan_multiline = true,
        .context = "QuantizedLinearPairWorkerCtx",
        .job = "quantizedLinearPairWorkerJob",
        .fields = &.{
            .input,
            .weight_raw_a,
            .weight_raw_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .block_size,
            .values_per_block,
            .row_stride,
            .dot_fn_a,
            .dot_fn_b,
            .err,
        },
    },
    .{
        .name = "linearNoBiasQ8_0Parallel",
        .parameters = &.{ .io, .input, .weight_raw, .prepared_blocks_optional, .prepared_panel_blocks, .output, .rows, .in_dim, .out_dim },
        .initial = setup_legacy,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q8_0_parallel_cols" },
        .row_debug = .{ .label = "q8_0_parallel_rows" },
        .row_plan_multiline = true,
        .context = "Q8_0WorkerCtx",
        .job = "q8_0WorkerJob",
        .fields = &.{
            .input,
            .weight_raw,
            .prepared_blocks,
            .prepared_panel_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearNoBiasQ8_0ActivationParallel",
        .parameters = &.{ .io, .q_input, .input_scales, .weight_raw, .prepared_blocks_optional, .prepared_panel_blocks, .prepared_panel8_blocks, .bias, .output, .rows, .in_dim, .out_dim },
        .initial = setup_legacy,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q8_0_activation_parallel_cols" },
        .row_debug = .{ .label = "q8_0_activation_parallel_rows" },
        .row_plan_multiline = true,
        .context = "Q8_0ActivationWorkerCtx",
        .job = "q8_0ActivationWorkerJob",
        .fields = &.{
            .q_input_legacy,
            .input_scales,
            .weight_raw,
            .prepared_blocks,
            .prepared_panel_blocks,
            .prepared_panel8_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .bias,
            .err,
        },
    },
    .{
        .name = "linearQ8_0QuantizedInputPairParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .weight_raw_a, .prepared_blocks_a_optional, .prepared_panel_blocks_a, .prepared_panel8_blocks_a, .bias_a, .output_a, .weight_raw_b, .prepared_blocks_b_optional, .prepared_panel_blocks_b, .prepared_panel8_blocks_b, .bias_b, .output_b, .rows, .in_dim, .out_dim },
        .initial = setup_legacy,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q8_0_activation_pair_parallel_cols", .multiline = false },
        .row_debug = .{ .label = "q8_k_q8_k_activation_pair_parallel_rows" },
        .context = "Q8_0ActivationPairWorkerCtx",
        .job = "q8_0ActivationPairWorkerJob",
        .fields = &.{
            .q_input_legacy_values,
            .input_scales,
            .weight_raw_a,
            .weight_raw_b,
            .prepared_blocks_a,
            .prepared_blocks_b,
            .prepared_panel_blocks_a,
            .prepared_panel_blocks_b,
            .prepared_panel8_blocks_a,
            .prepared_panel8_blocks_b,
            .bias_a,
            .bias_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ8_0QuantizedInputTripleParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .weight_raw_a, .prepared_blocks_a_optional, .prepared_panel_blocks_a, .prepared_panel8_blocks_a, .bias_a, .output_a, .weight_raw_b, .prepared_blocks_b_optional, .prepared_panel_blocks_b, .prepared_panel8_blocks_b, .bias_b, .output_b, .weight_raw_c, .prepared_blocks_c_optional, .prepared_panel_blocks_c, .prepared_panel8_blocks_c, .bias_c, .output_c, .rows, .in_dim, .out_dim },
        .initial = setup_legacy,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q8_0_activation_triple_parallel_cols", .multiline = false },
        .row_debug = .{ .label = "q8_k_q8_k_activation_triple_parallel_rows" },
        .context = "Q8_0ActivationTripleWorkerCtx",
        .job = "q8_0ActivationTripleWorkerJob",
        .fields = &.{
            .q_input_legacy_values,
            .input_scales,
            .weight_raw_a,
            .weight_raw_b,
            .weight_raw_c,
            .prepared_blocks_a,
            .prepared_blocks_b,
            .prepared_blocks_c,
            .prepared_panel_blocks_a,
            .prepared_panel_blocks_b,
            .prepared_panel_blocks_c,
            .prepared_panel8_blocks_a,
            .prepared_panel8_blocks_b,
            .prepared_panel8_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearNoBiasQ8KQ8KActivationParallel",
        .parameters = &.{ .io, .q_input, .weight_raw, .panel_blocks, .panel8_blocks, .panel16_blocks_optional, .output, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q8_k_q8_k_activation_parallel_cols" },
        .row_plan_multiline = true,
        .context = "Q8KQ8KActivationWorkerCtx",
        .job = "q8KQ8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .weight_raw,
            .panel_blocks,
            .panel8_blocks,
            .panel16_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .err,
        },
    },
    .{
        .name = "linearQ8KQ8KActivationPairParallel",
        .parameters = &.{ .io, .q_input, .weight_raw_a, .panel_blocks_a, .panel8_blocks_a, .panel16_blocks_a, .output_a, .weight_raw_b, .panel_blocks_b, .panel8_blocks_b, .panel16_blocks_b, .output_b, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q8_k_q8_k_activation_pair_parallel_cols", .multiline = false },
        .context = "Q8KQ8KActivationPairWorkerCtx",
        .job = "q8KQ8KActivationPairWorkerJob",
        .fields = &.{
            .q_input,
            .weight_raw_a,
            .weight_raw_b,
            .panel_blocks_a,
            .panel_blocks_b,
            .panel8_blocks_a,
            .panel8_blocks_b,
            .panel16_blocks_a,
            .panel16_blocks_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ8KQ8KActivationTripleParallel",
        .parameters = &.{ .io, .q_input, .weight_raw_a, .panel_blocks_a, .panel8_blocks_a, .panel16_blocks_a, .output_a, .weight_raw_b, .panel_blocks_b, .panel8_blocks_b, .panel16_blocks_b, .output_b, .weight_raw_c, .panel_blocks_c, .panel8_blocks_c, .panel16_blocks_c, .bias_a, .bias_b, .bias_c, .output_c, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q8_k_q8_k_activation_triple_parallel_cols", .multiline = false },
        .context = "Q8KQ8KActivationTripleWorkerCtx",
        .job = "q8KQ8KActivationTripleWorkerJob",
        .fields = &.{
            .q_input,
            .weight_raw_a,
            .weight_raw_b,
            .weight_raw_c,
            .panel_blocks_a,
            .panel_blocks_b,
            .panel_blocks_c,
            .panel8_blocks_a,
            .panel8_blocks_b,
            .panel8_blocks_c,
            .panel16_blocks_a,
            .panel16_blocks_b,
            .panel16_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearNoBiasQ6KPreparedQ8KActivationParallel",
        .parameters = &.{ .io, .q_input, .prepared_blocks_required, .panel_blocks, .panel8_blocks, .panel16_blocks_optional, .output, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary_multiline,
        .column_debug = .{ .label = "q6_k_prepared_q8_k_activation_parallel_cols" },
        .row_plan_multiline = true,
        .context = "Q6KPreparedQ8KActivationWorkerCtx",
        .job = "q6KPreparedQ8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks,
            .panel_blocks,
            .panel8_blocks,
            .panel16_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .err,
        },
    },
    .{
        .name = "linearNoBiasQ4Q5KPreparedQ8KActivationParallel",
        .parameters = &.{ .io, .q_input, .prepared_blocks_required, .panel_blocks, .panel8_blocks, .panel16_blocks_optional, .bias, .output, .rows, .in_dim, .out_dim, .known_required },
        .initial =
        \\if (rows == 1) return false;
        \\const values_per_block: usize = 256;
        \\const q8_block_size: usize = 292;
        \\const row_blocks = in_dim / values_per_block;
        \\const dispatch_available = quantParallelDispatchAvailable(io);
        ,
        .column_plan = .q4q5_single,
        .column_debug = .{ .label = "q4_q5_k_prepared_q8_k_activation_parallel_cols" },
        .row_setup =
        \\const use_panel16_direct = useQ4Q5Panel16DirectForShape(rows, out_dim, row_blocks, panel16_blocks != null);
        \\const use_panel16_mr8 = useQ4Q5Panel16MR8DirectForShape(rows, out_dim, row_blocks, known, panel16_blocks != null) and row_blocks <= q8_k_prepared_sum_stack_max_row_blocks;
        ,
        .row_plan_multiline = true,
        .row_chunks =
        \\if (use_panel16_direct)
        \\if (use_panel16_mr8)
        \\chunkRowsForWorkersAligned(rows, plan, 8, &chunks)
        \\else
        \\chunkRowsForWorkersAligned(rows, plan, 4, &chunks)
        \\else
        \\chunkRowsForWorkers(rows, plan, &chunks)
        ,
        .context = "Q4Q5KPreparedQ8KActivationWorkerCtx",
        .job = "q4Q5KPreparedQ8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks,
            .panel_blocks,
            .panel8_blocks,
            .panel16_blocks,
            .bias,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .known,
            .out_start,
            .out_end,
            .err,
        },
    },
    .{
        .name = "linearKPreparedQ8KActivationPairParallel",
        .parameters = &.{ .q6, .io, .q_input, .prepared_blocks_a_required, .panel_blocks_a, .panel8_blocks_a, .panel16_blocks_a, .output_a, .prepared_blocks_b_required, .panel_blocks_b, .panel8_blocks_b, .panel16_blocks_b, .output_b, .rows, .in_dim, .out_dim, .known_optional },
        .initial =
        \\if (rows == 1) return false;
        \\const values_per_block: usize = 256;
        \\const q8_block_size: usize = 292;
        \\const row_blocks = in_dim / values_per_block;
        \\const dispatch_available = quantParallelDispatchAvailable(io);
        \\const panel16_available = panel16_blocks_a != null and panel16_blocks_b != null;
        ,
        .column_plan = .q4q5_grouped,
        .column_debug = .{ .label = "k_prepared_q8_k_activation_pair_parallel_cols", .flag = "q6", .multiline = false },
        .row_setup =
        \\const use_panel16_direct = !q6 and useQ4Q5Panel16DirectForShape(rows, out_dim, row_blocks, panel16_blocks_a != null and panel16_blocks_b != null);
        \\const use_panel16_mr8 = use_panel16_direct and if (known) |value| useQ4Q5Panel16MR8DirectForShape(rows, out_dim, row_blocks, value, panel16_blocks_a != null and panel16_blocks_b != null) and row_blocks <= q8_k_prepared_sum_stack_max_row_blocks else false;
        ,
        .row_chunks =
        \\if (use_panel16_direct)
        \\if (use_panel16_mr8) chunkRowsForWorkersAligned(rows, plan, 8, &chunks) else chunkRowsForWorkersAligned(rows, plan, 4, &chunks)
        \\else
        \\chunkRowsForWorkers(rows, plan, &chunks)
        ,
        .context = "KPreparedQ8KActivationPairWorkerCtx",
        .job = "kPreparedQ8KActivationPairWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .panel_blocks_a,
            .panel8_blocks_a,
            .panel16_blocks_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .panel8_blocks_b,
            .panel16_blocks_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .q6,
            .known,
        },
    },
    .{
        .name = "linearKPreparedQ8KActivationTripleParallel",
        .parameters = &.{ .q6, .io, .q_input, .prepared_blocks_a_required, .panel_blocks_a, .panel8_blocks_a, .panel16_blocks_a, .bias_a, .output_a, .prepared_blocks_b_required, .panel_blocks_b, .panel8_blocks_b, .panel16_blocks_b, .bias_b, .output_b, .prepared_blocks_c_required, .panel_blocks_c, .panel8_blocks_c, .panel16_blocks_c, .packed_qkv_panel16_blocks, .packed_qkv_panel16_mr2, .bias_c, .output_c, .rows, .in_dim, .out_dim, .known_optional },
        .initial =
        \\if (rows == 1) return false;
        \\const values_per_block: usize = 256;
        \\const q8_block_size: usize = 292;
        \\const row_blocks = in_dim / values_per_block;
        \\const dispatch_available = quantParallelDispatchAvailable(io);
        \\const panel16_available = panel16_blocks_a != null and panel16_blocks_b != null and panel16_blocks_c != null;
        \\const use_panel16_direct = !q6 and useQ4Q5Panel16DirectForShape(rows, out_dim, row_blocks, panel16_available);
        \\const use_panel16_mr8 = use_panel16_direct and if (known) |value| useQ4Q5Panel16MR8DirectForShape(rows, out_dim, row_blocks, value, panel16_available) and row_blocks <= q8_k_prepared_sum_stack_max_row_blocks else false;
        \\const use_packed_qkv_panel16_mr8 = use_panel16_mr8 and packed_qkv_panel16_blocks != null;
        ,
        .column_plan = .q4q5_grouped,
        .column_debug = .{ .label = "k_prepared_q8_k_activation_triple_parallel_cols", .flag = "q6", .multiline = false },
        .column_chunks =
        \\if (use_panel16_direct)
        \\chunkColsForWorkersAligned(out_dim, col_plan, prepared_k_panel16_nr, &chunks)
        \\else
        \\chunkColsForWorkers(out_dim, col_plan, &chunks)
        ,
        .row_chunks =
        \\if (use_panel16_direct)
        \\if (use_panel16_mr8) chunkRowsForWorkersAligned(rows, plan, 8, &chunks) else chunkRowsForWorkersAligned(rows, plan, 4, &chunks)
        \\else
        \\chunkRowsForWorkers(rows, plan, &chunks)
        ,
        .context = "KPreparedQ8KActivationTripleWorkerCtx",
        .job = "kPreparedQ8KActivationTripleWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .panel_blocks_a,
            .panel8_blocks_a,
            .panel16_blocks_a,
            .bias_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .panel8_blocks_b,
            .panel16_blocks_b,
            .bias_b,
            .prepared_blocks_c,
            .panel_blocks_c,
            .panel8_blocks_c,
            .panel16_blocks_c,
            .packed_qkv_panel16_blocks,
            .packed_qkv_panel16_mr2,
            .packed_qkv_panel16_mr8,
            .output_a,
            .output_b,
            .bias_c,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .q6,
            .known,
        },
    },
    .{
        .name = "linearLegacyPreparedActivationParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .input_sums, .prepared_blocks_required, .prepared_panel_blocks, .bias, .use_mr4, .output, .rows, .in_dim, .out_dim },
        .initial = setup_legacy,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "legacy_prepared_activation_parallel_cols", .multiline = false },
        .context = "LegacyPreparedActivationWorkerCtx",
        .job = "legacyPreparedActivationWorkerJob",
        .fields = &.{
            .q_values_legacy,
            .input_scales,
            .input_sums,
            .prepared_blocks,
            .prepared_panel_blocks,
            .bias,
            .use_mr4,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ1_0PreparedActivationParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .prepared_blocks_required, .panel_blocks, .bias, .output, .rows, .in_dim, .out_dim },
        .initial = setup_q1,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q1_0_prepared_activation_parallel_cols", .multiline = false },
        .context = "Q1_0PreparedActivationWorkerCtx",
        .job = "q1_0PreparedActivationWorkerJob",
        .fields = &.{
            .q_values,
            .input_scales_128,
            .prepared_blocks,
            .panel_blocks,
            .bias,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ1_0PreparedActivationPairParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .prepared_blocks_a_required, .panel_blocks_a, .prepared_blocks_b_required, .panel_blocks_b, .output_a, .output_b, .rows, .in_dim, .out_dim },
        .initial = setup_q1,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q1_0_prepared_activation_pair_parallel_cols", .multiline = false },
        .context = "Q1_0PreparedActivationPairWorkerCtx",
        .job = "q1_0PreparedActivationPairWorkerJob",
        .fields = &.{
            .q_values,
            .input_scales_128,
            .prepared_blocks_a,
            .panel_blocks_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ1_0PreparedActivationTripleParallel",
        .parameters = &.{ .io, .q_values, .input_scales, .prepared_blocks_a_required, .panel_blocks_a, .prepared_blocks_b_required, .panel_blocks_b, .prepared_blocks_c_required, .panel_blocks_c, .bias_a, .bias_b, .bias_c, .output_a, .output_b, .output_c, .rows, .in_dim, .out_dim },
        .initial = setup_q1,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q1_0_prepared_activation_triple_parallel_cols", .multiline = false },
        .context = "Q1_0PreparedActivationTripleWorkerCtx",
        .job = "q1_0PreparedActivationTripleWorkerJob",
        .fields = &.{
            .q_values,
            .input_scales_128,
            .prepared_blocks_a,
            .panel_blocks_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .prepared_blocks_c,
            .panel_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearK16RawQ8KActivationParallel",
        .parameters = &.{ .has_min_comptime, .io, .q_input, .weight_raw, .output, .rows, .in_dim, .out_dim },
        .initial = setup_k16,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "k16_raw_q8_k_activation_parallel_cols", .flag = "has_min", .multiline = false },
        .context = "K16RawQ8KActivationWorkerCtx",
        .job = "k16RawQ8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .weight_raw,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .has_min,
        },
    },
    .{
        .name = "linearQ3KNoDmnPanelActivationParallel",
        .parameters = &.{ .io, .q_input, .prepared_blocks_required, .q3_panel_blocks, .output, .rows, .in_dim, .out_dim },
        .initial = setup_k16,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q3_k_nodmn_panel_parallel_cols", .multiline = false },
        .context = "Q3KNoDmnPanelActivationWorkerCtx",
        .job = "q3KNoDmnPanelActivationWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks,
            .q3_panel_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ3KNoDmnPanelActivationPairParallel",
        .parameters = &.{ .io, .q_input, .prepared_blocks_a_required, .prepared_blocks_b_required, .q3_panel_blocks_a, .q3_panel_blocks_b, .output_a, .output_b, .rows, .in_dim, .out_dim },
        .initial = setup_k16,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q3_k_nodmn_panel_pair_parallel_cols", .multiline = false },
        .context = "Q3KNoDmnPanelActivationPairWorkerCtx",
        .job = "q3KNoDmnPanelActivationPairWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .prepared_blocks_b,
            .q3_panel_blocks_a,
            .q3_panel_blocks_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearQ3KNoDmnPanelActivationTripleParallel",
        .parameters = &.{ .io, .q_input, .prepared_blocks_a_required, .prepared_blocks_b_required, .prepared_blocks_c_required, .q3_panel_blocks_a, .q3_panel_blocks_b, .q3_panel_blocks_c, .bias_a, .bias_b, .bias_c, .output_a, .output_b, .output_c, .rows, .in_dim, .out_dim },
        .initial = setup_k16,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "q3_k_nodmn_panel_triple_parallel_cols", .multiline = false },
        .context = "Q3KNoDmnPanelActivationTripleWorkerCtx",
        .job = "q3KNoDmnPanelActivationTripleWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .prepared_blocks_b,
            .prepared_blocks_c,
            .q3_panel_blocks_a,
            .q3_panel_blocks_b,
            .q3_panel_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
        },
    },
    .{
        .name = "linearK16PreparedQ8KActivationPanel16Parallel",
        .parameters = &.{ .has_min_comptime, .io, .q_input, .prepared_blocks_required, .panel16_blocks_required, .output, .rows, .in_dim, .out_dim },
        .initial = setup_k16,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "k16_panel16_q8_k_activation_parallel_cols", .flag = "has_min", .multiline = false },
        .context = "K16Panel16Q8KActivationWorkerCtx",
        .job = "k16Panel16Q8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks,
            .panel16_blocks,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .has_min,
        },
    },
    .{
        .name = "linearK16PreparedQ8KActivationParallel",
        .parameters = &.{ .has_min_comptime, .io, .q_input, .prepared_blocks_required, .panel_blocks, .panel8_blocks, .bias, .output, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "k16_prepared_q8_k_activation_parallel_cols", .flag = "has_min", .multiline = false },
        .context = "K16PreparedQ8KActivationWorkerCtx",
        .job = "k16PreparedQ8KActivationWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks,
            .panel_blocks,
            .panel8_blocks,
            .bias,
            .output,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .has_min,
        },
    },
    .{
        .name = "linearK16PreparedQ8KActivationPairParallel",
        .parameters = &.{ .has_min_comptime, .io, .q_input, .prepared_blocks_a_required, .panel_blocks_a, .panel8_blocks_a, .prepared_blocks_b_required, .panel_blocks_b, .panel8_blocks_b, .output_a, .output_b, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "k16_prepared_q8_k_activation_pair_parallel_cols", .flag = "has_min", .multiline = false },
        .context = "K16PreparedQ8KActivationPairWorkerCtx",
        .job = "k16PreparedQ8KActivationPairWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .panel_blocks_a,
            .panel8_blocks_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .panel8_blocks_b,
            .output_a,
            .output_b,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .has_min,
        },
    },
    .{
        .name = "linearK16PreparedQ8KActivationTripleParallel",
        .parameters = &.{ .has_min_comptime, .io, .q_input, .prepared_blocks_a_required, .panel_blocks_a, .panel8_blocks_a, .prepared_blocks_b_required, .panel_blocks_b, .panel8_blocks_b, .prepared_blocks_c_required, .panel_blocks_c, .panel8_blocks_c, .bias_a, .bias_b, .bias_c, .output_a, .output_b, .output_c, .rows, .in_dim, .out_dim },
        .initial = setup_q8k_multiline,
        .column_plan = .ordinary,
        .column_debug = .{ .label = "k16_prepared_q8_k_activation_triple_parallel_cols", .flag = "has_min", .multiline = false },
        .context = "K16PreparedQ8KActivationTripleWorkerCtx",
        .job = "k16PreparedQ8KActivationTripleWorkerJob",
        .fields = &.{
            .q_input,
            .prepared_blocks_a,
            .panel_blocks_a,
            .panel8_blocks_a,
            .prepared_blocks_b,
            .panel_blocks_b,
            .panel8_blocks_b,
            .prepared_blocks_c,
            .panel_blocks_c,
            .panel8_blocks_c,
            .bias_a,
            .bias_b,
            .bias_c,
            .output_a,
            .output_b,
            .output_c,
            .rows,
            .in_dim,
            .out_dim,
            .out_start,
            .out_end,
            .has_min,
        },
    },
};

fn Owner(comptime index: usize) type {
    return struct {
        pub const name = schedules[index].name;
        pub const begin = "// quant-kernel-codegen:begin CPU worker schedule " ++ name;
        pub const end = "// quant-kernel-codegen:end CPU worker schedule " ++ name;
        pub fn render(allocator: std.mem.Allocator) ![]u8 {
            return renderSchedule(allocator, schedules[index]);
        }
    };
}

fn append(allocator: std.mem.Allocator, out: *std.ArrayList(u8), comptime fmt: []const u8, args: anytype) !void {
    const bytes = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(bytes);
    try out.appendSlice(allocator, bytes);
}

fn renderSchedule(allocator: std.mem.Allocator, schedule: Schedule) ![]u8 {
    const column_debug = try schedule.column_debug.native(allocator, "col_plan");
    defer allocator.free(column_debug);
    const row_debug = if (schedule.row_debug) |debug| try debug.native(allocator, "plan") else try allocator.dupe(u8, "");
    defer allocator.free(row_debug);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    try append(allocator, &out, "fn {s}(\n", .{schedule.name});
    for (schedule.parameters) |parameter| try append(allocator, &out, "{s},\n", .{parameter.declaration()});
    try append(allocator, &out, ") !bool {{\n{s}\nconst col_plan = {s};\nif (col_plan.worker_count > 1) {{\n{s}\nvar chunks: [max_q4_k_parallel_workers]QuantColChunk = undefined;\nconst num_chunks = {s};\n", .{ schedule.initial, schedule.column_plan.native(), column_debug, schedule.column_chunks });
    try renderDispatch(allocator, &out, schedule, false);
    try out.appendSlice(allocator, "}\nconst plan = chooseQuantLinearParallelPlan(");
    const arguments = "quantParallelEnabled(), rows, out_dim, row_blocks, quantParallelMaxWorkers()";
    try append(allocator, &out, "{s}{s}{s});\nif (plan.worker_count <= 1) return false;\n{s}\nvar chunks: [max_q4_k_parallel_workers]QuantRowChunk = undefined;\n{s}\nconst num_chunks = {s};\n", .{ if (schedule.row_plan_multiline) "\n" else "", arguments, if (schedule.row_plan_multiline) ",\n" else "", row_debug, schedule.row_setup, schedule.row_chunks });
    try renderDispatch(allocator, &out, schedule, true);
    try out.appendSlice(allocator, "}\n");
    return cpu.formatZig(allocator, out.items);
}

fn renderDispatch(allocator: std.mem.Allocator, out: *std.ArrayList(u8), schedule: Schedule, row: bool) !void {
    try append(allocator, out, "var contexts: [max_q4_k_parallel_workers]{s} = undefined;\nvar jobs: [max_q4_k_parallel_workers]linalg.pool.Job = undefined;\nfor (chunks[0..num_chunks], 0..) |chunk, i| {{\ncontexts[i] = .{{\n", .{schedule.context});
    for (schedule.fields) |field_tag| {
        const field = field_tag.binding();
        const name = field.name;
        const source = field.source orelse name;
        const stride: ?[]const u8 = field.stride orelse if (std.mem.startsWith(u8, name, "output")) "out_dim" else null;
        const value = if (std.mem.eql(u8, name, "out_start")) (if (row) "0" else "chunk.start") else if (std.mem.eql(u8, name, "out_end")) (if (row) "out_dim" else "chunk.end") else if (row and std.mem.eql(u8, name, "rows")) "chunk.end - chunk.start" else if (std.mem.eql(u8, name, "err")) "null" else source;
        if (row and stride != null) {
            try append(allocator, out, ".{s} = {s}[chunk.start * {s} .. chunk.end * {s}],\n", .{ name, source, stride.?, stride.? });
        } else try append(allocator, out, ".{s} = {s},\n", .{ name, value });
    }
    try append(allocator, out, "}};\njobs[i] = .{{ .fn_ptr = {s}, .ctx = @ptrCast(&contexts[i]) }};\n}}\ntry dispatchAndCheckQuantWorkers(io, jobs[0..num_chunks], contexts[0..num_chunks]);\nreturn true;\n", .{schedule.job});
}

test "worker schedule lowering parses all policies without leaking" {
    inline for (owners) |ScheduleOwner| {
        const source = try ScheduleOwner.render(std.testing.allocator);
        defer std.testing.allocator.free(source);
        try std.testing.expect(std.mem.indexOf(u8, source, ScheduleOwner.name) != null);
    }
}

const setup_legacy =
    \\const values_per_block: usize = 32;
    \\const row_blocks = in_dim / values_per_block;
;

const setup_q8k_multiline =
    \\if (rows == 1) return false;
    \\const values_per_block: usize = 256;
    \\const q8_block_size: usize = 292;
    \\const row_blocks = in_dim / values_per_block;
;

const setup_q1 =
    \\const row_blocks = in_dim / 128;
    \\const input_blocks = in_dim / 32;
;

const setup_k16 =
    \\const values_per_block: usize = 256;
    \\const q8_block_size: usize = 292;
    \\const row_blocks = in_dim / values_per_block;
;

const ColumnPlan = enum {
    ordinary_multiline,
    ordinary,
    q4q5_single,
    q4q5_grouped,

    fn native(self: ColumnPlan) []const u8 {
        return switch (self) {
            .ordinary_multiline =>
            \\chooseQuantLinearColumnParallelPlan(
            \\quantParallelEnabled(),
            \\quantParallelDispatchAvailable(io),
            \\rows,
            \\out_dim,
            \\row_blocks,
            \\quantParallelMaxWorkers(),
            \\)
            ,
            .ordinary =>
            \\chooseQuantLinearColumnParallelPlan(quantParallelEnabled(), quantParallelDispatchAvailable(io), rows, out_dim, row_blocks, quantParallelMaxWorkers())
            ,
            .q4q5_single =>
            \\chooseQuantLinearColumnParallelPlanWithMaxRows(
            \\quantParallelEnabled(),
            \\rows,
            \\out_dim,
            \\row_blocks,
            \\quantParallelMaxWorkers(),
            \\q4q5PreparedQ8KColumnParallelMaxRowsForShape(dispatch_available, known, panel16_blocks != null),
            \\)
            ,
            .q4q5_grouped =>
            \\chooseQuantLinearColumnParallelPlanWithMaxRows(
            \\quantParallelEnabled(),
            \\rows,
            \\out_dim,
            \\row_blocks,
            \\quantParallelMaxWorkers(),
            \\q4q5PreparedQ8KColumnParallelMaxRowsForShape(dispatch_available, known, !q6 and panel16_available),
            \\)
            ,
        };
    }
};

// Debug output has the same shape across all workers. Native spelling policies
// preserve the existing format string, conditional prefix and call punctuation.
const Debug = struct {
    label: []const u8,
    select_kind: bool = false,
    flag: ?[]const u8 = null,
    multiline: bool = true,
    fn native(self: Debug, allocator: std.mem.Allocator, plan: []const u8) ![]u8 {
        var out = cpu.Emitter{ .allocator = allocator };
        errdefer out.bytes.deinit(allocator);
        try out.raw("if (quantParallelDebugRequested()) {\nstd.debug.print(");
        if (self.multiline) try out.raw("\n");
        if (self.select_kind) try out.raw("(if (kind == .Q4_K) \"q4k\" else \"q6k\") ++ ");
        const format = try std.mem.concat(allocator, u8, &.{ self.label, if (self.flag) |_| " " else "", self.flag orelse "", if (self.flag) |_| "={}" else "", " workers={d} rows={d} out={d} row_blocks={d}\n" });
        defer allocator.free(format);
        const quoted = try std.json.Stringify.valueAlloc(allocator, format, .{});
        defer allocator.free(quoted);
        try out.raw(quoted);
        try out.raw(if (self.multiline) ",\n.{ " else ", .{ ");
        if (self.flag) |flag| try out.add("{s}, ", .{flag});
        try out.add("{s}.worker_count, rows, out_dim, row_blocks }}", .{plan});
        try out.raw(if (self.multiline) ",\n);\n}\n" else ");\n}\n");
        return out.bytes.toOwnedSlice(allocator);
    }
};
