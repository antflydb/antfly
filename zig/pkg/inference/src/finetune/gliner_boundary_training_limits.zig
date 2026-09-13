// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Numeric resource ceilings for the version-1 training job JSON contract.
//! Defaults preserve the existing library limits. Raising a ceiling never
//! changes precision, graph mathematics, sampling, dropout or run identity.
//! Per-shape checked admission and the enclosing host/device reservations
//! remain mandatory; these are limits, not preallocated memory or RSS claims.
const std = @import("std");
const native = @import("gliner_boundary_native_trainer.zig");
const mib: usize = 1024 * 1024;
const gib: usize = 1024 * mib;

pub const Input = struct {
    max_batch: usize = 8,
    max_sequence_tokens: usize = 16384,
    max_batch_tokens: usize = 65536,
    max_text_words: usize = 8192,
    max_queries: usize = 256,
    max_classification_labels: usize = 512,
    max_groups: usize = 64,
    max_relations: usize = 64,
    max_encoder_output_bytes: usize = 128 * mib,
    max_routed_bytes: usize = 128 * mib,
    max_attention_work_items: u64 = 4 * gib,
    max_attention_scratch_bytes: usize = 8 * mib,
};
pub const Encoder = struct {
    input: Input = .{},
    /// Per-layer B * attention_heads * encoded_sequence_length^2.
    max_attention_score_elements: u64 = 64 * mib,
    max_dropout_mask_bytes: u64 = 512 * mib,
    /// Conservative sum of logical forward intermediates across all layers,
    /// before lowering/lifetime reuse. This is NOT peak physical memory.
    max_forward_tensor_bytes: u64 = 4 * gib,
    max_constant_bytes: u64 = 512 * mib,
};
pub const HeadGraph = struct {
    max_nodes: usize = 100000,
    max_tensor_elements: usize = 64 * mib,
    max_graph_elements: usize = 512 * mib,
    /// The shared graph constant pool includes encoder constants.
    max_constant_bytes: usize = 64 * mib,
    max_attention_elements: usize = 16 * mib,
};
pub const Transfers = struct {
    max_upload_bytes: usize = 256 * mib,
    max_readback_bytes: usize = 128 * mib,
};
pub const Recomputation = struct {
    max_regions: usize = 64,
    max_source_nodes: usize = 1_000_000,
    /// Reclaiming owner for regional compilation and replay metadata.
    max_plan_host_bytes: usize = 256 * mib,
    /// All regional and cut-head programs, before the first forward.
    max_compile_bytes: usize = gib,
    max_checkpoint_bytes: usize = 256 * mib,
    max_gradient_bytes: usize = gib,
    /// Enclosing source, optimizer, head and largest replay ownership must
    /// still fit both these aggregate bounds and the job's memory reservation.
    max_backend_bytes: usize = 8 * gib,
    max_host_bytes: usize = 6 * gib,
    max_total_work: u64 = 1 << 42,
};
pub const Replay = struct {
    max_source_nodes: usize = 1_000_000,
    max_recipes: usize = 16384,
    max_candidates: usize = 65536,
    max_name_bytes: usize = 1024,
    max_host_bytes: usize = 64 * mib,
    /// One generated host mask; resident upload admission is additional.
    max_mask_bytes: usize = 64 * mib,
    /// Borrowed explicit masks remain charged to their immutable owner.
    max_explicit_bytes: usize = 512 * mib,
    max_work_items: usize = 1 << 28,
};
pub const Step = struct {
    recomputation: Recomputation = .{},
    replay: Replay = .{},
    max_outputs: usize = 8192,
    max_dropout_sites: usize = 16384,
    max_record_groups: usize = 4096,
    max_relation_pairs: usize = 65536,
    max_step_host_bytes: usize = 512 * mib,
    max_step_work: usize = 500 * mib,
    max_total_host_bytes: usize = 2 * gib,
    transfers: Transfers = .{},
};
pub const Gradient = struct {
    max_forward_nodes: usize = 1_000_000,
    max_gradient_nodes: usize = 4_000_000,
};
pub const Differentiation = struct {
    gradient: Gradient = .{},
    max_tape_bytes: usize = 256 * mib,
    max_cotangent_bytes: usize = 64 * mib,
};
pub const Primitive = struct {
    /// Version 1 keeps the qualified physical F32/i32 per-tensor ABI bound.
    max_tensor_bytes: usize = gib,
    /// Dense zero writes plus submitted scatter values, in elements, not bytes.
    max_scatter_work: usize = 64 * mib,
    max_index_metadata_bytes: usize = 128 * mib,
};
pub const Instruction = struct {
    primitive: Primitive = .{},
    /// Scalar iterations, not FLOPs or elapsed time.
    max_instruction_work: u64 = 1 << 40,
    max_scratch_bytes: usize = 512 * mib,
};
pub const Program = struct {
    instruction: Instruction = .{},
    max_source_nodes: usize = 262144,
    max_program_nodes: usize = 131072,
    max_outputs: usize = 4096,
    max_compile_bytes: usize = 256 * mib,
    max_constant_bytes: usize = 64 * mib,
    max_binding_bytes: usize = 8 * gib,
    max_working_bytes: usize = 512 * mib,
    /// Aggregate independent output snapshots, including parameter gradients.
    max_capture_bytes: usize = gib,
    max_device_bytes: usize = 10 * gib,
    max_host_metadata_bytes: usize = 256 * mib,
    max_total_work: u64 = 1 << 42,
};
pub const Resident = struct {
    program: Program = .{},
    /// Aggregate over all retained forward and backward programs.
    max_compile_bytes: usize = gib,
    max_device_bytes: usize = 8 * gib,
    max_host_metadata_bytes: usize = 256 * mib,
};
pub const Config = struct {
    version: u32 = 1,
    /// Complete per-batch caller arena for regional runs, including growth
    /// slack, draws, binding metadata and native gradient readback arrays.
    max_recomputed_batch_scratch_bytes: usize = 512 * mib,
    encoder: Encoder = .{},
    head_graph: HeadGraph = .{},
    step: Step = .{},
    differentiation: Differentiation = .{},
    resident: Resident = .{},

    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!Config {
        // The standard integer parser also accepts quoted numbers and integral
        // floats. This wire contract deliberately accepts JSON integers only.
        const value = try std.json.innerParse(std.json.Value, a, source, options);
        try validateJsonNumbers(value);
        const result = try std.json.innerParseFromValue(Config, a, value, options);
        validate(result) catch return error.InvalidNumber;
        return result;
    }
};

fn validateJsonNumbers(value: std.json.Value) error{InvalidNumber}!void {
    switch (value) {
        .object => |object| for (object.values()) |child| try validateJsonNumbers(child),
        .integer => |number| if (number <= 0) return error.InvalidNumber,
        else => return error.InvalidNumber,
    }
}

/// Versioned parser/admission ceilings, independent of available hardware.
/// No value in this table qualifies a geometry or bypasses a lower enclosing
/// budget. Representation checks (physical i32/rank/strides) still apply.
pub const hard = Config{
    .max_recomputed_batch_scratch_bytes = 8 * gib,
    .encoder = .{
        .input = .{
            .max_batch = 64,
            .max_sequence_tokens = 16384,
            .max_batch_tokens = 262144,
            .max_text_words = 8192,
            .max_queries = 1024,
            .max_classification_labels = 4096,
            .max_groups = 256,
            .max_relations = 256,
            .max_encoder_output_bytes = 2 * gib,
            .max_routed_bytes = 2 * gib,
            .max_attention_work_items = 1 << 40,
            .max_attention_scratch_bytes = 128 * mib,
        },
        .max_attention_score_elements = 256 * mib,
        .max_dropout_mask_bytes = 8 * gib,
        .max_forward_tensor_bytes = 1024 * gib,
        .max_constant_bytes = 2 * gib,
    },
    .head_graph = .{
        .max_nodes = 1_000_000,
        .max_tensor_elements = 256 * mib,
        .max_graph_elements = 8 * gib,
        .max_constant_bytes = 2 * gib,
        .max_attention_elements = 256 * mib,
    },
    .step = .{
        .recomputation = .{
            .max_regions = 256,
            .max_source_nodes = 1_000_000,
            .max_plan_host_bytes = 8 * gib,
            .max_compile_bytes = 8 * gib,
            .max_checkpoint_bytes = 16 * gib,
            .max_gradient_bytes = 16 * gib,
            .max_backend_bytes = 128 * gib,
            .max_host_bytes = 32 * gib,
            .max_total_work = 1 << 48,
        },
        // These closed ceilings match Recipes.init's representation bounds.
        .replay = .{
            .max_source_nodes = 1_000_000,
            .max_recipes = 16384,
            .max_candidates = 65536,
            .max_name_bytes = 1024,
            .max_host_bytes = gib,
            .max_mask_bytes = gib,
            .max_explicit_bytes = gib,
            .max_work_items = 1 << 30,
        },
        .max_outputs = 32768,
        .max_dropout_sites = 65536,
        .max_record_groups = 65536,
        .max_relation_pairs = 1_048_576,
        .max_step_host_bytes = 8 * gib,
        .max_step_work = 1 << 40,
        .max_total_host_bytes = 32 * gib,
        .transfers = .{ .max_upload_bytes = 8 * gib, .max_readback_bytes = gib },
    },
    .differentiation = .{
        .gradient = .{ .max_forward_nodes = 1_000_000, .max_gradient_nodes = 4_000_000 },
        .max_tape_bytes = 16 * gib,
        .max_cotangent_bytes = gib,
    },
    .resident = .{
        .program = .{
            .instruction = .{
                .primitive = .{ .max_tensor_bytes = gib, .max_scatter_work = 512 * mib, .max_index_metadata_bytes = 128 * mib },
                .max_instruction_work = 1 << 46,
                .max_scratch_bytes = 8 * gib,
            },
            .max_source_nodes = 1_000_000,
            .max_program_nodes = 1_000_000,
            .max_outputs = 4096,
            .max_compile_bytes = 4 * gib,
            .max_constant_bytes = 2 * gib,
            .max_binding_bytes = 64 * gib,
            .max_working_bytes = 32 * gib,
            .max_capture_bytes = 16 * gib,
            .max_device_bytes = 128 * gib,
            .max_host_metadata_bytes = 2 * gib,
            .max_total_work = 1 << 48,
        },
        .max_compile_bytes = 8 * gib,
        .max_device_bytes = 128 * gib,
        .max_host_metadata_bytes = 2 * gib,
    },
};

pub fn validate(config: Config) !void {
    if (config.version != 1) return error.UnsupportedBoundaryTrainingLimitsVersion;
    try validateGroup(config, hard);
}

fn validateGroup(value: anytype, ceiling: @TypeOf(value)) !void {
    inline for (std.meta.fields(@TypeOf(value))) |field| {
        const current = @field(value, field.name);
        const maximum = @field(ceiling, field.name);
        switch (@typeInfo(field.type)) {
            .@"struct" => try validateGroup(current, maximum),
            .int => {
                if (current == 0) return error.InvalidBoundaryTrainingLimits;
                if (current > maximum) return error.BoundaryTrainingLimitsExceeded;
            },
            else => @compileError("Training resource wire fields must be positive integers or nested numeric structs"),
        }
    }
}

/// Copy only the explicit numeric resource fields. Existing callback/context,
/// execution, gradient strictness, sampling and optimizer options are retained.
fn copyGroup(destination: anytype, source: anytype) void {
    inline for (std.meta.fields(@TypeOf(source))) |field| switch (@typeInfo(field.type)) {
        .@"struct" => copyGroup(&@field(destination.*, field.name), @field(source, field.name)),
        .int => @field(destination.*, field.name) = @intCast(@field(source, field.name)),
        else => @compileError("Only numeric resource fields can be mapped"),
    };
}

pub fn apply(config: Config, base: native.Limits) !native.Limits {
    try validate(config);
    var result = base;
    result.max_recomputed_batch_scratch_bytes = config.max_recomputed_batch_scratch_bytes;
    copyGroup(&result.step.encoder, config.encoder);
    copyGroup(&result.step.graph, config.head_graph);
    copyGroup(&result.step, config.step);
    copyGroup(&result.differentiation, config.differentiation);
    copyGroup(&result.differentiation.resident, config.resident);
    return result;
}

fn checkEachBound(value: anytype, ceiling: @TypeOf(value)) !void {
    inline for (std.meta.fields(@TypeOf(value))) |field| {
        switch (@typeInfo(field.type)) {
            .@"struct" => try checkEachBound(@field(value, field.name), @field(ceiling, field.name)),
            .int => {
                var changed = value;
                @field(changed, field.name) = 0;
                try std.testing.expectError(error.InvalidBoundaryTrainingLimits, validateGroup(changed, ceiling));
                @field(changed, field.name) = @field(ceiling, field.name) + 1;
                try std.testing.expectError(error.BoundaryTrainingLimitsExceeded, validateGroup(changed, ceiling));
                @field(changed, field.name) = @field(ceiling, field.name);
                try validateGroup(changed, ceiling);
            },
            else => unreachable,
        }
    }
}

fn expectMapped(source: anytype, destination: anytype) !void {
    inline for (std.meta.fields(@TypeOf(source))) |field| switch (@typeInfo(field.type)) {
        .@"struct" => try expectMapped(@field(source, field.name), @field(destination, field.name)),
        .int => try std.testing.expectEqual(@field(source, field.name), @field(destination, field.name)),
        else => unreachable,
    };
}

test "boundary training limits preserve defaults and only map numeric resources" {
    try std.testing.expect(std.meta.eql(native.Limits{}, try apply(.{}, .{})));
    var base = native.Limits{};
    base.max_host_bytes = 123;
    base.optimizer.max_state_bytes = 456;
    base.differentiation.execution = .resident_metal;
    base.differentiation.gradient.require_all_gradients = true;
    base.step.relation.pair_cap = 17;
    const mapped = try apply(hard, base);
    try std.testing.expectEqual(base.max_host_bytes, mapped.max_host_bytes);
    try std.testing.expectEqual(base.optimizer, mapped.optimizer);
    try std.testing.expectEqual(base.differentiation.execution, mapped.differentiation.execution);
    try std.testing.expect(mapped.differentiation.gradient.require_all_gradients);
    try std.testing.expectEqual(base.step.relation, mapped.step.relation);
    try std.testing.expectEqual(hard.max_recomputed_batch_scratch_bytes, mapped.max_recomputed_batch_scratch_bytes);
    try expectMapped(hard.encoder, mapped.step.encoder);
    try expectMapped(hard.head_graph, mapped.step.graph);
    try expectMapped(hard.step, mapped.step);
    try expectMapped(hard.differentiation, mapped.differentiation);
    try expectMapped(hard.resident, mapped.differentiation.resident);
}

test "boundary training limits bound every numeric field and reject unsupported versions" {
    try validate(.{});
    try validate(hard);
    try checkEachBound(Config{}, hard);
    try std.testing.expectError(error.UnsupportedBoundaryTrainingLimitsVersion, validate(.{ .version = 0 }));
    try std.testing.expectError(error.UnsupportedBoundaryTrainingLimitsVersion, validate(.{ .version = 2 }));
}

test "boundary training limits JSON rejects unknown noninteger negative and overflow values" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"execution\":\"native\"}",
        "{\"resident\":{\"program\":{\"allow_host_fallback\":true}}}",
        "{\"encoder\":{\"max_forward_tensor_bytes\":-1}}",
        "{\"encoder\":{\"max_forward_tensor_bytes\":1.5}}",
        "{\"encoder\":{\"max_forward_tensor_bytes\":1.0}}",
        "{\"encoder\":{\"max_forward_tensor_bytes\":\"1024\"}}",
        "{\"encoder\":{\"max_forward_tensor_bytes\":18446744073709551616}}",
        "{\"resident\":{\"program\":{\"max_capture_bytes\":null}}}",
    }) |bytes| {
        if (std.json.parseFromSlice(Config, a, bytes, .{})) |parsed| {
            parsed.deinit();
            return error.InvalidResourceConfigAccepted;
        } else |_| {}
    }
    const valid = try std.json.parseFromSlice(Config, a,
        \\{"encoder":{"max_forward_tensor_bytes":17179869184},"differentiation":{"max_tape_bytes":2147483648},"resident":{"program":{"max_working_bytes":2147483648,"max_capture_bytes":2147483648,"instruction":{"primitive":{"max_scatter_work":268435456}}}}}
    , .{});
    defer valid.deinit();
    const mapped = try apply(valid.value, .{});
    try std.testing.expectEqual(@as(u64, 16 * gib), mapped.step.encoder.max_forward_tensor_bytes);
    try std.testing.expectEqual(@as(usize, 2 * gib), mapped.differentiation.max_tape_bytes);
    try std.testing.expectEqual(@as(usize, 256 * mib), mapped.differentiation.resident.program.instruction.primitive.max_scatter_work);
    try std.testing.expectEqual(@as(usize, 2 * gib), mapped.differentiation.resident.program.max_working_bytes);
}

fn exerciseParse(a: std.mem.Allocator) !void {
    const parsed = try std.json.parseFromSlice(Config, a,
        \\{"encoder":{"max_forward_tensor_bytes":17179869184},"resident":{"program":{"instruction":{"primitive":{"max_scatter_work":268435456}}}}}
    , .{});
    defer parsed.deinit();
    const mapped = try apply(parsed.value, .{});
    try std.testing.expectEqual(@as(usize, 256 * mib), mapped.differentiation.resident.program.instruction.primitive.max_scatter_work);
}

test "boundary training limits parser cleans every allocation failure" {
    try exerciseParse(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseParse, .{});
}

fn exerciseRegionalParse(a: std.mem.Allocator) !void {
    const parsed = try std.json.parseFromSlice(Config, a,
        \\{"max_recomputed_batch_scratch_bytes":33554432,"step":{"recomputation":{"max_regions":25,"max_source_nodes":750000,"max_plan_host_bytes":67108864,"max_compile_bytes":536870912,"max_checkpoint_bytes":67108864,"max_gradient_bytes":536870912,"max_backend_bytes":2147483648,"max_host_bytes":536870912,"max_total_work":8589934592},"replay":{"max_source_nodes":750000,"max_recipes":8192,"max_candidates":32768,"max_name_bytes":512,"max_host_bytes":16777216,"max_mask_bytes":8388608,"max_explicit_bytes":134217728,"max_work_items":134217728}}}
    , .{});
    defer parsed.deinit();
    const mapped = try apply(parsed.value, .{});
    try std.testing.expectEqual(@as(usize, 32 * mib), mapped.max_recomputed_batch_scratch_bytes);
    try expectMapped(parsed.value.step.recomputation, mapped.step.recomputation);
    try expectMapped(parsed.value.step.replay, mapped.step.replay);
    try std.testing.expectEqual(@as(u64, 1 << 33), mapped.step.recomputation.max_total_work);
    try std.testing.expectEqual(@as(usize, 25), mapped.step.recomputation.max_regions);
    try std.testing.expectEqual(@as(usize, 8 * mib), mapped.step.replay.max_mask_bytes);
    // Unmentioned numeric groups and all runtime/semantic options keep defaults.
    try std.testing.expectEqual((native.Limits{}).step.encoder, mapped.step.encoder);
    try std.testing.expectEqual((native.Limits{}).differentiation, mapped.differentiation);
}

test "boundary training limits regional groups map exact integers and unwind every parse allocation failure" {
    try exerciseRegionalParse(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseRegionalParse, .{});
}

test "boundary training limits regional JSON rejects invalid types bounds and integer overflow" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "{\"max_recomputed_batch_scratch_bytes\":0}",
        "{\"max_recomputed_batch_scratch_bytes\":-1}",
        "{\"max_recomputed_batch_scratch_bytes\":1.0}",
        "{\"max_recomputed_batch_scratch_bytes\":\"33554432\"}",
        "{\"max_recomputed_batch_scratch_bytes\":18446744073709551616}",
        "{\"step\":{\"recomputation\":null}}",
        "{\"step\":{\"recomputation\":{\"max_regions\":257}}}",
        "{\"step\":{\"recomputation\":{\"max_total_work\":18446744073709551616}}}",
        "{\"step\":{\"recomputation\":{\"max_host_bytes\":1e3}}}",
        "{\"step\":{\"recomputation\":{\"allow_host_fallback\":1}}}",
        "{\"step\":{\"replay\":{\"max_recipes\":16385}}}",
        "{\"step\":{\"replay\":{\"max_mask_bytes\":1073741825}}}",
        "{\"step\":{\"replay\":{\"max_work_items\":false}}}",
        "{\"step\":{\"replay\":{\"max_name_bytes\":[1024]}}}",
    }) |bytes| {
        if (std.json.parseFromSlice(Config, a, bytes, .{})) |parsed| {
            parsed.deinit();
            return error.InvalidRegionalResourceConfigAccepted;
        } else |_| {}
    }
}

test "boundary training limits omitted regional fields preserve the version one native defaults" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "{}", "{\"version\":1}", "{\"step\":{\"recomputation\":{},\"replay\":{}}}" }) |bytes| {
        const parsed = try std.json.parseFromSlice(Config, a, bytes, .{});
        defer parsed.deinit();
        try std.testing.expect(std.meta.eql(native.Limits{}, try apply(parsed.value, .{})));
    }
    const old = try std.json.parseFromSlice(Config, a,
        \\{"version":1,"step":{"max_step_host_bytes":134217728},"resident":{"max_compile_bytes":2147483648}}
    , .{});
    defer old.deinit();
    const mapped = try apply(old.value, .{});
    try std.testing.expectEqual(@as(usize, 128 * mib), mapped.step.max_step_host_bytes);
    try std.testing.expectEqual(@as(usize, 2 * gib), mapped.differentiation.resident.max_compile_bytes);
    try std.testing.expectEqual((native.Limits{}).step.recomputation, mapped.step.recomputation);
    try std.testing.expectEqual((native.Limits{}).step.replay, mapped.step.replay);
    try std.testing.expectEqual((native.Limits{}).max_recomputed_batch_scratch_bytes, mapped.max_recomputed_batch_scratch_bytes);
}
