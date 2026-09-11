// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded PEFT LoRA/DoRA artifacts for the published boundary architectures.
//! Receipts bind bytes and training provenance; they are not quality approval
//! or publisher signatures. Every base parameter is frozen in this profile.
const std = @import("std");
const graph = @import("gliner_boundary_peft_graph.zig");
const model = @import("../models/gliner_boundary.zig");
const policy = @import("../models/gliner_boundary_artifact.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const safetensors = @import("../models/safetensors.zig");
const checkpoint = @import("safetensors_checkpoint.zig");
const publication = @import("../gliner_boundary_export.zig");
const c_file = @import("../util/c_file.zig");
const files = @import("../runtime/file_snapshot.zig");
const compat = @import("../io/compat.zig");
const access_mod = @import("../models/tensor_access.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const config_name = "adapter_config.json";
pub const tensor_name = "adapter_model.safetensors";
pub const receipt_name = "antfly_gliner25_adapter.json";
pub const family = "gliner_boundary_adapter/v1";
pub const NamedTensor = checkpoint.NamedTensor;
pub const Limits = struct {
    max_config_bytes: usize = 128 * 1024,
    max_receipt_bytes: usize = 64 * 1024,
    max_tensor_bytes: usize = 512 * 1024 * 1024,
    max_tensor_header_bytes: usize = 8 * 1024 * 1024,
    max_targets: usize = 4096,
    max_rank: u32 = 1024,
    max_merge_tensor_bytes: usize = 64 * 1024 * 1024,
};
pub const Binding = struct {
    /// Exact already-consumed original FP32 model and all four sidecars.
    source: bundle.Identity,
    /// Fingerprint of the training schema/data contract selected by the caller.
    schema_sha256: [32]u8,
    /// This profile freezes the complete original weight artifact; therefore
    /// this digest must equal source.weight.sha256, decoded from hex.
    frozen_weight_sha256: [32]u8,
};
pub const Receipt = struct {
    family: []const u8 = family,
    version: u32 = 1,
    architecture_version: u32 = model.architecture_version,
    config_version: u32 = model.config_version,
    source: bundle.Identity,
    schema_sha256: [32]u8,
    frozen_weight_sha256: [32]u8,
    target_sha256: [32]u8,
    parameter_sha256: [32]u8,
    config: bundle.Digest,
    weights: bundle.Digest,
};
pub const Target = struct {
    module: []const u8,
    base_weight: []const u8,
    in_dim: u32,
    out_dim: u32,
    a_key: []const u8,
    b_key: []const u8,
    magnitude_key: ?[]const u8,
};
pub const Configuration = struct {
    kind: graph.Kind,
    rank: u32,
    alpha: f32,
    dropout: f32,
    targets: []const Target,
};
pub const Module = struct {
    target: Target,
    rank: u32,
    scale: f32,
    a: []const f32,
    b: []const f32,
    magnitude: ?[]const f32,
};
pub const Loaded = struct {
    arena: std.heap.ArenaAllocator,
    config: Configuration,
    config_bytes: []const u8,
    tensors: []const NamedTensor,
    modules: []const Module,
    receipt: Receipt,
    /// Exact optional receipt bytes consumed by import. Null means absent.
    receipt_digest: ?bundle.Digest = null,
    pub fn deinit(self: *Loaded) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const ExportResult = struct {
    allocator: Allocator,
    receipt_json: []u8,
    pub fn deinit(self: *ExportResult) void {
        self.allocator.free(self.receipt_json);
        self.* = undefined;
    }
};

pub const Slot = struct { name: []const u8, data: []const f32 };
pub const GraphSnapshot = struct {
    arena: std.heap.ArenaAllocator,
    config_bytes: []const u8,
    /// Payloads borrow the caller's immutable parameter epoch until export
    /// completes. Names and shapes are owned by this snapshot.
    tensors: []const NamedTensor,
    pub fn deinit(self: *GraphSnapshot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
fn slotData(slots: []const Slot, name: []const u8, count: usize) ![]const f32 {
    var found: ?[]const f32 = null;
    for (slots) |slot| if (std.mem.eql(u8, slot.name, name)) {
        if (found != null) return error.DuplicateBoundaryAdapterTensor;
        if (slot.data.len != count) return error.InvalidBoundaryAdapterTensorShape;
        found = slot.data;
    };
    return found orelse error.MissingBoundaryAdapterTensor;
}
/// Project exact graph slot names into the PEFT saved-key contract. This
/// exports adapter slots only; full/head-only checkpoints use full-model I/O.
pub fn graphSnapshot(a: Allocator, result: *const graph.Result, config: graph.Config, slots: []const Slot, base_model_name: []const u8) !GraphSnapshot {
    if (result.adapters.len == 0 or base_model_name.len > 4096 or std.mem.indexOfScalar(u8, base_model_name, 0) != null) return error.InvalidBoundaryAdapterConfig;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const names = try scratch.alloc([]const u8, result.adapters.len);
    const tensors = try scratch.alloc(NamedTensor, try mul(result.adapters.len, if (config.kind == .dora) 3 else 2));
    var index: usize = 0;
    for (result.adapters, names) |adapter, *name| {
        if (adapter.rank != config.rank or adapter.scale != config.alpha / @as(f32, @floatFromInt(config.rank)) or
            (adapter.magnitude_name != null) != (config.kind == .dora)) return error.InvalidBoundaryAdapterConfig;
        name.* = try scratch.dupe(u8, adapter.module_name);
        tensors[index] = .{ .name = try scratch.dupe(u8, adapter.a_saved_key), .shape = try scratch.dupe(usize, &.{ adapter.rank, adapter.in_dim }), .data = try slotData(slots, adapter.a_name, try mul(adapter.rank, adapter.in_dim)) };
        tensors[index + 1] = .{ .name = try scratch.dupe(u8, adapter.b_saved_key), .shape = try scratch.dupe(usize, &.{ adapter.out_dim, adapter.rank }), .data = try slotData(slots, adapter.b_name, try mul(adapter.out_dim, adapter.rank)) };
        index += 2;
        if (adapter.magnitude_name) |magnitude_name| {
            tensors[index] = .{ .name = try scratch.dupe(u8, adapter.magnitude_saved_key.?), .shape = try scratch.dupe(usize, &.{adapter.out_dim}), .data = try slotData(slots, magnitude_name, adapter.out_dim) };
            index += 1;
        }
    }
    const json = try std.json.Stringify.valueAlloc(scratch, .{
        .peft_type = "LORA",
        .task_type = @as(?[]const u8, null),
        .base_model_name_or_path = base_model_name,
        .revision = @as(?[]const u8, null),
        .r = config.rank,
        .lora_alpha = config.alpha,
        .lora_dropout = config.dropout,
        .target_modules = names,
        .bias = "none",
        .use_dora = config.kind == .dora,
        .fan_in_fan_out = false,
        .inference_mode = true,
    }, .{ .whitespace = .indent_2 });
    return .{ .arena = arena, .config_bytes = json, .tensors = tensors };
}

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.BoundaryAdapterLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryAdapterLimitExceeded;
}
fn empty(value: std.json.Value) bool {
    return switch (value) {
        .null => true,
        .array => |items| items.items.len == 0,
        .object => |items| items.count() == 0,
        else => false,
    };
}
fn boolean(value: std.json.Value) !bool {
    if (value != .bool) return error.InvalidBoundaryAdapterConfig;
    return value.bool;
}
fn number(value: std.json.Value) !f32 {
    const parsed: f64 = switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| v,
        else => return error.InvalidBoundaryAdapterConfig,
    };
    if (!std.math.isFinite(parsed) or @abs(parsed) > std.math.floatMax(f32)) return error.InvalidBoundaryAdapterConfig;
    return @floatCast(parsed);
}
fn text(value: std.json.Value) ![]const u8 {
    if (value != .string) return error.InvalidBoundaryAdapterConfig;
    return value.string;
}
fn is(value: std.json.Value, wanted: []const u8) bool {
    return value == .string and std.mem.eql(u8, value.string, wanted);
}
fn validBinding(binding: Binding) !void {
    if (binding.source.precision != .fp32) return error.UnsupportedBoundaryAdapterBasePrecision;
    const frozen_hex = std.fmt.bytesToHex(binding.frozen_weight_sha256, .lower);
    if (!std.mem.eql(u8, &binding.source.weight.sha256, &frozen_hex)) return error.BoundaryAdapterFrozenWeightMismatch;
    if (binding.source.weight.size_bytes == 0) return error.InvalidBoundaryAdapterBinding;
    for (binding.source.sidecars) |sidecar| if (sidecar.size_bytes == 0) return error.InvalidBoundaryAdapterBinding;
}
/// Arena-owned canonical Linear target descriptor shared with run-level layout
/// resolution. The caller releases all returned names with its arena.
pub fn expectedTarget(a: Allocator, backbone: model.Backbone, module: []const u8, dora: bool) !Target {
    if (module.len == 0 or module.len > 1024 or std.mem.indexOfScalar(u8, module, 0) != null) return error.InvalidBoundaryAdapterTarget;
    const name = try std.fmt.allocPrint(a, "{s}.weight", .{module});
    const bias_name = try std.fmt.allocPrint(a, "{s}.bias", .{module});
    var dimensions: ?[2]u32 = null;
    for (policy.specs(backbone)) |spec| if (std.mem.eql(u8, spec.name, name)) {
        if (spec.shape.len != 2) return error.InvalidBoundaryAdapterTarget;
        dimensions = .{ std.math.cast(u32, spec.shape[0]) orelse return error.InvalidBoundaryAdapterTarget, std.math.cast(u32, spec.shape[1]) orelse return error.InvalidBoundaryAdapterTarget };
    };
    const dims = dimensions orelse return error.InvalidBoundaryAdapterTarget;
    // All published nn.Linear modules have a matching output bias. This
    // excludes embeddings/relative tables/biaffine parameters from PEFT targets.
    var linear = false;
    for (policy.specs(backbone)) |spec| if (std.mem.eql(u8, spec.name, bias_name) and spec.shape.len == 1 and spec.shape[0] == dims[0]) {
        linear = true;
        break;
    };
    if (!linear) return error.InvalidBoundaryAdapterTarget;
    return .{ .module = try a.dupe(u8, module), .base_weight = name, .out_dim = dims[0], .in_dim = dims[1], .a_key = try std.fmt.allocPrint(a, "base_model.model.{s}.lora_A.weight", .{module}), .b_key = try std.fmt.allocPrint(a, "base_model.model.{s}.lora_B.weight", .{module}), .magnitude_key = if (dora) try std.fmt.allocPrint(a, "base_model.model.{s}.lora_magnitude_vector", .{module}) else null };
}

/// Strict pinned PEFT config subset: ordinary linear LoRA or DoRA, constant
/// rank/alpha, no learned bias, full-head replacement or weight-changing init.
/// Unknown or active extra behaviors fail rather than being silently ignored.
fn parseConfig(a: Allocator, backbone: model.Backbone, raw: []const u8, limits: Limits) !Configuration {
    if (raw.len == 0 or raw.len > limits.max_config_bytes) return error.BoundaryAdapterLimitExceeded;
    const parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBoundaryAdapterConfig;
    const object = parsed.value.object;
    if (!is(object.get("peft_type") orelse return error.InvalidBoundaryAdapterConfig, "LORA")) return error.UnsupportedBoundaryAdapterConfig;
    const rank_value = object.get("r") orelse return error.InvalidBoundaryAdapterConfig;
    if (rank_value != .integer or rank_value.integer <= 0 or rank_value.integer > limits.max_rank) return error.InvalidBoundaryAdapterConfig;
    const rank: u32 = @intCast(rank_value.integer);
    const alpha = try number(object.get("lora_alpha") orelse return error.InvalidBoundaryAdapterConfig);
    const dropout = try number(object.get("lora_dropout") orelse .{ .float = 0 });
    if (alpha <= 0 or dropout < 0 or dropout >= 1) return error.InvalidBoundaryAdapterConfig;
    const dora = try boolean(object.get("use_dora") orelse .{ .bool = false });
    const target_value = object.get("target_modules") orelse return error.InvalidBoundaryAdapterConfig;
    if (target_value != .array or target_value.array.items.len == 0 or target_value.array.items.len > limits.max_targets)
        return error.InvalidBoundaryAdapterTarget;
    var iterator = object.iterator();
    while (iterator.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "peft_type") or std.mem.eql(u8, key, "r") or std.mem.eql(u8, key, "lora_alpha") or
            std.mem.eql(u8, key, "lora_dropout") or std.mem.eql(u8, key, "use_dora") or std.mem.eql(u8, key, "target_modules")) continue;
        if (std.mem.eql(u8, key, "bias")) {
            if (!is(value, "none")) return error.UnsupportedBoundaryAdapterConfig;
        } else if (std.mem.eql(u8, key, "fan_in_fan_out") or std.mem.eql(u8, key, "use_rslora") or
            std.mem.eql(u8, key, "use_qalora") or std.mem.eql(u8, key, "lora_bias"))
        {
            if (try boolean(value)) return error.UnsupportedBoundaryAdapterConfig;
        } else if (std.mem.eql(u8, key, "inference_mode")) {
            _ = try boolean(value);
        } else if (std.mem.eql(u8, key, "init_lora_weights")) {
            if (value != .bool and !is(value, "gaussian")) return error.UnsupportedBoundaryAdapterConfig;
        } else if (std.mem.eql(u8, key, "base_model_name_or_path") or std.mem.eql(u8, key, "revision")) {
            if (value != .null) {
                const name = try text(value);
                if (name.len > 4096 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidBoundaryAdapterConfig;
            }
        } else if (std.mem.eql(u8, key, "megatron_core")) {
            if (value != .null and !is(value, "megatron.core")) return error.UnsupportedBoundaryAdapterConfig;
        } else if (std.mem.eql(u8, key, "qalora_group_size")) {
            if (value != .integer or value.integer != 16) return error.UnsupportedBoundaryAdapterConfig;
        } else if (std.mem.eql(u8, key, "auto_mapping")) {
            // Mapping names affect wrapper discovery only. Native execution
            // always uses the exact boundary model and target inventory.
            if (value != .null) {
                if (value != .object) return error.InvalidBoundaryAdapterConfig;
                var fields = value.object.iterator();
                while (fields.next()) |field| {
                    if (!std.mem.eql(u8, field.key_ptr.*, "base_model_class") and !std.mem.eql(u8, field.key_ptr.*, "parent_library")) return error.UnsupportedBoundaryAdapterConfig;
                    if ((try text(field.value_ptr.*)).len > 1024) return error.InvalidBoundaryAdapterConfig;
                }
            }
        } else {
            var allowed = false;
            for ([_][]const u8{ "task_type", "exclude_modules", "modules_to_save", "layers_to_transform", "layers_pattern", "rank_pattern", "alpha_pattern", "megatron_config", "trainable_token_indices", "loftq_config", "eva_config", "corda_config", "layer_replication", "target_parameters" }) |inactive| if (std.mem.eql(u8, key, inactive)) {
                allowed = empty(value);
                break;
            };
            if (!allowed) return error.UnsupportedBoundaryAdapterConfig;
        }
    }
    const targets = try a.alloc(Target, target_value.array.items.len);
    for (target_value.array.items, targets, 0..) |value, *target, index| {
        const module = try text(value);
        for (targets[0..index]) |previous| if (std.mem.eql(u8, module, previous.module)) return error.DuplicateBoundaryAdapterTarget;
        target.* = try expectedTarget(a, backbone, module, dora);
    }
    std.mem.sort(Target, targets, {}, struct {
        fn less(_: void, left: Target, right: Target) bool {
            return std.mem.lessThan(u8, left.module, right.module);
        }
    }.less);
    return .{ .kind = if (dora) .dora else .lora, .rank = rank, .alpha = alpha, .dropout = dropout, .targets = targets };
}
fn tensorFor(tensors: []const NamedTensor, name: []const u8, shape: []const usize) ![]const f32 {
    for (tensors) |tensor| if (std.mem.eql(u8, tensor.name, name)) {
        if (!std.mem.eql(usize, tensor.shape, shape)) return error.InvalidBoundaryAdapterTensorShape;
        return tensor.data;
    };
    return error.MissingBoundaryAdapterTensor;
}
fn validateTensors(a: Allocator, config: Configuration, tensors: []const NamedTensor, limits: Limits, control: ?Control) ![]Module {
    if (tensors.len != try mul(config.targets.len, if (config.kind == .dora) 3 else 2)) return error.IncompleteBoundaryAdapterTensorInventory;
    var total: usize = 0;
    for (tensors, 0..) |tensor, index| {
        try check(control);
        for (tensors[0..index]) |previous| if (std.mem.eql(u8, tensor.name, previous.name)) return error.DuplicateBoundaryAdapterTensor;
        var elements: usize = 1;
        if (tensor.shape.len == 0 or tensor.shape.len > 2) return error.InvalidBoundaryAdapterTensorShape;
        for (tensor.shape) |dim| {
            if (dim == 0) return error.InvalidBoundaryAdapterTensorShape;
            elements = try mul(elements, dim);
        }
        if (elements != tensor.data.len) return error.InvalidBoundaryAdapterTensorShape;
        total = try add(total, try mul(elements, 4));
        if (total > limits.max_tensor_bytes) return error.BoundaryAdapterLimitExceeded;
        for (tensor.data, 0..) |value, offset| {
            if (offset % 65536 == 0) try check(control);
            if (!std.math.isFinite(value)) return error.NonFiniteBoundaryAdapterTensor;
        }
    }
    const modules = try a.alloc(Module, config.targets.len);
    for (config.targets, modules) |target, *module| {
        module.* = .{ .target = target, .rank = config.rank, .scale = config.alpha / @as(f32, @floatFromInt(config.rank)), .a = try tensorFor(tensors, target.a_key, &.{ config.rank, target.in_dim }), .b = try tensorFor(tensors, target.b_key, &.{ target.out_dim, config.rank }), .magnitude = if (target.magnitude_key) |key| try tensorFor(tensors, key, &.{target.out_dim}) else null };
    }
    return modules;
}
fn targetDigest(config: Configuration, control: ?Control) ![32]u8 {
    try check(control);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-gliner25-peft-targets/v1\x00");
    hash.update(@tagName(config.kind));
    var number_bytes: [4]u8 = undefined;
    for ([_]u32{ config.rank, @bitCast(config.alpha), @bitCast(config.dropout) }) |value| {
        std.mem.writeInt(u32, &number_bytes, value, .little);
        hash.update(&number_bytes);
    }
    for (config.targets) |target| {
        try check(control);
        std.mem.writeInt(u32, &number_bytes, @intCast(target.module.len), .little);
        hash.update(&number_bytes);
        hash.update(target.module);
        for ([_]u32{ target.in_dim, target.out_dim }) |value| {
            std.mem.writeInt(u32, &number_bytes, value, .little);
            hash.update(&number_bytes);
        }
    }
    try check(control);
    return hash.finalResult();
}
fn parameterDigest(modules: []const Module, control: ?Control) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-gliner25-peft-parameters/v1\x00");
    for (modules) |module| {
        try check(control);
        hash.update(module.target.module);
        hash.update("\x00");
        for ([_][]const f32{ module.a, module.b, module.magnitude orelse &.{} }) |values| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, values.len, .little);
            hash.update(&bytes);
            if (comptime @import("builtin").cpu.arch.endian() == .little) {
                try hashBytes(&hash, std.mem.sliceAsBytes(values), control);
            } else {
                for (values, 0..) |value, index| {
                    if (index % 65536 == 0) try check(control);
                    std.mem.writeInt(u32, bytes[0..4], @bitCast(value), .little);
                    hash.update(bytes[0..4]);
                }
            }
        }
    }
    try check(control);
    return hash.finalResult();
}
fn hashBytes(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8, control: ?Control) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        hash.update(bytes[offset..end]);
        offset = end;
    }
    try check(control);
}
fn digestBytes(bytes: []const u8, control: ?Control) !bundle.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    try hashBytes(&hash, bytes, control);
    return .{ .size_bytes = bytes.len, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) };
}
fn copyF32(values: []f32, bytes: []const u8, control: ?Control) !void {
    if (bytes.len != try mul(values.len, 4)) return error.InvalidBoundaryAdapterTensorShape;
    for (values, 0..) |*value, element| {
        if (element % 65536 == 0) try check(control);
        value.* = @bitCast(std.mem.readInt(u32, bytes[element * 4 ..][0..4], .little));
    }
    try check(control);
}
fn makeReceipt(binding: Binding, config: Configuration, modules: []const Module, config_bytes: []const u8, tensor_bytes: []const u8, control: ?Control) !Receipt {
    return .{ .source = binding.source, .schema_sha256 = binding.schema_sha256, .frozen_weight_sha256 = binding.frozen_weight_sha256, .target_sha256 = try targetDigest(config, control), .parameter_sha256 = try parameterDigest(modules, control), .config = try digestBytes(config_bytes, control), .weights = try digestBytes(tensor_bytes, control) };
}
fn verifyReceipt(a: Allocator, expected: Receipt, raw: []const u8, limits: Limits) !void {
    if (raw.len > limits.max_receipt_bytes) return error.BoundaryAdapterLimitExceeded;
    const parsed = try std.json.parseFromSlice(Receipt, a, raw, .{});
    defer parsed.deinit();
    const actual = parsed.value;
    if (!std.mem.eql(u8, actual.family, family) or actual.version != 1 or actual.architecture_version != model.architecture_version or actual.config_version != model.config_version)
        return error.UnsupportedBoundaryAdapterReceipt;
    if (!std.meta.eql(actual.source, expected.source) or !std.meta.eql(actual.schema_sha256, expected.schema_sha256) or
        !std.meta.eql(actual.frozen_weight_sha256, expected.frozen_weight_sha256) or !std.meta.eql(actual.target_sha256, expected.target_sha256) or
        !std.meta.eql(actual.parameter_sha256, expected.parameter_sha256) or
        !std.meta.eql(actual.config, expected.config) or !std.meta.eql(actual.weights, expected.weights)) return error.BoundaryAdapterArtifactMismatch;
}

/// Bytes may be released immediately after import. All metadata, tensor values
/// and config are owned by the returned arena. Standard PEFT files can omit a
/// receipt only when the caller supplies the exact verified source binding.
pub fn importBytes(a: Allocator, config_bytes: []const u8, tensor_bytes: []const u8, receipt_bytes: ?[]const u8, binding: Binding, limits: Limits, control: ?Control) !Loaded {
    try check(control);
    try validBinding(binding);
    if (tensor_bytes.len < 8 or tensor_bytes.len > limits.max_tensor_bytes or
        std.mem.readInt(u64, tensor_bytes[0..8], .little) > limits.max_tensor_header_bytes) return error.BoundaryAdapterLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const config = try parseConfig(scratch, binding.source.backbone, config_bytes, limits);
    // fromBytes borrows payload; only its header owns allocations here.
    var reader = try safetensors.MMapReader.fromBytes(a, tensor_bytes);
    defer reader.header.deinit();
    try safetensors.validateReader(a, &reader);
    if (reader.header.tensors.count() != try mul(config.targets.len, if (config.kind == .dora) 3 else 2)) return error.IncompleteBoundaryAdapterTensorInventory;
    const tensors = try scratch.alloc(NamedTensor, reader.header.tensors.count());
    var iterator = reader.header.tensors.iterator();
    var index: usize = 0;
    while (iterator.next()) |entry| : (index += 1) {
        try check(control);
        const meta = entry.value_ptr.*;
        if (meta.dtype != .f32) return error.UnsupportedBoundaryAdapterTensorPrecision;
        if (meta.shape.len == 0 or meta.shape.len > 2) return error.InvalidBoundaryAdapterTensorShape;
        const dimensions = try scratch.alloc(usize, meta.shape.len);
        for (meta.shape, dimensions) |dim, *value| value.* = std.math.cast(usize, dim) orelse return error.InvalidBoundaryAdapterTensorShape;
        const bytes = tensor_bytes[@intCast(reader.data_offset + meta.data_start)..@intCast(reader.data_offset + meta.data_end)];
        const values = try scratch.alloc(f32, bytes.len / 4);
        try copyF32(values, bytes, control);
        tensors[index] = .{ .name = try scratch.dupe(u8, entry.key_ptr.*), .shape = dimensions, .data = values };
    }
    const modules = try validateTensors(scratch, config, tensors, limits, control);
    const receipt = try makeReceipt(binding, config, modules, config_bytes, tensor_bytes, control);
    if (receipt_bytes) |raw| try verifyReceipt(a, receipt, raw, limits);
    return .{ .arena = arena, .config = config, .config_bytes = try scratch.dupe(u8, config_bytes), .tensors = tensors, .modules = modules, .receipt = receipt, .receipt_digest = if (receipt_bytes) |raw| try digestBytes(raw, control) else null };
}
pub fn importDirectory(a: Allocator, directory: []const u8, binding: Binding, limits: Limits, control: ?Control) !Loaded {
    try check(control);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const config_bytes = try readFileControlled(scratch, try std.fs.path.join(scratch, &.{ directory, config_name }), limits.max_config_bytes, control);
    const tensor_bytes = try readFileControlled(scratch, try std.fs.path.join(scratch, &.{ directory, tensor_name }), limits.max_tensor_bytes, control);
    const receipt_bytes = readFileControlled(scratch, try std.fs.path.join(scratch, &.{ directory, receipt_name }), limits.max_receipt_bytes, control) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    return importBytes(a, config_bytes, tensor_bytes, receipt_bytes, binding, limits, control);
}

/// Row-major FP32 merge at eval semantics. The frozen bias is never touched.
/// Only one adapted matrix is allocated; caller retains the original artifact.
pub fn mergeTensor(a: Allocator, module: Module, base: []align(1) const f32, limits: Limits, control: ?Control) ![]f32 {
    try check(control);
    const count = try mul(module.target.out_dim, module.target.in_dim);
    if (count != base.len or try mul(count, 4) > limits.max_merge_tensor_bytes) return error.InvalidBoundaryAdapterTensorShape;
    const output = try a.alloc(f32, count);
    errdefer a.free(output);
    try mergeInto(module, base, output, limits, control);
    return output;
}

/// Fill one caller-owned matrix without a second payload allocation. The
/// destination may be a byte-aligned SafeTensors record. It must not alias
/// base/A/B/magnitude; callers retain these immutable inputs until return.
pub fn mergeInto(module: Module, base: []align(1) const f32, output: []align(1) f32, limits: Limits, control: ?Control) !void {
    try check(control);
    const output_dim = module.target.out_dim;
    const input_dim = module.target.in_dim;
    const count = try mul(output_dim, input_dim);
    if (count != base.len or count != output.len or try mul(count, 4) > limits.max_merge_tensor_bytes or module.rank == 0 or module.rank > limits.max_rank or
        module.a.len != try mul(module.rank, input_dim) or module.b.len != try mul(output_dim, module.rank) or
        (module.magnitude != null and module.magnitude.?.len != output_dim) or !std.math.isFinite(module.scale)) return error.InvalidBoundaryAdapterTensorShape;
    const destination = std.mem.sliceAsBytes(output);
    if (overlap(destination, std.mem.sliceAsBytes(base)) or overlap(destination, std.mem.sliceAsBytes(module.a)) or
        overlap(destination, std.mem.sliceAsBytes(module.b)) or (module.magnitude != null and overlap(destination, std.mem.sliceAsBytes(module.magnitude.?))))
        return error.InvalidBoundaryAdapterTensorShape;
    for (0..output_dim) |row| {
        try check(control);
        var squared: f64 = 0;
        for (0..input_dim) |column| {
            if (column % 1024 == 0) try check(control);
            var delta: f32 = 0;
            for (0..module.rank) |rank| delta += module.b[row * module.rank + rank] * module.a[rank * input_dim + column];
            const value = base[row * input_dim + column] + module.scale * delta;
            if (!std.math.isFinite(value)) return error.NonFiniteBoundaryAdapterTensor;
            output[row * input_dim + column] = value;
            squared += @as(f64, value) * value;
        }
        if (module.magnitude) |magnitude| {
            const norm: f32 = @floatCast(@sqrt(squared));
            if (!std.math.isFinite(norm) or norm <= 0 or !std.math.isFinite(magnitude[row])) return error.InvalidBoundaryAdapterNorm;
            const scale = magnitude[row] / norm;
            for (output[row * input_dim ..][0..input_dim]) |*value| {
                value.* *= scale;
                if (!std.math.isFinite(value.*)) return error.NonFiniteBoundaryAdapterTensor;
            }
        }
    }
}

fn overlap(left: []const u8, right: []const u8) bool {
    if (left.len == 0 or right.len == 0) return false;
    const l = @intFromPtr(left.ptr);
    const r = @intFromPtr(right.ptr);
    return if (l <= r) r - l < left.len else l - r < right.len;
}

pub const Validated = struct { config: Configuration, modules: []const Module };

/// Arena-owned metadata with borrowed adapter tensor values. Rebuild from
/// authoritative owned config/tensors and validate recorded digests instead
/// of trusting caller-mutable cached module descriptors.
pub fn validateLoaded(a: Allocator, loaded: *const Loaded, limits: Limits, control: ?Control) !Validated {
    try check(control);
    try validBinding(.{ .source = loaded.receipt.source, .schema_sha256 = loaded.receipt.schema_sha256, .frozen_weight_sha256 = loaded.receipt.frozen_weight_sha256 });
    if (!std.mem.eql(u8, loaded.receipt.family, family) or loaded.receipt.version != 1 or loaded.receipt.architecture_version != model.architecture_version or loaded.receipt.config_version != model.config_version)
        return error.UnsupportedBoundaryAdapterReceipt;
    const config = try parseConfig(a, loaded.receipt.source.backbone, loaded.config_bytes, limits);
    const modules = try validateTensors(a, config, loaded.tensors, limits, control);
    if (!std.meta.eql(try digestBytes(loaded.config_bytes, control), loaded.receipt.config) or
        !std.meta.eql(try targetDigest(config, control), loaded.receipt.target_sha256) or
        !std.meta.eql(try parameterDigest(modules, control), loaded.receipt.parameter_sha256)) return error.BoundaryAdapterArtifactMismatch;
    return .{ .config = config, .modules = modules };
}

fn stagingPath(a: Allocator, io: std.Io, output: []const u8) ![]const u8 {
    if (output.len == 0 or std.mem.eql(u8, std.fs.path.basename(output), ".") or std.mem.eql(u8, std.fs.path.basename(output), "..")) return error.InvalidOutputPath;
    const cwd = std.Io.Dir.cwd();
    if (cwd.access(io, output, .{})) |_| return error.PathAlreadyExists else |err| if (err != error.FileNotFound) return err;
    const parent = std.fs.path.dirname(output) orelse ".";
    try publication.syncDirectory(io, parent);
    var random: [16]u8 = undefined;
    try std.Io.randomSecure(io, &random);
    const path = try std.fs.path.join(a, &.{ parent, try std.fmt.allocPrint(a, ".gliner25-adapter-{s}", .{std.fmt.bytesToHex(random, .lower)}) });
    try cwd.createDir(io, path, .default_dir);
    return path;
}
fn readFileControlled(a: Allocator, path: []const u8, limit: usize, control: ?Control) ![]u8 {
    return files.read(a, compat.io(), std.Io.Dir.cwd(), path, limit, control);
}
fn writeFile(a: Allocator, io: std.Io, directory: []const u8, name: []const u8, bytes: []const u8, control: ?Control) !void {
    try check(control);
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
    defer file.close(io);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try check(control);
        const end = @min(bytes.len, offset +| (256 * 1024));
        try file.writeStreamingAll(io, bytes[offset..end]);
        offset = end;
    }
    try file.sync(io);
    try check(control);
}
pub fn exportDirectory(a: Allocator, io: std.Io, output: []const u8, config_bytes: []const u8, tensors: []const NamedTensor, binding: Binding, limits: Limits, control: ?Control) !ExportResult {
    try check(control);
    try validBinding(binding);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const config = try parseConfig(scratch, binding.source.backbone, config_bytes, limits);
    _ = try validateTensors(scratch, config, tensors, limits, control);
    const stage = try stagingPath(scratch, io, output);
    var published = false;
    defer if (!published) publication.cleanupPrivateTree(io, stage);
    const sorted = try scratch.dupe(NamedTensor, tensors);
    std.mem.sort(NamedTensor, sorted, {}, struct {
        fn less(_: void, left: NamedTensor, right: NamedTensor) bool {
            return std.mem.lessThan(u8, left.name, right.name);
        }
    }.less);
    const path = try std.fs.path.join(scratch, &.{ stage, tensor_name });
    try checkpoint.saveControlled(a, path, sorted, control, true);
    try check(control);
    var mapping = try c_file.MmapRegion.init(a, path);
    defer mapping.deinit();
    // Validate the complete serialized file before publication, independently
    // of caller tensor descriptors and the writer's internal offsets.
    var verified = try importBytes(a, config_bytes, mapping.data, null, binding, limits, control);
    defer verified.deinit();
    const receipt_json = try std.json.Stringify.valueAlloc(a, verified.receipt, .{ .whitespace = .indent_2 });
    errdefer a.free(receipt_json);
    if (receipt_json.len > limits.max_receipt_bytes) return error.BoundaryAdapterLimitExceeded;
    try writeFile(a, io, stage, config_name, config_bytes, control);
    try writeFile(a, io, stage, receipt_name, receipt_json, control);
    try publication.syncDirectory(io, stage);
    try check(control);
    try publication.publishDirectory(a, io, stage, output);
    published = true;
    try publication.syncDirectory(io, std.fs.path.dirname(output) orelse ".");
    return .{ .allocator = a, .receipt_json = receipt_json };
}

const MergedAccess = struct {
    base: access_mod.TensorAccess,
    modules: []const Module,
    limits: Limits,
    control: ?Control,
    const vtable = access_mod.TensorAccess.VTable{ .getRecord = getRecord, .listNames = listNames, .deinit = deinit };
    fn access(self: *MergedAccess) access_mod.TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn getRecord(raw: *anyopaque, a: Allocator, name: []const u8) !access_mod.Record {
        const self: *MergedAccess = @ptrCast(@alignCast(raw));
        try check(self.control);
        var record = try self.base.getRecord(a, name);
        errdefer record.deinit();
        if (record.descriptor.encoding != .dense or record.descriptor.encoding.dense != .f32 or record.raw_bytes.len % 4 != 0)
            return error.UnsupportedBoundaryAdapterBasePrecision;
        for (std.mem.bytesAsSlice(f32, record.raw_bytes), 0..) |value, index| {
            if (index % 65536 == 0) try check(self.control);
            if (!std.math.isFinite(value)) return error.NonFiniteBoundaryAdapterTensor;
        }
        for (self.modules) |module| if (std.mem.eql(u8, module.target.base_weight, name)) {
            const merged = try mergeTensor(a, module, std.mem.bytesAsSlice(f32, record.raw_bytes), self.limits, self.control);
            defer a.free(merged);
            // Record owns byte-aligned storage; keep its allocator/free ABI
            // independent of the f32 scratch allocation's alignment.
            const bytes = try a.dupe(u8, std.mem.sliceAsBytes(merged));
            const descriptor = record.descriptor;
            record.deinit();
            return .{ .descriptor = descriptor, .raw_bytes = bytes, .allocator = a, .owns_bytes = true };
        };
        return record;
    }
    fn listNames(raw: *anyopaque, a: Allocator) ![][]const u8 {
        const self: *MergedAccess = @ptrCast(@alignCast(raw));
        return self.base.listNames(a);
    }
    fn deinit(_: *anyopaque) void {}
};
fn validateBase(a: Allocator, backbone: model.Backbone, owner: *access_mod.SafetensorsAccess, control: ?Control) !void {
    try safetensors.validateReader(a, &owner.source.reader);
    const access = owner.tensorAccess();
    const names = try access.listNames(a);
    defer a.free(names);
    const descriptors = try a.alloc(access_mod.Descriptor, names.len);
    defer a.free(descriptors);
    for (names, descriptors) |name, *descriptor| {
        var record = try access.getRecord(a, name);
        defer record.deinit();
        descriptor.* = record.descriptor;
    }
    _ = try policy.validate(a, backbone, .fp32, descriptors, control);
}

/// Materialize a complete canonical FP32 checkpoint. Base and sidecars must
/// match the imported adapter's exact binding. Untouched tensors and all
/// biases are copied verbatim. Scratch is at most two adapted matrices plus
/// parser/writer metadata; no complete float model is duplicated in memory.
/// The ordinary boundary bundle converter can quantize the resulting model.
pub fn materializeMerged(a: Allocator, io: std.Io, source_dir: []const u8, adapter: *const Loaded, output: []const u8, limits: Limits, control: ?Control) !ExportResult {
    try check(control);
    const expected = adapter.receipt.source;
    if (expected.precision != .fp32) return error.UnsupportedBoundaryAdapterBasePrecision;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var sidecars: [4][]const u8 = undefined;
    var digests: [4]bundle.Digest = undefined;
    for (bundle.sidecar_names, &sidecars, &digests) |name, *bytes, *digest| {
        try check(control);
        bytes.* = try readFileControlled(scratch, try std.fs.path.join(scratch, &.{ source_dir, name }), @intCast(try bundle.fileLimit(name)), control);
        digest.* = try digestBytes(bytes.*, control);
    }
    try expected.verifySidecars(digests);
    const config = try model.parseConfig(scratch, sidecars[0], sidecars[1]);
    if (config.backbone != expected.backbone) return error.BoundaryAdapterArtifactMismatch;
    const source_path = try std.fs.path.join(scratch, &.{ source_dir, "model.safetensors" });
    const owner = try access_mod.SafetensorsAccess.initAbsolute(a, source_path);
    defer owner.tensorAccess().deinit();
    const source_bytes = owner.source.reader.file_bytes;
    if (!std.meta.eql(expected.weight, try digestBytes(source_bytes, control))) return error.BoundaryAdapterArtifactMismatch;
    try validateBase(a, expected.backbone, owner, control);
    const modules = try validateTensors(scratch, adapter.config, adapter.tensors, limits, control);
    if (!std.meta.eql(try targetDigest(adapter.config, control), adapter.receipt.target_sha256) or
        !std.meta.eql(try parameterDigest(modules, control), adapter.receipt.parameter_sha256) or
        !std.meta.eql(try digestBytes(adapter.config_bytes, control), adapter.receipt.config)) return error.BoundaryAdapterArtifactMismatch;
    for (modules) |module| if (try mul(try mul(module.target.in_dim, module.target.out_dim), 4) > limits.max_merge_tensor_bytes)
        return error.BoundaryAdapterLimitExceeded;
    const stage = try stagingPath(scratch, io, output);
    var published = false;
    defer if (!published) publication.cleanupPrivateTree(io, stage);
    const weight_path = try std.fs.path.join(scratch, &.{ stage, "model.safetensors" });
    var merged = MergedAccess{ .base = owner.tensorAccess(), .modules = modules, .limits = limits, .control = control };
    try @import("../native_export_safetensors.zig").exportAccessWithMetadata(a, owner.tensorAccess(), merged.access(), weight_path, control);
    try check(control);
    const written = try access_mod.SafetensorsAccess.initAbsolute(a, weight_path);
    defer written.tensorAccess().deinit();
    try validateBase(a, expected.backbone, written, control);
    const merged_digest = try digestBytes(written.source.reader.file_bytes, control);
    for (bundle.sidecar_names, sidecars) |name, bytes| try writeFile(a, io, stage, name, bytes, control);
    // The mmap may reflect writes to the source inode; rehash immediately
    // before publication so a changing source cannot produce a stale receipt.
    if (!std.meta.eql(expected.weight, try digestBytes(source_bytes, control))) return error.BoundaryAdapterArtifactMismatch;
    const receipt_json = try std.json.Stringify.valueAlloc(a, .{
        .family = "gliner_boundary_merged/v1",
        .version = @as(u32, 1),
        .adapter = adapter.receipt,
        .weight = merged_digest,
        .sidecars = digests,
        .precision = policy.Precision.fp32,
    }, .{ .whitespace = .indent_2 });
    errdefer a.free(receipt_json);
    try writeFile(a, io, stage, "antfly_gliner25_merged.json", receipt_json, control);
    try publication.syncDirectory(io, try std.fs.path.join(scratch, &.{ stage, "encoder_config" }));
    try publication.syncDirectory(io, stage);
    try check(control);
    try publication.publishDirectory(a, io, stage, output);
    published = true;
    try publication.syncDirectory(io, std.fs.path.dirname(output) orelse ".");
    return .{ .allocator = a, .receipt_json = receipt_json };
}

fn testBinding(backbone: model.Backbone) Binding {
    var frozen: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("synthetic frozen checkpoint", &frozen, .{});
    return .{ .source = .{ .backbone = backbone, .precision = .fp32, .weight = bundle.Digest.of("synthetic frozen checkpoint"), .sidecars = .{bundle.Digest.of("synthetic sidecar")} ** 4 }, .schema_sha256 = .{0x25} ** 32, .frozen_weight_sha256 = frozen };
}

test "GLiNER2.5 adapter files preserve digest bytes with bounded copy and hash cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const target = try expectedTarget(arena.allocator(), .small, "boundary_head.count_head", true);
    const config = Configuration{ .kind = .dora, .rank = 2, .alpha = 3, .dropout = 0.125, .targets = &.{target} };
    // Independent hashlib + struct.pack("<Iff"/"<Q"/"<f") serialization of
    // the existing v1 domains/order. Chunking must not alter artifact receipts.
    const module = Module{ .target = target, .rank = 2, .scale = 1.5, .a = &.{ 0, -0.0, 0.125 }, .b = &.{ -0.75, 1.5 }, .magnitude = &.{2} };
    const target_digest = try targetDigest(config, null);
    const parameter_digest = try parameterDigest(&.{module}, null);
    try std.testing.expectEqualStrings("fb923bf0a6fa851699fc8c05975b9872aa96e766ed79ca285ca9680d3a1f98c4", &std.fmt.bytesToHex(target_digest, .lower));
    try std.testing.expectEqualStrings("689a00c70a7408305273fcf66452278578c73ff1ae5481d92d8da55e06958a36", &std.fmt.bytesToHex(parameter_digest, .lower));
    const count = 65537;
    const bytes = try a.alloc(u8, count * 4);
    defer a.free(bytes);
    for (0..count) |index| std.mem.writeInt(u32, bytes[index * 4 ..][0..4], @bitCast(@as(f32, 0.125)), .little);
    const values = try a.alloc(f32, count);
    defer a.free(values);
    try copyF32(values, bytes, null);
    for (values) |value| try std.testing.expectEqual(@as(f32, 0.125), value);
    try std.testing.expectEqual(bundle.Digest.of(bytes), try digestBytes(bytes, null));
    const Cancel = struct {
        calls: usize = 0,
        fn call(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    @memset(values, 42);
    try std.testing.expectError(error.Cancelled, copyF32(values, bytes, .{ .ptr = &cancel, .check_fn = Cancel.call }));
    try std.testing.expectEqual(@as(usize, 2), cancel.calls);
    try std.testing.expectEqual(@as(f32, 0.125), values[65535]);
    try std.testing.expectEqual(@as(f32, 42), values[65536]);
    cancel = .{};
    try std.testing.expectError(error.Cancelled, digestBytes(bytes, .{ .ptr = &cancel, .check_fn = Cancel.call }));
    try std.testing.expectEqual(@as(usize, 2), cancel.calls);
    cancel = .{};
    try std.testing.expectError(error.Cancelled, parameterDigest(&.{module}, .{ .ptr = &cancel, .check_fn = Cancel.call }));
    try std.testing.expectEqual(@as(usize, 2), cancel.calls);
}
fn testFill(values: []f32, frequency: f32, scale: f32) void {
    for (values, 0..) |*value, index| value.* = @sin(@as(f32, @floatFromInt(index + 1)) * frequency) * scale;
}

test "GLiNER2.5 adapter files roundtrip LoRA DoRA all backbones and merged eval math" {
    const a = std.testing.allocator;
    const ml = @import("ml").graph;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const parent = try temp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    for ([_]model.Backbone{ .small, .base, .multi }) |backbone| for ([_]graph.Kind{ .lora, .dora }) |kind| {
        errdefer std.debug.print("adapter artifact {s}/{s}\n", .{ @tagName(backbone), @tagName(kind) });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const hidden: u32 = if (backbone == .small) 384 else 768;
        const base = try scratch.alloc(f32, hidden);
        testFill(base, 0.17, 0.05);
        var source = ml.Graph.init(a);
        defer source.deinit();
        var builder = ml.Builder.init(&source);
        const input = try builder.parameter("__input", ml.Shape.init(.f32, &.{ 2, hidden }));
        const weight = try builder.parameter("boundary_head.count_head.weight", ml.Shape.init(.f32, &.{ 1, hidden }));
        const bias = try builder.parameter("boundary_head.count_head.bias", ml.Shape.init(.f32, &.{1}));
        try source.markOutput(try builder.linear(input, weight, bias, 2, hidden, 1));
        const config = graph.Config{ .kind = kind, .mode = .eval, .rank = 2, .alpha = 3, .targets = &.{"boundary_head.count_head"} };
        var injected = try graph.inject(a, &source, config, .{});
        defer injected.deinit();
        const descriptor = injected.adapters[0];
        var initial = try graph.initialize(a, descriptor, base, 1701);
        defer initial.deinit();
        testFill(initial.a, 0.11, 0.03);
        initial.b[0] = 0.13;
        initial.b[1] = -0.27;
        if (initial.magnitude) |values| values[0] = 0.8;
        var slots = std.ArrayListUnmanaged(Slot).empty;
        try slots.append(scratch, .{ .name = descriptor.a_name, .data = initial.a });
        try slots.append(scratch, .{ .name = descriptor.b_name, .data = initial.b });
        if (initial.magnitude) |values| try slots.append(scratch, .{ .name = descriptor.magnitude_name.?, .data = values });
        var snapshot = try graphSnapshot(a, &injected, config, slots.items, try std.fmt.allocPrint(scratch, "fastino/gliner2.5-{s}-v1", .{@tagName(backbone)}));
        defer snapshot.deinit();
        for (snapshot.tensors) |tensor| try std.testing.expect(std.mem.indexOf(u8, tensor.name, ".default") == null);
        if (kind == .dora) try std.testing.expect(std.mem.endsWith(u8, snapshot.tensors[2].name, ".lora_magnitude_vector"));
        const directory = try std.fs.path.join(scratch, &.{ parent, try std.fmt.allocPrint(scratch, "{s}-{s}", .{ @tagName(backbone), @tagName(kind) }) });
        const binding = testBinding(backbone);
        var exported = try exportDirectory(a, std.testing.io, directory, snapshot.config_bytes, snapshot.tensors, binding, .{}, null);
        defer exported.deinit();
        try std.testing.expectError(error.PathAlreadyExists, exportDirectory(a, std.testing.io, directory, snapshot.config_bytes, snapshot.tensors, binding, .{}, null));
        var loaded = try importDirectory(a, directory, binding, .{}, null);
        defer loaded.deinit();
        try std.testing.expectEqual(@as(usize, 1), loaded.modules.len);
        const module = loaded.modules[0];
        try std.testing.expectEqualStrings("boundary_head.count_head.weight", module.target.base_weight);
        try std.testing.expectEqualSlices(f32, initial.a, module.a);
        try std.testing.expectEqualSlices(f32, initial.b, module.b);
        const merged = try mergeTensor(a, module, base, .{}, null);
        defer a.free(merged);
        // Independent unmerged equation in f64. Bias is added exactly once
        // outside the merged matrix, including when DoRA scales directions.
        const x = try scratch.alloc(f32, 2 * hidden);
        testFill(x, 0.23, 0.4);
        var norm_squared: f64 = 0;
        for (0..hidden) |column| {
            const direction = @as(f64, base[column]) + 1.5 * (@as(f64, initial.a[column]) * initial.b[0] + @as(f64, initial.a[hidden + column]) * initial.b[1]);
            norm_squared += direction * direction;
        }
        for (0..2) |row| {
            var actual: f64 = 0.71;
            var base_score: f64 = 0;
            var left = [2]f64{ 0, 0 };
            for (0..hidden) |column| {
                const value = x[row * hidden + column];
                actual += @as(f64, value) * merged[column];
                base_score += @as(f64, value) * base[column];
                left[0] += @as(f64, value) * initial.a[column];
                left[1] += @as(f64, value) * initial.a[hidden + column];
            }
            var expected = base_score + 1.5 * (left[0] * initial.b[0] + left[1] * initial.b[1]);
            if (kind == .dora) expected *= @as(f64, initial.magnitude.?[0]) / @sqrt(norm_squared);
            expected += 0.71;
            try std.testing.expectApproxEqAbs(expected, actual, 2e-6);
        }
        const config_bytes = try c_file.readFileMax(scratch, try std.fs.path.join(scratch, &.{ directory, config_name }), 128 * 1024);
        const weight_bytes = try c_file.readFileMax(scratch, try std.fs.path.join(scratch, &.{ directory, tensor_name }), 32 * 1024);
        const saved_receipt = try c_file.readFileMax(scratch, try std.fs.path.join(scratch, &.{ directory, receipt_name }), 64 * 1024);
        var bad_binding = binding;
        bad_binding.schema_sha256[0] ^= 1;
        try std.testing.expectError(error.BoundaryAdapterArtifactMismatch, importBytes(a, config_bytes, weight_bytes, saved_receipt, bad_binding, .{}, null));
        bad_binding = binding;
        bad_binding.frozen_weight_sha256[0] ^= 1;
        try std.testing.expectError(error.BoundaryAdapterFrozenWeightMismatch, importBytes(a, config_bytes, weight_bytes, saved_receipt, bad_binding, .{}, null));
        weight_bytes[weight_bytes.len - 4] ^= 1;
        try std.testing.expectError(error.BoundaryAdapterArtifactMismatch, importBytes(a, config_bytes, weight_bytes, saved_receipt, binding, .{}, null));
    };
}

const test_config = "{\"peft_type\":\"LORA\",\"r\":2,\"lora_alpha\":3,\"lora_dropout\":0.25,\"use_dora\":true,\"bias\":\"none\",\"target_modules\":[\"boundary_head.count_head\"]}";
const test_a_key = "base_model.model.boundary_head.count_head.lora_A.weight";
const test_b_key = "base_model.model.boundary_head.count_head.lora_B.weight";
const test_m_key = "base_model.model.boundary_head.count_head.lora_magnitude_vector";
fn testTensors(left: []const f32) [3]NamedTensor {
    return .{
        .{ .name = test_a_key, .shape = &.{ 2, 384 }, .data = left },
        .{ .name = test_b_key, .shape = &.{ 1, 2 }, .data = &.{ 0.1, -0.2 } },
        .{ .name = test_m_key, .shape = &.{1}, .data = &.{0.8} },
    };
}
fn importAllocationFailures(a: Allocator, config_bytes: []const u8, tensor_bytes: []const u8) !void {
    var loaded = try importBytes(a, config_bytes, tensor_bytes, null, testBinding(.small), .{}, null);
    defer loaded.deinit();
    const merged = try mergeTensor(a, loaded.modules[0], &([_]f32{1} ** 384), .{}, null);
    defer a.free(merged);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const validated = try validateLoaded(arena.allocator(), &loaded, .{}, null);
    const unaligned = try a.alloc(u8, merged.len * 4 + 1);
    defer a.free(unaligned);
    try mergeInto(validated.modules[0], &([_]f32{1} ** 384), std.mem.bytesAsSlice(f32, unaligned[1..]), .{}, null);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(merged), unaligned[1..]);
}

test "GLiNER2.5 adapter files reject unsupported malformed config and clean cancelled staging" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    for ([_][]const u8{
        "{\"peft_type\":\"LORA\",\"r\":-1,\"lora_alpha\":3,\"target_modules\":[\"boundary_head.count_head\"]}",
        "{\"peft_type\":\"LORA\",\"r\":2,\"lora_alpha\":0,\"target_modules\":[\"boundary_head.count_head\"]}",
    }) |json| try std.testing.expectError(error.InvalidBoundaryAdapterConfig, parseConfig(scratch, .small, json, .{}));
    for ([_][]const u8{ "\"use_rslora\":true", "\"bias\":\"all\"", "\"rank_pattern\":{\"count_head\":4}", "\"unknown_behavior\":null", "\"modules_to_save\":[\"classifier\"]" }) |field| {
        const json = try std.fmt.allocPrint(scratch, "{{\"peft_type\":\"LORA\",\"r\":2,\"lora_alpha\":3,\"target_modules\":[\"boundary_head.count_head\"],{s}}}", .{field});
        try std.testing.expectError(error.UnsupportedBoundaryAdapterConfig, parseConfig(scratch, .small, json, .{}));
    }
    try std.testing.expectError(error.InvalidBoundaryAdapterTarget, parseConfig(scratch, .small, "{\"peft_type\":\"LORA\",\"r\":2,\"lora_alpha\":3,\"target_modules\":[\"encoder.embeddings.word_embeddings\"]}", .{}));
    const config = try parseConfig(scratch, .small, test_config, .{});
    var left: [768]f32 = undefined;
    testFill(&left, 0.11, 0.03);
    var tensors = testTensors(&left);
    _ = try validateTensors(scratch, config, &tensors, .{}, null);
    tensors[1].name = test_a_key;
    try std.testing.expectError(error.DuplicateBoundaryAdapterTensor, validateTensors(scratch, config, &tensors, .{}, null));
    tensors = testTensors(&left);
    left[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteBoundaryAdapterTensor, validateTensors(scratch, config, &tensors, .{}, null));
    left[0] = 0.1;
    var temp = std.testing.tmpDir(.{ .iterate = true });
    defer temp.cleanup();
    const parent = try temp.dir.realPathFileAlloc(std.testing.io, ".", scratch);
    const output = try std.fs.path.join(scratch, &.{ parent, "cancelled" });
    const Cancel = struct {
        count: usize = 0,
        fn call(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.count += 1;
            if (self.count == 8) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, exportDirectory(a, std.testing.io, output, test_config, &tensors, testBinding(.small), .{}, .{ .ptr = &cancel, .check_fn = Cancel.call }));
    try std.testing.expectEqual(@as(usize, 8), cancel.count);
    var iterator = temp.dir.iterate();
    try std.testing.expect((try iterator.next(std.testing.io)) == null);
    const weight_path = try std.fs.path.join(scratch, &.{ parent, "tiny.safetensors" });
    try checkpoint.save(a, weight_path, &tensors);
    const bytes = try c_file.readFileMax(scratch, weight_path, 32 * 1024);
    try std.testing.checkAllAllocationFailures(a, importAllocationFailures, .{ test_config, bytes });
    var loaded = try importBytes(a, test_config, bytes, null, testBinding(.small), .{}, null);
    defer loaded.deinit();
    const modules = @constCast(loaded.modules);
    modules[0].scale = 999;
    const rebuilt = try validateLoaded(scratch, &loaded, .{}, null);
    try std.testing.expectEqual(@as(f32, 1.5), rebuilt.modules[0].scale);
    const changed = @constCast(loaded.tensors[0].data);
    const prior = changed[0];
    changed[0] += 0.125;
    try std.testing.expectError(error.BoundaryAdapterArtifactMismatch, validateLoaded(scratch, &loaded, .{}, null));
    changed[0] = prior;
    loaded.receipt.target_sha256[0] ^= 1;
    try std.testing.expectError(error.BoundaryAdapterArtifactMismatch, validateLoaded(scratch, &loaded, .{}, null));
}

test "GLiNER2.5 adapter full merge rejects incomplete canonical model before publication" {
    const a = std.testing.allocator;
    const fixture = @import("../architectures/gliner_boundary_parity_test.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const parent = try temp.dir.realPathFileAlloc(std.testing.io, ".", scratch);
    const source = try std.fs.path.join(scratch, &.{ parent, "source" });
    const output = try std.fs.path.join(scratch, &.{ parent, "merged" });
    const sidecars = [_][]const u8{
        try fixture.fixtureBytes(scratch, "models/small/config.json"),
        try fixture.fixtureBytes(scratch, "models/small/encoder_config.json"),
        "{}",
        "{}",
    };
    var binding = testBinding(.small);
    for (bundle.sidecar_names, sidecars, &binding.source.sidecars) |name, bytes, *digest| {
        try writeFile(a, std.testing.io, source, name, bytes, null);
        digest.* = bundle.Digest.of(bytes);
    }
    const model_path = try std.fs.path.join(scratch, &.{ source, "model.safetensors" });
    try checkpoint.save(a, model_path, &.{.{ .name = "boundary_head.count_head.weight", .shape = &.{ 1, 384 }, .data = &([_]f32{1} ** 384) }});
    const model_bytes = try c_file.readFileMax(scratch, model_path, 32 * 1024);
    binding.source.weight = bundle.Digest.of(model_bytes);
    std.crypto.hash.sha2.Sha256.hash(model_bytes, &binding.frozen_weight_sha256, .{});
    const tensors = testTensors(&([_]f32{0.1} ** 768));
    const tensor_path = try std.fs.path.join(scratch, &.{ parent, "adapter.safetensors" });
    try checkpoint.save(a, tensor_path, &tensors);
    const tensor_bytes = try c_file.readFileMax(scratch, tensor_path, 32 * 1024);
    var loaded = try importBytes(a, test_config, tensor_bytes, null, binding, .{}, null);
    defer loaded.deinit();
    try std.testing.expectError(error.IncompleteGlinerBoundaryTensorInventory, materializeMerged(a, std.testing.io, source, &loaded, output, .{}, null));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, output, .{}));
}
