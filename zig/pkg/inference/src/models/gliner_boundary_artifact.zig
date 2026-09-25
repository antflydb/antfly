// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact published tensor inventories and a versioned precision policy.
//! This validates tensor metadata, not contents or quality. A serving loader
//! must also authenticate every artifact's bytes and qualified configuration.
//! Names are original checkpoint names; conversion must preserve a complete
//! reversible name map rather than silently dropping unrecognized tensors.
const std = @import("std");
const model = @import("gliner_boundary.zig");
const inventory = @import("gliner_boundary_tensor_inventory.zig");
const access = @import("tensor_access.zig");
const gguf = @import("../gguf/tensor_types.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const policy_version: u32 = 1;
pub const Precision = enum { fp32, fp16_encoder, q8_0, q4_k, q4_0 };
pub const Role = enum { encoder_matrix, protected_encoder, extraction_head };
pub const Summary = struct { tensors: usize, parameters: u64, stored_bytes: u64 };

/// Static published inventories. A ModernBERT inventory depends on its
/// config (`modernBertSpecs`), so it has none here.
pub fn specs(backbone: model.Backbone) []const inventory.Spec {
    return switch (backbone) {
        .base => &inventory.base,
        .small => &inventory.small,
        .multi => &inventory.multi,
        .modern_bert => &.{},
    };
}

/// A ModernBERT checkpoint's exact inventory, sorted by name: the encoder's
/// Hugging Face tensors derived from its config, and the published boundary
/// heads re-dimensioned for its width. Each head dimension is `k * hidden + c`
/// with `k` and `c` solved from the base (768) and small (384) inventories,
/// which share one head configuration; a non-integer fit is rejected.
/// Registration order follows `named_parameters`: encoder, then heads.
pub const Derived = struct {
    arena: std.heap.ArenaAllocator,
    specs: []const inventory.Spec,

    pub fn deinit(self: *Derived) void {
        self.arena.deinit();
    }
};

pub fn modernBertSpecs(allocator: std.mem.Allocator, encoder: model.EncoderConfig) !Derived {
    if (encoder.family != .modern_bert or encoder.num_hidden_layers == 0 or encoder.hidden_size == 0) return error.UnsupportedGlinerBoundaryEncoder;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const h: i64 = encoder.hidden_size;
    var list = std.ArrayListUnmanaged(inventory.Spec).empty;
    var order: u16 = 0;
    const Add = struct {
        fn add(al: std.mem.Allocator, out: *std.ArrayListUnmanaged(inventory.Spec), next: *u16, name: []const u8, shape: []const i64) !void {
            try out.append(al, .{ .name = name, .shape = try al.dupe(i64, shape), .registration_order = next.* });
            next.* += 1;
        }
    };
    try Add.add(a, &list, &order, "encoder.embeddings.tok_embeddings.weight", &.{ encoder.vocab_size, h });
    try Add.add(a, &list, &order, "encoder.embeddings.norm.weight", &.{h});
    for (0..encoder.num_hidden_layers) |layer| {
        if (layer != 0) try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.attn_norm.weight", .{layer}), &.{h});
        try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.attn.Wqkv.weight", .{layer}), &.{ 3 * h, h });
        try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.attn.Wo.weight", .{layer}), &.{ h, h });
        try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.mlp_norm.weight", .{layer}), &.{h});
        try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.mlp.Wi.weight", .{layer}), &.{ 2 * @as(i64, encoder.intermediate_size), h });
        try Add.add(a, &list, &order, try std.fmt.allocPrint(a, "encoder.layers.{d}.mlp.Wo.weight", .{layer}), &.{ h, encoder.intermediate_size });
    }
    try Add.add(a, &list, &order, "encoder.final_norm.weight", &.{h});
    const encoder_count = order;
    const published_heads: u16 = 198; // first head registration ordinal in every published inventory
    for (inventory.base) |spec| {
        if (std.mem.startsWith(u8, spec.name, "encoder.")) continue;
        const small = inventory.small[find(&inventory.small, spec.name) orelse return error.InvalidGlinerBoundaryTensorShape];
        if (small.shape.len != spec.shape.len or small.registration_order != spec.registration_order or spec.registration_order < published_heads)
            return error.InvalidGlinerBoundaryTensorShape;
        const shape = try a.alloc(i64, spec.shape.len);
        for (shape, spec.shape, small.shape) |*out, base_dim, small_dim| {
            const scale = std.math.divExact(i64, base_dim - small_dim, 768 - 384) catch return error.InvalidGlinerBoundaryTensorShape;
            const offset = base_dim - scale * 768;
            if (scale < 0 or offset < 0) return error.InvalidGlinerBoundaryTensorShape;
            out.* = scale * h + offset;
        }
        try list.append(a, .{ .name = spec.name, .shape = shape, .registration_order = encoder_count + (spec.registration_order - published_heads) });
    }
    std.mem.sort(inventory.Spec, list.items, {}, struct {
        fn less(_: void, lhs: inventory.Spec, rhs: inventory.Spec) bool {
            return std.mem.order(u8, lhs.name, rhs.name) == .lt;
        }
    }.less);
    return .{ .arena = arena, .specs = list.items };
}

pub fn role(spec: inventory.Spec) Role {
    if (!std.mem.startsWith(u8, spec.name, "encoder.")) return .extraction_head;
    if (spec.shape.len == 2 and (std.mem.eql(u8, spec.name, "encoder.embeddings.word_embeddings.weight") or
        (std.mem.startsWith(u8, spec.name, "encoder.encoder.layer.") and std.mem.endsWith(u8, spec.name, ".weight"))))
        return .encoder_matrix;
    // Biases, normalization, and learned relative-position tables stay FP32.
    return .protected_encoder;
}

pub fn validatePrecision(backbone: model.Backbone, precision: Precision) !void {
    // ModernBERT checkpoints are FP32 training sources only.
    if ((backbone == .modern_bert and precision != .fp32) or
        (backbone == .small and precision == .q4_k) or (backbone != .small and precision == .q4_0))
        return error.UnsupportedGlinerBoundaryPrecision;
}

/// Initial mixed profiles quantize/cast only declared encoder matrices.
/// Every task head and protected encoder tensor remains FP32, including
/// matrices that happen to satisfy generic quantization size heuristics.
pub fn targetType(backbone: model.Backbone, precision: Precision, spec: inventory.Spec) !gguf.KnownTensorType {
    try validatePrecision(backbone, precision);
    if (role(spec) != .encoder_matrix) return .F32;
    const result: gguf.KnownTensorType = switch (precision) {
        .fp32 => .F32,
        .fp16_encoder => .F16,
        .q8_0 => .Q8_0,
        .q4_k => .Q4_K,
        .q4_0 => .Q4_0,
    };
    if (result == .F32 or result == .F16) return result;
    const block = gguf.valuesPerBlock(.{ .known = result }) orelse return error.UnsupportedGlinerBoundaryPrecision;
    if (spec.shape[spec.shape.len - 1] <= 0 or @mod(spec.shape[spec.shape.len - 1], block) != 0)
        return error.InvalidGlinerBoundaryQuantizationShape;
    return result;
}

fn typeOf(encoding: access.Encoding) !gguf.KnownTensorType {
    return switch (encoding) {
        .dense => |dtype| switch (dtype) {
            .f32 => .F32,
            .f16 => .F16,
            else => error.UnsupportedGlinerBoundaryPrecision,
        },
        .gguf => |kind| switch (kind) {
            .known => |known| known,
            else => error.UnsupportedGlinerBoundaryPrecision,
        },
    };
}

fn find(expected: []const inventory.Spec, name: []const u8) ?usize {
    var lo: usize = 0;
    var hi = expected.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (std.mem.order(u8, expected[mid].name, name) == .lt) lo = mid + 1 else hi = mid;
    }
    return if (lo < expected.len and std.mem.eql(u8, expected[lo].name, name)) lo else null;
}

pub fn tensorBytes(spec: inventory.Spec, kind: gguf.KnownTensorType) !u64 {
    if (spec.shape.len > 4) return error.InvalidGlinerBoundaryTensorShape;
    var dimensions: [4]u64 = undefined;
    for (spec.shape, 0..) |dimension, i| {
        if (dimension <= 0) return error.InvalidGlinerBoundaryTensorShape;
        dimensions[spec.shape.len - 1 - i] = @intCast(dimension);
    }
    return gguf.byteLen(.{ .known = kind }, dimensions[0..spec.shape.len]) orelse error.InvalidGlinerBoundaryTensorShape;
}

/// Validates against a static published inventory. ModernBERT checkpoints
/// have no static inventory; use `validateDerived`.
pub fn validate(allocator: std.mem.Allocator, backbone: model.Backbone, precision: Precision, descriptors: []const access.Descriptor, control: ?Control) !Summary {
    if (backbone == .modern_bert) return error.UnsupportedGlinerBoundaryEncoder;
    return validateAgainst(allocator, backbone, specs(backbone), precision, descriptors, control);
}

/// Validates a ModernBERT FP32 checkpoint against its config-derived inventory.
pub fn validateDerived(allocator: std.mem.Allocator, encoder: model.EncoderConfig, descriptors: []const access.Descriptor, control: ?Control) !Summary {
    var derived = try modernBertSpecs(allocator, encoder);
    defer derived.deinit();
    return validateAgainst(allocator, .modern_bert, derived.specs, .fp32, descriptors, control);
}

fn validateAgainst(allocator: std.mem.Allocator, backbone: model.Backbone, expected: []const inventory.Spec, precision: Precision, descriptors: []const access.Descriptor, control: ?Control) !Summary {
    if (control) |c| try c.check();
    try validatePrecision(backbone, precision);
    if (descriptors.len != expected.len) return error.IncompleteGlinerBoundaryTensorInventory;
    const seen = try allocator.alloc(bool, expected.len);
    defer allocator.free(seen);
    @memset(seen, false);
    var summary = Summary{ .tensors = descriptors.len, .parameters = 0, .stored_bytes = 0 };
    for (descriptors) |descriptor| {
        if (control) |c| try c.check();
        const index = find(expected, descriptor.name) orelse return error.UnexpectedGlinerBoundaryTensor;
        if (seen[index]) return error.DuplicateGlinerBoundaryTensor;
        seen[index] = true;
        const spec = expected[index];
        if (!std.mem.eql(i64, descriptor.shape, spec.shape)) return error.InvalidGlinerBoundaryTensorShape;
        const target = try targetType(backbone, precision, spec);
        if (try typeOf(descriptor.encoding) != target) return error.InvalidGlinerBoundaryTensorPrecision;
        const quantized = target != .F32 and target != .F16;
        if (descriptor.quantized != quantized) return error.InvalidGlinerBoundaryTensorPrecision;
        const bytes = try tensorBytes(spec, target);
        if (descriptor.byte_len != bytes) return error.InvalidGlinerBoundaryTensorByteLength;
        var parameters: u64 = 1;
        for (spec.shape) |dimension| parameters = try std.math.mul(u64, parameters, @intCast(dimension));
        summary.parameters = try std.math.add(u64, summary.parameters, parameters);
        summary.stored_bytes = try std.math.add(u64, summary.stored_bytes, bytes);
    }
    return summary;
}

test "gliner boundary artifact profiles account for every tensor and protect all task heads" {
    const a = std.testing.allocator;
    for ([_]model.Backbone{ .base, .small, .multi }) |backbone| {
        const expected = specs(backbone);
        try std.testing.expectEqual(@as(usize, 334), expected.len);
        const descriptors = try a.alloc(access.Descriptor, expected.len);
        defer a.free(descriptors);
        for ([_]Precision{ .fp32, .fp16_encoder, .q8_0, if (backbone == .small) .q4_0 else .q4_k }) |precision| {
            var quantized_count: usize = 0;
            for (expected, descriptors) |spec, *descriptor| {
                const target = try targetType(backbone, precision, spec);
                if (role(spec) != .encoder_matrix) try std.testing.expectEqual(gguf.KnownTensorType.F32, target);
                if (target != .F32 and target != .F16) quantized_count += 1;
                descriptor.* = .{ .name = spec.name, .shape = spec.shape, .encoding = .{ .gguf = .{ .known = target } }, .byte_len = @intCast(try tensorBytes(spec, target)), .quantized = target != .F32 and target != .F16 };
            }
            const summary = try validate(a, backbone, precision, descriptors, null);
            try std.testing.expectEqual(@as(usize, 334), summary.tensors);
            try std.testing.expect(summary.parameters > 70_000_000);
            if (precision == .q8_0 or precision == .q4_0 or precision == .q4_k) try std.testing.expect(quantized_count > 70);
        }
    }
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, validatePrecision(.small, .q4_k));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, validatePrecision(.base, .q4_0));
}

test "gliner boundary artifact rejects missing duplicate reshaped and silently quantized heads" {
    const a = std.testing.allocator;
    const expected = specs(.small);
    const descriptors = try a.alloc(access.Descriptor, expected.len);
    defer a.free(descriptors);
    for (expected, descriptors) |spec, *descriptor| descriptor.* = .{ .name = spec.name, .shape = spec.shape, .encoding = .{ .dense = .f32 }, .byte_len = @intCast(try tensorBytes(spec, .F32)), .quantized = false };
    try std.testing.expectError(error.IncompleteGlinerBoundaryTensorInventory, validate(a, .small, .fp32, descriptors[1..], null));
    const first = descriptors[0];
    descriptors[0] = descriptors[1];
    try std.testing.expectError(error.DuplicateGlinerBoundaryTensor, validate(a, .small, .fp32, descriptors, null));
    descriptors[0] = first;
    descriptors[0].name = "span_rep.weight";
    try std.testing.expectError(error.UnexpectedGlinerBoundaryTensor, validate(a, .small, .fp32, descriptors, null));
    descriptors[0] = first;
    descriptors[0].shape = &.{1};
    try std.testing.expectError(error.InvalidGlinerBoundaryTensorShape, validate(a, .small, .fp32, descriptors, null));
    descriptors[0] = first;
    descriptors[0].encoding = .{ .dense = .f16 };
    try std.testing.expectError(error.InvalidGlinerBoundaryTensorPrecision, validate(a, .small, .fp32, descriptors, null));
    descriptors[0] = first;
    descriptors[0].byte_len += 4;
    try std.testing.expectError(error.InvalidGlinerBoundaryTensorByteLength, validate(a, .small, .fp32, descriptors, null));
}

test "gliner boundary ModernBERT inventory reproduces the published heads at base width and validates exactly" {
    const a = std.testing.allocator;
    var encoder = std.mem.zeroes(model.EncoderConfig);
    encoder.family = .modern_bert;
    encoder.hidden_size = 768;
    encoder.intermediate_size = 1152;
    encoder.num_hidden_layers = 3;
    encoder.num_attention_heads = 12;
    encoder.vocab_size = 50368;
    var derived = try modernBertSpecs(a, encoder);
    defer derived.deinit();
    // 2 embedding tensors, 5 per layer plus attn_norm after layer 0, final norm; 136 heads.
    try std.testing.expectEqual(@as(usize, 2 + 3 * 6 - 1 + 1 + 136), derived.specs.len);
    for (derived.specs[1..], derived.specs[0 .. derived.specs.len - 1]) |spec, previous| try std.testing.expect(std.mem.order(u8, previous.name, spec.name) == .lt);
    // At the base width, every head equals the published base inventory.
    var heads: usize = 0;
    for (inventory.base) |spec| {
        if (std.mem.startsWith(u8, spec.name, "encoder.")) continue;
        const index = find(derived.specs, spec.name) orelse return error.MissingTestTensor;
        try std.testing.expectEqualSlices(i64, spec.shape, derived.specs[index].shape);
        heads += 1;
    }
    try std.testing.expectEqual(@as(usize, 136), heads);
    const rows = derived.specs[find(derived.specs, "encoder.layers.2.mlp.Wi.weight").?];
    try std.testing.expectEqualSlices(i64, &.{ 2304, 768 }, rows.shape);
    try std.testing.expect(find(derived.specs, "encoder.layers.0.attn_norm.weight") == null);

    const descriptors = try a.alloc(access.Descriptor, derived.specs.len);
    defer a.free(descriptors);
    for (derived.specs, descriptors) |spec, *descriptor| descriptor.* = .{ .name = spec.name, .shape = spec.shape, .encoding = .{ .dense = .f32 }, .byte_len = @intCast(try tensorBytes(spec, .F32)), .quantized = false };
    _ = try validateDerived(a, encoder, descriptors, null);
    try std.testing.expectError(error.IncompleteGlinerBoundaryTensorInventory, validateDerived(a, encoder, descriptors[1..], null));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryEncoder, validate(a, .modern_bert, .fp32, descriptors, null));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, validatePrecision(.modern_bert, .q8_0));
    try std.testing.expectEqual(@as(usize, 0), specs(.modern_bert).len);
}
