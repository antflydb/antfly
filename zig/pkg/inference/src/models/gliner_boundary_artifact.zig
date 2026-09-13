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

pub fn specs(backbone: model.Backbone) []const inventory.Spec {
    return switch (backbone) {
        .base => &inventory.base,
        .small => &inventory.small,
        .multi => &inventory.multi,
    };
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
    if ((backbone == .small and precision == .q4_k) or (backbone != .small and precision == .q4_0))
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

pub fn validate(allocator: std.mem.Allocator, backbone: model.Backbone, precision: Precision, descriptors: []const access.Descriptor, control: ?Control) !Summary {
    if (control) |c| try c.check();
    try validatePrecision(backbone, precision);
    const expected = specs(backbone);
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
