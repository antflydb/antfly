// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual Transformers 4.55.4 projected-attention forward and five-leaf VJPs.
//! The 40 MiB fixture stays external and is admitted by exact byte identities.
//! No Python, checkpoint, source formula replica, or Torch RNG executes here.
const std = @import("std");
const attention = @import("deberta_training_attention.zig");
const parity = @import("../architectures/gliner_boundary_parity_test.zig");
const safetensors = @import("../models/safetensors.zig");
const snapshot = @import("../runtime/file_snapshot.zig");
const bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const tensor = @import("../backends/tensor.zig");
const ops = @import("ops.zig");
const resident = @import("../graph/resident_training_fixture.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Allocator = std.mem.Allocator;

// Fixed before the first native/source comparison. These are the direct CPU
// primitive's scalar-reference gates; finite-difference tolerances are separate.
const absolute: f32 = 2e-5;
const relative: f32 = 3e-5;
const Pin = struct { size_bytes: usize, sha256: []const u8 };
const capture_pin = Pin{ .size_bytes = 29667, .sha256 = "6a536daec41bfd515d7e3fe5ae2522a4fdd2e67c15880071b732783f8cb59dc1" };
const tensors_pin = Pin{ .size_bytes = 40241300, .sha256 = "45c7b042146132e9103807f61e49b7a44a7ef59e70c338e542e9de709d38b9e3" };
const weights_pin = Pin{ .size_bytes = 20560, .sha256 = "715aa526fb9cc711352b2b57531d8648e76cba29972c1c04ecad720044d4fa54" };
const generator_pin = Pin{ .size_bytes = 20879, .sha256 = "c67ee517899871b4ad36a11408ff321028ad10ab88f0941aa0c09ef48f6a1a6f" };
const contract_pin = Pin{ .size_bytes = 12057, .sha256 = "7e512fe4418d006619088093ed7410ddd9a209dcb421954c9fbb14287bbecfcc" };
const source_pin = Pin{ .size_bytes = 56783, .sha256 = "98fa398f62e446e1f6303ff67fa7aceddac4f746a1a6013226896c3fa4e6cdd6" };

const Names = struct {
    control: []const u8,
    qkv: []const u8,
    relative: []const u8,
    context: []const u8,
    cotangent: []const u8,
    gradient: []const u8,
    gradient_q: []const u8,
    gradient_k: []const u8,
    gradient_v: []const u8,
    gradient_qr: []const u8,
    gradient_kr: []const u8,
    probabilities_before_dropout: []const u8,
    probabilities_after_dropout: []const u8,
    probability_mask: []const u8,
    relative_normalized: []const u8,
    relative_dropout: []const u8,
    relative_mask: []const u8,
};
const Case = struct {
    id: []const u8,
    batch: u32,
    sequence: u32,
    heads: u32,
    head_dim: u32,
    hidden_size: u32,
    relative_rows: u32,
    probability: f32,
    probability_stream_id: u64,
    relative_stream_id: u64,
    logical_layer: u32,
    seed: u64,
    micro_batch: u64,
    replica: u64,
    lengths: []u32,
    control_elements: usize,
    cotangent: []const u8,
    gradient_leaf_order: [][]const u8,
    original_vs_leaf_replay_exact: bool,
    uniform_fully_masked_queries: bool,
    tensors: Names,

    fn attrs(self: Case) attention.Attrs {
        return .{ .batch = self.batch, .seq_len = self.sequence, .num_heads = self.heads, .head_dim = self.head_dim, .relative_rows = self.relative_rows, .dropout_probability = self.probability, .dropout_stream_id = self.probability_stream_id };
    }
};
const Manifest = struct {
    version: u32,
    scope: []const u8,
    qualification: bool,
    generator: Pin,
    contract: Pin,
    transformers_source: Pin,
    files: struct { @"weights.safetensors": Pin, @"tensors.safetensors": Pin },
    provenance: struct { commit: []const u8, device: []const u8, dtype: []const u8, runtime: struct { python: []const u8, unicode: []const u8, packages: struct { torch: []const u8, transformers: []const u8, peft: []const u8 } } },
    cases: []Case,
};

fn expectPin(expected: Pin, actual: Pin) !void {
    try std.testing.expectEqual(expected.size_bytes, actual.size_bytes);
    try std.testing.expectEqualStrings(expected.sha256, actual.sha256);
}

fn readPinned(a: Allocator, directory: []const u8, name: []const u8, expected: Pin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const raw = try snapshot.read(a, std.testing.io, .cwd(), path, expected.size_bytes, null);
    errdefer a.free(raw);
    var hashed: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &hashed, .{});
    try expectPin(expected, .{ .size_bytes = raw.len, .sha256 = &std.fmt.bytesToHex(hashed, .lower) });
    return raw;
}

fn checkedTensor(reference: *const parity.TensorFixture, name: []const u8, dtype: tensor.DType, shape: []const i64) !tensor.Tensor {
    const value = try reference.tensor(name);
    try std.testing.expectEqual(dtype, value.dtype);
    try std.testing.expectEqualSlices(i64, shape, value.shape);
    var count: usize = 1;
    for (shape) |dim| count = try std.math.mul(usize, count, @intCast(dim));
    try std.testing.expectEqual(count * 4, value.data.len);
    return value;
}

fn floats(reference: *const parity.TensorFixture, name: []const u8, shape: []const i64) ![]const f32 {
    _ = try checkedTensor(reference, name, .f32, shape);
    const values = try reference.floats(name);
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteFixtureTensor;
    return values;
}

fn checkWeights(reference: *const parity.TensorFixture) !void {
    try std.testing.expectEqual(@as(usize, 19), reference.reader.header.tensors.count());
    for ([_][]const u8{ "LayerNorm.bias", "LayerNorm.weight", "layer.0.attention.output.LayerNorm.bias", "layer.0.attention.output.LayerNorm.weight", "layer.0.attention.output.dense.bias", "layer.0.attention.self.key_proj.bias", "layer.0.attention.self.query_proj.bias", "layer.0.attention.self.value_proj.bias", "layer.0.output.LayerNorm.bias", "layer.0.output.LayerNorm.weight", "layer.0.output.dense.bias" }) |name|
        _ = try floats(reference, name, &.{8});
    for ([_][]const u8{ "layer.0.attention.output.dense.weight", "layer.0.attention.self.key_proj.weight", "layer.0.attention.self.query_proj.weight", "layer.0.attention.self.value_proj.weight" }) |name|
        _ = try floats(reference, name, &.{ 8, 8 });
    _ = try floats(reference, "layer.0.intermediate.dense.bias", &.{16});
    _ = try floats(reference, "layer.0.intermediate.dense.weight", &.{ 16, 8 });
    _ = try floats(reference, "layer.0.output.dense.weight", &.{ 8, 16 });
    _ = try floats(reference, "rel_embeddings.weight", &.{ 512, 8 });
}

const Inputs = struct { qkv: []const f32, relative: []const f32, control: []align(1) const i32, dout: []const f32, output: []const f32, gradient: []const f32 };

fn checkCase(a: Allocator, reference: *const parity.TensorFixture, case: Case, index: usize) !Inputs {
    const sequence: u32 = if (index < 6) 7 else 512;
    const probabilities = [_]f32{ 0, 0.1, 0.125 };
    const layers = [_]u32{ 0, 2, 7 };
    const suffixes = [_][]const u8{ "p0", "p01", "p0125" };
    const expected_id = try std.fmt.allocPrint(a, "{s}_{s}", .{ ([_][]const u8{ "ragged7", "fully_masked7", "buckets512" })[index / 3], suffixes[index % 3] });
    defer a.free(expected_id);
    try std.testing.expectEqualStrings(expected_id, case.id);
    try std.testing.expectEqual(sequence, case.sequence);
    try std.testing.expectEqual(@as(u32, 2), case.batch);
    try std.testing.expectEqual(@as(u32, 2), case.heads);
    try std.testing.expectEqual(@as(u32, 4), case.head_dim);
    try std.testing.expectEqual(@as(u32, 8), case.hidden_size);
    try std.testing.expectEqual(@as(u32, 512), case.relative_rows);
    try std.testing.expectEqual(probabilities[index % 3], case.probability);
    try std.testing.expectEqual(layers[index % 3], case.logical_layer);
    try std.testing.expectEqual((@as(u64, case.logical_layer) << 32) | 3, case.probability_stream_id);
    try std.testing.expectEqual((@as(u64, case.logical_layer) << 32) | 2, case.relative_stream_id);
    try std.testing.expect(case.original_vs_leaf_replay_exact and case.uniform_fully_masked_queries);
    try std.testing.expectEqual(@as(u64, 0xfedcba9876543210), case.seed);
    try std.testing.expectEqual(@as(u64, 0x100000002), case.micro_batch);
    try std.testing.expectEqual(@as(u64, 0x8000000000000003), case.replica);
    const second: u32 = if (index < 3) 4 else if (index < 6) 0 else 259;
    try std.testing.expectEqualSlices(u32, &.{ sequence, second }, case.lengths);
    try std.testing.expectEqualStrings(if (index >= 3 and index < 6) "fully_masked_sample_only" else "all_rows", case.cotangent);
    try std.testing.expectEqual(@as(usize, 5), case.gradient_leaf_order.len);
    for ([_][]const u8{ "q", "k", "v", "qr", "kr" }, case.gradient_leaf_order) |expected, actual|
        try std.testing.expectEqualStrings(expected, actual);
    inline for (std.meta.fields(Names)) |field| {
        const expected = try std.fmt.allocPrint(a, "{s}.{s}", .{ case.id, field.name });
        defer a.free(expected);
        try std.testing.expectEqualStrings(expected, @field(case.tensors, field.name));
    }
    const bs: i64 = @as(i64, case.batch) * sequence;
    const h: i64 = case.hidden_size;
    const r: i64 = case.relative_rows;
    const control_elements: usize = @intCast(6 + bs + 2 * sequence - 1);
    try std.testing.expectEqual(control_elements, case.control_elements);
    const control_tensor = try checkedTensor(reference, case.tensors.control, .i32, &.{@intCast(control_elements)});
    const words = std.mem.bytesAsSlice(i32, control_tensor.data);
    const view = try attention.validateControl(case.attrs(), words, .{});
    try std.testing.expectEqual(attention.Replay{ .seed = case.seed, .micro_batch = case.micro_batch, .replica = case.replica }, view.replay);
    // Check high limbs as physical bits; a float-cast metadata path cannot pass.
    try std.testing.expect(words[1] < 0 and words[5] < 0 and words[3] == 1);
    for (view.token_valid, 0..) |valid, i| try std.testing.expectEqual(@as(i32, @intFromBool(i % sequence < case.lengths[i / sequence])), valid);
    const probability_mask = try floats(reference, case.tensors.probability_mask, &.{ case.batch, case.heads, sequence, sequence });
    for (probability_mask, 0..) |expected, i| try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(view.dropout.value(i))));
    _ = try floats(reference, case.tensors.probabilities_before_dropout, &.{ case.batch, case.heads, sequence, sequence });
    _ = try floats(reference, case.tensors.probabilities_after_dropout, &.{ case.batch, case.heads, sequence, sequence });
    inline for (.{ "relative_normalized", "relative_dropout", "relative_mask" }) |field|
        _ = try floats(reference, @field(case.tensors, field), &.{ r, h });
    const packed_gradient = try floats(reference, case.tensors.gradient, &.{ 3 * bs + 2 * r, h });
    var offset: usize = 0;
    inline for (.{ "gradient_q", "gradient_k", "gradient_v", "gradient_qr", "gradient_kr" }, 0..) |field, component| {
        const count = if (component < 3) bs else r;
        const values = try floats(reference, @field(case.tensors, field), &.{ count, h });
        try std.testing.expectEqualSlices(f32, values, packed_gradient[offset..][0..values.len]);
        offset += values.len;
    }
    try std.testing.expectEqual(packed_gradient.len, offset);
    return .{ .qkv = try floats(reference, case.tensors.qkv, &.{ 3 * bs, h }), .relative = try floats(reference, case.tensors.relative, &.{ 2 * r, h }), .control = words, .dout = try floats(reference, case.tensors.cotangent, &.{ bs, h }), .output = try floats(reference, case.tensors.context, &.{ bs, h }), .gradient = packed_gradient };
}

fn compare(case: Case, expected: []const f32, actual: []const f32, kind: []const u8) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual, 0..) |want, got, i| {
        if (!std.math.isFinite(got) or @abs(want - got) > absolute + relative * @abs(want)) {
            std.debug.print("source attention {s}/{s}[{d}]: expected={d} actual={d} tolerance={d}\n", .{ case.id, kind, i, want, got, absolute + relative * @abs(want) });
            return error.TestExpectedApproxEqAbs;
        }
    }
}

fn compareGradients(case: Case, expected: []const f32, actual: []const f32) !void {
    const token_values: usize = @as(usize, case.batch) * case.sequence * case.hidden_size;
    const relative_values: usize = @as(usize, case.relative_rows) * case.hidden_size;
    try std.testing.expectEqual(3 * token_values + 2 * relative_values, actual.len);
    var offset: usize = 0;
    for ([_][]const u8{ "q", "k", "v", "qr", "kr" }, 0..) |name, component| {
        const count = if (component < 3) token_values else relative_values;
        try compare(case, expected[offset..][0..count], actual[offset..][0..count], name);
        if (std.mem.eql(u8, case.cotangent, "fully_masked_sample_only") and component != 2)
            for (actual[offset..][0..count]) |value| try std.testing.expectEqual(@as(f32, 0), value);
        offset += count;
    }
    if (std.mem.eql(u8, case.cotangent, "fully_masked_sample_only")) {
        const first = actual[2 * token_values ..][0 .. token_values / 2];
        for (first) |value| try std.testing.expectEqual(@as(f32, 0), value);
        var nonzero = false;
        for (actual[2 * token_values + token_values / 2 ..][0 .. token_values / 2]) |value| nonzero = nonzero or value != 0;
        try std.testing.expect(nonzero);
    }
}

fn upload(cb: *const ops.ComputeBackend, values: []const f32, shape: []const i32) !ops.CT {
    return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = shape } }, .{});
}

fn deviceCase(a: Allocator, device: *resident.Device, case: Case, input: Inputs) !void {
    const cb = device.backend.computeBackend();
    try std.testing.expect(cb.kind() == .metal);
    const bs: i32 = @intCast(case.batch * case.sequence);
    const h: i32 = @intCast(case.hidden_size);
    const r: i32 = @intCast(case.relative_rows);
    const qkv = try upload(&cb, input.qkv, &.{ 3 * bs, h });
    defer cb.free(qkv);
    const projected = try upload(&cb, input.relative, &.{ 2 * r, h });
    defer cb.free(projected);
    const dout = try upload(&cb, input.dout, &.{ bs, h });
    defer cb.free(dout);
    const controls = try a.alloc(i32, input.control.len);
    defer a.free(controls);
    @memcpy(controls, input.control);
    const control = try cb.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = controls, .shape = &.{@intCast(controls.len)} } }, .{});
    defer cb.free(control);
    try std.testing.expectEqual(tensor.DType.i32, try cb.tensorDType(control));
    const before = metal_tensor.memoryStatsSnapshot();
    const output = try cb.debertaTrainingAttentionV1(qkv, projected, control, case.attrs());
    defer cb.free(output);
    const gradient = try cb.debertaTrainingAttentionBackwardV1(qkv, projected, control, dout, case.attrs());
    defer cb.free(gradient);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    // Only final bounded diagnostic tensors are downloaded, after both strict
    // dedicated operations complete. No generic fallback is accepted.
    const out_shape = try cb.tensorShape(output, a);
    defer a.free(out_shape);
    const grad_shape = try cb.tensorShape(gradient, a);
    defer a.free(grad_shape);
    try std.testing.expectEqualSlices(i64, &.{ bs, h }, out_shape);
    try std.testing.expectEqualSlices(i64, &.{ 3 * bs + 2 * r, h }, grad_shape);
    try std.testing.expectEqual(tensor.DType.f32, try cb.tensorDType(output));
    try std.testing.expectEqual(tensor.DType.f32, try cb.tensorDType(gradient));
    const output_values = try a.alloc(f32, input.output.len);
    defer a.free(output_values);
    const gradient_values = try a.alloc(f32, input.gradient.len);
    defer a.free(gradient_values);
    try cb.glinerBoundaryDownload(output, output_values);
    try cb.glinerBoundaryDownload(gradient, gradient_values);
    try compare(case, input.output, output_values, "context");
    try compareGradients(case, input.gradient, gradient_values);
}

fn exercise(comptime on_device: bool) !void {
    if (comptime on_device) {
        if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
        if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    }
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_TRAINING_ATTENTION_FIXTURE_DIR") orelse return error.SkipZigTest;
    var owner = bounded{ .backing = std.testing.allocator, .limit = 64 * 1024 * 1024 };
    const a = owner.allocator();
    const metadata = try readPinned(a, directory, "capture.json", capture_pin);
    defer a.free(metadata);
    const parsed = try std.json.parseFromSlice(Manifest, a, metadata, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const manifest = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), manifest.version);
    try std.testing.expectEqualStrings("gliner25_projected_training_attention/v1", manifest.scope);
    try std.testing.expect(!manifest.qualification);
    try expectPin(generator_pin, manifest.generator);
    try expectPin(contract_pin, manifest.contract);
    try expectPin(source_pin, manifest.transformers_source);
    try expectPin(tensors_pin, manifest.files.@"tensors.safetensors");
    try expectPin(weights_pin, manifest.files.@"weights.safetensors");
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", manifest.provenance.commit);
    try std.testing.expectEqualStrings("cpu", manifest.provenance.device);
    try std.testing.expectEqualStrings("float32", manifest.provenance.dtype);
    try std.testing.expectEqualStrings("3.12.3", manifest.provenance.runtime.python);
    try std.testing.expectEqualStrings("15.0.0", manifest.provenance.runtime.unicode);
    try std.testing.expectEqualStrings("2.9.1", manifest.provenance.runtime.packages.torch);
    try std.testing.expectEqualStrings("4.55.4", manifest.provenance.runtime.packages.transformers);
    try std.testing.expectEqualStrings("0.17.1", manifest.provenance.runtime.packages.peft);
    try std.testing.expectEqual(@as(usize, 9), manifest.cases.len);
    const weight_bytes = try readPinned(a, directory, "weights.safetensors", weights_pin);
    defer a.free(weight_bytes);
    var weights = parity.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.fromBorrowedBytesLimited(a, weight_bytes, 4096) };
    defer weights.deinit();
    try checkWeights(&weights);
    const tensor_bytes = try readPinned(a, directory, "tensors.safetensors", tensors_pin);
    defer a.free(tensor_bytes);
    var reference = parity.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.fromBorrowedBytesLimited(a, tensor_bytes, 32 * 1024) };
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 9 * 17), reference.reader.header.tensors.count());
    var device: if (on_device) resident.Device else void = if (on_device) try resident.Device.init(a) else {};
    defer if (comptime on_device) device.deinit();
    for (manifest.cases, 0..) |case, index| {
        errdefer std.debug.print("pinned projected attention case {s} device={}\n", .{ case.id, on_device });
        const input = try checkCase(a, &reference, case, index);
        if (comptime on_device) {
            try deviceCase(a, &device, case, input);
        } else {
            const output = try attention.forward(a, case.attrs(), input.qkv, input.relative, input.control, .{});
            defer a.free(output);
            const gradient = try attention.backward(a, case.attrs(), input.qkv, input.relative, input.control, input.dout, .{});
            defer a.free(gradient);
            try compare(case, input.output, output, "context");
            try compareGradients(case, input.gradient, gradient);
        }
    }
    try std.testing.expect(owner.peak <= owner.limit and !owner.denied);
}

test "replay DeBERTa training attention pinned source CPU nine cases forward and five VJPs" {
    try exercise(false);
}

test "replay DeBERTa training attention pinned source Metal nine cases forward and five VJPs" {
    try exercise(true);
}
