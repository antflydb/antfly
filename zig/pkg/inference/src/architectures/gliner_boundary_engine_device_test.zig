// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const engine = @import("gliner_boundary_engine_device.zig");
const cpu_engine = @import("gliner_boundary_engine.zig");
const device_head = @import("gliner_boundary_device.zig");
const device_math = @import("gliner_boundary_device_math.zig");
const parity = @import("gliner_boundary_parity_test.zig");
const head_tests = @import("gliner_boundary_device_test.zig");
const native = @import("../ops/native_compute.zig");
const metal = @import("../ops/metal_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");

test "gliner boundary device encoder admission and failed allocation cleanup" {
    const a = std.testing.allocator;
    var fixture = cpu_engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const config = cpu_engine.TestBatch.config();
    const plan = try engine.plan(&config, &prepared, .{});
    try std.testing.expectEqual(@max(plan.embedding_phase_upper_bound_bytes, plan.encoder_weight_bytes + plan.activation_upper_bound_bytes + plan.metadata_upper_bound_bytes + plan.late_weight_upload_scratch_bytes), plan.device_upper_bound_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, engine.plan(&config, &prepared, .{ .max_device_bytes = plan.device_upper_bound_bytes - 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, engine.plan(&config, &prepared, .{ .admission = .{ .max_sequence_tokens = 11 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, engine.plan(&config, &prepared, .{ .admission = .{ .max_attention_work_items = 1 } }));
    fixture.query_indices[0] = -1;
    try std.testing.expectError(error.InvalidBoundaryRouting, engine.plan(&config, &prepared, .{}));
    fixture.query_indices[0] = 1;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    var cb = backend.computeBackend();
    try std.testing.expectError(error.UnsupportedGlinerBoundaryDevice, engine.encodeDevice(&cb, a, &config, &prepared, .{}));
    const MissingDevice = struct {
        fn kind(_: *anyopaque) ops.BackendKind {
            return .metal;
        }
        fn execute(_: *anyopaque, _: *const ops.gliner_boundary_device.Request) anyerror!ops.CT {
            return error.UnsupportedGlinerBoundaryDevice;
        }
        fn check(allocator: std.mem.Allocator, backend_: *const ops.ComputeBackend, cfg: *const model.Config, batch: *const @import("../pipelines/gliner_boundary_processor.zig").PreparedBatch) !void {
            var result = engine.encodeDevice(backend_, allocator, cfg, batch, .{}) catch |err| switch (err) {
                error.UnsupportedGlinerBoundaryDevice => return,
                else => return err,
            };
            defer result.deinit();
            return error.ExpectedUnsupportedDevice;
        }
    };
    var vt = cb.vtable.*;
    vt.backendKind = MissingDevice.kind;
    vt.glinerBoundaryDevice = MissingDevice.execute;
    cb.vtable = &vt;
    try std.testing.checkAllAllocationFailures(a, MissingDevice.check, .{ &cb, &config, &prepared });
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, engine.encodeDevice(&cb, a, &config, &prepared, .{ .control = .{ .check_fn = Cancel.check } }));
}

test "gliner boundary optimized encoder separates immutable residency from remaining request admission" {
    var fixture = cpu_engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const config = cpu_engine.TestBatch.config();
    const remaining: usize = 1024 * 1024;
    const plan = try engine.plan(&config, &prepared, .{ .execution_policy = .optimized_v2, .max_device_bytes = remaining });
    try std.testing.expectEqual(remaining, plan.device_upper_bound_bytes);
    try std.testing.expectEqual(@as(usize, 0), plan.encoder_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), plan.max_weight_upload_scratch_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, engine.plan(&config, &prepared, .{ .execution_policy = .optimized_v2, .max_device_bytes = plan.embedding_phase_upper_bound_bytes - 1 }));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, engine.plan(&config, &prepared, .{ .execution_policy = .optimized_v2, .precision = .fp16_encoder }));
}

test "gliner boundary device reduced encoder plans include exact storage and slot biases" {
    var fixture = cpu_engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    var config = cpu_engine.TestBatch.config();
    config.backbone = .small;
    config.encoder.hidden_size = 384;
    config.encoder.num_attention_heads = 6;
    config.encoder.intermediate_size = 1536;
    const full = try engine.plan(&config, &prepared, .{});
    try std.testing.expectEqual(@as(usize, 0), full.encoder_slot_bias_bytes);
    var previous = full.encoder_weight_bytes;
    for ([_]@import("../models/gliner_boundary_artifact.zig").Precision{ .fp16_encoder, .q8_0, .q4_0 }) |precision| {
        const reduced = try engine.plan(&config, &prepared, .{ .precision = precision });
        try std.testing.expect(reduced.encoder_weight_bytes < previous);
        try std.testing.expectEqual(@as(usize, config.encoder.num_hidden_layers) * (5 * 384 + 1536) * 4, reduced.encoder_slot_bias_bytes);
        try std.testing.expectEqual(full.activation_upper_bound_bytes, reduced.activation_upper_bound_bytes);
        try std.testing.expectEqual(full.metadata_upper_bound_bytes, reduced.metadata_upper_bound_bytes);
        try std.testing.expect(reduced.max_weight_upload_scratch_bytes > 0);
        try std.testing.expectEqual(@max(reduced.embedding_phase_upper_bound_bytes, reduced.encoder_weight_bytes + reduced.encoder_slot_bias_bytes + reduced.activation_upper_bound_bytes + reduced.metadata_upper_bound_bytes + reduced.late_weight_upload_scratch_bytes), reduced.device_upper_bound_bytes);
        try std.testing.expectError(error.ResourceLimitExceeded, engine.plan(&config, &prepared, .{ .precision = precision, .max_device_bytes = reduced.device_upper_bound_bytes - 1 }));
        previous = reduced.encoder_weight_bytes;
    }
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, engine.plan(&config, &prepared, .{ .precision = .q4_k }));
}

fn patterned(a: std.mem.Allocator, count: usize, scale: f32, offset: usize) ![]f32 {
    const values = try a.alloc(f32, count);
    for (values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt((i * 7 + offset) % 1009)) * 0.07) * scale;
    return values;
}

fn upload(math: *device_math.Context, values: []const f32) !ops.CT {
    return math.execute(.{ .upload_f32 = .{ .values = values, .shape = &.{@intCast(values.len)} } }, values.len);
}

test "gliner boundary device Metal relative attention and physical integer gathers" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var store = gpu_store.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const tiled = @import("../ops/deberta_tiled_attention.zig");
    for ([_]tiled.Shape{ .{ .batch = 2, .sequence = 7, .heads = 3, .head_dim = 64 }, .{ .batch = 2, .sequence = 131, .heads = 2, .head_dim = 33 } }) |shape| {
        const math = try device_math.Context.create(a, &cb, .{}, null);
        defer math.destroy();
        const h = shape.heads * shape.head_dim;
        const n = shape.batch * shape.sequence * h;
        const q = try patterned(a, n, 0.5, 17);
        defer a.free(q);
        const k = try patterned(a, n, 0.7, 32);
        defer a.free(k);
        const v = try patterned(a, n, 1.1, 51);
        defer a.free(v);
        const qr = try patterned(a, 512 * h, 0.3, 29);
        defer a.free(qr);
        const kr = try patterned(a, 512 * h, 0.4, 77);
        defer a.free(kr);
        const indices = try a.alloc(i32, 2 * shape.sequence - 1);
        defer a.free(indices);
        const expanded_qr = try a.alloc(f32, indices.len * h);
        defer a.free(expanded_qr);
        const expanded_kr = try a.alloc(f32, indices.len * h);
        defer a.free(expanded_kr);
        for (indices, 0..) |*index, i| {
            index.* = @intCast(@import("../models/deberta.zig").relativePositionBucket(@as(i64, @intCast(i)) - @as(i64, @intCast(shape.sequence - 1)), 256, 512));
            const row: usize = @intCast(index.*);
            @memcpy(expanded_qr[i * h ..][0..h], qr[row * h ..][0..h]);
            @memcpy(expanded_kr[i * h ..][0..h], kr[row * h ..][0..h]);
        }
        const mask = try a.alloc(i32, shape.batch * shape.sequence);
        defer a.free(mask);
        const mask64 = try a.alloc(i64, mask.len);
        defer a.free(mask64);
        for (mask, mask64, 0..) |*m, *m64, i| {
            m.* = @intFromBool(i < shape.sequence or i % shape.sequence < shape.sequence / 2);
            m64.* = m.*;
        }
        const expected = try tiled.forward(a, shape, .{ .q = q, .k = k, .v = v, .qr = expanded_qr, .kr = expanded_kr, .mask = mask64 }, .{ .query_tile = 5, .key_tile = 17 });
        defer a.free(expected);
        // HF masks query and key positions. The CPU primitive admits key masks
        // for reuse by other encoders; zero the inactive query rows explicitly.
        for (mask, 0..) |m, row| if (m == 0) {
            @memset(expected[row * h ..][0..h], 0);
        };
        const q_ct = try upload(math, q);
        const k_ct = try upload(math, k);
        const v_ct = try upload(math, v);
        const qr_ct = try upload(math, qr);
        const kr_ct = try upload(math, kr);
        const ids = try math.uploadIntegers(indices);
        const mask_ct = try math.uploadIntegers(mask);
        const output = try math.kernel(.deberta_attention, &.{ shape.batch, shape.sequence, shape.heads, shape.head_dim, 512 }, &.{ q_ct, k_ct, v_ct, qr_ct, kr_ct, ids, mask_ct }, 0);
        try std.testing.expectEqual(@as(usize, 0), math.stats.result_download_calls);
        const actual = try math.download(output, n, false);
        defer a.free(actual);
        try parity.expectFloats(expected, actual, 3e-5, 3e-5);
        const gather_ids = try math.uploadIntegers(&.{ -1, 0, 2 });
        const gathered = try math.kernel(.gather_i32, &.{ shape.batch * shape.sequence, h, 3 }, &.{ q_ct, gather_ids }, 0);
        const gathered_values = try math.download(gathered, 3 * h, false);
        defer a.free(gathered_values);
        for (gathered_values[0..h]) |value| try std.testing.expectEqual(@as(f32, 0), value);
        try std.testing.expectEqualSlices(f32, q[0..h], gathered_values[h..][0..h]);
        try std.testing.expectEqualSlices(f32, q[2 * h ..][0..h], gathered_values[2 * h ..][0..h]);
        const bad_ids = try math.uploadIntegers(&.{@intCast(shape.batch * shape.sequence)});
        const invalid = try math.kernel(.gather_i32, &.{ shape.batch * shape.sequence, h, 1 }, &.{ q_ct, bad_ids }, 0);
        try std.testing.expectError(error.NonFiniteBoundaryScore, math.download(invalid, h, false));
    }
}

test "gliner boundary device Metal pinned small encoder routes and resident head parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(path);
    var weights = parity.TensorFixture{ .allocator = a, .reader = try @import("../models/safetensors.zig").MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(weights.reader.file_bytes, &digest, .{});
    try std.testing.expectEqualStrings("4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b", &std.fmt.bytesToHex(digest, .lower));
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    std.crypto.hash.sha2.Sha256.hash(tokenizer_bytes, &digest, .{});
    try std.testing.expectEqualStrings("cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3", &std.fmt.bytesToHex(digest, .lower));
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try parity.fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try parity.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try model.parseConfig(a, config_bytes, encoder_bytes);
    var store = try head_tests.loadMetalWeights(a, &weights);
    defer store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&store, a);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var backend = try metal.MetalCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const cases = [_]struct { id: []const u8, text: []const u8, schema: []const u8 }{
        .{ .id = "mixed_tasks", .text = "John works at Apple. Alice works at Google.", .schema =
        \\{"entities":["person","organization"],"relations":[{"type":"works_for"}],"classifications":[{"name":"sentiment","labels":["positive","negative"]}]}
        },
        .{ .id = "unicode_offsets", .text = "İpek works at Apple in 東京. 🙂 Alice visits café é.", .schema =
        \\{"entities":["person","location"],"entity_definitions":{"person":{"description":"A named person"},"location":{"description":"A named place"}}}
        },
        .{ .id = "enum_field", .text = "The iPhone camera is good.", .schema =
        \\{"structures":{"review":{"fields":{"product":{"type":"str"},"sentiment":{"type":"str","choices":["positive","negative"]}}}}}
        },
    };
    for (cases) |case| {
        errdefer std.debug.print("small checkpoint resident encoder fixture: {s}\n", .{case.id});
        const fixture_name = try std.fmt.allocPrint(a, "small_reference/{s}.safetensors", .{case.id});
        defer a.free(fixture_name);
        var reference = try parity.TensorFixture.init(a, fixture_name);
        defer reference.deinit();
        var schema = try @import("../pipelines/extraction_schema.zig").compile(a, case.schema, .{});
        defer schema.deinit();
        var prepared = try @import("../pipelines/gliner_boundary_processor.zig").prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        const expected_ids = try reference.tensor("input.ids");
        try std.testing.expectEqual(expected_ids.data.len, prepared.input_ids.len * 8);
        for (prepared.input_ids, 0..) |id, i| try std.testing.expectEqual(std.mem.readInt(i64, expected_ids.data[i * 8 ..][0..8], .little), id);
        const Probe = struct {
            calls: usize = 0,
            fn check(raw: ?*anyopaque) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.calls += 1;
            }
        };
        var probe = Probe{};
        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{ .control = .{ .ptr = &probe, .check_fn = Probe.check } });
        defer encoded.deinit();
        try std.testing.expect(probe.calls > config.encoder.num_hidden_layers);
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().proposal_download_calls);
        try std.testing.expectEqual((try engine.plan(&config, &prepared, .{})).encoder_weight_bytes, encoded.stats().charged_weight_bytes);
        var scored = try device_head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{ .query_chunk = 3 });
        defer scored.deinit();
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        try std.testing.expectEqual(@as(usize, 4), scored.stats().proposal_download_calls);
        const expected_text = try reference.floats("encoded.text");
        const actual_text = try encoded.download(encoded.text_states, expected_text.len);
        defer a.free(actual_text);
        try parity.expectFloats(expected_text, actual_text, 5e-4, 5e-5);
        const expected_queries = try reference.floats("encoded.query");
        const actual_queries = try encoded.download(encoded.query_states.?, expected_queries.len);
        defer a.free(actual_queries);
        try parity.expectFloats(expected_queries, actual_queries, 5e-4, 5e-5);
        const expected_logits = try reference.floats("candidate.logits");
        const actual_logits = try scored.download(scored.pair_logits, expected_logits.len);
        defer a.free(actual_logits);
        try parity.expectFloats(expected_logits, actual_logits, 7e-4, 5e-5);
        const indices = try reference.tensor("candidate.indices");
        const valid = try reference.booleans(a, "candidate.mask");
        defer a.free(valid);
        const pool = scored.pool();
        for (0..prepared.query_width) |q| for (0..pool.capacity) |c| {
            const i = q * pool.capacity + c;
            try std.testing.expectEqual(valid[i], pool.valid[c]);
            try std.testing.expectEqual(std.mem.readInt(i64, indices.data[i * 16 ..][0..8], .little), @as(i64, @intCast(pool.indices[c].start)));
            try std.testing.expectEqual(std.mem.readInt(i64, indices.data[i * 16 + 8 ..][0..8], .little), @as(i64, @intCast(pool.indices[c].end)));
        };
    }
}
