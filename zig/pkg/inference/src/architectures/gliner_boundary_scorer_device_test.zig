// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const model = @import("../models/gliner_boundary.zig");
const engine = @import("gliner_boundary_engine_device.zig");
const head = @import("gliner_boundary_device.zig");
const adapter = @import("gliner_boundary_scorer_device.zig");
const fixtures = @import("gliner_boundary_parity_test.zig");
const head_tests = @import("gliner_boundary_device_test.zig");
const metal = @import("../ops/metal_compute.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");

fn expectPin(pin: pipeline.PublishedModelPin, bytes: []const u8) !void {
    try std.testing.expectEqual(pin.size_bytes, bytes.len);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    try std.testing.expectEqualStrings(pin.sha256, &std.fmt.bytesToHex(digest, .lower));
}
fn readPinned(a: std.mem.Allocator, directory: []const u8, name: []const u8, pin: pipeline.PublishedModelPin) ![]u8 {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const bytes = try @import("../util/c_file.zig").readFile(a, path);
    errdefer a.free(bytes);
    try expectPin(pin, bytes);
    return bytes;
}
fn parity(comptime variant: []const u8, comptime environment: [:0]const u8, comptime fixture_name: []const u8) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const directory = @import("antfly_platform").env.getenv(environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const bytes = try fixtures.fixtureBytes(a, fixture_name);
    defer a.free(bytes);
    const capture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer capture.deinit();
    try std.testing.expectEqual(@as(u32, 2), capture.value.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", capture.value.source_commit);
    try std.testing.expectEqualStrings(variant, capture.value.model);
    const reference = try fixtures.fixtureBytes(a, variant ++ "_reference/capture.json");
    defer a.free(reference);
    try expectPin(.{ .sha256 = capture.value.reference_sha256, .size_bytes = reference.len }, reference);
    const requests = try fixtures.fixtureBytes(a, "requests.json");
    defer a.free(requests);
    try expectPin(.{ .sha256 = capture.value.requests_sha256, .size_bytes = requests.len }, requests);
    const path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try @import("../models/safetensors.zig").MMapReader.openFileAbsolute(a, path) };
    defer weights.deinit();
    try expectPin(capture.value.model_files.@"model.safetensors", weights.reader.file_bytes);
    const tokenizer_bytes = try readPinned(a, directory, "tokenizer.json", capture.value.model_files.@"tokenizer.json");
    defer a.free(tokenizer_bytes);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try readPinned(a, directory, "config.json", capture.value.model_files.@"config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try readPinned(a, directory, "encoder_config/config.json", capture.value.model_files.@"encoder_config/config.json");
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
    for (capture.value.cases, 0..) |case, case_index| {
        errdefer std.debug.print(variant ++ " resident pipeline case: {s}\n", .{case.id});
        const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        defer a.free(schema_json);
        var schema = try @import("../pipelines/extraction_schema.zig").compile(a, schema_json, .{});
        defer schema.deinit();
        var prepared = try @import("../pipelines/gliner_boundary_processor.zig").prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        var encoded = try engine.encodeDevice(&cb, a, &config, &prepared, .{});
        defer encoded.deinit();
        var headed: ?head.Result = if (prepared.query_width > 0) try head.forwardDevice(&cb, a, &config, try encoded.asHeadInput(null), .{}) else null;
        defer if (headed) |*value| value.deinit();
        try std.testing.expectEqual(@as(usize, 0), encoded.stats().result_download_calls);
        var scorer = try adapter.Context.init(a, &config, &encoded, if (headed) |*value| value else null, .{});
        defer scorer.deinit();
        var result = try pipeline.runScored(a, &config, &prepared, &.{&schema}, scorer.scores, scorer.scorer(), .{ .offset_unit = .unicode_codepoints });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.samples.len);
        try pipeline.expectSample(case.expected, result.samples[0]);
        // The only encoder-owner readback is the scalar classification output.
        try std.testing.expectEqual(@as(usize, @intFromBool(prepared.classification_width > 0)), encoded.stats().result_download_calls);
        const stats = try scorer.stats();
        try std.testing.expect(stats.result_download_bytes <= scorer.limits.max_result_download_bytes);
        if (case_index == 0) {
            // Admission rejects cumulative transfers before copying a tensor.
            const before = try scorer.stats();
            scorer.limits.max_result_download_bytes = before.result_download_bytes;
            if (prepared.classification_width > 0) try std.testing.expectError(error.ResourceLimitExceeded, scorer.scorer().classify(a, .{}, null));
            try std.testing.expectEqual(before.result_download_bytes, (try scorer.stats()).result_download_bytes);
        }
    }
}

test "gliner boundary device Metal pinned small full inference pipeline parity" {
    try parity("small", "ANTFLY_GLINER25_SMALL_MODEL_DIR", "pipeline_cases.json");
}
test "gliner boundary device Metal pinned base full inference pipeline parity" {
    try parity("base", "ANTFLY_GLINER25_BASE_MODEL_DIR", "pipeline_cases_base.json");
}
test "gliner boundary device Metal pinned multi full inference pipeline parity" {
    try parity("multi", "ANTFLY_GLINER25_MULTI_MODEL_DIR", "pipeline_cases_multi.json");
}
