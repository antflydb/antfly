// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Emits immutable-bundle outputs for independent comparison. No timing
//! claims, expected-label access in execution, or numerical qualification.
const std = @import("std");
const inference = @import("inference_internal");
const pipeline = inference.pipelines.gliner_boundary_pipeline;
const processor = inference.pipelines.gliner_boundary_processor;
const engine = inference.architectures.gliner_boundary_engine;
const factory = inference.architectures.session_factory;
const bundle = inference.models.gliner_boundary_bundle;
const Allocator = std.mem.Allocator;

fn emit(a: Allocator, io: std.Io, value: anytype) !void {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > 8 * 1024 * 1024) return error.ExtractionOutputLimitExceeded;
    try std.Io.File.stdout().writeStreamingAll(io, bytes);
    try std.Io.File.stdout().writeStreamingAll(io, "\n");
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var directory: ?[]const u8 = null;
    var fixture_path: ?[]const u8 = null;
    var evaluation_fixture: ?[]const u8 = null;
    var backend: enum { native, metal } = .native;
    var backend_seen = false;
    while (args.next()) |arg| {
        const value = args.next() orelse return error.MissingArgument;
        if (std.mem.eql(u8, arg, "--model-dir") and directory == null) directory = value else if (std.mem.eql(u8, arg, "--fixture") and fixture_path == null) fixture_path = value else if (std.mem.eql(u8, arg, "--evaluation-fixture") and evaluation_fixture == null) evaluation_fixture = value else if (std.mem.eql(u8, arg, "--backend") and !backend_seen) {
            backend = std.meta.stringToEnum(@TypeOf(backend), value) orelse return error.InvalidArgument;
            backend_seen = true;
        } else return error.InvalidArgument;
    }
    const path = directory orelse return error.MissingArgument;
    if (evaluation_fixture) |evaluation_path| {
        if (fixture_path != null) return error.InvalidArgument;
        return @import("gliner25_evaluation_worker.zig").run(a, init.io, path, evaluation_path, backend == .metal);
    }
    const fixture_bytes = try inference.util.c_file.readFileMax(a, fixture_path orelse return error.MissingArgument, 2 * 1024 * 1024);
    defer a.free(fixture_bytes);
    // Deliberately do not deserialize expected results into the execution
    // contract. The independent driver compares them after inference.
    const Fixture = struct {
        format_version: u32,
        source_commit: []const u8,
        model: []const u8,
        model_files: std.json.Value,
        cases: []const struct { id: []const u8, text: []const u8, schema: std.json.Value },
    };
    var fixture = try std.json.parseFromSlice(Fixture, a, fixture_bytes, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    if (fixture.value.format_version != 2 or fixture.value.cases.len != 10 or
        !std.mem.eql(u8, fixture.value.source_commit, "3c913c7369301133d3b7699252074c4303ada50e")) return error.InvalidReferenceFixture;
    var manifest = try inference.models.manifest.loadFromDir(a, path);
    defer manifest.deinit();
    const receipt = manifest.gliner_boundary_bundle orelse return error.InvalidGlinerBoundaryBundle;
    if (!std.mem.eql(u8, @tagName(receipt.value.backbone), fixture.value.model) or fixture.value.model_files != .object) return error.GlinerBoundaryArtifactMismatch;
    for (receipt.value.source_files) |source| {
        const expected = fixture.value.model_files.object.get(source.path) orelse return error.InvalidReferenceFixture;
        if (expected != .object) return error.InvalidReferenceFixture;
        const size = expected.object.get("size_bytes") orelse return error.InvalidReferenceFixture;
        const sha = expected.object.get("sha256") orelse return error.InvalidReferenceFixture;
        if (size != .integer or sha != .string or size.integer < 0 or source.size_bytes != @as(u64, @intCast(size.integer)) or
            !std.mem.eql(u8, sha.string, source.sha256)) return error.GlinerBoundaryArtifactMismatch;
    }
    const session = switch (backend) {
        .native => try factory.createNativeSession(a, path),
        .metal => try factory.createMetalSession(a, path),
    };
    defer session.close();
    const identity = try factory.getGlinerBoundaryIdentity(session);
    try identity.verifySidecars(try manifest.boundarySidecarDigests());
    try identity.weight.verify(try bundle.pinFor(receipt.value.files, bundle.model_name));
    if (identity.backbone != receipt.value.backbone or identity.precision != receipt.value.precision) return error.GlinerBoundaryArtifactMismatch;
    const config = try factory.getGlinerBoundaryConfig(session);
    // This executable is a disposable worker owned by the Python resource
    // guard. A wedged GPU request must terminate this worker, never leave a
    // shared runtime alive with borrowed buffers still in use by a driver.
    const watchdog = if (backend == .metal) try inference.HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owner| owner.destroy();
    if (watchdog) |owner| try owner.start(init.io);
    const tok_bytes = try inference.util.c_file.readFileMax(a, manifest.tokenizer_json_path orelse return error.NoTokenizerFound, 32 * 1024 * 1024);
    defer a.free(tok_bytes);
    try manifest.verifyBoundarySidecar("tokenizer.json", tok_bytes);
    const tokenizer = try inference.hf_tokenizer.HfTokenizer.loadFromBytesWithOptions(a, tok_bytes, .{ .strict_unigram_normalizer = true });
    defer tokenizer.tokenizer().deinitTokenizer();
    const digest = bundle.Digest.of(fixture_bytes);
    try emit(a, init.io, .{ .event = "ready", .scope = "converted_bundle_diagnostic", .backend = backend, .math_policy = "strict_f32_activations_v1", .weight_precision = identity.precision, .activation_precision = "f32", .accumulation_precision = "f32", .head_precision = "f32", .qualification = false, .build_mode = @tagName(@import("builtin").mode), .zig_version = @import("builtin").zig_version_string, .fixture_sha256 = digest.sha256[0..], .receipt = receipt.value });
    for (fixture.value.cases) |case| {
        const schema_json = try std.json.Stringify.valueAlloc(a, case.schema, .{});
        defer a.free(schema_json);
        var schema = try inference.pipelines.extraction_schema.compile(a, schema_json, .{});
        defer schema.deinit();
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        const control = inference.InferenceExecutionControl{ .hard_cancellation = if (watchdog) |owner| owner.boundary() else null, .deadline_ns = inference.platform.time.monotonicNs() + 120 * std.time.ns_per_s };
        var managed = try factory.getManagedComputeBackend(session, a, null, control);
        defer managed.deinit();
        var result = switch (backend) {
            .native => blk: {
                var encoded = try engine.encodeNative(&managed.backend, a, &config, &prepared, .{ .control = control });
                defer encoded.deinit();
                break :blk try pipeline.runNative(&managed.backend, a, &config, &prepared, &.{&schema}, .{
                    .text_states = encoded.text_states,
                    .query_states = encoded.query_states,
                    .classification_states = encoded.classification_states,
                    .text_lengths = encoded.text_lengths,
                }, .{ .offset_unit = .unicode_codepoints, .control = control });
            },
            .metal => blk: {
                const device = try inference.architectures.gliner_boundary_request_device.run(&managed.backend, a, &config, &prepared, &.{&schema}, .{ .precision = identity.precision, .pipeline = .{ .offset_unit = .unicode_codepoints, .control = control } });
                break :blk device.outputs;
            },
        };
        defer result.deinit();
        if (result.samples.len != 1) return error.InvalidExtractionOutput;
        try emit(a, init.io, .{ .event = "result", .case_id = case.id, .input_ids = prepared.input_ids, .output = result.samples[0] });
    }
    // Reopen and rehash on exit as well; this is still immutable publication,
    // not permission to mutate a model while any session holds its mapping.
    _ = try inference.gliner_boundary_export.verifyDirectory(a, path, null);
    try emit(a, init.io, .{ .event = "complete", .cases = fixture.value.cases.len, .qualification = false });
}
