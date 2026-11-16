// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Complete schema/extraction execution through the public task controller.
//! A compiled selection requires a real PJRT session for every learned stage.
const std = @import("std");
const inference = @import("inference_internal");
const factory = inference.architectures.session_factory;
const executor = inference.gliner_boundary_executor;
const wire = inference.extraction_v2;

pub fn run(init: std.process.Init, model: *inference.server.model_manager.LoadedModel, a: std.mem.Allocator, options: anytype) !void {
    const is_pjrt = options.backend == .xla;
    if (is_pjrt and model.session.backend() != .pjrt) return error.ExtractionBackendSelectionMismatch;
    const config = try factory.getGlinerBoundaryConfig(model.session);
    const identity = try factory.getGlinerBoundaryIdentity(model.session);
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, options.extraction_request.?, a, .limited(16 * 1024 * 1024));
    defer a.free(bytes);
    var geometry_request = try wire.parseJson(a, bytes, .{});
    defer geometry_request.deinit();
    const geometry = try executor.measureRequestGeometry(a, &config, model.getTokenizer(), &geometry_request, .{ .compiled_session = if (is_pjrt) model.session else null, .compiled_batch_size = options.pjrt.extraction_batch_size });
    const samples = try a.alloc(u64, options.iterations);
    defer a.free(samples);
    var response: ?[]u8 = null;
    defer if (response) |value| a.free(value);
    var cold_ns: u64 = 0;
    for (0..options.warmup + options.iterations) |index| {
        const started = inference.platform.time.awakeNs(init.io);
        var request = try wire.parseJson(a, bytes, .{});
        defer request.deinit();
        const actual = try executor.execute(&cb, a, &config, model.getTokenizer(), &request, .{ .identity = identity, .compiled_session = if (is_pjrt) model.session else null, .compiled_batch_size = options.pjrt.extraction_batch_size, .control = .{ .native_call_policy = .in_process } });
        const elapsed = inference.platform.time.awakeNs(init.io) - started;
        if (index == 0) cold_ns = elapsed;
        if (index >= options.warmup) samples[index - options.warmup] = elapsed;
        if (response) |expected| {
            defer a.free(actual);
            if (!std.mem.eql(u8, expected, actual)) return error.ExtractionResetOutputChanged;
        } else response = actual;
    }
    const held = response orelse return error.MissingExtractionOutput;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, held, .{});
    defer parsed.deinit();
    const report = try std.json.Stringify.valueAlloc(a, .{
        .task = "public_extraction",
        .backend = @tagName(model.session.backend()),
        .model_dir = options.model_dir,
        .pjrt_extraction_batch_size = options.pjrt.extraction_batch_size,
        .pjrt_execution_profile = factory.getPjrtExecutionProfile(model.session),
        .pjrt_device = if (is_pjrt) @tagName(options.pjrt.device) else null,
        .pjrt_gemm = if (is_pjrt) @tagName(options.pjrt.gemm) else null,
        .pjrt_precision = if (is_pjrt) @tagName(options.pjrt.precision) else null,
        .compiled_stages_required = is_pjrt,
        .model_load_in_timing = false,
        .timing_scope = "request_parse+schema+tokenization+encoder+heads+decode+cleanup",
        .observed_geometry = geometry,
        .release_serving_qualification = false,
        .cold_request_ns = cold_ns,
        .warmup = options.warmup,
        .samples_ns = samples,
        .response = parsed.value,
    }, .{});
    defer a.free(report);
    try std.Io.File.stdout().writeStreamingAll(init.io, report);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}
