// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact-artifact Metal batch serving: distinct schemas, ragged prompts,
//! output order, physical GPU work and request resource lifetimes.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const server = @import("server.zig");
const Node = server.Node;
const Loopback = @import("gliner_boundary_socket_test.zig").Loopback;
const batch = @import("gliner_family_cuda_socket_test.zig");
const factory = @import("../architectures/session_factory.zig");
const metal = @import("../backends/metal_tensor.zig");
const decide_qualification = @import("../models/gliner_decide_qualification.zig");
const Role = enum { base, multi, decide };
const batch_sizes = [_]usize{ 2, 4, 8 };

fn response(transport: *Loopback, body: []const u8) ![]u8 {
    var result = try transport.request(.POST, "/ai/v1/extract", body);
    defer result.deinit();
    errdefer std.debug.print("Metal batch HTTP: {d} {s}\n", .{ result.status.code, result.body orelse "<absent>" });
    try std.testing.expectEqual(@as(u16, 200), result.status.code);
    return std.testing.allocator.dupe(u8, result.body orelse return error.MissingResponseBody);
}

fn rejectBeforeDeviceWork(node: *Node, transport: *Loopback, body: []const u8) !void {
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    const retained = domain.admission.snapshot();
    const before = metal.memoryStatsSnapshot();
    var rejected = try transport.request(.POST, "/ai/v1/extract", body);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 413), rejected.status.code);
    try transport.idle();
    try std.testing.expectEqual(before.device_owned_buffers_created, metal.memoryStatsSnapshot().device_owned_buffers_created);
    try std.testing.expectEqual(retained, domain.admission.snapshot());
}

fn batchPool(node: *Node, transport: *Loopback, directory: []const u8, comptime role: Role, cases: []const batch.BatchCase, sizes: []const usize) !void {
    const a = std.testing.allocator;
    const name = std.fs.path.basename(directory);
    const baselines = try a.alloc([]u8, cases.len);
    defer a.free(baselines);
    const allocations = try a.alloc(u64, cases.len);
    defer a.free(allocations);
    var built: usize = 0;
    defer for (baselines[0..built]) |body| a.free(body);
    for (cases, baselines, allocations) |case, *baseline, *work| {
        const body = try batch.batchBody(a, name, &.{case}, 1, false);
        defer a.free(body);
        const warmup = try response(transport, body);
        a.free(warmup);
        try transport.idle();
        const before = metal.memoryStatsSnapshot();
        baseline.* = try response(transport, body);
        try transport.idle();
        work.* = metal.memoryStatsSnapshot().device_owned_buffers_created - before.device_owned_buffers_created;
        built += 1;
    }
    try transport.idle();
    var handle = node.model_manager.acquireLoadedModel(directory) orelse return error.MissingLoadedModel;
    defer handle.release();
    try std.testing.expectEqual(.metal, handle.get().session.backend());
    if (role != .decide) try std.testing.expect(factory.isGlinerBoundaryResidentReady(handle.get().session));
    const domain = node.model_manager.resource_domain orelse return error.MissingResourceDomain;
    // The first pass may grow the resident workspace; the second must reuse it.
    for (0..2) |pass| for (sizes) |n| for ([_]bool{ false, true }) |reverse| {
        const body = try batch.batchBody(a, name, cases, n, reverse);
        defer a.free(body);
        const retained = domain.admission.snapshot();
        const before = metal.memoryStatsSnapshot();
        const actual = try response(transport, body);
        defer a.free(actual);
        const after = metal.memoryStatsSnapshot();
        const work = after.device_owned_buffers_created - before.device_owned_buffers_created;
        try std.testing.expect(work > 0);
        var serial_work: u64 = 0;
        for (0..n) |i| serial_work += allocations[(if (reverse) n - 1 - i else i) % cases.len];
        try std.testing.expect(work < serial_work);
        if (role == .decide) {
            // Large public requests still use <=8 rows and one readback per
            // physical forward, including their final partial group.
            const forwards: u64 = @intCast((n - 1) / decide_qualification.metal_batch_items + 1);
            try std.testing.expectEqual(before.to_host_device_calls + forwards, after.to_host_device_calls);
        }
        var parsed = try std.json.parseFromSlice(std.json.Value, a, actual, .{});
        defer parsed.deinit();
        const rows = parsed.value.object.get("data").?.array.items;
        try std.testing.expectEqual(n, rows.len);
        for (rows, 0..) |row, i| {
            const index = (if (reverse) n - 1 - i else i) % cases.len;
            var expected = try std.json.parseFromSlice(std.json.Value, a, baselines[index], .{});
            defer expected.deinit();
            var id: [32]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&id, "row-{d}", .{i}), row.object.get("id").?.string);
            try batch.equalBatchValue(expected.value.object.get("data").?.array.items[0], row, if (role == .decide) 5e-3 else 5e-4);
        }
        try transport.idle();
        if (pass != 0 or role == .decide) try std.testing.expectEqual(retained, domain.admission.snapshot());
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        std.debug.print("production_metal_batch role={s} items={d} reverse={} pass={d} buffers={d} serial_buffers={d} parity=true leases_released=true\n", .{ @tagName(role), n, reverse, pass, work, serial_work });
    };
    const limit = if (role == .decide) server.max_serial_family_batch_items else 8;
    const oversized = try batch.batchBody(a, name, cases, limit + 1, false);
    defer a.free(oversized);
    try rejectBeforeDeviceWork(node, transport, oversized);
}

fn additionalMultilingualProfiles(node: *Node, transport: *Loopback, directory: []const u8, intents: []const batch.BatchCase) !void {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner/boundary_parity_test.zig");
    for (batch.additional_multi_batch_fixtures, 0..) |file, file_index| {
        std.debug.print("production_metal_batch_profile fixture={s}\n", .{file});
        const bytes = try fixtures.fixtureBytes(a, file);
        defer a.free(bytes);
        var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer fixture.deinit();
        const values = fixture.value.object.get("requests").?.array.items;
        const cases = try a.alloc(batch.BatchCase, values.len);
        defer a.free(cases);
        const schemas = try a.alloc(std.json.Parsed(std.json.Value), values.len);
        defer a.free(schemas);
        var built: usize = 0;
        defer for (schemas[0..built]) |*schema| schema.deinit();
        for (values, cases, schemas) |value, *out, *schema| {
            schema.* = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            built += 1;
            out.* = .{ .text = value.object.get("text").?.string, .schema = schema.value };
        }
        switch (file_index) {
            0 => {
                var bare: [5]batch.BatchCase = undefined;
                @memcpy(bare[0..4], intents);
                bare[4] = cases[0];
                try batchPool(node, transport, directory, .multi, &bare, &batch_sizes);
                for ([_]usize{ 1, 3, 5 }) |i| try batchPool(node, transport, directory, .multi, cases[i..][0..2], &batch_sizes);
            },
            1 => try batchPool(node, transport, directory, .multi, cases, &batch_sizes),
            4 => {
                for ([_]usize{ 0, 2 }) |i| {
                    // The one-byte diagnostic remains outside the serving
                    // profile. Exercise its rejection and the clean floor.
                    const invalid = try batch.batchBody(a, std.fs.path.basename(directory), cases[i..][0..1], 2, false);
                    defer a.free(invalid);
                    try rejectBeforeDeviceWork(node, transport, invalid);
                    var cohort = [_]batch.BatchCase{ cases[i], cases[i + 1] };
                    cohort[0].text = "a.";
                    try batchPool(node, transport, directory, .multi, &cohort, &batch_sizes);
                }
            },
            else => for (cases) |case| try batchPool(node, transport, directory, .multi, &.{case}, &batch_sizes),
        }
    }
}

fn parity(comptime role: Role, environment: [:0]const u8) !void {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const source = platform.env.getenv(environment) orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const directory = try @import("../models/gliner_decide_qualification_fixture.zig").prepare(a, &temporary, source);
    defer a.free(directory);
    const GiB = 1024 * 1024 * 1024;
    var node = try Node.init(a, .{
        .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
        .max_loaded_models = 1,
        .max_concurrent_requests = 2,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 6 * GiB, .backend_limit_bytes = 8 * GiB, .combined_limit_bytes = 12 * GiB, .scratch_limit_bytes = 4 * GiB },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &.{.metal};
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &.{.metal};
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(std.testing.io);
    const transport = try Loopback.initWithCapacity(a, &node, 2);
    defer transport.deinit();
    try transport.start();
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, switch (role) {
        .base => "pipeline_cases_base.json",
        .multi => "family/multi_v1_capture.json",
        .decide => "decide/cases.json",
    });
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer fixture.deinit();
    const values = fixture.value.object.get(switch (role) {
        .base => "cases",
        .multi => "requests",
        .decide => "classification",
    }).?.array.items;
    // The source-default JointIE beam fixture remains outside the public base
    // profile. Cover its other nine frozen mixed-task cases in two cohorts.
    const count = if (role == .base) 9 else values.len;
    const cases = try a.alloc(batch.BatchCase, count);
    defer a.free(cases);
    const schemas = try a.alloc(std.json.Parsed(std.json.Value), if (role == .multi) count else 0);
    defer a.free(schemas);
    var schemas_built: usize = 0;
    defer for (schemas[0..schemas_built]) |*schema| schema.deinit();
    for (cases, values[0..count], 0..) |*out, value, i| {
        const schema = if (role == .multi) parsed: {
            schemas[i] = try std.json.parseFromSlice(std.json.Value, a, value.object.get("native_schema_json").?.string, .{});
            schemas_built += 1;
            break :parsed schemas[i].value;
        } else value.object.get(if (role == .base) "schema" else "v2_schema").?;
        out.* = .{ .text = value.object.get("text").?.string, .schema = schema };
    }
    const Cohort = struct { start: usize, end: usize };
    const cohorts: []const Cohort = switch (role) {
        .base => &.{ .{ .start = 0, .end = 8 }, .{ .start = 7, .end = 9 } },
        // Each multilingual profile remains independent; a union of their
        // features must never acquire a new qualification row through batching.
        .multi => &.{ .{ .start = 1, .end = 4 }, .{ .start = 4, .end = 8 }, .{ .start = 0, .end = 1 }, .{ .start = 8, .end = 9 }, .{ .start = 9, .end = 10 }, .{ .start = 10, .end = 11 } },
        .decide => &.{.{ .start = 0, .end = count }},
    };
    for (cohorts) |range| {
        const sizes: []const usize = if (role == .decide) &.{ 2, 4, 8, 9, 17, server.max_serial_family_batch_items } else &batch_sizes;
        try batchPool(&node, transport, directory, role, cases[range.start..range.end], sizes);
    }
    if (role == .multi) try additionalMultilingualProfiles(&node, transport, directory, cases[4..8]);
    if (role == .decide) {
        // Preferred-only Metal selection must preserve the same public envelope
        // as an explicitly required Metal backend.
        node.session_manager.required_backend = null;
        node.model_manager.session_manager.required_backend = null;
        std.debug.print("production_metal_batch_preferred_backend required=false\n", .{});
        try batchPool(&node, transport, directory, .decide, cases[0..2], &.{9});
    }
    try transport.finish();
}

test "GLiNER production Metal distinct input batches base FP32" {
    try parity(.base, "ANTFLY_GLINER25_BASE_MODEL_DIR");
}

test "GLiNER production Metal distinct input batches Decide FP32" {
    try parity(.decide, "ANTFLY_GLINER_DECIDE_MODEL");
}

test "GLiNER production Metal distinct input batches multilingual FP32" {
    try parity(.multi, "ANTFLY_GLINER25_MULTI_V1_MODEL_DIR");
}
