// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const request = @import("gliner_boundary_request_device.zig");
const engine = @import("gliner_boundary_engine.zig");
const model = @import("../models/gliner_boundary.zig");
const schema = @import("../pipelines/extraction_schema.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const math = @import("gliner_boundary_device_math.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");

fn testConfig() model.Config {
    var config = engine.TestBatch.config();
    config.head.candidate_pool = .shared;
    config.head.candidate_attention_layers = 0;
    config.head.query_attention_layers = 0;
    return config;
}

test "gliner boundary optimized required outputs preserve only requested record work" {
    const a = std.testing.allocator;
    const cases = [_]struct { json: []const u8, records: bool }{
        .{ .json = "{\"entities\":[\"person\"]}", .records = false },
        .{ .json = "{\"structures\":{\"item\":{\"fields\":{\"name\":\"str\"}}}}", .records = false },
        .{ .json = "{\"structures\":{\"item\":{\"mode\":\"natural\",\"fields\":{\"name\":\"str\"}}}}", .records = true },
        .{ .json = "{\"structures\":{\"item\":{\"mode\":\"latent\",\"fields\":{\"name\":\"str\"}}}}", .records = true },
        .{ .json = "{\"structures\":{\"item\":{\"mode\":\"anchorless\",\"fields\":{\"name\":\"str\"}}}}", .records = true },
    };
    var entities = try schema.compile(a, cases[0].json, .{});
    defer entities.deinit();
    for (cases) |case| {
        var compiled = try schema.compile(a, case.json, .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case.records, request.requiredHeadOutputs(&.{&compiled}).record_candidates);
        // A record task anywhere in a mixed batch keeps the shared branch.
        try std.testing.expectEqual(case.records, request.requiredHeadOutputs(&.{ &entities, &compiled }).record_candidates);
    }
}

test "gliner boundary device request preflight validates schemas and combined admission" {
    var compiled = try schema.compile(std.testing.allocator, "{\"entities\":[\"person\"]}", .{});
    defer compiled.deinit();
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    for (&fixture.samples) |*sample| sample.schema_fingerprint = compiled.fingerprint;
    const config = testConfig();
    const schemas = [_]*const schema.CompiledSchema{ &compiled, &compiled };
    const plan = try request.plan(&config, &prepared, &schemas, .{});
    try std.testing.expect(plan.head != null);
    try std.testing.expectEqual(plan.encoder_budget_bytes + plan.head_budget_bytes, plan.combined_device_upper_bound_bytes);
    try std.testing.expect(plan.minimum_result_download_bytes > 0);
    try std.testing.expectError(error.ResourceLimitExceeded, request.plan(&config, &prepared, &schemas, .{ .limits = .{ .max_combined_device_bytes = plan.combined_device_upper_bound_bytes - 1 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, request.plan(&config, &prepared, &schemas, .{ .limits = .{ .scorer = .{ .max_result_download_bytes = plan.minimum_result_download_bytes - 1 } } }));
    try std.testing.expectError(error.ResourceLimitExceeded, request.plan(&config, &prepared, &schemas, .{ .limits = .{ .head = .{ .max_proposal_download_bytes = 1 } } }));
    try std.testing.expectError(error.InvalidBoundaryPipelineOptions, request.plan(&config, &prepared, &schemas, .{ .pipeline = .{ .threshold = std.math.nan(f32) } }));
    const per_sample = [_]pipeline.Options{.{}};
    try std.testing.expectError(error.InvalidBoundaryPipelineOptions, request.plan(&config, &prepared, &schemas, .{ .per_sample = &per_sample }));
    fixture.samples[0].schema_fingerprint[0] ^= 1;
    try std.testing.expectError(error.InvalidBoundaryPipelineRouting, request.plan(&config, &prepared, &schemas, .{}));
}

const MissingDevice = struct {
    fn kind(_: *anyopaque) ops.BackendKind {
        return .metal;
    }
    fn execute(_: *anyopaque, _: *const ops.gliner_boundary_device.Request) anyerror!ops.CT {
        return error.UnsupportedGlinerBoundaryDevice;
    }
    fn download(_: *anyopaque, _: ops.CT, _: []f32) anyerror!void {
        return error.UnsupportedGlinerBoundaryDevice;
    }
    fn active(_: *anyopaque) bool {
        return true;
    }
    fn allocationFailure(a: std.mem.Allocator, cb: *const ops.ComputeBackend, config: *const model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema) !void {
        for ([_]bool{ false, true }) |windows| {
            if (windows) {
                var result = request.runWindows(cb, a, config, prepared, schemas, .{}) catch |err| switch (err) {
                    error.UnsupportedGlinerBoundaryDevice => continue,
                    else => return err,
                };
                defer result.deinit();
            } else {
                var result = request.run(cb, a, config, prepared, schemas, .{}) catch |err| switch (err) {
                    error.UnsupportedGlinerBoundaryDevice => continue,
                    else => return err,
                };
                defer result.deinit();
            }
            return error.ExpectedUnsupportedDevice;
        }
    }
};

test "gliner boundary device request rejects external frames and unwinds failed encoding" {
    const a = std.testing.allocator;
    var compiled = try schema.compile(a, "{\"entities\":[\"person\"]}", .{});
    defer compiled.deinit();
    var fixture = engine.TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    for (&fixture.samples) |*sample| sample.schema_fingerprint = compiled.fingerprint;
    const config = testConfig();
    const schemas = [_]*const schema.CompiledSchema{ &compiled, &compiled };
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    var cb = backend.computeBackend();
    var vtable = cb.vtable.*;
    vtable.backendKind = MissingDevice.kind;
    vtable.glinerBoundaryDevice = MissingDevice.execute;
    vtable.glinerBoundaryDownload = MissingDevice.download;
    vtable.decoderRuntimeHasActiveFrame = MissingDevice.active;
    cb.vtable = &vtable;
    try std.testing.expectError(error.GlinerBoundaryExternalFrame, request.run(&cb, a, &config, &prepared, &schemas, .{}));
    try std.testing.expectError(error.GlinerBoundaryExternalFrame, request.runWindows(&cb, a, &config, &prepared, &schemas, .{}));
    try std.testing.expectError(error.GlinerBoundaryExternalFrame, math.Context.create(a, &cb, .{}, null));
    vtable.decoderRuntimeHasActiveFrame = null;
    try std.testing.checkAllAllocationFailures(a, MissingDevice.allocationFailure, .{ &cb, &config, &prepared, &schemas });
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, request.run(&cb, a, &config, &prepared, &schemas, .{ .pipeline = .{ .control = .{ .check_fn = Cancel.check } } }));
    // Net resident size would fit. Its private-buffer upload staging does not.
    const context = try math.Context.create(a, &cb, .{ .max_device_bytes = 31 }, null);
    defer context.destroy();
    try std.testing.expectError(error.ResourceLimitExceeded, context.execute(.{ .upload_f32 = .{ .values = &.{ 1, 2, 3, 4 }, .shape = &.{4} } }, 4));
    try std.testing.expectEqual(@as(usize, 0), context.current_bytes);
    try std.testing.expectEqual(@as(usize, 0), context.stats.device_dispatches);
}

const ContextScopeFixture = struct {
    const device = ops.gliner_boundary_device;
    const Handle = struct {
        owner: *ContextScopeFixture,
        request_bytes: usize,
        ct_live: bool = true,
        scope_held: bool = false,
    };
    const Retirement = struct { generation: u64, dispatches: usize, bytes: usize, cancelled: bool };

    allocator: std.mem.Allocator,
    accounting: device.ScopeAccounting = .{},
    retained: [64]*Handle = undefined,
    retained_count: usize = 0,
    retirements: [4]Retirement = undefined,
    retirement_count: usize = 0,
    handles_created: usize = 0,
    live_handles: usize = 0,
    free_calls: usize = 0,
    physical_live_bytes: usize = 0,
    physical_peak_bytes: usize = 0,
    cancelled: bool = false,

    fn kind(_: *anyopaque) ops.BackendKind {
        return .metal;
    }

    fn active(raw: *anyopaque) bool {
        const self: *ContextScopeFixture = @ptrCast(@alignCast(raw));
        return self.accounting.stats.active;
    }

    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *ContextScopeFixture = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }

    fn makeHandle(self: *ContextScopeFixture, request_bytes: usize) !*Handle {
        const handle = try self.allocator.create(Handle);
        handle.* = .{ .owner = self, .request_bytes = request_bytes };
        self.handles_created += 1;
        self.live_handles += 1;
        self.physical_live_bytes += request_bytes;
        self.physical_peak_bytes = @max(self.physical_peak_bytes, self.physical_live_bytes);
        return handle;
    }

    fn destroyHandle(self: *ContextScopeFixture, handle: *Handle) void {
        std.debug.assert(handle.owner == self and !handle.scope_held);
        self.physical_live_bytes -= handle.request_bytes;
        self.live_handles -= 1;
        self.allocator.destroy(handle);
    }

    fn free(raw: *anyopaque, tensor: ops.CT) void {
        const self: *ContextScopeFixture = @ptrCast(@alignCast(raw));
        const handle: *Handle = @ptrCast(@alignCast(tensor));
        std.debug.assert(handle.owner == self and handle.ct_live);
        self.free_calls += 1;
        handle.ct_live = false;
        if (!handle.scope_held) self.destroyHandle(handle);
    }

    fn immutable(self: *ContextScopeFixture, shape: []const i64) !ops.CT {
        if (!std.mem.eql(i64, shape, &.{1024})) return error.InvalidGlinerBoundaryWeightShape;
        // Only a fresh CT is request-owned. Its immutable 4096-byte payload
        // belongs to the separately admitted model, not the pending frame.
        return @ptrCast(try self.makeHandle(0));
    }

    fn execute(raw: *anyopaque, operation: *const device.Request) anyerror!ops.CT {
        const self: *ContextScopeFixture = @ptrCast(@alignCast(raw));
        if (!self.accounting.stats.active) return error.GlinerBoundaryScopeNotActive;
        return switch (operation.*) {
            .load_f32_weight => |weight| blk: {
                if (weight.name.len == 0) return error.InvalidGlinerBoundaryWeightShape;
                const output = try self.immutable(weight.shape);
                self.accounting.stats.resident_weight_acquires += 1;
                break :blk output;
            },
            .load_f32_derived => |derived| blk: {
                if (std.meta.activeTag(derived.key) != .relative_normalized) return error.InvalidBoundaryDeviceState;
                const output = try self.immutable(derived.shape);
                self.accounting.stats.resident_derived_acquires += 1;
                break :blk output;
            },
            .kernel => |kernel| blk: {
                if (kernel.kind != .fill_zero) return error.UnsupportedGlinerBoundaryDevice;
                const layout = try kernel.layout();
                if (layout.output_elements != 1 or self.retained_count == self.retained.len)
                    return error.UnexpectedContextScopeGeometry;
                const handle = try self.makeHandle(4);
                errdefer self.destroyHandle(handle);
                try self.accounting.reserve(4, 1);
                handle.scope_held = true;
                self.retained[self.retained_count] = handle;
                self.retained_count += 1;
                self.accounting.stats.dispatches += 1;
                break :blk @ptrCast(handle);
            },
            else => error.UnsupportedGlinerBoundaryDevice,
        };
    }

    fn retire(self: *ContextScopeFixture, cancelled: bool) !void {
        if (!self.accounting.stats.active) return;
        if (self.retirement_count == self.retirements.len) return error.UnexpectedContextScopeCount;
        self.retirements[self.retirement_count] = .{
            .generation = self.accounting.stats.generation,
            .dispatches = self.accounting.scope_dispatches,
            .bytes = self.accounting.stats.pending_device_bytes,
            .cancelled = cancelled,
        };
        self.retirement_count += 1;
        for (self.retained[0..self.retained_count]) |handle| {
            std.debug.assert(handle.scope_held);
            handle.scope_held = false;
            if (!handle.ct_live) self.destroyHandle(handle);
        }
        self.retained_count = 0;
        if (cancelled) {
            self.accounting.stats.cancellations += 1;
        } else if (self.accounting.scope_dispatches != 0) {
            self.accounting.stats.submissions += 1;
        }
        self.accounting.retired();
    }

    fn scopeOperation(raw: *anyopaque, operation: *const device.ScopeRequest) anyerror!device.ScopeStats {
        const self: *ContextScopeFixture = @ptrCast(@alignCast(raw));
        switch (operation.*) {
            .snapshot => {},
            .begin => |limits| try self.accounting.begin(limits),
            .finish => |token| {
                try self.accounting.validateGeneration(token.generation);
                try self.retire(false);
            },
            .cancel => |token| {
                try self.accounting.validateGeneration(token.generation);
                try self.retire(true);
            },
        }
        return self.accounting.stats;
    }

    fn exercise(cancel_after_limit: bool) !void {
        const a = std.testing.allocator;
        var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        defer store.deinitOwned();
        var backend = native.NativeCompute.init(a, &store, null);
        defer backend.deinit();
        var vtable = backend.computeBackend().vtable.*;
        vtable.backendKind = kind;
        vtable.freeTensor = free;
        vtable.glinerBoundaryDevice = execute;
        vtable.glinerBoundaryScope = scopeOperation;
        vtable.decoderRuntimeHasActiveFrame = active;
        var fixture = ContextScopeFixture{ .allocator = a };
        const model_bytes = 65 * 4096; // 64 distinct weights and one shared derived table.
        fixture.accounting.stats.resident_model_live_bytes = model_bytes;
        const cb = ops.ComputeBackend{
            .ptr = &fixture,
            .vtable = &vtable,
            .execution_control = .{ .ptr = &fixture, .check_fn = check },
        };
        const context = try math.Context.create(a, &cb, .{ .max_device_bytes = 1024 }, null);
        var context_live = true;
        defer if (context_live) context.destroy();
        context.configure(.optimized_v2);

        for (0..64) |i| {
            var name: [32]u8 = undefined;
            _ = try context.weight(try std.fmt.bufPrint(&name, "immutable.weight.{d}", .{i}), &.{1024});
            const derived = try context.derived(.relative_normalized, &.{1024});
            // A CT release cannot change the model's persistent byte charge.
            context.drop(derived);
            try std.testing.expectEqual(@as(usize, 0), context.current_bytes);
            const output = try context.kernel(.fill_zero, &.{1}, &.{}, 0);
            context.drop(output);
            try std.testing.expectEqual(@as(usize, 0), context.current_bytes);
        }

        const first = fixture.accounting.stats.generation;
        try std.testing.expectEqual(@as(u64, 64), fixture.accounting.stats.dispatches);
        try std.testing.expectEqual(@as(u64, 64), fixture.accounting.stats.resident_weight_acquires);
        try std.testing.expectEqual(@as(u64, 64), fixture.accounting.stats.resident_derived_acquires);
        // Wrapper operations include immutable acquires; the scope must count
        // only strict device dispatches, so all 64 kernels share one frame.
        try std.testing.expectEqual(@as(usize, 192), context.stats.device_dispatches);
        try std.testing.expectEqual(@as(u64, 1), fixture.accounting.stats.begins);
        try std.testing.expectEqual(@as(u64, 0), fixture.accounting.stats.submissions);
        try std.testing.expectEqual(@as(usize, 0), fixture.retirement_count);
        try std.testing.expectEqual(@as(usize, 256), fixture.accounting.stats.pending_device_bytes);
        try std.testing.expectEqual(@as(usize, 256), fixture.physical_live_bytes);
        try std.testing.expectEqual(@as(usize, 128), fixture.free_calls);
        try std.testing.expectEqual(@as(usize, 128), fixture.live_handles);
        try std.testing.expectEqual(@as(usize, 64 * 4096), context.stats.charged_weight_bytes);
        try std.testing.expectEqual(@as(usize, 256), context.stats.peak_device_bytes);
        try std.testing.expectEqual(@as(usize, model_bytes), fixture.accounting.stats.resident_model_live_bytes);
        try std.testing.expectEqual(@as(u64, 0), fixture.accounting.stats.actual_weight_upload_bytes);

        if (cancel_after_limit) {
            fixture.cancelled = true;
            try std.testing.expectError(error.Cancelled, context.kernel(.fill_zero, &.{1}, &.{}, 0));
            try std.testing.expectEqual(@as(u64, 64), fixture.accounting.stats.dispatches);
            try std.testing.expectEqual(@as(u64, 1), fixture.accounting.stats.begins);
            try std.testing.expectEqual(@as(usize, 256), fixture.physical_live_bytes);
            context.destroy();
            context_live = false;
            try std.testing.expectEqual(@as(usize, 1), fixture.retirement_count);
            try std.testing.expectEqual(Retirement{ .generation = first, .dispatches = 64, .bytes = 256, .cancelled = true }, fixture.retirements[0]);
            try std.testing.expectEqual(@as(u64, 1), fixture.accounting.stats.cancellations);
            try std.testing.expectEqual(@as(u64, 0), fixture.accounting.stats.submissions);
        } else {
            // The next real dispatch rotates before submission. All ordinary
            // payloads whose CTs were freed are released by that first fence.
            const last = try context.kernel(.fill_zero, &.{1}, &.{}, 0);
            try std.testing.expectEqual(first + 1, fixture.accounting.stats.generation);
            try std.testing.expectEqual(@as(u64, 2), fixture.accounting.stats.begins);
            try std.testing.expectEqual(@as(u64, 65), fixture.accounting.stats.dispatches);
            try std.testing.expectEqual(@as(u64, 1), fixture.accounting.stats.submissions);
            try std.testing.expectEqual(@as(usize, 1), fixture.retirement_count);
            try std.testing.expectEqual(Retirement{ .generation = first, .dispatches = 64, .bytes = 256, .cancelled = false }, fixture.retirements[0]);
            try std.testing.expectEqual(@as(usize, 4), fixture.physical_live_bytes);
            try std.testing.expectEqual(@as(usize, 4), context.current_bytes);
            try std.testing.expectEqual(@as(usize, 4), fixture.accounting.stats.pending_device_bytes);
            // A stale owner must not finish or cancel the new generation.
            try std.testing.expectError(error.GlinerBoundaryScopeIdentityMismatch, cb.glinerBoundaryScope(&.{ .finish = .{ .generation = first } }));
            try std.testing.expectError(error.GlinerBoundaryScopeIdentityMismatch, cb.glinerBoundaryScope(&.{ .cancel = .{ .generation = first } }));
            try std.testing.expect(fixture.accounting.stats.active);
            try std.testing.expectEqual(@as(usize, 1), fixture.retirement_count);
            context.drop(last);
            try std.testing.expectEqual(@as(usize, 4), fixture.physical_live_bytes);
            try context.finishSegment();
            try std.testing.expectEqual(@as(usize, 2), fixture.retirement_count);
            try std.testing.expectEqual(Retirement{ .generation = first + 1, .dispatches = 1, .bytes = 4, .cancelled = false }, fixture.retirements[1]);
            try std.testing.expectEqual(@as(u64, 2), fixture.accounting.stats.submissions);
            try std.testing.expectEqual(@as(u64, 0), fixture.accounting.stats.cancellations);
            try std.testing.expectEqual(@as(usize, 260), context.stats.peak_device_bytes);
            context.destroy();
            context_live = false;
        }
        try std.testing.expectEqual(@as(usize, if (cancel_after_limit) 192 else 193), fixture.handles_created);
        try std.testing.expectEqual(fixture.handles_created, fixture.free_calls);
        try std.testing.expectEqual(@as(usize, 0), fixture.live_handles);
        try std.testing.expectEqual(@as(usize, 0), fixture.retained_count);
        try std.testing.expectEqual(@as(usize, 0), fixture.physical_live_bytes);
        try std.testing.expectEqual(@as(usize, 256), fixture.physical_peak_bytes);
        try std.testing.expectEqual(@as(usize, 0), fixture.accounting.stats.pending_device_bytes);
        try std.testing.expectEqual(@as(usize, model_bytes), fixture.accounting.stats.resident_model_live_bytes);
        try std.testing.expect(!fixture.accounting.stats.active);
    }
};

test "gliner boundary context scope counts strict dispatches across immutable acquisitions" {
    try ContextScopeFixture.exercise(false);
}

test "gliner boundary context scope cancellation after 64 drains physical owners" {
    try ContextScopeFixture.exercise(true);
}
