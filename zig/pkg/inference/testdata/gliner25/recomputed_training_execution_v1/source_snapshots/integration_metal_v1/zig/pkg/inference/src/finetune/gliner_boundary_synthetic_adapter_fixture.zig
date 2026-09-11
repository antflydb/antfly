// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-only synthetic model seam. Public training always derives its layout
//! from the immutable published inventory. This module cannot be called from
//! a non-test executable and has no Options/JSON/environment representation.
const std = @import("std");
const builtin = @import("builtin");
const layouts = @import("gliner_boundary_adapter_layout.zig");
const artifacts = @import("gliner_boundary_adapter.zig");
const peft = @import("gliner_boundary_peft_graph.zig");
const run = @import("gliner_boundary_run.zig");
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const ops = @import("../ops/ops.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const step = @import("gliner_boundary_train_step.zig");
const Allocator = std.mem.Allocator;

pub const Gradient = struct { name: []const u8, kind: step.GradientPresence, tensor: ?ops.CT, elements: usize };
pub const Observation = struct {
    backend: *const ops.ComputeBackend,
    prepared: *const processor.PreparedBatch,
    result: *const step.StepResult,
    gradients: []const Gradient,
    optimizer_loss: f32,
    zero_loss_fallback: bool,
};
pub const Fixture = struct {
    /// Digests are verified by the fixture loader before this borrowed view is
    /// supplied. They bind the synthetic layout/checkpoint identity as well.
    capture: bundle.Digest,
    tensors: bundle.Digest,
    /// Exact captured step-zero slots, sorted by full graph parameter name.
    /// Construction is the only injection point: no live owner can replace
    /// adapter weights or moments through this seam.
    initial: []const run.Parameter,
    observer_context: ?*anyopaque = null,
    observe: ?*const fn (?*anyopaque, Observation) anyerror!void = null,

    pub fn initialFor(self: *const Fixture, a: Allocator, descriptor: layouts.Descriptor) !peft.InitialWeights {
        if (!builtin.is_test) @compileError("Synthetic adapter initialization is test-only");
        const av = try find(self.initial, descriptor.a_name);
        const bv = try find(self.initial, descriptor.b_name);
        const owned_a = try a.dupe(f32, av.values);
        errdefer a.free(owned_a);
        const owned_b = try a.dupe(f32, bv.values);
        errdefer a.free(owned_b);
        const magnitude = if (descriptor.magnitude_name) |name| try a.dupe(f32, (try find(self.initial, name)).values) else null;
        return .{ .allocator = a, .a = owned_a, .b = owned_b, .magnitude = magnitude };
    }

    pub fn layout(self: *const Fixture, a: Allocator, config: model.Config, peft_config: peft.Config, original: []const run.Parameter, limits: peft.Limits) !layouts.Layout {
        if (!builtin.is_test) @compileError("Synthetic adapter layouts are test-only");
        try peft.validateConfig(peft_config, limits);
        if (self.capture.size_bytes == 0 or self.tensors.size_bytes == 0 or self.initial.len == 0 or self.initial.len > limits.max_adapters * 3) return error.InvalidSyntheticAdapterFixture;
        for (self.initial, 0..) |parameter, index| {
            if (parameter.kind != .adapter or !std.mem.eql(u8, parameter.name, parameter.canonical_name) or parameter.name.len > 1024 or
                (index != 0 and std.mem.order(u8, self.initial[index - 1].name, parameter.name) != .lt)) return error.InvalidSyntheticAdapterFixture;
            for (parameter.values) |value| if (!std.math.isFinite(value)) return error.InvalidSyntheticAdapterFixture;
        }
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        const seen = try scratch.alloc(bool, self.initial.len);
        @memset(seen, false);
        const matched = try scratch.alloc(bool, peft_config.targets.len);
        @memset(matched, false);
        var modules = std.ArrayListUnmanaged(layouts.Descriptor).empty;
        var parameter_bytes: usize = 0;
        for (original) |base| {
            if (base.kind != .original) return error.InvalidSyntheticAdapterFixture;
            if (base.dimensions.len != 2 or !std.mem.endsWith(u8, base.canonical_name, ".weight")) continue;
            if (!std.mem.endsWith(u8, base.name, ".weight")) return error.InvalidSyntheticAdapterFixture;
            const canonical = base.canonical_name[0 .. base.canonical_name.len - ".weight".len];
            const native_name = base.name[0 .. base.name.len - ".weight".len];
            var selected = false;
            for (peft_config.targets) |alias| selected = selected or peft.matchesModule(native_name, canonical, alias);
            if (!selected) continue;
            // Source create_mlp has no Dropout node at p=0. Its final classifier
            // Linear is index2; qualify that known topology against index3's
            // published Linear identity, then retain the exact captured name.
            const whitelist_name = if (config.head.dropout == 0 and std.mem.eql(u8, canonical, "classifier.2")) "classifier.3" else canonical;
            var target = artifacts.expectedTarget(scratch, config.backbone, whitelist_name, peft_config.kind == .dora) catch |err| switch (err) {
                error.InvalidBoundaryAdapterTarget => continue,
                else => return err,
            };
            if (base.dimensions[0] <= 0 or base.dimensions[1] <= 0 or base.dimensions[0] > 4096 or base.dimensions[1] > 4096) return error.InvalidSyntheticAdapterFixture;
            target.out_dim = @intCast(base.dimensions[0]);
            target.in_dim = @intCast(base.dimensions[1]);
            if (base.values.len != try std.math.mul(usize, target.in_dim, target.out_dim)) return error.InvalidSyntheticAdapterFixture;
            target.module = try scratch.dupe(u8, canonical);
            target.base_weight = try scratch.dupe(u8, base.canonical_name);
            target.a_key = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_A.weight", .{canonical});
            target.b_key = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_B.weight", .{canonical});
            target.magnitude_key = if (peft_config.kind == .dora) try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_magnitude_vector", .{canonical}) else null;
            const descriptor = layouts.Descriptor{
                .target = target,
                .base_name = try scratch.dupe(u8, base.name),
                .a_name = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_A.default.weight", .{canonical}),
                .b_name = try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_B.default.weight", .{canonical}),
                .magnitude_name = if (peft_config.kind == .dora) try std.fmt.allocPrint(scratch, "base_model.model.{s}.lora_magnitude_vector.default.weight", .{canonical}) else null,
                .rank = peft_config.rank,
                .scale = peft_config.alpha / @as(f32, @floatFromInt(peft_config.rank)),
            };
            const av = try consume(self.initial, seen, descriptor.a_name, &.{ @intCast(peft_config.rank), @intCast(target.in_dim) });
            const bv = try consume(self.initial, seen, descriptor.b_name, &.{ @intCast(target.out_dim), @intCast(peft_config.rank) });
            var elements = try std.math.add(usize, av.values.len, bv.values.len);
            if (descriptor.magnitude_name) |name| {
                const magnitude = try consume(self.initial, seen, name, &.{@intCast(target.out_dim)});
                for (magnitude.values) |value| if (value < 0) return error.InvalidSyntheticAdapterFixture;
                elements = try std.math.add(usize, elements, magnitude.values.len);
            }
            parameter_bytes = try std.math.add(usize, parameter_bytes, try std.math.mul(usize, elements, 4));
            if (parameter_bytes > limits.max_adapter_bytes or modules.items.len >= limits.max_adapters) return error.BoundaryPeftLimitExceeded;
            try modules.append(scratch, descriptor);
            for (peft_config.targets, matched) |alias, *hit| hit.* = hit.* or peft.matchesModule(native_name, canonical, alias);
        }
        for (seen) |hit| if (!hit) return error.InvalidSyntheticAdapterFixture;
        for (matched) |hit| if (!hit) return error.BoundaryPeftTargetNotResolved;
        std.mem.sort(layouts.Descriptor, modules.items, {}, struct {
            fn lessThan(_: void, lhs: layouts.Descriptor, rhs: layouts.Descriptor) bool {
                return std.mem.order(u8, lhs.target.module, rhs.target.module) == .lt;
            }
        }.lessThan);
        const aliases = try scratch.alloc([]const u8, peft_config.targets.len);
        for (peft_config.targets, aliases) |source, *target| target.* = try scratch.dupe(u8, source);
        var owned_config = peft_config;
        owned_config.targets = aliases;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-gliner25-synthetic-test-initial-layout/v1\x00");
        hash.update(&self.capture.sha256);
        hash.update(&self.tensors.sha256);
        const metadata = try std.json.Stringify.valueAlloc(scratch, .{ .model = config, .peft = owned_config }, .{});
        hash.update(metadata);
        for (self.initial) |parameter| {
            hash.update(parameter.name);
            for (parameter.dimensions) |dimension| {
                var bytes: [4]u8 = undefined;
                std.mem.writeInt(i32, &bytes, dimension, .little);
                hash.update(&bytes);
            }
            hash.update(std.mem.sliceAsBytes(parameter.values));
        }
        return .{ .arena = arena, .backbone = config.backbone, .config = owned_config, .modules = modules.items, .fingerprint = hash.finalResult(), .parameter_bytes = parameter_bytes, .slot_count = self.initial.len };
    }
};

fn find(parameters: []const run.Parameter, name: []const u8) !run.Parameter {
    for (parameters) |parameter| if (std.mem.eql(u8, name, parameter.name)) return parameter;
    return error.InvalidSyntheticAdapterFixture;
}
fn consume(parameters: []const run.Parameter, seen: []bool, name: []const u8, dimensions: []const i32) !run.Parameter {
    for (parameters, seen) |parameter, *hit| if (std.mem.eql(u8, name, parameter.name)) {
        if (hit.* or !std.mem.eql(i32, parameter.dimensions, dimensions)) return error.InvalidSyntheticAdapterFixture;
        var elements: usize = 1;
        for (dimensions) |dimension| elements = try std.math.mul(usize, elements, @intCast(dimension));
        if (elements != parameter.values.len) return error.InvalidSyntheticAdapterFixture;
        hit.* = true;
        return parameter;
    };
    return error.InvalidSyntheticAdapterFixture;
}

fn testLayout(a: Allocator, dora: bool) !void {
    const base0 = [_]f32{1} ** 32;
    const base2 = [_]f32{1} ** 8;
    const initial_a0 = [_]f32{0.2} ** 8;
    const initial_b0 = [_]f32{0} ** 16;
    const initial_m0 = [_]f32{2} ** 8;
    const initial_a2 = [_]f32{0.1} ** 16;
    const initial_b2 = [_]f32{0} ** 2;
    const initial_m2 = [_]f32{2.828427};
    const original = [_]run.Parameter{
        .{ .name = "classifier.0.weight", .canonical_name = "classifier.0.weight", .dimensions = &.{ 8, 4 }, .values = &base0, .kind = .original },
        .{ .name = "classifier.2.weight", .canonical_name = "classifier.2.weight", .dimensions = &.{ 1, 8 }, .values = &base2, .kind = .original },
    };
    var parameters: [6]run.Parameter = undefined;
    var count: usize = 0;
    const names = [_][]const u8{
        "base_model.model.classifier.0.lora_A.default.weight",
        "base_model.model.classifier.0.lora_B.default.weight",
        "base_model.model.classifier.0.lora_magnitude_vector.default.weight",
        "base_model.model.classifier.2.lora_A.default.weight",
        "base_model.model.classifier.2.lora_B.default.weight",
        "base_model.model.classifier.2.lora_magnitude_vector.default.weight",
    };
    const dimensions = [_][]const i32{ &.{ 2, 4 }, &.{ 8, 2 }, &.{8}, &.{ 2, 8 }, &.{ 1, 2 }, &.{1} };
    const values_ = [_][]const f32{ &initial_a0, &initial_b0, &initial_m0, &initial_a2, &initial_b2, &initial_m2 };
    for (names, dimensions, values_, 0..) |name, dims, input, index| {
        if (!dora and (index == 2 or index == 5)) continue;
        parameters[count] = .{ .name = name, .canonical_name = name, .dimensions = dims, .values = input, .kind = .adapter };
        count += 1;
    }
    const source = Fixture{ .capture = bundle.Digest.of("tiny source capture"), .tensors = bundle.Digest.of("tiny source tensors"), .initial = parameters[0..count] };
    const config = @import("gliner_boundary_train_step_test.zig").config();
    var layout = try source.layout(a, config, .{ .kind = if (dora) .dora else .lora, .rank = 2, .alpha = 3, .targets = &.{"classification_head"} }, &original, .{});
    defer layout.deinit();
    try std.testing.expectEqual(@as(usize, 2), layout.modules.len);
    try std.testing.expectEqual(count, layout.slot_count);
    try std.testing.expectEqualStrings("classifier.2", layout.modules[1].target.module);
    for (layout.modules) |descriptor| {
        var initialized = try source.initialFor(a, descriptor);
        defer initialized.deinit();
        try std.testing.expectEqualSlices(f32, (try find(source.initial, descriptor.a_name)).values, initialized.a);
        try std.testing.expectEqualSlices(f32, (try find(source.initial, descriptor.b_name)).values, initialized.b);
        if (descriptor.magnitude_name) |name| try std.testing.expectEqualSlices(f32, (try find(source.initial, name)).values, initialized.magnitude.?);
    }
}

test "boundary synthetic adapter fixture exact target shapes initial slots and all allocation cleanup" {
    for ([_]bool{ false, true }) |dora| try std.testing.checkAllAllocationFailures(std.testing.allocator, testLayout, .{dora});
}

test "boundary synthetic adapter fixture rejects missing reordered nonfinite and substituted initial descriptors" {
    const a = std.testing.allocator;
    const config = @import("gliner_boundary_train_step_test.zig").config();
    const original = [_]run.Parameter{.{ .name = "classifier.0.weight", .canonical_name = "classifier.0.weight", .dimensions = &.{ 2, 2 }, .values = &.{ 1, 1, 1, 1 }, .kind = .original }};
    const names = [_][]const u8{ "base_model.model.classifier.0.lora_A.default.weight", "base_model.model.classifier.0.lora_B.default.weight" };
    const config_peft = peft.Config{ .rank = 1, .targets = &.{"classification_head"} };
    const valid = [_]run.Parameter{
        .{ .name = names[0], .canonical_name = names[0], .dimensions = &.{ 1, 2 }, .values = &.{ 1, 1 }, .kind = .adapter },
        .{ .name = names[1], .canonical_name = names[1], .dimensions = &.{ 2, 1 }, .values = &.{ 0, 0 }, .kind = .adapter },
    };
    var source = Fixture{ .capture = bundle.Digest.of("capture"), .tensors = bundle.Digest.of("tensors"), .initial = &valid };
    var baseline = try source.layout(a, config, config_peft, &original, .{});
    defer baseline.deinit();
    for (0..5) |kind| {
        var bad = valid;
        switch (kind) {
            0 => source.initial = bad[0..1],
            1 => {
                std.mem.swap(run.Parameter, &bad[0], &bad[1]);
                source.initial = &bad;
            },
            2 => {
                bad[0].dimensions = &.{ 2, 1 };
                source.initial = &bad;
            },
            3 => {
                bad[0].values = &.{ std.math.nan(f32), 1 };
                source.initial = &bad;
            },
            4 => {
                bad[0].name = "base_model.model.classifier.99.lora_A.default.weight";
                bad[0].canonical_name = bad[0].name;
                source.initial = &bad;
            },
            else => unreachable,
        }
        try std.testing.expectError(error.InvalidSyntheticAdapterFixture, source.layout(a, config, config_peft, &original, .{}));
    }
    source.initial = &valid;
    source.capture = bundle.Digest.of("different captured initialization");
    var changed = try source.layout(a, config, config_peft, &original, .{});
    defer changed.deinit();
    try std.testing.expect(!std.mem.eql(u8, &baseline.fingerprint, &changed.fingerprint));
}
