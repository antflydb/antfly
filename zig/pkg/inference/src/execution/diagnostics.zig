// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Optional snapshots. Disabled execution starts no observation scopes or
//! provider sampling. An explicitly enabled allocation observer stays installed
//! until owner retirement so escaped allocations keep a valid release path.
//! Snapshots never expose mutable backend owners.
const std = @import("std");
const ops = @import("../ops/ops.zig");
pub const ExecutionStats = @import("stats.zig").ExecutionStats;
pub const Specializations = @import("specialization_cache.zig").Stats;
const observation = @import("runtime_observation.zig");
pub const Start = struct { runtime: ops.RuntimeTelemetry, scope: observation.Scope };
pub const Snapshot = struct { specializations: ?Specializations = null, execution: ExecutionStats = .{}, provider: ops.NativeQuantTimingStats = .{}, runtime: ops.RuntimeTelemetry = .{} };
pub const Diagnostics = struct {
    enabled: std.atomic.Value(bool) = .init(false),
    mutex: std.atomic.Mutex = .unlocked,
    last: ?Snapshot = null,

    pub fn configure(self: *Diagnostics, enabled: bool) void {
        const previous = self.enabled.swap(enabled, .acq_rel);
        if (previous and !enabled) {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            self.last = null;
        }
    }
    pub fn isEnabled(self: *const Diagnostics) bool {
        return self.enabled.load(.acquire);
    }
    pub fn snapshot(self: *Diagnostics) ?Snapshot {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.last;
    }
    pub fn publish(self: *Diagnostics, value: Snapshot) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.isEnabled()) self.last = value;
    }
    pub fn recordSpecializations(self: *Diagnostics, stats: Specializations) void {
        if (!self.isEnabled()) return;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.isEnabled()) if (self.last) |*last| {
            last.specializations = stats;
        };
    }
    pub fn begin(self: *const Diagnostics, backend: *const ops.ComputeBackend) ?Start {
        if (!self.isEnabled()) return null;
        backend.enableAllocationObservation();
        return .{ .scope = observation.Scope.init(), .runtime = backend.debugTimingSnapshot().runtime };
    }
    /// Every caller defers this immediately after begin, including error exits.
    pub fn end(before: ?Start) void {
        if (before) |value| {
            var scope = value.scope;
            scope.deinit();
        }
    }
    pub fn record(self: *Diagnostics, stats: ExecutionStats, backend: *const ops.ComputeBackend, before: ?Start) void {
        if (!self.isEnabled()) return;
        const after = backend.debugTimingSnapshot();
        var runtime = if (before) |start| after.runtime.since(start.runtime) else ops.RuntimeTelemetry{};
        if (before) |start| runtime.submission = start.scope.snapshot();
        self.publish(.{ .execution = stats, .provider = after.provider, .runtime = runtime });
    }
};

test "native execution observations include nested work and unwind every failed invocation" {
    const a = std.testing.allocator;
    const ml = @import("ml");
    const contexts = @import("../backends/compute_context.zig");
    const programs = @import("program.zig");
    const executables = @import("executable.zig");
    var context = try contexts.BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.relu(input));
    var program = try programs.ModelProgram.init(a, &graph, &.{});
    defer program.deinit();
    var executable = try executables.prepare(a, &program, &backend, .compiled_preferred);
    defer executable.deinit();
    executable.diagnostics.configure(true);
    const value = try backend.fromFloat32Shape(&.{ -1, 2, -3, 4 }, &.{4});
    defer backend.free(value);
    const inputs = [_]@import("../graph/interpreter.zig").RuntimeInput{.{ .node_id = program.id_map[input], .value = value }};
    var outer = observation.Scope.init();
    defer outer.deinit();
    for (0..2) |_| {
        var result = try executable.run(a, .{ .runtime_inputs = &inputs, .collect_partition_stats = true });
        defer result.deinit(&executable.runtime);
        const measured = executable.diagnostics.snapshot().?;
        try std.testing.expect(measured.runtime.submission != null);
        const observed = measured.runtime.native_allocations.?.compute.?;
        try std.testing.expectEqual(@import("allocation_observer.zig").Coverage.since_enable, observed.coverage);
        try std.testing.expect(observed.counter_scope == .owner_interval);
        try std.testing.expectEqual(result.stats.assigned_kernel_dispatches, measured.runtime.submission.?.native_assigned_dispatches);
        try std.testing.expect(measured.runtime.submission.?.native_assigned_dispatches > 0);
        const values = try backend.toFloat32(result.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 0, 2, 0, 4 }, values);
    }
    try std.testing.expectEqual(@as(u64, 2), outer.snapshot().native_assigned_dispatches);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, prepared: *executables.Executable, bindings: []const @import("../graph/interpreter.zig").RuntimeInput) !void {
            var result = try prepared.run(allocator, .{ .runtime_inputs = bindings, .collect_partition_stats = true });
            defer result.deinit(&prepared.runtime);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{ &executable, @as([]const @import("../graph/interpreter.zig").RuntimeInput, &inputs) });
    // Child failures must not turn off the still-live outer scope.
    const before = outer.snapshot().native_assigned_dispatches;
    executable.diagnostics.configure(false);
    var result = try executable.run(a, .{ .runtime_inputs = &inputs });
    defer result.deinit(&executable.runtime);
    try std.testing.expectEqual(before + 1, outer.snapshot().native_assigned_dispatches);
}
