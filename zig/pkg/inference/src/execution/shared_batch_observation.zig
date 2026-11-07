// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed model-owned observation of an actual shared numerical cohort.
//! Participant records are metadata, never retained request or tensor pointers.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const diagnostics = @import("diagnostics.zig");
const invocation = @import("invocation_observation.zig");
var next_identity: std.atomic.Value(u64) = .init(1);

pub inline fn call(binding: ?invocation.Binding, backend: ops.ComputeBackend, kind: diagnostics.Batch.Kind, items: anytype, comptime describe: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    const source = binding orelse return @call(.auto, callback, args);
    if (!source.diagnostics.isEnabled()) return @call(.auto, callback, args);
    return observed(source, backend, kind, items, describe, callback, args);
}
noinline fn observed(binding: invocation.Binding, backend: ops.ComputeBackend, kind: diagnostics.Batch.Kind, items: anytype, comptime describe: anytype, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
    var selected = binding.forBackend(backend);
    selected.capture_programs = true;
    selected.publication_scope = .shared_batch_numerical_dispatch;
    const cohort = metadata(kind, items, describe);
    selected.batch = &cohort;
    selected.operation = if (kind == .positioned_prefill) .prefill else .forward;
    return invocation.call(selected, callback, args);
}

/// A failed acquisition has no authority to sample the independently active
/// backend. Its fresh model-level cohort outcome still carries exact identity.
pub fn acquisitionFailure(binding: ?invocation.Binding, kind: diagnostics.Batch.Kind, items: anytype, comptime describe: anytype, failure: anyerror) void {
    const selected = binding orelse return;
    if (!selected.diagnostics.isEnabled()) return;
    selected.diagnostics.publish(.{
        .scope = .shared_batch_numerical_dispatch,
        .operation = if (kind == .positioned_prefill) .prefill else .forward,
        .batch = metadata(kind, items, describe),
        .outcome = .failed,
        .error_name = diagnostics.Text.init(@errorName(failure)),
    });
}
fn metadata(kind: diagnostics.Batch.Kind, items: anytype, comptime describe: anytype) diagnostics.Batch {
    var result = diagnostics.Batch{ .identity = next_identity.fetchAdd(1, .monotonic), .kind = kind, .participant_count = items.len };
    result.stored_count = @intCast(@min(items.len, result.participants.len));
    result.truncated = items.len > result.participants.len;
    for (items[0..result.stored_count], result.participants[0..result.stored_count]) |item, *slot| slot.* = describe(item);
    return result;
}

/// Every member must name the same model-owned publication capability and the
/// same numerical backend. A sink borrowed from one different request cannot
/// authorize attribution of the shared work. Legacy absent bindings stay off.
pub fn positionedBinding(items: anytype) ?invocation.Binding {
    if (items.len == 0) return null;
    const source = items[0].diagnostics_binding orelse return null;
    if (!source.diagnostics.isEnabled()) return null;
    for (items) |item| {
        const peer = item.diagnostics_binding orelse return null;
        if (peer.diagnostics != source.diagnostics or item.backend.ptr != items[0].backend.ptr or item.backend.vtable != items[0].backend.vtable) return null;
    }
    return source;
}

test "shared cohort observation is owned by model sink with exact participants and disabled zero traversal" {
    const Probe = struct {
        var descriptions: usize = 0;
        var calls: usize = 0;
        fn describe(id: u64) diagnostics.Batch.Participant {
            descriptions += 1;
            return .{ .request_id = id, .phase = .decode, .query_rows = 1, .total_rows = 5 };
        }
        fn run(fail: bool) !usize {
            calls += 1;
            if (fail) return error.SharedNumericalFailure;
            return 7;
        }
        fn kind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        const table: ops.ComputeBackend.VTable = blk: {
            var value: ops.ComputeBackend.VTable = undefined;
            value.backendKind = kind;
            value.registerRuntimeObservation = null;
            value.debugTimingSnapshot = null;
            break :blk value;
        };
    };
    Probe.descriptions = 0;
    Probe.calls = 0;
    var target: u8 = 0;
    const backend = ops.ComputeBackend{ .ptr = &target, .vtable = &Probe.table };
    var owner = diagnostics.Diagnostics{};
    const binding = invocation.Binding{ .diagnostics = &owner };
    const ids = [_]u64{ 101, 202 };
    try std.testing.expectEqual(@as(usize, 7), try call(binding, backend, .scheduled_step, &ids, Probe.describe, Probe.run, .{false}));
    try std.testing.expectEqual(@as(usize, 0), Probe.descriptions);
    owner.configure(true);
    try std.testing.expectEqual(@as(usize, 7), try call(binding, backend, .scheduled_step, &ids, Probe.describe, Probe.run, .{false}));
    const success = owner.snapshot().?;
    try std.testing.expect(success.scope == .shared_batch_numerical_dispatch);
    try std.testing.expectEqual(@as(usize, 2), success.batch.?.participant_count);
    try std.testing.expectEqual(@as(?u64, 101), success.batch.?.participants[0].request_id);
    try std.testing.expectEqual(@as(?u64, 202), success.batch.?.participants[1].request_id);
    try std.testing.expectEqualDeep(success.batch.?, success.report.?.batch.?);
    try std.testing.expect(success.report.?.pending_interval == null);
    try std.testing.expectEqual(.invocation_body, success.report.?.synchronous_interval);
    try std.testing.expectError(error.SharedNumericalFailure, call(binding, backend, .scheduled_step, &ids, Probe.describe, Probe.run, .{true}));
    const failed = owner.snapshot().?;
    try std.testing.expect(failed.outcome == .failed and failed.batch.?.identity != success.batch.?.identity);
    acquisitionFailure(binding, .scheduled_step, &ids, Probe.describe, error.Cancelled);
    const rejected = owner.snapshot().?;
    try std.testing.expect(rejected.runtime.submission == null and rejected.context_runtime_scope == .unavailable);
    try std.testing.expectEqualStrings("Cancelled", rejected.error_name.?.view());
    try std.testing.expectEqualDeep(rejected.batch.?, rejected.report.?.batch.?);
    try std.testing.expect(rejected.report.?.backend == null);
    try std.testing.expectEqual(@as(usize, 3), Probe.calls);
    var many: [40]u64 = @splat(303);
    _ = try call(binding, backend, .scheduled_step, &many, Probe.describe, Probe.run, .{false});
    many[0] = 999;
    const bounded = owner.snapshot().?.batch.?;
    try std.testing.expect(bounded.truncated);
    try std.testing.expectEqual(@as(usize, 40), bounded.participant_count);
    try std.testing.expectEqual(@as(u8, 32), bounded.stored_count);
    try std.testing.expectEqual(@as(?u64, 303), bounded.participants[0].request_id);
    const retained_report = owner.snapshot().?.report.?;
    owner.configure(false);
    try std.testing.expect(owner.snapshot() == null);
    try std.testing.expectEqualDeep(bounded, retained_report.batch.?);
    try std.testing.expect(retained_report.batch.?.truncated);
    try std.testing.expectEqual(@as(?u64, 303), retained_report.batch.?.participants[0].request_id);
}
