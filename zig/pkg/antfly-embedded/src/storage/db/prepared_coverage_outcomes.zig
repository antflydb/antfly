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

//! Owned desired local coverage outcomes. Mutable markers and count sidecars
//! are resolved in the final writer, never pinned by the preparation read set.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const admission = @import("../../common/read_admission.zig");
const readiness = @import("artifact_producer_readiness.zig");
pub const Outcome = @import("prepared_effect_types.zig").DerivedCoverageOutcome;
const tags = std.meta.tags(Outcome);
const A = std.mem.Allocator;
fn checkpoint(original: ?admission.Port) !void {
    if (original) |owner| try owner.checkpoint();
}
pub fn priority(outcome: Outcome) u8 {
    return switch (outcome) {
        .skipped => 0,
        .produced => 1,
        .terminal_failed => 2,
    };
}
pub const Desired = struct {
    marker: []u8,
    receipt: ?[]u8,
    outcome: Outcome,
    sequence: u64,
    fn deinit(self: *Desired, a: A) void {
        a.free(self.marker);
        if (self.receipt) |key| a.free(key);
    }
};
pub const Group = struct {
    name: []u8,
    generation: u64,
    prefix: []u8,
    upper: []u8,
    counters: [tags.len][]u8,
    desired: std.ArrayListUnmanaged(Desired) = .empty,
    fn init(a: A, name: []const u8, generation: u64) !Group {
        const owned_name = try a.dupe(u8, name);
        errdefer a.free(owned_name);
        const prefix = try keys.derivedCoverageOutcomeMarkerPrefixAlloc(a, name, generation);
        errdefer a.free(prefix);
        const upper = (try keys.nextPrefixAlloc(a, prefix)) orelse return error.InvalidDerivedCoverageOutcomeKey;
        errdefer a.free(upper);
        var counters: [tags.len][]u8 = undefined;
        var initialized: usize = 0;
        errdefer for (counters[0..initialized]) |key| a.free(key);
        inline for (tags, 0..) |outcome, i| {
            counters[i] = try keys.derivedCoverageOutcomeCountKeyAlloc(a, name, generation, @tagName(outcome));
            initialized += 1;
        }
        return .{ .name = owned_name, .generation = generation, .prefix = prefix, .upper = upper, .counters = counters };
    }
    fn deinit(self: *Group, a: A) void {
        a.free(self.name);
        a.free(self.prefix);
        a.free(self.upper);
        for (self.counters) |key| a.free(key);
        for (self.desired.items) |*entry| entry.deinit(a);
        self.desired.deinit(a);
    }
};
pub const Program = struct {
    allocator: A,
    poisoned: bool = false,
    groups: std.ArrayListUnmanaged(Group) = .empty,
    by_name: std.StringHashMapUnmanaged(usize) = .empty,
    by_marker: std.StringHashMapUnmanaged(struct { group: usize, entry: usize }) = .empty,
    pub fn init(a: A) Program {
        return .{ .allocator = a };
    }
    pub fn deinit(self: *Program) void {
        for (self.groups.items) |*group| group.deinit(self.allocator);
        self.groups.deinit(self.allocator);
        self.by_name.deinit(self.allocator);
        self.by_marker.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn append(self: *Program, name: []const u8, generation: u64, document: []const u8, desired: Outcome, sequence: u64, original: ?admission.Port) !void {
        errdefer self.poisoned = true;
        if (self.poisoned) return error.InvalidPreparedEffects;
        try checkpoint(original);
        const a = self.allocator;
        const selected = if (self.by_name.get(name)) |index| blk: {
            if (self.groups.items[index].generation != generation) return error.PreparedGenerationChanged;
            break :blk index;
        } else blk: {
            var group = try Group.init(a, name, generation);
            errdefer group.deinit(a);
            try self.groups.ensureUnusedCapacity(a, 1);
            try self.by_name.put(a, group.name, self.groups.items.len);
            const index = self.groups.items.len;
            self.groups.appendAssumeCapacity(group);
            break :blk index;
        };
        const marker = try keys.derivedCoverageOutcomeKeyAlloc(a, name, generation, document);
        errdefer a.free(marker);
        if (self.by_marker.get(marker)) |position| {
            const previous = &self.groups.items[position.group].desired.items[position.entry];
            if (previous.sequence != sequence) return error.PreparedEffectsMismatch;
            if (priority(desired) > priority(previous.outcome)) previous.outcome = desired;
            a.free(marker);
            return;
        }
        const receipt = if (!std.mem.eql(u8, name, readiness.localScopeIndex(name))) try readiness.localReceiptRevisionKeyAlloc(a, marker) else null;
        errdefer if (receipt) |key| a.free(key);
        try checkpoint(original);
        const group = &self.groups.items[selected];
        try group.desired.ensureUnusedCapacity(a, 1);
        try self.by_marker.put(a, marker, .{ .group = selected, .entry = group.desired.items.len });
        group.desired.appendAssumeCapacity(.{ .marker = marker, .receipt = receipt, .outcome = desired, .sequence = sequence });
    }
    /// The owner aborts the writer on error. The immutable program can be
    /// retried after abort, but must not be applied after a terminal decision.
    pub fn apply(self: *const Program, writer: anytype, original: ?admission.Port) !void {
        if (self.poisoned) return error.InvalidPreparedEffects;
        var trace: admission.RejectionTrace = .{};
        self.applyInner(writer, trace.bind(original)) catch |cause| return trace.failure orelse cause;
        if (trace.failure) |cause| return cause;
    }
    fn applyInner(self: *const Program, writer: anytype, original: ?admission.Port) !void {
        for (self.groups.items) |group| {
            if (group.desired.items.len == 0) continue;
            var counts: [tags.len]u64 = @splat(0);
            var missing: [tags.len]bool = @splat(false);
            inline for (group.counters, 0..) |key, i| {
                try checkpoint(original);
                const raw = writer.get(key) catch |cause| if (cause == error.NotFound) null else return cause;
                if (raw) |value| counts[i] = try keys.decodeDerivedCoverageOutcomeCount(value) else missing[i] = true;
            }
            if (std.mem.indexOfScalar(bool, &missing, true) != null) {
                // Legacy sidecars are reconstructed from this same writer view,
                // before any desired markers are staged. One bounded-prefix
                // cursor fills every missing lane without retaining row bytes.
                var cursor = try writer.openCursor();
                defer cursor.close();
                cursor.setUpperBound(group.upper);
                var row = if (original) |owner| try cursor.seekAtOrAfterWithAdmission(group.prefix, owner) else try cursor.seekAtOrAfter(group.prefix);
                while (row) |entry| : (row = if (original) |owner| try cursor.nextWithAdmission(owner) else try cursor.next()) {
                    try checkpoint(original);
                    if (!std.mem.startsWith(u8, entry.key, group.prefix)) break;
                    if (std.meta.stringToEnum(Outcome, entry.value)) |outcome| {
                        const lane = @backingInt(outcome);
                        if (missing[lane]) counts[lane] = std.math.add(u64, counts[lane], 1) catch return error.InvalidDerivedCoverageCounter;
                    }
                }
            }
            var changed = false;
            for (group.desired.items) |entry| {
                try checkpoint(original);
                if (entry.receipt) |key| {
                    var revision: [8]u8 = undefined;
                    std.mem.writeInt(u64, &revision, entry.sequence, .little);
                    try writer.put(key, &revision);
                }
                const raw = writer.get(entry.marker) catch |cause| if (cause == error.NotFound) null else return cause;
                const previous: ?Outcome = if (raw) |value| std.meta.stringToEnum(Outcome, value) orelse return error.InvalidDerivedCoverageOutcome else null;
                if (previous == entry.outcome) continue;
                if (previous) |old| {
                    const lane = @backingInt(old);
                    if (counts[lane] == 0) return error.InvalidDerivedCoverageCounter;
                    counts[lane] -= 1;
                }
                counts[@backingInt(entry.outcome)] +|= 1;
                try writer.put(entry.marker, @tagName(entry.outcome));
                changed = true;
            }
            if (changed) inline for (group.counters, 0..) |key, i| {
                try checkpoint(original);
                var raw: [8]u8 = undefined;
                std.mem.writeInt(u64, &raw, counts[i], .little);
                try writer.put(key, &raw);
            };
        }
        try checkpoint(original);
    }
};

test "coverage outcome program owns all construction allocations and original checkpoint rejections" {
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(a: A, checks: ?*@This()) !void {
            var source: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            const original: ?admission.Port = if (checks != null) .init(&source) else null;
            var program = Program.init(a);
            defer program.deinit();
            try program.append("vector", 42, "row\x00binary", .skipped, 19, original);
            try program.append("vector", 42, "row\x00binary", .produced, 19, original);
            try program.append("vector", 42, "row\x00binary", .terminal_failed, 19, original);
            try program.append("vector", 42, "row\x00binary", .skipped, 19, original);
            try program.append("vector\x00producer-source:body", 42, "row", .produced, 19, original);
            try std.testing.expectEqual(@as(usize, 2), program.groups.items.len);
            try std.testing.expectEqual(@as(usize, 1), program.groups.items[0].desired.items.len);
            try std.testing.expectEqual(Outcome.terminal_failed, program.groups.items[0].desired.items[0].outcome);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(std.testing.allocator, &checks));
    };
}

test "coverage outcome program rejects changed generation sequence and reuse after construction failure" {
    for ([_]bool{ false, true }) |generation| {
        var program = Program.init(std.testing.allocator);
        defer program.deinit();
        try program.append("vector", 42, "row", .produced, 19, null);
        try std.testing.expectError(if (generation) error.PreparedGenerationChanged else error.PreparedEffectsMismatch, program.append("vector", if (generation) 43 else 42, "row", .produced, if (generation) 19 else 20, null));
        try std.testing.expect(program.poisoned);
        try std.testing.expectError(error.InvalidPreparedEffects, program.append("vector", 42, "other", .produced, 19, null));
    }
}
