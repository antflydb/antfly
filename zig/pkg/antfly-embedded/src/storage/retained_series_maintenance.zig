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

//! Bounded, fair maintenance selection over already admitted source bindings.
//! Scheduling state is disposable: every chosen operation is revalidated and
//! committed by the owner leader through the replicated lifecycle protocol.
const std = @import("std");
const series = @import("retained_series.zig");

pub const Entry = struct { spec: series.Spec, progress: series.Progress };
pub const Task = struct {
    index: usize,
    action: enum { backfill, tick, cleanup },
    expected_cursor: []const u8 = "",
    work_limit: u16,
};
pub const Planner = struct {
    next_slot: usize = 0,
    tick_interval_ns: u64 = std.time.ns_per_s,
    work_limit: u16 = 128,

    /// At most one command per turn and 24 inspections, without allocation.
    /// Backfill, retention and marker reclamation share the same fair rotation;
    /// a hot source or a held bucket cannot starve another definition's work.
    pub fn next(self: *Planner, entries: []const Entry, admitted_ns: i128) !?Task {
        if (entries.len > 8 or self.tick_interval_ns == 0 or self.work_limit == 0 or self.work_limit > series.max_changes) return error.InvalidRetainedSeries;
        if (entries.len == 0) return null;
        const slots = entries.len * 3;
        for (0..slots) |_| {
            const slot = self.next_slot % slots;
            self.next_slot = (slot + 1) % slots;
            const index = slot / 3;
            const entry = entries[index];
            try entry.spec.validate();
            if (!std.mem.eql(u8, &entry.spec.definition, &entry.progress.definition) or entry.spec.source_incarnation != entry.progress.source_incarnation) return error.RetainedSeriesDefinitionMismatch;
            switch (slot % 3) {
                0 => if (!entry.progress.ready) return .{ .index = index, .action = .backfill, .expected_cursor = entry.progress.backfill_cursor, .work_limit = self.work_limit },
                1 => {
                    const now = @max(admitted_ns, entry.progress.admitted_ns);
                    if (entry.progress.maintenance_pending or now -| entry.progress.last_tick_ns >= self.tick_interval_ns)
                        return .{ .index = index, .action = .tick, .work_limit = self.work_limit };
                },
                2 => if (entry.progress.ready and !entry.progress.backfill_clean) return .{ .index = index, .action = .cleanup, .work_limit = self.work_limit },
                else => unreachable,
            }
        }
        return null;
    }
};

test "retained series maintenance rotation bounds hot-source work and respects durable cleanup completion" {
    const spec: series.Spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{.count} };
    var entries = [_]Entry{
        .{ .spec = spec, .progress = .{ .definition = spec.definition, .source_incarnation = 1, .source_sequence = 0, .admitted_ns = 0, .complete_from_ns = 0, .backfill_cursor = "page" } },
        .{ .spec = spec, .progress = .{ .definition = spec.definition, .source_incarnation = 1, .source_sequence = 0, .admitted_ns = 0, .complete_from_ns = 0, .ready = true, .maintenance_pending = true } },
    };
    var planner: Planner = .{ .tick_interval_ns = 100 };
    const first = (try planner.next(&entries, 100)).?;
    try std.testing.expect(first.action == .backfill);
    try std.testing.expectEqualStrings("page", first.expected_cursor);
    try std.testing.expectEqual(@as(usize, 0), first.index);
    const second = (try planner.next(&entries, 100)).?;
    try std.testing.expect(second.action == .tick and second.index == 0);
    const third = (try planner.next(&entries, 100)).?;
    try std.testing.expect(third.action == .tick and third.index == 1);
    const fourth = (try planner.next(&entries, 100)).?;
    try std.testing.expect(fourth.action == .cleanup and fourth.index == 1);
    for (&entries) |*entry| {
        entry.progress.ready = true;
        entry.progress.backfill_clean = true;
        entry.progress.maintenance_pending = false;
        entry.progress.last_tick_ns = 100;
    }
    try std.testing.expectEqual(@as(?Task, null), try planner.next(&entries, 199));
    const due = (try planner.next(&entries, 200)).?;
    try std.testing.expect(due.action == .tick);
    // A source's committed clock is a floor even after a leader clock regresses.
    entries[due.index].progress.admitted_ns = 1000;
    entries[1 - due.index].progress.admitted_ns = 1000;
    try std.testing.expect((try planner.next(&entries, 1)).?.action == .tick);
    try std.testing.expectEqual(@as(?Task, null), try planner.next(&.{}, 1000));
    planner.work_limit = 0;
    try std.testing.expectError(error.InvalidRetainedSeries, planner.next(&entries, 1000));
}
