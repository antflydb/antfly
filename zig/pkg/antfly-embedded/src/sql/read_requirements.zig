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

//! Shared freshness requirements for search, HTTP SQL, and PostgreSQL sessions.
const std = @import("std");
pub const Visibility = enum { accepted, committed, published };
pub const Receipt = struct { table_id: u64, object_generation: u64, wal_lsn: u64 };
pub const Requirements = struct {
    visibility: ?Visibility = null,
    after_write: ?Receipt = null,
    wait_ms: ?u32 = null,

    pub fn validate(self: Requirements, surface: enum { search, sql }) !void {
        if (self.wait_ms) |wait| if (wait > 60000) return error.InvalidReadRequirements;
        if (self.visibility) |visibility| {
            if ((surface == .search and visibility == .committed) or (surface == .sql and visibility == .published)) return error.UnsupportedReadVisibility;
        }
    }
    pub fn fromWire(wire: anytype) !Requirements {
        const result: Requirements = .{
            .visibility = if (wire.visibility) |value| std.meta.stringToEnum(Visibility, value) orelse return error.InvalidReadRequirements else null,
            .after_write = if (wire.after_write) |receipt| .{ .table_id = receipt.table_id, .object_generation = receipt.object_generation, .wal_lsn = receipt.wal_lsn } else null,
            .wait_ms = wire.wait_ms,
        };
        return result;
    }
    /// A transaction's first read cut cannot advance to satisfy a later receipt.
    pub fn requireCoverage(self: Requirements, table_id: u64, generation: u64, lsn: u64, pinned: bool) !void {
        const receipt = self.after_write orelse return;
        if (receipt.table_id != table_id or receipt.object_generation != generation) return error.ReadReceiptGenerationMismatch;
        if (lsn < receipt.wal_lsn) return if (pinned) error.ReadSnapshotTooOld else error.ReadRequirementNotReady;
    }
};

test "read requirements distinguish visibility, receipt generations and pinned cuts" {
    try (Requirements{ .visibility = .accepted }).validate(.sql);
    try (Requirements{ .visibility = .accepted }).validate(.search);
    try std.testing.expectError(error.UnsupportedReadVisibility, (Requirements{ .visibility = .published }).validate(.sql));
    try std.testing.expectError(error.UnsupportedReadVisibility, (Requirements{ .visibility = .committed }).validate(.search));
    try std.testing.expectError(error.InvalidReadRequirements, (Requirements{ .wait_ms = 60001 }).validate(.sql));
    const read: Requirements = .{ .after_write = .{ .table_id = 2, .object_generation = 3, .wal_lsn = 10 } };
    try read.requireCoverage(2, 3, 10, false);
    try std.testing.expectError(error.ReadRequirementNotReady, read.requireCoverage(2, 3, 9, false));
    try std.testing.expectError(error.ReadSnapshotTooOld, read.requireCoverage(2, 3, 9, true));
    try std.testing.expectError(error.ReadReceiptGenerationMismatch, read.requireCoverage(2, 4, 10, false));
}

pub const WaitBudget = struct {
    deadline_ns: ?u64 = null,

    pub fn deadline(self: *WaitBudget, now: u64, wait_ms: u32) u64 {
        // Zero still permits one check under the caller's I/O deadline.
        if (wait_ms == 0) return now;
        if (self.deadline_ns == null) self.deadline_ns = now +| @as(u64, wait_ms) * std.time.ns_per_ms;
        return self.deadline_ns.?;
    }
};
