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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BatchResult = struct { start_lsn: u64, end_lsn: u64 };

pub const Record = struct {
    lsn: u64,
    timestamp_ns: u64,
    payload: []u8,
    /// Durable request identity when the producer used idempotent append.
    /// Legacy records and stores that do not support that protocol leave it
    /// null.
    operation_id: ?[]u8 = null,
};

/// Immutable identities from a freshly captured WAL HEAD. Parent identity is
/// published by the same atomic append, never supplied by a reader cache.
pub const ReadSnapshot = struct {
    latest_lsn: u64,
    first_lsn: u64,
    identity: [32]u8,
    lineage: [32]u8,
    parent_identity: ?[32]u8 = null,
    parent_lsn: u64 = 0,
    small_tail: bool = false,
};
pub const PointRead = struct { indexed: bool = false, record: ?Record = null };

/// A captured, inclusive committed prefix. Exceeding any budget fails the
/// entire read; consumers must never mistake a truncated page for fresh state.
pub const ReadRange = struct {
    start_lsn: u64,
    end_lsn: u64,
    max_records: usize = 4096,
    max_bytes: usize = 8 * 1024 * 1024,
    cancellation: ?@import("objectstore").CancellationToken = null,

    pub fn check(self: @This()) !void {
        if (self.cancellation) |token| try token.check();
    }
};

pub fn freeRecords(alloc: Allocator, records: []Record) void {
    for (records) |record| {
        alloc.free(record.payload);
        if (record.operation_id) |operation_id| alloc.free(operation_id);
    }
    alloc.free(records);
}

test "freeRecords releases payload storage" {
    const alloc = std.testing.allocator;
    const records = try alloc.alloc(Record, 1);
    records[0] = .{
        .lsn = 1,
        .timestamp_ns = 2,
        .payload = try alloc.dupe(u8, "payload"),
    };
    freeRecords(alloc, records);
}
