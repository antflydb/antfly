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

pub const Record = struct {
    lsn: u64,
    timestamp_ns: u64,
    payload: []u8,
    /// Durable request identity when the producer used idempotent append.
    /// Legacy records and stores that do not support that protocol leave it
    /// null.
    operation_id: ?[]u8 = null,
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
