// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Record = struct {
    lsn: u64,
    timestamp_ns: u64,
    payload: []u8,
};

pub fn freeRecords(alloc: Allocator, records: []Record) void {
    for (records) |record| alloc.free(record.payload);
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
