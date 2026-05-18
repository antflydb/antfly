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
const db_types = @import("../storage/db/types.zig");
const read_gate = @import("read_gate.zig");

pub const FeatureReads = struct {
    gate: read_gate.EnrichmentReadGate,

    pub fn init(requester: read_gate.ReadableLeaseRequester) FeatureReads {
        return .{ .gate = read_gate.EnrichmentReadGate.init(requester) };
    }

    pub fn prepareSearchWithConsistency(
        self: FeatureReads,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.gate.prepareSearch(group_id, req, consistency);
    }

    pub fn prepareSearch(self: FeatureReads, group_id: u64, req: db_types.SearchRequest) !void {
        try self.prepareSearchWithConsistency(group_id, req, .read_index);
    }

    pub fn prepareLookupWithConsistency(
        self: FeatureReads,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.gate.prepareLookup(group_id, key, opts, consistency);
    }

    pub fn prepareLookup(self: FeatureReads, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.prepareLookupWithConsistency(group_id, key, opts, .read_index);
    }

    pub fn prepareScanWithConsistency(
        self: FeatureReads,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.gate.prepareScan(group_id, from_key, to_key, opts, consistency);
    }

    pub fn prepareScan(
        self: FeatureReads,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.prepareScanWithConsistency(group_id, from_key, to_key, opts, .read_index);
    }
};

test "feature reads facade forwards typed requests with explicit consistency" {
    const Recorder = struct {
        requester_count: usize = 0,

        fn requester(self: *@This()) read_gate.ReadableLeaseRequester {
            return .{
                .ptr = self,
                .vtable = &.{
                    .request_readable_lease = requestReadableLease,
                },
            };
        }

        fn requestReadableLease(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.requester_count += 1;
        }
    };

    var recorder = Recorder{};
    const reads = FeatureReads.init(recorder.requester());
    try reads.prepareSearchWithConsistency(1, .{}, .stale);
    try std.testing.expectEqual(@as(usize, 0), recorder.requester_count);
    try reads.prepareLookupWithConsistency(1, "doc:a", .{}, .leader_lease);
    try reads.prepareScan(1, "doc:a", "doc:z", .{});
    try std.testing.expectEqual(@as(usize, 2), recorder.requester_count);
}
