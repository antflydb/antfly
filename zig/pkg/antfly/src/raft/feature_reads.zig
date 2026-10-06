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
const db_types = @import("antfly_local_sources").storage_db_types;
const read_gate = @import("read_gate.zig");

pub const FeatureReads = @import("antfly_local_sources").storage_read_consistency.FeatureReads;

test "feature reads facade forwards typed requests with explicit consistency" {
    const Recorder = struct {
        wait_count: usize = 0,

        fn barrier(self: *@This()) read_gate.ReadSafetyBarrier {
            return .{
                .ptr = self,
                .vtable = &.{
                    .wait_read_safe = waitReadSafe,
                },
            };
        }

        fn waitReadSafe(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.wait_count += 1;
        }
    };

    var recorder = Recorder{};
    const reads = FeatureReads.init(recorder.barrier());
    try reads.prepareSearchWithConsistency(1, .{}, .stale);
    try std.testing.expectEqual(@as(usize, 0), recorder.wait_count);
    try reads.prepareLookupWithConsistency(1, "doc:a", .{}, .leader_lease);
    try reads.prepareScan(1, "doc:a", "doc:z", .{});
    try std.testing.expectEqual(@as(usize, 2), recorder.wait_count);
}
