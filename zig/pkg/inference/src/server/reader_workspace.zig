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

/// Retain two stream-ordered working sets, but only for pages that can actually
/// join the admitted cohort. The epoch owns the resulting high-water lease;
/// departing pages never shrink it while queued work or cached buffers exist.
pub const Decoder = struct {
    per_item: usize = 0,
    mixed_decode: usize = 0,
    head: usize = 0,
    max_items: usize = 1,

    pub fn forPrefill(working_bytes: usize, working_rows: usize, decode_peers: usize, head_bytes: usize, max_items: usize) !Decoder {
        const per_item = try std.math.mul(usize, 2, working_bytes);
        // All prefills in a mixed submission share the same one-token decode
        // peers. Charge the full per-row allowance for both working sets once.
        const mixed_decode = if (decode_peers != 0)
            try std.math.divCeil(usize, try std.math.mul(usize, per_item, decode_peers), working_rows)
        else
            0;
        return .{ .per_item = per_item, .mixed_decode = mixed_decode, .head = head_bytes, .max_items = max_items };
    }

    pub fn requiredBytes(self: Decoder, active_pages: usize) !usize {
        if (active_pages == 0 or self.max_items == 0) return error.ResourceLimitExceeded;
        const items = @min(active_pages, self.max_items);
        const working = try std.math.add(usize, try std.math.mul(usize, self.per_item, items), self.mixed_decode);
        return std.math.add(usize, working, self.head);
    }

    pub fn peak(self: Decoder, active_pages: usize) !usize {
        const target = try self.requiredBytes(active_pages);
        // CUDA scratch buffers grow to the requested size; a second geometric
        // lease rounding is unnecessary. Keep a device-page alignment margin.
        return (try std.math.add(usize, target, 65535)) & ~@as(usize, 65535);
    }

    pub fn merge(self: Decoder, next: Decoder) !Decoder {
        if (self.max_items != next.max_items) return error.InvalidBatchDecodeState;
        return .{ .per_item = @max(self.per_item, next.per_item), .mixed_decode = @max(self.mixed_decode, next.mixed_decode), .head = @max(self.head, next.head), .max_items = self.max_items };
    }
};

pub fn batchPeak(per_item: usize, active_pages: usize, max_items: usize) !usize {
    if (active_pages == 0 or max_items == 0) return error.ResourceLimitExceeded;
    return std.math.mul(usize, per_item, @min(active_pages, max_items));
}

test "reader workspace grows with available cohort and preserves both queued sets" {
    const plan = Decoder{ .per_item = 2 * 100 * 1024 * 1024, .head = 64 * 1024 * 1024, .max_items = 4 };
    try std.testing.expectEqual(@as(usize, 264 * 1024 * 1024), try plan.peak(1));
    try std.testing.expectEqual(@as(usize, 864 * 1024 * 1024), try plan.peak(4));
    try std.testing.expectEqual(try plan.peak(4), try plan.peak(64));
    const grown = try plan.merge(.{ .per_item = 400 * 1024 * 1024, .head = 32 * 1024 * 1024, .max_items = 4 });
    try std.testing.expect(grown.head == plan.head and grown.per_item > plan.per_item);
    try std.testing.expectEqual(@as(usize, 2 * 123), try batchPeak(123, 2, 8));
    try std.testing.expectError(error.Overflow, (Decoder{ .per_item = std.math.maxInt(usize), .max_items = 4 }).peak(4));
}
