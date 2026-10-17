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

//! Operational ceilings are independent of model policy and source length.
const std = @import("std");
pub const Limits = struct {
    max_working_bytes: usize = 512 * 1024 * 1024,
    max_output_bytes: usize = 100 * 1024 * 1024,
    max_event_bytes: usize = 16 * 1024 * 1024,
    max_chunks: usize = 4096,
    max_dependency_packets: usize = 8192,
    max_coded_window_bytes: usize = 64 * 1024 * 1024,
    max_index_packets: usize = 1_000_000,
    remote_fetched_bytes: u64 = 512 * 1024 * 1024,
    remote_requests: usize = 100_000,
    remote_cache_bytes: usize = 256 * 1024,

    /// The same knobs work for standalone and embedded inference hosts.
    pub fn environment(self: Limits, a: std.mem.Allocator) !Limits {
        var result = self;
        inline for (@typeInfo(Limits).@"struct".field_names) |name| {
            const key = try std.fmt.allocPrintSentinel(a, "ANTFLY_CHUNK_{s}", .{name}, 0);
            defer a.free(key);
            for (key) |*byte| byte.* = std.ascii.toUpper(byte.*);
            if (@import("antfly_platform").env.getenv(key)) |raw| {
                @field(result, name) = std.fmt.parseInt(@TypeOf(@field(result, name)), raw, 10) catch return error.InvalidChunkLimits;
            }
        }
        try result.validate();
        return result;
    }

    pub fn validate(self: Limits) !void {
        inline for (@typeInfo(Limits).@"struct".field_names) |name| {
            if (@field(self, name) == 0) return error.InvalidChunkLimits;
        }
        if (self.max_dependency_packets > 8192 or self.max_event_bytes > 16 * 1024 * 1024 or self.max_event_bytes > self.max_output_bytes or self.remote_cache_bytes > self.remote_fetched_bytes) return error.InvalidChunkLimits;
    }
};

test "chunk ceilings reject empty budgets independently of model capacity" {
    try (Limits{}).validate();
    try std.testing.expectError(error.InvalidChunkLimits, (Limits{ .remote_requests = 0 }).validate());
    try std.testing.expectError(error.InvalidChunkLimits, (Limits{ .max_output_bytes = 1 }).validate());
}
