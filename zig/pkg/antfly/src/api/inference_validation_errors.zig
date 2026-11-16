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

//! Transport-independent embedding validation failures for every DB API route.
const std = @import("std");

pub const Failure = struct {
    status: u16,
    code: []const u8,
    message: []const u8,
    retryable: bool,
    retry_after_seconds: ?u32 = null,

    pub fn bodyAlloc(self: Failure, alloc: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(alloc, .{
            .@"error" = self.code,
            .message = self.message,
            .retryable = self.retryable,
        }, .{});
    }
};

pub fn classify(err: anyerror) ?Failure {
    return switch (err) {
        error.ResourceLimitExceeded, error.MemoryBudgetExceeded => .{
            .status = 400,
            .code = "MODEL_RESOURCE_LIMIT",
            .message = "embedding validation exceeds the configured inference memory budget; inspect admission logs for requested bytes and host, backend, combined, KV and scratch limits",
            .retryable = false,
        },
        error.EmbeddingProbeUnavailable, error.ProbeUnavailable => .{
            .status = 503,
            .code = "index_probe_unavailable",
            .message = "embedding validation probe unavailable",
            .retryable = true,
            .retry_after_seconds = 1,
        },
        else => null,
    };
}

test "embedding validation failures share status identity and retry policy" {
    for ([_]anyerror{ error.ResourceLimitExceeded, error.MemoryBudgetExceeded, error.EmbeddingProbeUnavailable, error.ProbeUnavailable }) |err| {
        const failure = classify(err).?;
        const busy = err == error.EmbeddingProbeUnavailable or err == error.ProbeUnavailable;
        try std.testing.expectEqual(busy, failure.retryable);
        try std.testing.expectEqual(@as(u16, if (busy) 503 else 400), failure.status);
        const body = try failure.bodyAlloc(std.testing.allocator);
        defer std.testing.allocator.free(body);
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(failure.code, parsed.value.object.get("error").?.string);
    }
    try std.testing.expect(classify(error.OutOfMemory) == null);
    try std.testing.expect(classify(error.InvalidCreateTableRequest) == null);
}
