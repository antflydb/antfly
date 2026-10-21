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

/// Preserve explicit, transient inference admission failures across both the
/// HTTP provider and the in-process generation bridge. Permanent budget/model
/// failures and unstructured 503s must not masquerade as capacity retries.
pub fn localGenerationStatusError(alloc: std.mem.Allocator, status: u16, body: ?[]const u8) anyerror {
    if (status == 429) return error.RateLimit;
    if (status == 504) return error.Timeout;
    if (status == 400 or status == 503 or status == 502 or status == 500) {
        const Failure = struct { @"error": []const u8 = "", retryable: bool = false };
        if (body) |bytes| {
            var parsed = std.json.parseFromSlice(Failure, alloc, bytes, .{ .ignore_unknown_fields = true }) catch
                return error.GenerateRequestFailed;
            defer parsed.deinit();
            if (status == 400 and std.mem.eql(u8, parsed.value.@"error", "UNSUPPORTED_CONSTRAINED_PROBABILITIES")) return error.UnsupportedConstrainedProbabilities;
            if (status == 500 and std.mem.eql(u8, parsed.value.@"error", "STRUCTURED_OUTPUT_INVALID")) return error.InvalidStructuredOutput;
            if (parsed.value.retryable and status == 503 and std.mem.eql(u8, parsed.value.@"error", "MODEL_RESOURCE_BUSY"))
                return error.GenerationCapacityUnavailable;
            if (parsed.value.retryable and status == 502 and std.mem.eql(u8, parsed.value.@"error", "TOOL_ARGUMENTS_INVALID"))
                return error.InvalidGeneratedToolArguments;
        }
    }
    return error.GenerateRequestFailed;
}

test "local generation bridge preserves retryable capacity without retrying permanent failures" {
    const alloc = std.testing.allocator;
    const busy = "{\"error\":\"MODEL_RESOURCE_BUSY\",\"retryable\":true,\"reason\":\"inference_capacity\",\"retry_after_ms\":1000}";
    try std.testing.expectEqual(error.GenerationCapacityUnavailable, localGenerationStatusError(alloc, 503, busy));
    try std.testing.expectEqual(error.GenerateRequestFailed, localGenerationStatusError(alloc, 500, busy));
    for ([_]?[]const u8{ null, "unavailable", "{}", "{\"error\":\"MODEL_RESOURCE_BUSY\",\"retryable\":false}", "{\"error\":\"MODEL_RESOURCE_LIMIT\",\"retryable\":false}", "{\"error\":\"MODEL_NOT_FOUND\",\"retryable\":true}" }) |body| {
        try std.testing.expectEqual(error.GenerateRequestFailed, localGenerationStatusError(alloc, 503, body));
    }
    const malformed = "{\"error\":\"TOOL_ARGUMENTS_INVALID\",\"retryable\":true}";
    try std.testing.expectEqual(error.InvalidGeneratedToolArguments, localGenerationStatusError(alloc, 502, malformed));
    try std.testing.expectEqual(error.GenerateRequestFailed, localGenerationStatusError(alloc, 503, malformed));
    try std.testing.expectEqual(error.GenerateRequestFailed, localGenerationStatusError(alloc, 502, busy));
    try std.testing.expectEqual(error.GenerateRequestFailed, localGenerationStatusError(alloc, 502, "{\"error\":\"TOOL_ARGUMENTS_INVALID\",\"retryable\":false}"));
    try std.testing.expectEqual(error.RateLimit, localGenerationStatusError(alloc, 429, null));
    try std.testing.expectEqual(error.Timeout, localGenerationStatusError(alloc, 504, null));
}

test "local generation bridge preserves unsupported finite vocabulary admission" {
    try std.testing.expectEqual(error.UnsupportedConstrainedProbabilities, localGenerationStatusError(std.testing.allocator, 400, "{\"error\":\"UNSUPPORTED_CONSTRAINED_PROBABILITIES\",\"retryable\":false}"));
}
