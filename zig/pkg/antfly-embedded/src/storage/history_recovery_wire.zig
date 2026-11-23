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

//! Owned-control-job handoff; acceptance is scheduling, never completion.
const std = @import("std");
const completion = @import("transaction_completion.zig");
pub const max_bytes = 8192;
pub const Request = struct {
    version: u8 = 1,
    owner: completion.RoutedOwner,
    transaction_id: [16]u8,
    admission_digest: [32]u8,
    coordinator_locator: []const u8,
    /// Zero preserves accepted source-purpose work instead of auto-aborting it.
    abort_before_ns: u64 = 0,
    resolution_timestamp_ns: u64,
    pub fn validate(self: Request) !void {
        try self.owner.validate();
        if (self.version != 1 or self.resolution_timestamp_ns == 0 or
            std.mem.allEqual(u8, &self.transaction_id, 0) or std.mem.allEqual(u8, &self.admission_digest, 0) or
            self.coordinator_locator.len == 0 or self.coordinator_locator.len > completion.max_locator_bytes or
            !std.unicode.utf8ValidateSlice(self.coordinator_locator) or self.abort_before_ns > self.resolution_timestamp_ns) return error.InvalidTxnRecord;
    }
    pub fn identity(self: Request) [32]u8 {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:completion-recovery-job:v1");
        sha.update(&self.owner.digest());
        sha.update(&self.transaction_id);
        sha.update(&self.admission_digest);
        return sha.finalResult();
    }
};
pub fn decode(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Request) {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidTxnRecord;
    var parsed = std.json.parseFromSlice(Request, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTxnRecord;
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
