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

//! Storage-owned standalone witnesses, committed beside the artifact. Never
//! accepted on a replicated owner; a provider's output cannot supply this key.
const std = @import("std");
const contract = @import("contract.zig");
const A = std.mem.Allocator;
const prefix = "\x00\x00__privacy_publication:";
const keys = @import("../storage/internal_keys.zig");
const compact_prefix = prefix ++ "v2:";
pub fn legacyKey(a: A, artifact: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ prefix, artifact });
}
pub fn key(a: A, artifact: []const u8) ![]u8 {
    return keys.documentMetadataKeyAlloc(a, prefix, compact_prefix, artifact);
}
pub fn encode(version: [64]u8, source: []const u8, output: []const u8) [192]u8 {
    var value: [192]u8 = undefined;
    @memcpy(value[0..64], &version);
    @memcpy(value[64..128], &contract.fingerprint(source));
    @memcpy(value[128..192], &contract.fingerprint(output));
    return value;
}
pub fn encodeDigest(version: [64]u8, source_digest: [32]u8, output: []const u8) [192]u8 {
    var value: [192]u8 = undefined;
    @memcpy(value[0..64], &version);
    @memcpy(value[64..128], &std.fmt.bytesToHex(source_digest, .lower));
    @memcpy(value[128..192], &contract.fingerprint(output));
    return value;
}
pub fn require(a: A, read: anytype, artifact: []const u8, version: [64]u8, source: []const u8, output: []const u8) !void {
    const actual = keys.readDocumentMetadata(a, read, prefix, compact_prefix, artifact) catch |err| if (err == error.NotFound) return error.PrivacyPublicationPending else return err;
    if (!std.mem.eql(u8, actual, &encode(version, source, output))) return error.PrivacyStale;
}

test "privacy standalone witness independently binds source, policy and output" {
    const Fake = struct {
        value: []const u8,
        pub fn get(self: @This(), _: []const u8) ![]const u8 {
            return self.value;
        }
    };
    const version = contract.fingerprint("policy");
    const value = encode(version, "source", "output");
    const read = Fake{ .value = &value };
    try require(std.testing.allocator, read, "artifact", version, "source", "output");
    try std.testing.expectError(error.PrivacyStale, require(std.testing.allocator, read, "artifact", version, "edited", "output"));
    try std.testing.expectError(error.PrivacyStale, require(std.testing.allocator, read, "artifact", contract.fingerprint("next"), "source", "output"));
    try std.testing.expectError(error.PrivacyStale, require(std.testing.allocator, read, "artifact", version, "source", "forged"));
}
