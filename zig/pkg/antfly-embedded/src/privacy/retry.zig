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

//! Owner-local retry floors. Queue admission and the floor commit together;
//! an older in-flight failure cannot terminate a newly admitted scan attempt.
const std = @import("std");
const A = std.mem.Allocator;
const prefix = "\x00\x00__privacy_retry:";

pub fn key(a: A, state_key: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ prefix, state_key });
}

pub fn encode(sequence: u64) [8]u8 {
    return std.mem.toBytes(std.mem.nativeToLittle(u64, sequence));
}

pub fn current(a: A, read: anytype, state_key: []const u8, failed_sequence: u64) !bool {
    const selected = try key(a, state_key);
    defer a.free(selected);
    const raw = read.get(selected) catch |err| if (err == error.NotFound) return true else return err;
    if (raw.len != 8) return error.InvalidPrivacyReceipt;
    return failed_sequence >= std.mem.readInt(u64, raw[0..8], .little);
}

test "privacy retry admission invalidates older failures without hiding a new failure" {
    const Fake = struct {
        bytes: [8]u8,
        pub fn get(self: *const @This(), _: []const u8) ![]const u8 {
            return &self.bytes;
        }
    };
    const read = Fake{ .bytes = encode(7) };
    try std.testing.expect(!try current(std.testing.allocator, read, "state", 0));
    try std.testing.expect(!try current(std.testing.allocator, read, "state", 6));
    try std.testing.expect(try current(std.testing.allocator, read, "state", 7));
    try std.testing.expect(try current(std.testing.allocator, read, "state", 8));
}
