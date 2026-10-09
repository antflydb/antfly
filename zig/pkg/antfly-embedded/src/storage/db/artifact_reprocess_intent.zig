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

//! A document-owned target generation, staged with the source mutation.
//! Deferred replay reads this instead of depending on a transient force flag.
//! One bounded record per producer remains after fulfillment; source deletion
//! retires it with the document range, and config changes cannot inherit it.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const kind = keys.asset_reprocess_intent_kind;
pub const encoded_len = 76;

pub fn keyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    var result: std.ArrayList(u8) = .empty;
    defer result.deinit(alloc);
    try keys.appendDocumentPrefix(&result, alloc, document);
    try result.append(alloc, kind);
    try keys.appendEncodedComponent(&result, alloc, producer);
    return result.toOwnedSlice(alloc);
}

pub fn encode(config: []const u8, target: u64) [encoded_len]u8 {
    var raw: [encoded_len]u8 = undefined;
    @memcpy(raw[0..4], "ARP1");
    std.crypto.hash.Blake3.hash(config, raw[4..36], .{});
    std.mem.writeInt(u64, raw[36..44], target, .little);
    std.crypto.hash.Blake3.hash(raw[0..44], raw[44..76], .{});
    return raw;
}

pub fn pending(raw: []const u8, config: []const u8, generation: u64) !bool {
    if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "ARP1")) return error.InvalidArtifactPayload;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw[0..44], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[44..76])) return error.InvalidArtifactPayload;
    var config_hash: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(config, &config_hash, .{});
    return std.mem.eql(u8, &config_hash, raw[4..36]) and std.mem.readInt(u64, raw[36..44], .little) > generation;
}

test "deferred reprocess intent is bounded generation and config fenced" {
    var raw = encode("producer", 2);
    try std.testing.expect(try pending(&raw, "producer", 1));
    try std.testing.expect(!try pending(&raw, "producer", 2));
    try std.testing.expect(!try pending(&raw, "replacement", 1));
    raw[36] ^= 1;
    try std.testing.expectError(error.InvalidArtifactPayload, pending(&raw, "producer", 1));
    const alloc = std.testing.allocator;
    const key = try keyAlloc(alloc, "doc\x00\xff", "producer\x00");
    defer alloc.free(key);
    const owner = (try keys.decodeDocumentComponentAlloc(alloc, key)).?;
    defer alloc.free(owner);
    try std.testing.expectEqualStrings("doc\x00\xff", owner);
}
