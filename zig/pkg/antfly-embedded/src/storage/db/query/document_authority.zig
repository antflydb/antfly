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

//! Private candidate/hydration proof. Public request parsers never enable it.
const std = @import("std");
pub const Algorithm = @import("../../document_commitment.zig").Algorithm;
pub const Mode = enum { none, candidates, hydrate };
pub const Authority = struct {
    group: u64,
    version: u64,
    digest: [32]u8,
    algorithm: Algorithm = .sha256_json,
    pub fn eql(self: @This(), other: @This()) bool {
        return self.group == other.group and self.version == other.version and self.algorithm == other.algorithm and std.mem.eql(u8, &self.digest, &other.digest);
    }
};
pub const Envelope = struct { authority: Authority, document: ?[]const u8 = null };
const marker = "__antfly_document_authority_v1";
const logical_marker = "__antfly_document_authority_v2";
/// Capture only validated root-member ranges during the source traversal.
/// Interpretation is deferred until response capability metadata is checked:
/// an old provider may return arbitrary user fields with these names.
pub const Pending = struct {
    proof: ?[]const u8 = null,
    proof_version: u32 = 1,
    document: ?[]const u8 = null,
    unknown: bool = false,

    pub fn observe(ctx: *anyopaque, name: []const u8, raw: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, name, marker) or std.mem.eql(u8, name, logical_marker)) {
            if (self.proof != null) self.unknown = true;
            self.proof = raw;
            self.proof_version = if (std.mem.eql(u8, name, marker)) 1 else 2;
        } else if (std.mem.eql(u8, name, "document")) self.document = raw else self.unknown = true;
    }
    pub fn finish(self: @This(), alloc: std.mem.Allocator) !?Envelope {
        return self.finishVersion(alloc, 1);
    }
    pub fn finishVersion(self: @This(), alloc: std.mem.Allocator, version: u32) !?Envelope {
        const raw = self.proof orelse return null;
        if (self.unknown or self.proof_version > version or version > 2) return error.InvalidRemoteResponse;
        if (self.document) |document| if (document.len == 0 or document[0] != '{') return error.InvalidRemoteResponse;
        return .{ .authority = try decodeProof(alloc, raw, self.proof_version), .document = self.document };
    }
};

fn decodeProof(alloc: std.mem.Allocator, raw: []const u8, version: u32) !Authority {
    if (version == 1) {
        const parsed = std.json.parseFromSlice(struct { group: u64, version: u64, sha256: []const u8 }, alloc, raw, .{ .allocate = .alloc_if_needed }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteResponse;
        defer parsed.deinit();
        return decodeDigest(parsed.value.group, parsed.value.version, parsed.value.sha256, .sha256_json);
    }
    const parsed = std.json.parseFromSlice(struct { group: u64, version: u64, algorithm: []const u8, digest: []const u8 }, alloc, raw, .{ .allocate = .alloc_if_needed }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteResponse;
    defer parsed.deinit();
    if (version != 2 or !std.mem.eql(u8, parsed.value.algorithm, "logical-json-v1")) return error.InvalidRemoteResponse;
    return decodeDigest(parsed.value.group, parsed.value.version, parsed.value.digest, .logical_json_v1);
}
fn decodeDigest(group: u64, version: u64, hex: []const u8, algorithm: Algorithm) !Authority {
    if (group == 0 or hex.len != 64) return error.InvalidRemoteResponse;
    var digest: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&digest, hex) catch return error.InvalidRemoteResponse;
    return .{ .group = group, .version = version, .digest = digest, .algorithm = algorithm };
}

pub fn encode(alloc: std.mem.Allocator, authority: Authority, document: ?[]const u8) ![]u8 {
    if (authority.group == 0) return error.InvalidQueryRequest;
    if (authority.algorithm == .logical_json_v1) {
        if (document) |bytes| return std.fmt.allocPrint(alloc, "{{\"" ++ logical_marker ++ "\":{{\"group\":{d},\"version\":{d},\"algorithm\":\"logical-json-v1\",\"digest\":\"{x}\"}},\"document\":{s}}}", .{ authority.group, authority.version, authority.digest, bytes });
        return std.fmt.allocPrint(alloc, "{{\"" ++ logical_marker ++ "\":{{\"group\":{d},\"version\":{d},\"algorithm\":\"logical-json-v1\",\"digest\":\"{x}\"}}}}", .{ authority.group, authority.version, authority.digest });
    }
    if (document) |bytes| return std.fmt.allocPrint(alloc, "{{\"" ++ marker ++ "\":{{\"group\":{d},\"version\":{d},\"sha256\":\"{x}\"}},\"document\":{s}}}", .{ authority.group, authority.version, authority.digest, bytes });
    return std.fmt.allocPrint(alloc, "{{\"" ++ marker ++ "\":{{\"group\":{d},\"version\":{d},\"sha256\":\"{x}\"}}}}", .{ authority.group, authority.version, authority.digest });
}
// Only the small proof is materialized. Document bytes borrow the validated
// wire owner; a winning document is copied once into final result ownership.
pub fn parse(alloc: std.mem.Allocator, bytes: []const u8) !?Envelope {
    return parseInner(alloc, bytes) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteResponse;
}
fn parseInner(alloc: std.mem.Allocator, bytes: []const u8) !?Envelope {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var scanner = std.json.Scanner.initCompleteInput(a, bytes);
    defer scanner.deinit();
    const options: std.json.ParseOptions = .{ .allocate = .alloc_if_needed, .max_value_len = bytes.len };
    if (try scanner.next() != .object_begin) return null;
    var proof: ?Authority = null;
    var document: ?[]const u8 = null;
    var document_seen = false;
    var unknown = false;
    while (try scanner.peekNextTokenType() != .object_end) {
        const name = try std.json.innerParse([]const u8, a, &scanner, options);
        if (std.mem.eql(u8, name, marker) or std.mem.eql(u8, name, logical_marker)) {
            if (proof != null) return error.InvalidRemoteResponse;
            var begin = scanner.cursor;
            while (begin < bytes.len and (std.ascii.isWhitespace(bytes[begin]) or bytes[begin] == ':')) : (begin += 1) {}
            try scanner.skipValue();
            proof = try decodeProof(a, bytes[begin..scanner.cursor], if (std.mem.eql(u8, name, marker)) 1 else 2);
        } else if (std.mem.eql(u8, name, "document")) {
            if (document_seen) return error.InvalidRemoteResponse;
            document_seen = true;
            if (try scanner.peekNextTokenType() != .object_begin) return error.InvalidRemoteResponse;
            var start = scanner.cursor;
            while (start < bytes.len and (std.ascii.isWhitespace(bytes[start]) or bytes[start] == ':')) : (start += 1) {}
            try scanner.skipValue();
            document = bytes[start..scanner.cursor];
        } else {
            unknown = true;
            try scanner.skipValue();
        }
    }
    _ = try scanner.next();
    if (try scanner.next() != .end_of_document) return error.InvalidRemoteResponse;
    const authority = proof orelse return null;
    if (unknown) return error.InvalidRemoteResponse;
    return .{ .authority = authority, .document = document };
}

test "public graph metric borrowed wire document authority preserves raw sources and strict proofs" {
    const a = std.testing.allocator;
    const authority: Authority = .{ .group = std.math.maxInt(u64), .version = std.math.maxInt(u64), .digest = @splat(0xaa) };
    const encoded = try encode(a, authority, "{\"text\":\"a\\n\",\"n\":1e2}");
    defer a.free(encoded);
    const decoded = (try parse(a, encoded)).?;
    try std.testing.expect(decoded.authority.eql(authority));
    try std.testing.expectEqualStrings("{\"text\":\"a\\n\",\"n\":1e2}", decoded.document.?);
    try std.testing.expect((try parse(a, "{\"ordinary\":1}")) == null);
    for ([_][]const u8{ "[]", "null", "1" }) |invalid_document| {
        const invalid = try encode(a, authority, invalid_document);
        defer a.free(invalid);
        try std.testing.expectError(error.InvalidRemoteResponse, parse(a, invalid));
    }
    const Faults = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const proof: Authority = .{ .group = 7, .version = 9, .digest = @splat(1) };
            const bytes = try encode(alloc, proof, "{\"a\":true}");
            defer alloc.free(bytes);
            try std.testing.expect((try parse(alloc, bytes)).?.authority.eql(proof));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Faults.run, .{});
}

test "public graph metric borrowed wire logical authorities are versioned and never relabeled sha256" {
    const a = std.testing.allocator;
    const proof: Authority = .{ .group = 1, .version = 4, .digest = @splat(2), .algorithm = .logical_json_v1 };
    const encoded = try encode(a, proof, "{\"a\":1}");
    defer a.free(encoded);
    try std.testing.expect((try parse(a, encoded)).?.authority.eql(proof));
    try std.testing.expect(std.mem.indexOf(u8, encoded, "sha256") == null);
    var legacy = proof;
    legacy.algorithm = .sha256_json;
    try std.testing.expect(!legacy.eql(proof));
    var pending: Pending = .{};
    const raw = try std.fmt.allocPrint(a, "{{\"group\":1,\"version\":4,\"algorithm\":\"logical-json-v1\",\"digest\":\"{x}\"}}", .{proof.digest});
    defer a.free(raw);
    try Pending.observe(&pending, logical_marker, raw);
    try std.testing.expectError(error.InvalidRemoteResponse, pending.finish(a));
    try std.testing.expect((try pending.finishVersion(a, 2)).?.authority.eql(proof));
}
