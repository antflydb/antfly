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
/// Internal read modes. Lookup returns whole primary rows under the admitted
/// read fence; candidates/hydrate retain the two-phase authority protocol.
pub const Mode = enum { none, candidates, hydrate, lookup };
pub const Authority = struct {
    group: u64,
    version: u64,
    digest: [32]u8,
    pub fn eql(self: @This(), other: @This()) bool {
        return self.group == other.group and self.version == other.version and std.mem.eql(u8, &self.digest, &other.digest);
    }
};
pub const Envelope = struct { authority: Authority, document: ?[]const u8 = null };
const marker = "__antfly_document_authority_v1";
/// Capture only validated root-member ranges during the source traversal.
/// Interpretation is deferred until response capability metadata is checked:
/// an old provider may return arbitrary user fields with these names.
pub const Pending = struct {
    proof: ?[]const u8 = null,
    document: ?[]const u8 = null,
    unknown: bool = false,

    pub fn observe(ctx: *anyopaque, name: []const u8, raw: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (std.mem.eql(u8, name, marker)) self.proof = raw else if (std.mem.eql(u8, name, "document")) self.document = raw else self.unknown = true;
    }
    pub fn finish(self: @This(), alloc: std.mem.Allocator) !?Envelope {
        const raw = self.proof orelse return null;
        if (self.unknown) return error.InvalidRemoteResponse;
        if (self.document) |document| if (document.len == 0 or document[0] != '{') return error.InvalidRemoteResponse;
        const parsed = std.json.parseFromSlice(struct { group: u64, version: u64, sha256: []const u8 }, alloc, raw, .{ .allocate = .alloc_if_needed }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRemoteResponse;
        defer parsed.deinit();
        const wire = parsed.value;
        if (wire.group == 0 or wire.sha256.len != 64) return error.InvalidRemoteResponse;
        var digest: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&digest, wire.sha256) catch return error.InvalidRemoteResponse;
        return .{ .authority = .{ .group = wire.group, .version = wire.version, .digest = digest }, .document = self.document };
    }
};

pub fn encode(alloc: std.mem.Allocator, authority: Authority, document: ?[]const u8) ![]u8 {
    if (authority.group == 0) return error.InvalidQueryRequest;
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
        if (std.mem.eql(u8, name, marker)) {
            if (proof != null) return error.InvalidRemoteResponse;
            const wire = try std.json.innerParse(struct { group: u64, version: u64, sha256: []const u8 }, a, &scanner, options);
            if (wire.group == 0 or wire.sha256.len != 64) return error.InvalidRemoteResponse;
            var digest: [32]u8 = undefined;
            _ = std.fmt.hexToBytes(&digest, wire.sha256) catch return error.InvalidRemoteResponse;
            proof = .{ .group = wire.group, .version = wire.version, .digest = digest };
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
