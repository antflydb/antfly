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

//! Exact observation checks for provider input, preserving the existing whole-
//! source SHA-256 guard. Borrowed source bytes are hashed before verification
//! or later reads can retire them. Native metadata-only comparison remains a
//! rejected, test-only qualification candidate; it grants no scoped dependency,
//! publication, ownership or reclamation authority.
const std = @import("std");
const payload = @import("document_payload.zig");
const keys = @import("internal_keys.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const Verifier = struct {
    ptr: *anyopaque,
    verify: *const fn (*anyopaque, A, []const u8, []const u8, []const u8) anyerror!void,
};

/// `reader` is the exact authority snapshot held by the caller. Hash borrowed
/// input before invoking a verifier or performing another read: either may
/// retire backend decode buffers. Relational values keep their physical digest,
/// matching existing publication validation; native documents keep their logical
/// serialized digest, matching Txn.get and existing producer proofs.
pub fn captureDigest(a: A, reader: anytype, key: []const u8, observed: []const u8, verifier: ?Verifier) !Digest {
    const raw = try reader.get(key);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
    if (verifier) |port| {
        try port.verify(port.ptr, a, key, raw, observed);
    } else if (!std.mem.eql(u8, raw, observed)) return error.EnrichmentSourceChanged;
    return digest;
}

/// Reproducible rejected optimization, available only in test builds. Avoiding
/// a body read with the old SHA guard needs two hashing passes. Qualification
/// measured about 1.9x latency on both tested datasets, so this is NOT selected
/// by production provider capture. A composable guard needs a versioned proof.
pub const Qualification = if (@import("builtin").is_test) struct {
    pub fn nativeDigest(a: A, reader: anytype, key: []const u8, observed: []const u8, verifier: ?Verifier) !Digest {
        const raw = try reader.getPhysical(key);
        if (keys.isStoredDocumentRowKey(key) and payload.isManifest(raw)) {
            const manifest = try payload.Manifest.decode(a, raw);
            defer a.free(manifest.parts);
            if (!matches(manifest, observed)) return error.EnrichmentSourceChanged;
            var digest: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(observed, &digest, .{});
            return digest;
        }
        return captureDigest(a, reader, key, observed, verifier);
    }
    /// Exact serialized equality, including whitespace, escapes, number spelling
    /// and member order. Canonical JSON equality alone is insufficient for a
    /// producer that renders or hashes source bytes. No immutable value is read.
    fn matches(manifest: payload.Manifest, observed: []const u8) bool {
        if (manifest.logical_bytes != observed.len) return false;
        var offset: usize = 0;
        for (manifest.parts) |part| {
            const length = part.length();
            if (length > observed.len -| offset) return false;
            const bytes = observed[offset..][0..length];
            switch (part) {
                .inline_json => |inline_bytes| if (!std.mem.eql(u8, inline_bytes, bytes)) return false,
                .blob => |blob| {
                    var digest: Digest = undefined;
                    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
                    if (!std.mem.eql(u8, &digest, &blob.digest)) return false;
                },
            }
            offset += length;
        }
        return offset == observed.len;
    }
} else struct {};

test "document payload provider input qualification verifies exact manifest bytes without a body read" {
    const a = std.testing.allocator;
    const key = try keys.documentKeyAlloc(a, "doc");
    defer a.free(key);
    const body = try a.alloc(u8, payload.minimum_value_bytes);
    defer a.free(body);
    @memset(body, 'x');
    const prefix = "{\"body\":\"";
    const suffix = "\",\"metadata\":1}";
    const observed = try std.mem.concat(a, u8, &.{ prefix, body, suffix });
    defer a.free(observed);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const parts = [_]payload.Part{ .{ .inline_json = prefix }, .{ .blob = .{ .length = body.len, .digest = digest } }, .{ .inline_json = suffix } };
    const encoded = try payload.encodeRooted(a, &parts);
    defer a.free(encoded);
    const Reader = struct {
        raw: []const u8,
        key: []const u8,
        reads: usize = 0,
        pub fn getPhysical(self: *@This(), candidate: []const u8) ![]const u8 {
            if (!std.mem.eql(u8, candidate, self.key)) return error.UnexpectedBodyRead;
            self.reads += 1;
            return self.raw;
        }
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.UnexpectedLogicalRead;
        }
        fn verify(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8) !void {
            return error.UnexpectedMaterialization;
        }
    };
    var reader: Reader = .{ .raw = encoded, .key = key };
    const port: Verifier = .{ .ptr = &reader, .verify = Reader.verify };
    var fixed_bytes: [64 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&fixed_bytes);
    var expected: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(observed, &expected, .{});
    try std.testing.expectEqualDeep(expected, try Qualification.nativeDigest(fixed.allocator(), &reader, key, observed, port));
    try std.testing.expectEqual(@as(usize, 1), reader.reads);
    observed[prefix.len + body.len / 2] = 'y';
    try std.testing.expectError(error.EnrichmentSourceChanged, Qualification.nativeDigest(a, &reader, key, observed, port));
    observed[prefix.len + body.len / 2] = 'x';
    observed[observed.len - 2] = '2';
    try std.testing.expectError(error.EnrichmentSourceChanged, Qualification.nativeDigest(a, &reader, key, observed, port));
    try std.testing.expectError(error.EnrichmentSourceChanged, Qualification.nativeDigest(a, &reader, key, observed[0 .. observed.len - 1], port));
    reader.raw = encoded[0 .. encoded.len - 1];
    try std.testing.expectError(error.InvalidDocumentManifest, Qualification.nativeDigest(a, &reader, key, observed, port));
}

test "document payload provider input preserves serialized and relational digest semantics" {
    const a = std.testing.allocator;
    const Reader = struct {
        raw: []const u8,
        output: []const u8,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.raw;
        }
        fn verify(ptr: *anyopaque, _: A, _: []const u8, _: []const u8, observed: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Simulate a backend retiring its last borrowed buffer during
            // verification. The guard must already own its digest.
            @memset(@constCast(self.raw), 0);
            if (!std.mem.eql(u8, self.output, observed)) return error.EnrichmentSourceChanged;
        }
    };
    const raw = try a.dupe(u8, "AROW physical bytes");
    defer a.free(raw);
    var reader: Reader = .{ .raw = raw, .output = "{\"text\":\"hello\"}" };
    var expected: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &expected, .{});
    try std.testing.expectEqualDeep(expected, try captureDigest(a, &reader, "doc\x00\x11", reader.output, .{ .ptr = &reader, .verify = Reader.verify }));
    reader.raw = "{\"n\":1}";
    try std.testing.expectError(error.EnrichmentSourceChanged, captureDigest(a, &reader, "doc\x00\x10", "{\"n\":1.0}", null));
    try std.testing.expectError(error.EnrichmentSourceChanged, captureDigest(a, &reader, "doc\x00\x10", "{ \"n\":1}", null));
    try std.testing.expectError(error.EnrichmentSourceChanged, captureDigest(a, &reader, "doc\x00\x10", "{\"n\":\"1\"}", null));
}
