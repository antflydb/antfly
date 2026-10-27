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

//! Disposable validation provenance. Only a successful schema preparation may
//! publish a certificate. Missing, stale or malformed records require ordinary
//! validation; this record is never authority for source presence or ownership.
const std = @import("std");
const revisions = @import("document_mutation_revision.zig");
const keys = @import("internal_keys.zig");
pub const Digest = [32]u8;
const prefix = "\x00\x00__metadata__:document_validation_v2:";
pub const Key = [prefix.len + 32]u8;
pub const encoded_len = 137;
const magic = "DVF2";
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
pub const Stamp = struct { rules: Digest, schema_version: u32, key: Digest };

/// Domain-separated from retirement signatures and every other use of the
/// private root identity. The private seed never enters the LSM or an archive.
pub fn deriveKey(seed: Digest) Digest {
    var key: Digest = undefined;
    Hmac.create(&key, "antfly.document.validation.key/1", &seed);
    return key;
}

fn authenticate(key: Digest, row: Key, payload: []const u8) Digest {
    var mac = Hmac.init(&key);
    mac.update("antfly.document.validation.certificate/1\x00");
    mac.update(&row);
    mac.update(payload);
    return mac.finalResult();
}

/// Bump this domain when validator semantics or proof interpretation changes.
/// Raw-schema hashing is intentionally conservative: a schema reformat can
/// invalidate provenance, but can never authorize a different validation rule.
pub fn fingerprint(schema_json: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.document.validation/1\x00");
    hash.update(schema_json);
    return hash.finalResult();
}

pub fn keyForRevision(source: revisions.Key) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], source[source.len - 32 ..]);
    return result;
}

pub fn keyForPhysical(physical: []const u8) ?Key {
    if (!keys.isPrimaryDocumentKey(physical)) return null;
    return keyForRevision(revisions.keyForPhysical(physical) orelse return null);
}

pub const Certificate = struct {
    root: Digest,
    rules: Digest,
    schema_version: u32,
    physical: Digest,

    pub fn matches(self: Certificate, source: revisions.Content, rules: Digest, schema_version: u32, physical: Digest) bool {
        return source.kind == keys.primary_kind and source.algorithm == .logical_json_v1 and
            self.schema_version == schema_version and std.mem.eql(u8, &self.physical, &physical) and std.mem.eql(u8, &self.root, &source.digest) and
            std.mem.eql(u8, &self.rules, &rules);
    }

    pub fn encode(self: Certificate, key: Digest, row: Key) [encoded_len]u8 {
        var result: [encoded_len]u8 = undefined;
        @memcpy(result[0..4], magic);
        result[4] = keys.primary_kind;
        std.mem.writeInt(u32, result[5..9], self.schema_version, .little);
        @memcpy(result[9..41], &self.root);
        @memcpy(result[41..73], &self.rules);
        @memcpy(result[73..105], &self.physical);
        @memcpy(result[105..137], &authenticate(key, row, result[0..105]));
        return result;
    }

    pub fn decode(raw: []const u8, key: Digest, row: Key) ?Certificate {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], magic) or raw[4] != keys.primary_kind) return null;
        const digest = authenticate(key, row, raw[0..105]);
        if (!std.crypto.timing_safe.eql([32]u8, digest, raw[105..137].*)) return null;
        return .{ .schema_version = std.mem.readInt(u32, raw[5..9], .little), .root = raw[9..41].*, .rules = raw[41..73].*, .physical = raw[73..105].* };
    }
};

pub fn load(reader: anytype, physical: []const u8, authority: Digest) !?Certificate {
    const key = keyForPhysical(physical) orelse return null;
    const bytes = @import("document_payload.zig").rawGet(reader, &key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    return Certificate.decode(bytes, authority, key);
}

test "document payload validation certificates reject stale rules roots versions and corruption" {
    const a = std.testing.allocator;
    const physical = try keys.documentKeyAlloc(a, "doc\x00id");
    defer a.free(physical);
    try std.testing.expect(keyForPhysical(physical) != null);
    try std.testing.expect(keyForPhysical("not a physical row") == null);
    const source: revisions.Content = .{ .revision = 8, .bytes = 100, .kind = keys.primary_kind, .digest = @splat(0x11), .algorithm = .logical_json_v1 };
    const rules = fingerprint("{\"version\":3}");
    const certificate: Certificate = .{ .root = source.digest, .rules = rules, .schema_version = 3, .physical = @splat(0x45) };
    const key = deriveKey(@splat(0x23));
    const row = keyForPhysical(physical).?;
    const raw = certificate.encode(key, row);
    try std.testing.expect(!certificate.matches(source, rules, 3, @splat(0x46)));
    try std.testing.expect(Certificate.decode(&raw, deriveKey(@splat(0x24)), row) == null);
    var other_row = row;
    other_row[other_row.len - 1] ^= 1;
    try std.testing.expect(Certificate.decode(&raw, key, other_row) == null);
    try std.testing.expect(Certificate.decode(&raw, key, row).?.matches(source, rules, 3, certificate.physical));
    try std.testing.expect(!certificate.matches(source, fingerprint("{}"), 3, certificate.physical));
    try std.testing.expect(!certificate.matches(source, rules, 4, certificate.physical));
    var different = source;
    different.digest[0] ^= 1;
    try std.testing.expect(!certificate.matches(different, rules, 3, certificate.physical));
    different = source;
    different.algorithm = .sha256_json;
    try std.testing.expect(!certificate.matches(different, rules, 3, certificate.physical));
    for (0..raw.len) |i| {
        var damaged = raw;
        damaged[i] ^= 1;
        try std.testing.expect(Certificate.decode(&damaged, key, row) == null);
        try std.testing.expect(Certificate.decode(raw[0..i], key, row) == null);
    }
}
