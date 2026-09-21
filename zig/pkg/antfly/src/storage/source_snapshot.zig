// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Transferable identity for an immutable portable source cut. The local seal
//! authenticates capture; this certificate authenticates logical content after
//! transfer. Neither a filesystem path/inode nor the local seal digest is part
//! of its identity. A certificate is NOT authority to recapture a missing pin.
//! Feed only blocks accepted by the shared portable reader/exporter. This
//! bounded, allocation-free observer does not replace archive validation.
const std = @import("std");
const Sha256 = std.crypto.hash.sha2.Sha256;
pub const Namespace = @import("db/doc_identity_namespace.zig").Namespace;
const cohort_key = "\x00\x00__metadata__:portable_cohort";
pub const encoded_size = 224;
pub const integrity_key = "\x00\x00__metadata__:portable_source_integrity";
pub const IntegrityBinding = @import("db/merge_page_contract.zig").IntegrityBinding;
pub const max_block_bytes = 128 * 1024 * 1024;

pub const Cut = struct {
    namespace: Namespace,
    /// Raft applied index and retained-effects sequence are different clocks.
    applied_index: u64,
    retained_start: u64,

    pub fn validate(self: Cut) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or
            self.namespace.range_id == 0 or self.applied_index == 0)
            return error.InvalidSourceSnapshot;
    }
};

pub const Certificate = struct {
    cut: Cut,
    objects: u64,
    content_bytes: u64,
    schema_manifest_digest: [32]u8,
    ordered_content_digest: [32]u8,
    integrity: ?IntegrityBinding = null,

    pub fn encode(self: Certificate) ![encoded_size]u8 {
        try self.cut.validate();
        if (self.objects == 0 or self.content_bytes == 0 or
            std.mem.allEqual(u8, &self.schema_manifest_digest, 0) or
            std.mem.allEqual(u8, &self.ordered_content_digest, 0)) return error.InvalidSourceSnapshot;
        var bytes: [encoded_size]u8 = @splat(0);
        @memcpy(bytes[0..4], "ASS2");
        var position: usize = 8;
        for ([_]u64{ self.cut.namespace.table_id, self.cut.namespace.shard_id, self.cut.namespace.range_id, self.cut.applied_index, self.cut.retained_start, self.objects, self.content_bytes }) |value| {
            std.mem.writeInt(u64, bytes[position..][0..8], value, .little);
            position += 8;
        }
        @memcpy(bytes[64..96], &self.schema_manifest_digest);
        @memcpy(bytes[96..128], &self.ordered_content_digest);
        if (self.integrity) |binding| {
            if (std.mem.allEqual(u8, &binding.catalog_digest, 0) or std.mem.allEqual(u8, &binding.generation_set, 0)) return error.InvalidSourceSnapshot;
            bytes[4] = 1;
            @memcpy(bytes[128..160], &binding.catalog_digest);
            @memcpy(bytes[160..192], &binding.generation_set);
        }
        Sha256.hash(bytes[0..192], bytes[192..224], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Certificate {
        if (bytes.len != encoded_size or !std.mem.eql(u8, bytes[0..4], "ASS2") or
            bytes[4] > 1 or !std.mem.allEqual(u8, bytes[5..8], 0) or
            (bytes[4] == 0 and !std.mem.allEqual(u8, bytes[128..192], 0))) return error.InvalidSourceSnapshot;
        var expected: [32]u8 = undefined;
        Sha256.hash(bytes[0..192], &expected, .{});
        if (!std.mem.eql(u8, &expected, bytes[192..224])) return error.SourceSnapshotCorrupt;
        const result: Certificate = .{
            .cut = .{
                .namespace = .{ .table_id = readInt(bytes, 8), .shard_id = readInt(bytes, 16), .range_id = readInt(bytes, 24) },
                .applied_index = readInt(bytes, 32),
                .retained_start = readInt(bytes, 40),
            },
            .objects = readInt(bytes, 48),
            .content_bytes = readInt(bytes, 56),
            .schema_manifest_digest = bytes[64..96].*,
            .ordered_content_digest = bytes[96..128].*,
            .integrity = if (bytes[4] == 1) .{ .catalog_digest = bytes[128..160].*, .generation_set = bytes[160..192].* } else null,
        };
        _ = try result.encode();
        return result;
    }

    pub fn digest(self: Certificate) ![32]u8 {
        const bytes = try self.encode();
        return bytes[192..224].*;
    }

    pub fn eql(self: Certificate, other: Certificate) bool {
        const lhs = self.digest() catch return false;
        const rhs = other.digest() catch return false;
        return std.mem.eql(u8, &lhs, &rhs);
    }
};

fn readInt(bytes: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, bytes[offset..][0..8], .little);
}

fn hashPart(hash: *Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}

/// Chain each logical object rather than persisting implementation-specific
/// SHA state. Metadata is normalized per entry, so block packing and the
/// local cohort proof cannot change the schema/content identity.
pub const Builder = struct {
    certificate: Certificate,
    finished: bool = false,
    started: bool = false,

    pub fn init(cut: Cut) !Builder {
        try cut.validate();
        var schema_digest: [32]u8 = undefined;
        var content_digest: [32]u8 = undefined;
        Sha256.hash("antfly-source-schema-v1", &schema_digest, .{});
        Sha256.hash("antfly-source-content-v1", &content_digest, .{});
        return .{ .certificate = .{ .cut = cut, .objects = 0, .content_bytes = 0, .schema_manifest_digest = schema_digest, .ordered_content_digest = content_digest } };
    }

    fn append(self: *Builder, kind: u8, key: []const u8, value: []const u8) !void {
        self.certificate.objects = std.math.add(u64, self.certificate.objects, 1) catch return error.SourceSnapshotTooLarge;
        self.certificate.content_bytes = std.math.add(u64, self.certificate.content_bytes, key.len + value.len) catch return error.SourceSnapshotTooLarge;
        var hash = Sha256.init(.{});
        hash.update(&self.certificate.ordered_content_digest);
        hash.update(&.{kind});
        hashPart(&hash, key);
        hashPart(&hash, value);
        hash.final(&self.certificate.ordered_content_digest);
        if (kind == 0x18) {
            hash = Sha256.init(.{});
            hash.update(&self.certificate.schema_manifest_digest);
            hashPart(&hash, key);
            hashPart(&hash, value);
            hash.final(&self.certificate.schema_manifest_digest);
        }
    }

    /// Block tags are the existing AFB logical block tags, not a new format.
    /// Copy-on-success keeps a malformed/cancelled block from advancing durable
    /// progress; callers may checkpoint only after this method succeeds.
    pub fn addBlock(self: *Builder, kind: u8, payload: []const u8) !void {
        if (self.finished or payload.len > max_block_bytes) return error.InvalidSourceSnapshot;
        var next = self.*;
        if (!next.started) {
            if (kind != 0x01) return error.InvalidSourceSnapshot;
            next.started = true;
        }
        switch (kind) {
            0x18 => {
                if (payload.len < 4) return error.InvalidSourceSnapshot;
                const count = std.mem.readInt(u32, payload[0..4], .little);
                // Each entry needs at least two length words; reject malicious
                // counts without allocating an entry array or iterating it.
                if (count > (payload.len - 4) / 8) return error.InvalidSourceSnapshot;
                var offset: usize = 4;
                for (0..count) |_| {
                    const key = try readPart(payload, &offset);
                    const value = try readPart(payload, &offset);
                    if (std.mem.eql(u8, key, integrity_key)) {
                        if (value.len != 64 or next.certificate.integrity != null) return error.InvalidSourceSnapshot;
                        next.certificate.integrity = .{ .catalog_digest = value[0..32].*, .generation_set = value[32..64].* };
                    }
                    if (!std.mem.eql(u8, key, cohort_key)) try next.append(kind, key, value);
                }
                if (offset != payload.len) return error.InvalidSourceSnapshot;
            },
            // The last eight footer bytes count physical archive bytes; the
            // cohort proof's envelope is deliberately not logical identity.
            0xff => {
                if (payload.len != 24) return error.InvalidSourceSnapshot;
                try next.append(kind, "", payload[0..16]);
                next.finished = true;
            },
            0x1b => {
                if (next.certificate.integrity == null) return error.InvalidSourceSnapshot;
                try next.append(kind, "", payload);
            },
            0x01, 0x02, 0x03, 0x10...0x17, 0x19, 0x1a, 0xf0 => try next.append(kind, "", payload),
            else => return error.InvalidSourceSnapshot,
        }
        self.* = next;
    }

    pub fn finish(self: *const Builder) !Certificate {
        if (!self.started or !self.finished) return error.SourceSnapshotIncomplete;
        _ = try self.certificate.encode();
        return self.certificate;
    }
};

fn readPart(bytes: []const u8, offset: *usize) ![]const u8 {
    if (bytes.len - offset.* < 4) return error.InvalidSourceSnapshot;
    const length = std.mem.readInt(u32, bytes[offset.*..][0..4], .little);
    offset.* += 4;
    if (length > bytes.len - offset.*) return error.InvalidSourceSnapshot;
    defer offset.* += length;
    return bytes[offset.*..][0..length];
}

test "source snapshot certificate binds namespace independent clocks and corrupt bytes" {
    const cut: Cut = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .applied_index = 81, .retained_start = 0 };
    var builder = try Builder.init(cut);
    try std.testing.expectError(error.SourceSnapshotIncomplete, builder.finish());
    try builder.addBlock(1, "{}");
    try builder.addBlock(0x10, "typed historical rows");
    try builder.addBlock(0xff, &(@as([24]u8, @splat(0))));
    const certificate = try builder.finish();
    var encoded = try certificate.encode();
    try std.testing.expect(certificate.eql(try Certificate.decode(&encoded)));
    var changed = certificate;
    changed.cut.retained_start = 81;
    try std.testing.expect(!certificate.eql(changed));
    changed = certificate;
    changed.cut.namespace.range_id += 1;
    try std.testing.expect(!certificate.eql(changed));
    changed = certificate;
    changed.schema_manifest_digest[0] ^= 1;
    try std.testing.expect(!certificate.eql(changed));
    encoded[80] ^= 1;
    try std.testing.expectError(error.SourceSnapshotCorrupt, Certificate.decode(&encoded));
    try std.testing.expectError(error.InvalidSourceSnapshot, builder.addBlock(0x10, "late row"));
}

test "source snapshot malformed metadata never advances progress" {
    var builder = try Builder.init(.{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .applied_index = 1, .retained_start = 0 });
    try builder.addBlock(1, "{}");
    const before = builder.certificate;
    try std.testing.expectError(error.InvalidSourceSnapshot, builder.addBlock(0x18, &.{ 255, 255, 255, 255 }));
    try std.testing.expect(before.eql(builder.certificate));
}

test "relational index system source snapshot integrity generation comes only from immutable manifest and binds content" {
    const alloc = std.testing.allocator;
    var builder = try Builder.init(.{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .applied_index = 1, .retained_start = 0 });
    try builder.addBlock(1, "{}");
    try std.testing.expectError(error.InvalidSourceSnapshot, builder.addBlock(0x1b, "uncertified claims"));
    var binding: [64]u8 = @splat(1);
    @memset(binding[32..], 2);
    const metadata = try testMetadata(integrity_key, &binding);
    defer alloc.free(metadata);
    try builder.addBlock(0x18, metadata);
    try std.testing.expectError(error.InvalidSourceSnapshot, builder.addBlock(0x18, metadata));
    try builder.addBlock(0x1b, "validated physical claim/reference batch");
    try builder.addBlock(0xff, &(@as([24]u8, @splat(0))));
    const certificate = try builder.finish();
    try std.testing.expectEqualSlices(u8, binding[0..32], &certificate.integrity.?.catalog_digest);
    const bytes = try certificate.encode();
    try std.testing.expect(certificate.eql(try Certificate.decode(&bytes)));
    var missing = certificate;
    missing.integrity = null;
    try std.testing.expect(!missing.eql(certificate));
    var changed = certificate;
    changed.integrity.?.generation_set[0] ^= 1;
    try std.testing.expect(!changed.eql(certificate));
}

fn testMetadata(key: []const u8, value: []const u8) ![]u8 {
    const bytes = try std.testing.allocator.alloc(u8, 12 + key.len + value.len);
    std.mem.writeInt(u32, bytes[0..4], 1, .little);
    std.mem.writeInt(u32, bytes[4..8], @intCast(key.len), .little);
    @memcpy(bytes[8..][0..key.len], key);
    std.mem.writeInt(u32, bytes[8 + key.len ..][0..4], @intCast(value.len), .little);
    @memcpy(bytes[12 + key.len ..], value);
    return bytes;
}

test "source snapshot normalized logical identity excludes local pin and physical footer only" {
    const cut: Cut = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .applied_index = 81, .retained_start = 12 };
    const schema = try testMetadata("\x00\x00__metadata__:schema_v1", "immutable layout v1");
    defer std.testing.allocator.free(schema);
    const first_proof = try testMetadata(cohort_key, "inode A mtime A original local pin");
    defer std.testing.allocator.free(first_proof);
    const second_proof = try testMetadata(cohort_key, "inode B, another local seal");
    defer std.testing.allocator.free(second_proof);
    var certificates: [4]Certificate = undefined;
    for (&certificates, 0..) |*certificate, index| {
        var builder = try Builder.init(cut);
        try builder.addBlock(1, "{}");
        try builder.addBlock(0x18, if (index == 0) first_proof else second_proof);
        try builder.addBlock(0x18, schema);
        try builder.addBlock(0x10, if (index == 2) "row B" else "row A");
        try builder.addBlock(0x10, if (index == 2) "row A" else "row B");
        var footer: [24]u8 = @splat(0);
        std.mem.writeInt(u64, footer[16..24], 100 + index, .little);
        if (index == 3) footer[0] = 2; // logical counts remain bound
        try builder.addBlock(0xff, &footer);
        certificate.* = try builder.finish();
    }
    try std.testing.expect(certificates[0].eql(certificates[1]));
    try std.testing.expect(!certificates[0].eql(certificates[2]));
    try std.testing.expect(!certificates[0].eql(certificates[3]));
    try std.testing.expectEqualSlices(u8, &certificates[0].schema_manifest_digest, &certificates[2].schema_manifest_digest);
}
