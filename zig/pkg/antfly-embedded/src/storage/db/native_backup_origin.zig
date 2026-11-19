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

//! Source provenance captured at the SAME apply fence as the primary snapshot.
//! Artifact hashes authenticate the image; these facts join it to its physical
//! origin and control cut. Equality is NOT metadata root-adoption authority.
const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const namespaceBytes = @import("online_source_contract.zig").namespaceBytes;
const source = @import("../source_authority.zig");
const receipts = @import("apply_receipts.zig");
const outcomes = @import("relational_schema_apply_outcome.zig");
const debt = @import("../transaction_debt.zig");
const job_key = "\x00\x00__metadata__:schema_validation_job:v1";
const Digest = [32]u8;

pub const Origin = struct {
    pub const encoded_len = 288;
    version: u8 = 1,
    physical_root_incarnation: u128,
    namespace: ?Namespace,
    authority: ?source.State,
    applied: ?receipts.OrderedApplyReceipt,
    schema_header: ?Digest,
    transaction_directory: ?Digest,
    schema_outcomes: ?Digest,

    /// Fixed-size, canonical transfer/checkpoint representation. No allocator
    /// or JSON parser is needed on the snapshot envelope's admission path.
    pub fn encode(self: Origin) ![encoded_len]u8 {
        try self.validate();
        var raw: [encoded_len]u8 = @splat(0);
        @memcpy(raw[0..8], "ANOR\x01\x00\x00\x00");
        std.mem.writeInt(u128, raw[16..32], self.physical_root_incarnation, .little);
        if (self.namespace) |ns| {
            raw[8] |= 1;
            raw[32..56].* = namespaceBytes(ns);
        }
        if (self.authority) |state| {
            raw[8] |= 2;
            raw[56..128].* = state.encode();
        }
        if (self.applied) |cut| {
            raw[8] |= 4;
            std.mem.writeInt(u64, raw[128..136], cut.term, .little);
            std.mem.writeInt(u64, raw[136..144], cut.index, .little);
        }
        inline for (.{ self.schema_header, self.transaction_directory, self.schema_outcomes }, 0..) |value, i| if (value) |bytes| {
            raw[8] |= @as(u8, 8) << @intCast(i);
            raw[144 + i * 32 ..][0..32].* = bytes;
        };
        std.crypto.hash.Blake3.hash(raw[0..256], raw[256..288], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Origin {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..8], "ANOR\x01\x00\x00\x00") or raw[8] & 0xc0 != 0) return error.InvalidNativeBackupOrigin;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(raw[0..256], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[256..288])) return error.InvalidNativeBackupOrigin;
        var result: Origin = .{
            .physical_root_incarnation = std.mem.readInt(u128, raw[16..32], .little),
            .namespace = if (raw[8] & 1 == 0) null else .{ .table_id = std.mem.readInt(u64, raw[32..40], .big), .shard_id = std.mem.readInt(u64, raw[40..48], .big), .range_id = std.mem.readInt(u64, raw[48..56], .big) },
            .authority = if (raw[8] & 2 == 0) null else source.State.decode(raw[56..128]) catch return error.InvalidNativeBackupOrigin,
            .applied = if (raw[8] & 4 == 0) null else .{ .term = std.mem.readInt(u64, raw[128..136], .little), .index = std.mem.readInt(u64, raw[136..144], .little) },
            .schema_header = null,
            .transaction_directory = null,
            .schema_outcomes = null,
        };
        inline for (.{ "schema_header", "transaction_directory", "schema_outcomes" }, 0..) |field, i|
            if (raw[8] & (@as(u8, 8) << @intCast(i)) != 0) {
                @field(result, field) = raw[144 + i * 32 ..][0..32].*;
            };
        const canonical = try result.encode();
        if (!std.mem.eql(u8, raw, &canonical)) return error.InvalidNativeBackupOrigin;
        return result;
    }

    pub fn validate(self: Origin) !void {
        if (self.version != 1 or self.physical_root_incarnation == 0) return error.InvalidNativeBackupOrigin;
        if (self.namespace) |ns| {
            // Released embedded document tables may use the zero/default
            // namespace. Only a schema job claims a fully scoped owner here.
            if (self.schema_header != null and (ns.table_id == 0 or ns.shard_id == 0 or ns.range_id == 0)) return error.InvalidNativeBackupOrigin;
        } else if (self.schema_header != null or self.transaction_directory != null or self.schema_outcomes != null)
            return error.InvalidNativeBackupOrigin;
        if (self.authority) |state| {
            if (self.namespace) |ns| if (!std.mem.eql(u8, &namespaceBytes(ns), &state.namespace)) return error.InvalidNativeBackupOrigin;
            if (state.kind == .native and self.applied != null) return error.InvalidNativeBackupOrigin;
            _ = source.State.decode(&state.encode()) catch return error.InvalidNativeBackupOrigin;
        }
        if (self.applied) |cut| if (cut.term == 0 or cut.index == 0) return error.InvalidNativeBackupOrigin;
        inline for (.{ self.schema_header, self.transaction_directory, self.schema_outcomes }) |commitment|
            if (commitment) |value| if (std.mem.allEqual(u8, &value, 0)) return error.InvalidNativeBackupOrigin;
    }

    /// O(1) point reads. Caller holds the snapshot fence or supplies the
    /// independently qualified immutable imported image. No borrowed values
    /// survive a subsequent provider read.
    pub fn capture(txn: anytype, root: u128, namespace: ?Namespace) !Origin {
        const result: Origin = .{
            .physical_root_incarnation = root,
            .namespace = namespace,
            .authority = try source.load(txn),
            .applied = try receipts.readOrderedApplyReceiptTxn(txn),
            .schema_header = try pointDigest(txn, job_key),
            .transaction_directory = try pointDigest(txn, debt.head_key),
            .schema_outcomes = if (namespace) |ns| try pointDigest(txn, &outcomes.headKey(ns)) else null,
        };
        try result.validate();
        return result;
    }

    /// Destination root identity is deliberately not equated with the source.
    pub fn requireImage(self: Origin, txn: anytype, namespace: ?Namespace) !void {
        try self.validate();
        const actual = try capture(txn, self.physical_root_incarnation, namespace);
        if (!std.meta.eql(self, actual)) return error.NativeBackupOriginMismatch;
    }

    pub fn digest(self: Origin) !Digest {
        try self.validate();
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update("antfly/native-backup-origin/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hasher, &.{});
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }

    pub fn requireRaftTransfer(self: Origin, namespace: [24]u8, through_index: u64) !void {
        try self.validate();
        if (self.namespace) |ns| if (!std.mem.eql(u8, &namespaceBytes(ns), &namespace)) return error.InvalidNativeBackupOrigin;
        if (self.authority) |state| if (state.kind != .raft or !std.mem.eql(u8, &state.namespace, &namespace)) return error.InvalidNativeBackupOrigin;
        if (self.applied) |cut| if (cut.index > through_index) return error.InvalidNativeBackupOrigin;
    }
};

/// Receiver-owned durable evidence for an independently verified Raft image.
/// Checksums/root equality are not signer enrollment or an adoption grant.
pub const Installation = struct {
    pub const encoded_len = 416;
    pub const file_name = "replica_snapshot_origin.checkpoint";
    group_id: u64,
    through_index: u64,
    destination_root_incarnation: u128,
    namespace: [24]u8,
    source: Origin,
    stream_digest: Digest,

    pub fn validate(self: Installation) !void {
        if (self.group_id == 0 or self.through_index == 0 or self.destination_root_incarnation == 0 or std.mem.allEqual(u8, &self.stream_digest, 0)) return error.InvalidNativeBackupOrigin;
        try self.source.requireRaftTransfer(self.namespace, self.through_index);
    }
    pub fn encode(self: Installation) ![encoded_len]u8 {
        try self.validate();
        var raw: [encoded_len]u8 = @splat(0);
        @memcpy(raw[0..8], "NSOR\x01\x00\x00\x00");
        std.mem.writeInt(u64, raw[8..16], self.group_id, .little);
        std.mem.writeInt(u64, raw[16..24], self.through_index, .little);
        std.mem.writeInt(u128, raw[24..40], self.destination_root_incarnation, .little);
        raw[40..64].* = self.namespace;
        raw[64..352].* = try self.source.encode();
        raw[352..384].* = self.stream_digest;
        std.crypto.hash.Blake3.hash(raw[0..384], raw[384..416], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Installation {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..8], "NSOR\x01\x00\x00\x00")) return error.InvalidNativeBackupOrigin;
        var checksum: Digest = undefined;
        std.crypto.hash.Blake3.hash(raw[0..384], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[384..416])) return error.InvalidNativeBackupOrigin;
        const value: Installation = .{
            .group_id = std.mem.readInt(u64, raw[8..16], .little),
            .through_index = std.mem.readInt(u64, raw[16..24], .little),
            .destination_root_incarnation = std.mem.readInt(u128, raw[24..40], .little),
            .namespace = raw[40..64].*,
            .source = try Origin.decode(raw[64..352]),
            .stream_digest = raw[352..384].*,
        };
        try value.validate();
        return value;
    }
    pub fn requireDestination(self: Installation, group: u64, namespace: [24]u8, root: u128) !void {
        try self.validate();
        if (self.group_id != group or self.destination_root_incarnation != root or !std.mem.eql(u8, &self.namespace, &namespace)) return error.NativeBackupOriginMismatch;
    }
    pub fn historyAnchor(self: Installation, current: ?receipts.OrderedApplyReceipt) !outcomes.Anchor {
        try self.validate();
        // The verified transfer may precede the first data entry or authority
        // row. Preserve that absence; the owner/group admission is separate.
        var record_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(&try self.encode(), &record_digest, .{});
        const anchor: outcomes.Anchor = .{ .cut = self.source.applied, .head = self.source.schema_outcomes, .record_digest = record_digest };
        try anchor.validate(current);
        return anchor;
    }
};

/// Load only receiver-owned installation evidence from the actual retained DB
/// root. An absent sidecar is absence of lineage, never an inferred grant.
/// Present files must be regular, bounded, canonical and bound to the existing
/// destination identity. No identity is minted and no symlink is followed.
pub fn loadInstallation(alloc: std.mem.Allocator, io: std.Io, root: []const u8, group: u64, namespace: [24]u8, destination_root: u128, context: @import("../../api/operation.zig").RequestContext) !?Installation {
    try context.ensureActive();
    const directory = try std.Io.Dir.cwd().openDir(io, root, .{ .follow_symlinks = false });
    defer directory.close(io);
    const file = directory.openFile(io, Installation.file_name, .{ .follow_symlinks = false }) catch |err| {
        if (err == error.FileNotFound) return null;
        if (err == error.IsDir or err == error.SymLinkLoop) return error.InvalidNativeBackupOrigin;
        return err;
    };
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size != Installation.encoded_len) return error.InvalidNativeBackupOrigin;
    const identity = try @import("root_identity.zig").load(alloc, io, root);
    if (destination_root == 0 or identity.incarnation != destination_root) return error.NativeBackupOriginMismatch;
    var buffer: [Installation.encoded_len]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const bytes = try reader.interface.allocRemaining(alloc, .limited(Installation.encoded_len + 1));
    defer alloc.free(bytes);
    const result = try Installation.decode(bytes);
    try result.requireDestination(group, namespace, destination_root);
    try context.ensureActive();
    if ((try @import("root_identity.zig").load(alloc, io, root)).incarnation != destination_root) return error.NativeBackupOriginMismatch;
    return result;
}

fn pointDigest(txn: anytype, key: []const u8) !?Digest {
    const bytes = txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    if (bytes.len > 44 + @import("relational_schema_owner_contract.zig").max_header) return error.InvalidNativeBackupOrigin;
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

test "native backup origin binds image facts without confusing native and Raft clocks" {
    try std.testing.expectEqualStrings(@import("relational_nullability_job.zig").key, job_key);
    const ns: Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    const Reader = struct {
        authority: ?[source.encoded_size]u8 = null,
        marker: ?[16]u8 = null,
        header: ?[]const u8 = null,
        directory: ?[]const u8 = null,
        outcome: ?[]const u8 = null,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (std.mem.eql(u8, key, source.key)) return if (self.authority) |*v| v else error.NotFound;
            if (std.mem.eql(u8, key, &@import("../internal_keys.zig").ordered_document_applied_entry_key)) return if (self.marker) |*v| v else error.NotFound;
            if (std.mem.eql(u8, key, job_key)) return self.header orelse error.NotFound;
            if (std.mem.eql(u8, key, debt.head_key)) return self.directory orelse error.NotFound;
            if (std.mem.startsWith(u8, key, outcomes.head_prefix)) return self.outcome orelse error.NotFound;
            return error.NotFound;
        }
    };
    var read: Reader = .{};
    try (try Origin.capture(&read, 7, null)).requireImage(&read, null);
    try (try Origin.capture(&read, 7, .{})).requireImage(&read, .{});
    read.authority = (source.State{ .kind = .native, .namespace = namespaceBytes(ns), .sequence = 8 }).encode();
    // Provisioning may bind source authority before the first document writes
    // its namespace row. Preserve that absence rather than inventing a row.
    try (try Origin.capture(&read, 7, null)).requireImage(&read, null);
    read.header = "validated header";
    read.directory = "validated directory";
    const native = try Origin.capture(&read, 7, ns);
    try std.testing.expectEqualDeep(native, try Origin.decode(&try native.encode()));
    const oversized: [45 + @import("relational_schema_owner_contract.zig").max_header]u8 = @splat(0);
    read.header = &oversized;
    try std.testing.expectError(error.InvalidNativeBackupOrigin, Origin.capture(&read, 7, ns));
    read.header = "validated header";
    try native.requireImage(&read, ns);
    read.authority = (source.State{ .kind = .native, .namespace = namespaceBytes(ns), .sequence = 9 }).encode();
    try std.testing.expectError(error.NativeBackupOriginMismatch, native.requireImage(&read, ns));
    read.authority = (source.State{ .kind = .native, .namespace = namespaceBytes(ns), .sequence = 8 }).encode();
    const native_hash = try native.digest();
    var replaced = native;
    replaced.physical_root_incarnation = 9;
    try std.testing.expect(!std.mem.eql(u8, &native_hash, &try replaced.digest()));
    read.directory = "other canonical directory";
    try std.testing.expectError(error.NativeBackupOriginMismatch, native.requireImage(&read, ns));
    read.directory = "validated directory";
    read.outcome = "other valid history head";
    try std.testing.expectError(error.NativeBackupOriginMismatch, native.requireImage(&read, ns));
    read.outcome = null;
    read.marker = @splat(0);
    std.mem.writeInt(u64, read.marker.?[0..8], 2, .little);
    std.mem.writeInt(u64, read.marker.?[8..16], 8, .little);
    try std.testing.expectError(error.InvalidNativeBackupOrigin, native.requireImage(&read, ns));
    read.authority = (source.State{ .kind = .raft, .namespace = namespaceBytes(ns) }).encode();
    const raft = try Origin.capture(&read, 7, ns);
    try std.testing.expectEqualDeep(raft, try Origin.decode(&try raft.encode()));
    try raft.requireImage(&read, ns);
    try std.testing.expectError(error.NativeBackupOriginMismatch, native.requireImage(&read, ns));
    var future = raft;
    future.version = 2;
    try std.testing.expectError(error.InvalidNativeBackupOrigin, future.validate());
    future = raft;
    future.physical_root_incarnation = 0;
    try std.testing.expectError(error.InvalidNativeBackupOrigin, future.validate());
}

test "native backup origin fixed record rejects corruption and rechecks canonical absent fields" {
    const empty: Origin = .{ .physical_root_incarnation = 17, .namespace = null, .authority = null, .applied = null, .schema_header = null, .transaction_directory = null, .schema_outcomes = null };
    const encoded = try empty.encode();
    try std.testing.expectEqualDeep(empty, try Origin.decode(&encoded));
    for (0..encoded.len) |i| {
        var damaged = encoded;
        damaged[i] ^= 1;
        try std.testing.expectError(error.InvalidNativeBackupOrigin, Origin.decode(&damaged));
    }
    for ([_]usize{ 8, 12, 32, 56, 128, 144, 240 }) |offset| {
        var damaged = encoded;
        damaged[offset] = if (offset == 8) 0x80 else 1;
        std.crypto.hash.Blake3.hash(damaged[0..256], damaged[256..288], .{});
        try std.testing.expectError(error.InvalidNativeBackupOrigin, Origin.decode(&damaged));
    }
}

test "native backup origin installed receipt binds source image destination and independent completion cut" {
    const ns: Namespace = .{ .table_id = 11, .shard_id = 22, .range_id = 33 };
    const origin: Origin = .{ .physical_root_incarnation = 17, .namespace = ns, .authority = .{ .kind = .raft, .namespace = namespaceBytes(ns) }, .applied = .{ .term = 3, .index = 7 }, .schema_header = @splat(1), .transaction_directory = @splat(2), .schema_outcomes = @splat(3) };
    var installed: Installation = .{ .group_id = 22, .through_index = 10, .destination_root_incarnation = 29, .namespace = namespaceBytes(ns), .source = origin, .stream_digest = @splat(4) };
    const raw = try installed.encode();
    try std.testing.expectEqualDeep(installed, try Installation.decode(&raw));
    try installed.requireDestination(22, namespaceBytes(ns), 29);
    try std.testing.expectError(error.NativeBackupOriginMismatch, installed.requireDestination(23, namespaceBytes(ns), 29));
    try std.testing.expectError(error.NativeBackupOriginMismatch, installed.requireDestination(22, namespaceBytes(ns), 30));
    for (0..raw.len) |offset| {
        var damaged = raw;
        damaged[offset] ^= 1;
        try std.testing.expectError(error.InvalidNativeBackupOrigin, Installation.decode(&damaged));
    }
    installed.through_index = 6;
    try std.testing.expectError(error.InvalidNativeBackupOrigin, installed.validate());
}
