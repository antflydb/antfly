// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fixed native HA seed evidence shared across the provider boundary. These
//! values are not grants: accepting storage rejoins its seed image, while the
//! publisher independently reads genuine backup_start/COMPLETE evidence.
const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
pub const max_witness_bytes: usize = 8192;

pub const Name = struct {
    len: u8,
    bytes: [128]u8 = @splat(0),
    pub fn init(value: []const u8) !@This() {
        if (value.len == 0 or value.len > 128) return error.InvalidNativeSourceSeed;
        var result: @This() = .{ .len = @intCast(value.len) };
        @memcpy(result.bytes[0..value.len], value);
        try result.validate();
        return result;
    }
    pub fn slice(self: *const @This()) []const u8 {
        return self.bytes[0..self.len];
    }
    pub fn validate(self: @This()) !void {
        if (self.len == 0 or self.len > self.bytes.len or !std.mem.allEqual(u8, self.bytes[self.len..], 0)) return error.InvalidNativeSourceSeed;
        for (self.bytes[0..self.len]) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_' and byte != '.') return error.InvalidNativeSourceSeed;
        if (std.mem.eql(u8, self.bytes[0..self.len], ".") or std.mem.eql(u8, self.bytes[0..self.len], "..")) return error.InvalidNativeSourceSeed;
    }
};
pub const Lineage = struct {
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    pub fn validate(self: @This()) !void {
        if (self.cluster_id == 0 or self.timeline_id == 0 or self.epoch == 0) return error.InvalidNativeSourceSeed;
    }
    pub fn requireNamespace(self: @This(), namespace: Namespace) !void {
        try self.validate();
        // Existing node-wide HA identities use zero as an explicit wildcard;
        // the actual per-owner namespace below is always complete/nonzero.
        if (namespace.table_id == 0 or namespace.shard_id == 0 or namespace.range_id == 0 or
            (self.table_id != 0 and self.table_id != namespace.table_id) or
            (self.shard_id != 0 and self.shard_id != namespace.shard_id)) return error.NativeSourceSeedChanged;
    }
};
pub const Spec = struct {
    generation: Name,
    slot: Name,
    backup_lsn: u64,
    pub fn validate(self: @This()) !void {
        try self.generation.validate();
        try self.slot.validate();
        if (self.backup_lsn == 0) return error.InvalidNativeSourceSeed;
    }
};
/// The bound publisher derives this from its REAL durable backup_start and
/// retained seeding slot. Merely copying a requested LSN is not its issuer.
pub const Start = struct {
    spec: Spec,
    lineage: Lineage,
    pub fn validate(self: @This()) !void {
        try self.spec.validate();
        try self.lineage.validate();
    }
};
pub const Pending = struct {
    version: u16 = 1,
    start: Start,
    namespace: Namespace,
    physical_root_incarnation: u128,
    source_sequence: u64,
    commitment: [32]u8,
    pub fn init(start: Start, namespace: Namespace, root: u128, sequence: u64) !@This() {
        var value: @This() = .{ .start = start, .namespace = namespace, .physical_root_incarnation = root, .source_sequence = sequence, .commitment = undefined };
        value.commitment = pendingDigest(value);
        try value.validate();
        return value;
    }
    pub fn validate(self: @This()) !void {
        if (self.version != 1 or self.physical_root_incarnation == 0) return error.InvalidNativeSourceSeed;
        try self.start.validate();
        try self.start.lineage.requireNamespace(self.namespace);
        if (!std.mem.eql(u8, &self.commitment, &pendingDigest(self))) return error.InvalidNativeSourceSeed;
    }
};
/// Derived by the existing capture owner from COMPLETE + manifest/backup_end
/// and the exact retained replica snapshot, never from a caller completion bit.
pub const Completion = struct {
    pending_commitment: [32]u8,
    manifest_digest: [32]u8,
    capture_receipt_digest: [32]u8,
    replica_snapshot_digest: [32]u8,
    checkpoint_lsn: u64,
    end_record_lsn: u64,
    pub fn require(self: @This(), pending: Pending) !void {
        try pending.validate();
        if (!std.mem.eql(u8, &self.pending_commitment, &pending.commitment) or
            std.mem.allEqual(u8, &self.manifest_digest, 0) or std.mem.allEqual(u8, &self.capture_receipt_digest, 0) or
            std.mem.allEqual(u8, &self.replica_snapshot_digest, 0) or self.checkpoint_lsn < pending.start.spec.backup_lsn or
            self.end_record_lsn <= self.checkpoint_lsn) return error.InvalidNativeSourceSeed;
    }
};
pub const Active = struct {
    pending: Pending,
    completion: Completion,
    pub fn validate(self: @This()) !void {
        try self.completion.require(self.pending);
    }
    pub fn requireRecord(self: @This(), record: @import("replication_record.zig").RecordView) !void {
        try self.validate();
        const lineage = self.pending.start.lineage;
        if (record.cluster_id != lineage.cluster_id or record.timeline_id != lineage.timeline_id or record.epoch != lineage.epoch or
            record.table_id != self.pending.namespace.table_id or record.shard_id != self.pending.namespace.shard_id or
            record.lsn <= self.completion.end_record_lsn) return error.NativeSourceSeedChanged;
    }
};
/// Emitted only by the actual default DB snapshot provider, into a
/// content-addressed companion covered by the SAME capture manifest/WAL end.
pub const Witness = struct {
    pending: Pending,
    group_id: u64,
    replica_snapshot_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.pending.validate();
        if (self.group_id == 0 or std.mem.allEqual(u8, &self.replica_snapshot_digest, 0)) return error.InvalidNativeSourceSeed;
    }
    pub fn prefix(a: std.mem.Allocator, pending: Pending) ![]u8 {
        try pending.validate();
        return std.fmt.allocPrint(a, "native-source/{s}/", .{std.fmt.bytesToHex(pending.commitment, .lower)});
    }
};
pub const Record = union(enum) { pending: Pending, active: Active };

fn integer(hash: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    hash.update(&bytes);
}
fn name(hash: *std.crypto.hash.Blake3, value: Name) void {
    integer(hash, u8, value.len);
    hash.update(value.bytes[0..value.len]);
}
fn pendingDigest(value: Pending) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/ha-seed/native-source-frontier/v1\x00");
    integer(&hash, u16, value.version);
    name(&hash, value.start.spec.generation);
    name(&hash, value.start.spec.slot);
    integer(&hash, u64, value.start.spec.backup_lsn);
    inline for (.{ "cluster_id", "shard_id", "table_id", "timeline_id", "epoch" }) |field| integer(&hash, u64, @field(value.start.lineage, field));
    integer(&hash, u64, value.namespace.table_id);
    integer(&hash, u64, value.namespace.shard_id);
    integer(&hash, u64, value.namespace.range_id);
    integer(&hash, u128, value.physical_root_incarnation);
    integer(&hash, u64, value.source_sequence);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
