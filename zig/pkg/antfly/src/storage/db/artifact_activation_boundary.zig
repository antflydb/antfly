// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Immutable receiver-local replay boundary for an artifact authority epoch.
//! This identifies the historic baseline; it does NOT certify any projection.
//! Only a complete snapshot/replay adoption proof can cover historic rows.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const keys = @import("../internal_keys.zig");
pub const key = "\x00\x00__artifact_publication__:activation-boundary";
pub const Encoded = [108]u8;

pub const Boundary = struct {
    authority: publication.Authority,
    replay_sequence: u64,

    pub fn encode(self: Boundary) !Encoded {
        if (self.authority.epoch == 0 or std.mem.allEqual(u8, &self.authority.namespace, 0) or
            std.mem.allEqual(u8, &self.authority.catalog_digest, 0) or self.replay_sequence == std.math.maxInt(u64)) return error.ArtifactCatalogCorrupt;
        var raw: Encoded = undefined;
        @memcpy(raw[0..4], "AAB1");
        @memcpy(raw[4..28], &self.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.authority.epoch, .little);
        @memcpy(raw[36..68], &self.authority.catalog_digest);
        std.mem.writeInt(u64, raw[68..76], self.replay_sequence, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0..76], raw[76..108], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Boundary {
        if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "AAB1")) return error.ArtifactCatalogCorrupt;
        const value: Boundary = .{ .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .replay_sequence = std.mem.readInt(u64, raw[68..76], .little) };
        if (!std.mem.eql(u8, raw, &try value.encode())) return error.ArtifactCatalogCorrupt;
        return value;
    }

    pub fn requireCurrent(self: Boundary, txn: anytype) !void {
        const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(active, self.authority)) return error.ArtifactCatalogDrift;
        const current = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
        if (!std.meta.eql(current, self)) return error.EnrichmentSourceChanged;
    }
};

pub fn load(txn: anytype) !?Boundary {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
    return try Boundary.decode(raw);
}

pub fn requireAuthority(txn: anytype, authority: publication.Authority) !Boundary {
    const value = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
    if (!std.meta.eql(value.authority, authority)) return error.ArtifactCatalogDrift;
    return value;
}

/// Called BEFORE publishing the new authority in the same primary transaction.
/// Repeated activation cannot move the boundary to a newer journal tip. Neither
/// in-memory reserved sequence numbers nor a remote owner's cut are eligible.
pub fn stage(txn: anytype, next: publication.Authority) !void {
    const previous: ?Boundary = if (try publication.authority(txn)) |active| blk: {
        const existing = try requireAuthority(txn, active);
        if (std.meta.eql(active, next)) return;
        break :blk existing;
    } else blk: {
        if (try load(txn) != null) return error.ArtifactCatalogCorrupt;
        break :blk null;
    };
    const sequence = blk: {
        const raw = txn.get(&keys.replay_meta_next_sequence_key) catch |err| switch (err) {
            error.NotFound => break :blk 0,
            else => return err,
        };
        if (raw.len != 8) return error.CorruptReplayMetadata;
        const committed_next = std.mem.readInt(u64, raw[0..8], .little);
        if (committed_next == 0) return error.CorruptReplayMetadata;
        break :blk committed_next - 1;
    };
    if (previous) |old| if (std.mem.eql(u8, &old.authority.namespace, &next.namespace) and sequence < old.replay_sequence) return error.CorruptReplayMetadata;
    const boundary: Boundary = .{ .authority = next, .replay_sequence = sequence };
    try txn.put(key, &try boundary.encode());
}

test "ordered artifact inventory activation boundary authenticates its entire scope and cut" {
    const value: Boundary = .{ .authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) }, .replay_sequence = 4 };
    const raw = try value.encode();
    try std.testing.expectEqualDeep(value, try Boundary.decode(&raw));
    for (0..raw.len) |i| {
        var damaged = raw;
        damaged[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Boundary.decode(&damaged));
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Boundary.decode(raw[0..i]));
    }
    var invalid = value;
    invalid.replay_sequence = std.math.maxInt(u64);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, invalid.encode());
    invalid.replay_sequence = 0;
    try std.testing.expectEqualDeep(invalid, try Boundary.decode(&try invalid.encode()));
    try std.testing.expect(!@import("../portable_backup.zig").isPortableMetadataKey(key));
}

test "ordered artifact inventory activation boundary is atomic immutable local and survives reopen" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/activation-boundary", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    var command: publication.Command = .{ .mode = .activate, .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    var next_raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &next_raw, 18, .little);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(&keys.replay_meta_next_sequence_key, &next_raw);
        try txn.commit();
    }
    // Reserved but uncommitted sequences must not inflate activation's cut.
    _ = db.core.store.reserveNextReplaySequence(100);
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try publication.stageAuthority(&txn, command);
        try std.testing.expectEqual(@as(u64, 17), (try load(&txn)).?.replay_sequence);
    }
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try load(&read) == null);
        try std.testing.expect(try publication.authority(&read) == null);
    }
    var original: Boundary = undefined;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try publication.stageAuthority(&txn, command);
        original = (try load(&txn)).?;
        try original.requireCurrent(&txn);
        try txn.commit();
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        std.mem.writeInt(u64, &next_raw, 29, .little);
        try txn.put(&keys.replay_meta_next_sequence_key, &next_raw);
        try publication.stageAuthority(&txn, command);
        try std.testing.expectEqualDeep(original, (try load(&txn)).?);
        try txn.commit();
    }
    db.close();
    db = try db_mod.DB.open(alloc, path, options);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try original.requireCurrent(&read);
        try std.testing.expectEqualDeep(original, (try load(&read)).?);
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        command.authority_epoch = 2;
        try publication.stageAuthority(&txn, command);
        try std.testing.expectEqual(@as(u64, 28), (try load(&txn)).?.replay_sequence);
        try std.testing.expectError(error.ArtifactCatalogDrift, original.requireCurrent(&txn));
        try txn.commit();
    }
    // Missing evidence for an existing authority is corruption, not permission
    // to adopt today's newer replay tip on a retry.
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(key);
        try std.testing.expectError(error.ArtifactCatalogCorrupt, publication.stageAuthority(&txn, command));
    }
    {
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        command.authority_epoch = 3;
        try txn.put(&keys.replay_meta_next_sequence_key, &@as([8]u8, @splat(0)));
        try std.testing.expectError(error.CorruptReplayMetadata, publication.stageAuthority(&txn, command));
        try txn.put(&keys.replay_meta_next_sequence_key, "bad");
        try std.testing.expectError(error.CorruptReplayMetadata, publication.stageAuthority(&txn, command));
        try txn.delete(&keys.replay_meta_next_sequence_key);
        try std.testing.expectError(error.CorruptReplayMetadata, publication.stageAuthority(&txn, command));
        // A genuinely new namespace can have an empty local journal.
        command.namespace = @splat(3);
        try publication.stageAuthority(&txn, command);
        try std.testing.expectEqual(@as(u64, 0), (try load(&txn)).?.replay_sequence);
    }
}
