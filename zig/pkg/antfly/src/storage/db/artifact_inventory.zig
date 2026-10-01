// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Ordered desired artifact catalog and independently checked local materialization.
//! The expected bytes are replicated; replica-local readiness never influences
//! the deterministic result of applying the command. All records live in the
//! primary store, so native snapshots carry the complete authority together.
const std = @import("std");
pub const index_key = "\x00\x00__metadata__:indexes";
pub const enrichment_key = "\x00\x00__metadata__:enrichments";
pub const resolver_key = "\x00\x00__metadata__:resolvers";
pub const ordered_key = "\x00\x00__metadata__:ordered_artifact_catalog";
pub const local_key = "\x00\x00__metadata__:artifact_inventory";

pub const Binding = struct {
    epoch: u64,
    digest: [32]u8,
    semantic_digest: [32]u8,
    /// Part of the immutable transfer identity, not a replica-local feature
    /// flag. Protocol 14 captures direct vectors; 15 captures producer effects.
    effect_protocol: u16 = 14,

    pub fn valid(self: Binding) bool {
        return self.epoch != 0 and (self.effect_protocol == 14 or self.effect_protocol == 15);
    }

    pub fn compatible(a: Binding, b: Binding) bool {
        return a.valid() and b.valid() and a.effect_protocol == b.effect_protocol and std.mem.eql(u8, &a.semantic_digest, &b.semantic_digest);
    }
};
pub const Catalogs = struct {
    indexes: []const u8 = "",
    enrichments: []const u8 = "",
    resolvers: []const u8 = "",

    pub fn jsonStringify(self: Catalogs, stream: anytype) @TypeOf(stream.*).Error!void {
        // AIDX contains arbitrary generation/count bytes, not UTF-8. Catalog
        // control messages are bounded and occur once per schema/copy epoch.
        try @import("relational_integrity_json.zig").write(self, stream);
    }

    /// Compatibility is about definitions, not owner-local physical generation
    /// numbers or catalog insertion order. Exact `digest` still binds all bytes.
    pub fn semanticDigest(self: Catalogs, alloc: std.mem.Allocator) ![32]u8 {
        return @import("artifact_semantics.zig").digest(alloc, self.indexes, self.enrichments, self.resolvers);
    }

    pub fn digest(self: Catalogs) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-artifact-catalog-v1");
        inline for (.{ self.indexes, self.enrichments, self.resolvers }) |bytes| {
            var size: [8]u8 = undefined;
            std.mem.writeInt(u64, &size, bytes.len, .little);
            hash.update(&size);
            hash.update(bytes);
        }
        return hash.finalResult();
    }
    pub fn deinit(self: *Catalogs, alloc: std.mem.Allocator) void {
        alloc.free(self.indexes);
        alloc.free(self.enrichments);
        alloc.free(self.resolvers);
        self.* = undefined;
    }
};
pub const Command = struct {
    namespace: [24]u8,
    previous: ?Binding = null,
    binding: Binding,
    catalogs: Catalogs,

    pub fn validate(self: Command) !void {
        const previous_epoch = if (self.previous) |p| p.epoch else 0;
        if (!self.binding.valid() or previous_epoch == std.math.maxInt(u64) or self.binding.epoch != previous_epoch + 1 or
            !std.mem.eql(u8, &self.binding.digest, &self.catalogs.digest()) or
            self.catalogs.indexes.len > 1024 * 1024 or self.catalogs.enrichments.len > 1024 * 1024 or self.catalogs.resolvers.len > 1024 * 1024 or
            self.catalogs.indexes.len + self.catalogs.enrichments.len + self.catalogs.resolvers.len > 1024 * 1024)
            return error.InvalidArtifactCatalogCommand;
        {
            const semantic = self.binding.semantic_digest;
            const actual = self.catalogs.semanticDigest(std.heap.page_allocator) catch return error.InvalidArtifactCatalogCommand;
            if (!std.mem.eql(u8, &semantic, &actual)) return error.InvalidArtifactCatalogCommand;
        }
    }
};
pub const Ordered = struct { command: Command, applied_index: u64 };
pub const LocalBinding = struct { epoch: u64, digest: [32]u8 };
pub const Status = struct { ordered: ?Binding, applied_index: u64, local: LocalBinding, ready: bool };

test "ordered artifact inventory compatibility binds the complete effect protocol" {
    const direct: Binding = .{ .epoch = 1, .digest = @splat(2), .semantic_digest = @splat(3) };
    var extended = direct;
    extended.effect_protocol = 15;
    try std.testing.expect(direct.valid() and extended.valid());
    try std.testing.expect(!direct.compatible(extended));
    try std.testing.expect(extended.compatible(extended));
    extended.effect_protocol = 16;
    try std.testing.expect(!extended.valid() and !extended.compatible(extended));
}

test "ordered artifact inventory catalog JSON preserves arbitrary binary layout bytes" {
    const original: Catalogs = .{ .indexes = "AIDX\x00\xff\xc0\x80", .enrichments = "[\x80]", .resolvers = "\x00\xfe" };
    const alloc = std.testing.allocator;
    const encoded = try std.json.Stringify.valueAlloc(alloc, original, .{});
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(Catalogs, alloc, encoded, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualDeep(original, parsed.value);
}

pub fn validateRequest(req: anytype) !void {
    const command = req.artifact_catalog orelse return;
    try command.validate();
    const empty: @TypeOf(req) = .{};
    inline for (std.meta.fields(@TypeOf(req))) |field| {
        if (comptime !std.mem.eql(u8, field.name, "artifact_catalog") and !std.mem.eql(u8, field.name, "sync_level") and !std.mem.eql(u8, field.name, "online_source") and !std.mem.eql(u8, field.name, "merge_checkpoint") and !std.mem.eql(u8, field.name, "merge_replication")) {
            if (comptime @typeInfo(field.type) == .pointer and @typeInfo(field.type).pointer.size == .slice) {
                if (@field(req, field.name).len != 0) return error.InvalidArtifactCatalogCommand;
            } else if (!std.meta.eql(@field(req, field.name), @field(empty, field.name))) return error.InvalidArtifactCatalogCommand;
        }
    }
    if (req.online_source) |source| if (source != .admit or req.merge_checkpoint != null or req.merge_replication != null or !std.meta.eql(source.admit.artifact_catalog, @as(?Binding, command.binding))) return error.InvalidArtifactCatalogCommand;
    if (req.merge_checkpoint) |checkpoint| if (checkpoint.kind != .accept and checkpoint.kind != .begin_copy) return error.InvalidArtifactCatalogCommand;
    if (req.merge_replication) |context| {
        const checkpoint = req.merge_checkpoint orelse return error.InvalidArtifactCatalogCommand;
        if (checkpoint.transition_id != context.transition_id or checkpoint.donor_group_id != context.donor_group_id or checkpoint.receiver_group_id != context.receiver_group_id or !std.meta.eql(checkpoint.copy_attempt, context.copy_attempt)) return error.InvalidArtifactCatalogCommand;
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], context.identity_namespace.table_id, .big);
        std.mem.writeInt(u64, namespace[8..16], context.identity_namespace.shard_id, .big);
        std.mem.writeInt(u64, namespace[16..24], context.identity_namespace.range_id, .big);
        if (!std.mem.eql(u8, &namespace, &command.namespace)) return error.InvalidArtifactCatalogCommand;
    }
}

fn get(txn: anytype, key: []const u8) ![]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => "",
        else => return err,
    };
}
pub fn catalogs(txn: anytype) !Catalogs {
    return .{ .indexes = try get(txn, index_key), .enrichments = try get(txn, enrichment_key), .resolvers = try get(txn, resolver_key) };
}
pub fn copyCatalogs(alloc: std.mem.Allocator, txn: anytype) !Catalogs {
    const view = try catalogs(txn);
    const indexes = try alloc.dupe(u8, view.indexes);
    errdefer alloc.free(indexes);
    const enrichments = try alloc.dupe(u8, view.enrichments);
    errdefer alloc.free(enrichments);
    return .{ .indexes = indexes, .enrichments = enrichments, .resolvers = try alloc.dupe(u8, view.resolvers) };
}
pub fn local(txn: anytype) !LocalBinding {
    const bytes = try get(txn, local_key);
    const current = (try catalogs(txn)).digest();
    if (bytes.len == 0) return .{ .epoch = 0, .digest = current };
    if (bytes.len != 40) return error.ArtifactCatalogCorrupt;
    // A missed mutation hook or corrupt restore must not authorize readiness.
    if (!std.mem.eql(u8, bytes[8..40], &current)) return error.ArtifactCatalogDrift;
    return .{ .epoch = std.mem.readInt(u64, bytes[0..8], .little), .digest = current };
}
/// Called in the same write transaction as every local catalog mutation.
pub fn refresh(txn: anytype) !void {
    const bytes = try get(txn, local_key);
    if (bytes.len != 0 and bytes.len != 40) return error.ArtifactCatalogCorrupt;
    const current = (try catalogs(txn)).digest();
    if (bytes.len == 40 and std.mem.eql(u8, bytes[8..40], &current)) return;
    const previous = if (bytes.len == 40) std.mem.readInt(u64, bytes[0..8], .little) else 0;
    var encoded: [40]u8 = undefined;
    std.mem.writeInt(u64, encoded[0..8], std.math.add(u64, previous, 1) catch return error.ArtifactCatalogEpochExhausted, .little);
    @memcpy(encoded[8..40], &current);
    try txn.put(local_key, &encoded);
}
pub fn load(alloc: std.mem.Allocator, txn: anytype) !?std.json.Parsed(Ordered) {
    const raw = try get(txn, ordered_key);
    if (raw.len == 0) return null;
    const parsed = std.json.parseFromSlice(Ordered, alloc, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ArtifactCatalogCorrupt,
    };
    errdefer parsed.deinit();
    parsed.value.command.validate() catch return error.ArtifactCatalogCorrupt;
    if (parsed.value.applied_index == 0) return error.ArtifactCatalogCorrupt;
    return parsed;
}
pub fn stageOrdered(alloc: std.mem.Allocator, txn: anytype, command: Command, applied_index: u64) !void {
    const raw = try prepareOrdered(alloc, txn, command, applied_index) orelse return;
    defer alloc.free(raw);
    try txn.put(ordered_key, raw);
}

pub fn prepareOrdered(alloc: std.mem.Allocator, txn: anytype, command: Command, applied_index: u64) !?[]u8 {
    try command.validate();
    if (applied_index == 0) return error.InvalidArtifactCatalogCommand;
    const old = try load(alloc, txn);
    defer if (old) |value| value.deinit();
    if (old) |value| {
        if (!std.mem.eql(u8, &value.value.command.namespace, &command.namespace)) return error.ArtifactCatalogScopeChanged;
        if (std.meta.eql(value.value.command.binding, command.binding)) return null;
        if (!std.meta.eql(command.previous, @as(?Binding, value.value.command.binding)) or applied_index <= value.value.applied_index)
            return error.ArtifactCatalogEpochChanged;
    } else if (command.previous != null) return error.ArtifactCatalogEpochChanged;
    return try std.json.Stringify.valueAlloc(alloc, Ordered{ .command = command, .applied_index = applied_index }, .{});
}
pub fn status(alloc: std.mem.Allocator, txn: anytype, namespace: [24]u8) !Status {
    const current = try local(txn);
    const ordered = try load(alloc, txn);
    defer if (ordered) |value| value.deinit();
    const value = ordered orelse return .{ .ordered = null, .applied_index = 0, .local = current, .ready = false };
    return .{ .ordered = value.value.command.binding, .applied_index = value.value.applied_index, .local = current, .ready = std.mem.eql(u8, &value.value.command.namespace, &namespace) and std.mem.eql(u8, &value.value.command.binding.digest, &current.digest) };
}
pub fn requireReady(alloc: std.mem.Allocator, txn: anytype, namespace: [24]u8, expected: Binding) !void {
    const observed = try status(alloc, txn, namespace);
    if (!observed.ready or !std.meta.eql(observed.ordered, @as(?Binding, expected))) return error.ArtifactCatalogDrift;
}

pub fn requireCompatibleReady(alloc: std.mem.Allocator, txn: anytype, namespace: [24]u8, expected: Binding) !void {
    const observed = try status(alloc, txn, namespace);
    if (!observed.ready or observed.ordered == null or !observed.ordered.?.compatible(expected)) return error.ArtifactCatalogDrift;
}

/// A new owner must establish its own ordering. Copying a source's seal into a
/// split or restore target would incorrectly inherit that source's Raft cut.
pub fn invalidateOrdered(txn: anytype) !void {
    txn.delete(ordered_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    txn.delete("__metadata__:artifact_reconcile_intent") catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    txn.delete("__metadata__:artifact_reconcile_resolver_cursor") catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

const TestTxn = struct {
    values: std.StringHashMap([]u8),
    fn init() TestTxn {
        return .{ .values = std.StringHashMap([]u8).init(std.testing.allocator) };
    }
    fn deinit(self: *TestTxn) void {
        var it = self.values.valueIterator();
        while (it.next()) |value| std.testing.allocator.free(value.*);
        self.values.deinit();
    }
    fn get(self: *TestTxn, key: []const u8) anyerror![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }
    fn put(self: *TestTxn, key: []const u8, value: []const u8) !void {
        const copy = try std.testing.allocator.dupe(u8, value);
        errdefer std.testing.allocator.free(copy);
        if (try self.values.fetchPut(key, copy)) |old| std.testing.allocator.free(old.value);
    }
    fn delete(self: *TestTxn, key: []const u8) anyerror!void {
        const old = self.values.fetchRemove(key) orelse return error.NotFound;
        std.testing.allocator.free(old.value);
    }
};

test "ordered artifact inventory semantic compatibility ignores history order and owner generations" {
    const alloc = std.testing.allocator;
    const empty: Catalogs = .{};
    const historical: Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x00\x00\x00\x00", .enrichments = " [ ] ", .resolvers = "[]" };
    try std.testing.expectEqual(try empty.semanticDigest(alloc), try historical.semanticDigest(alloc));
    const header = "AIDX\x02\x00\x00\x00\x02\x00\x00\x00";
    const a = "\x01\x00\x00\x00a\x00\x02\x00\x00\x00{}";
    const b = "\x01\x00\x00\x00b\x00\x02\x00\x00\x00{}";
    const one = "\x01\x00\x00\x00\x00\x00\x00\x00";
    const two = "\x02\x00\x00\x00\x00\x00\x00\x00";
    const first: Catalogs = .{ .indexes = header ++ a ++ one ++ b ++ two };
    const second: Catalogs = .{ .indexes = header ++ b ++ one ++ a ++ two, .enrichments = "[]" };
    const left: Binding = .{ .epoch = 1, .digest = first.digest(), .semantic_digest = try first.semanticDigest(alloc) };
    const right: Binding = .{ .epoch = 9, .digest = second.digest(), .semantic_digest = try second.semanticDigest(alloc) };
    try std.testing.expect(!std.mem.eql(u8, &left.digest, &right.digest));
    try std.testing.expect(left.compatible(right));
    const json_prefix = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00a\x00\x0d\x00\x00\x00";
    const json_left: Catalogs = .{ .indexes = json_prefix ++ "{\"a\":1,\"b\":2}" ++ one };
    const json_right: Catalogs = .{ .indexes = json_prefix ++ "{\"b\":2,\"a\":1}" ++ two };
    const json_changed: Catalogs = .{ .indexes = json_prefix ++ "{\"b\":3,\"a\":1}" ++ two };
    try std.testing.expectEqual(try json_left.semanticDigest(alloc), try json_right.semanticDigest(alloc));
    try std.testing.expect(!std.mem.eql(u8, &try json_left.semanticDigest(alloc), &try json_changed.semanticDigest(alloc)));
    const changed: Catalogs = .{ .indexes = header ++ a ++ one ++ a ++ two };
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, changed.semanticDigest(alloc));
    const renamed: Catalogs = .{ .indexes = header ++ a ++ one ++ "\x01\x00\x00\x00c\x00\x02\x00\x00\x00{}" ++ two };
    try std.testing.expect(!std.mem.eql(u8, &left.semantic_digest, &try renamed.semanticDigest(alloc)));
    var forged: Command = .{ .namespace = @splat(1), .binding = left, .catalogs = first };
    forged.binding.semantic_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, forged.validate());
}

test "ordered artifact inventory is deterministic despite local drift and recovers after reconciliation" {
    const alloc = std.testing.allocator;
    var leader = TestTxn.init();
    defer leader.deinit();
    var follower = TestTxn.init();
    defer follower.deinit();
    const expected: Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x00\x00\x00\x00", .resolvers = "[]", .enrichments = "[]" };
    const command: Command = .{ .namespace = @splat(1), .binding = .{ .epoch = 1, .digest = expected.digest(), .semantic_digest = try expected.semanticDigest(alloc) }, .catalogs = expected };
    try leader.put(index_key, expected.indexes);
    try leader.put(resolver_key, expected.resolvers);
    try leader.put(enrichment_key, expected.enrichments);
    try refresh(&leader);
    try follower.put(index_key, "different-local-index");
    try refresh(&follower);
    try stageOrdered(alloc, &leader, command, 7);
    try stageOrdered(alloc, &follower, command, 7);
    try std.testing.expectEqualSlices(u8, try leader.get(ordered_key), try follower.get(ordered_key));
    try requireReady(alloc, &leader, command.namespace, command.binding);
    try std.testing.expectError(error.ArtifactCatalogDrift, requireReady(alloc, &follower, command.namespace, command.binding));
    try follower.put(index_key, expected.indexes);
    try follower.put(resolver_key, expected.resolvers);
    try follower.put(enrichment_key, expected.enrichments);
    try refresh(&follower);
    try requireReady(alloc, &follower, command.namespace, command.binding);
    const restored = (try load(alloc, &follower)).?;
    defer restored.deinit();
    try std.testing.expectEqualSlices(u8, expected.indexes, restored.value.command.catalogs.indexes);
    try std.testing.expect(!(try status(alloc, &follower, @splat(2))).ready);
    try invalidateOrdered(&follower);
    try std.testing.expect(!(try status(alloc, &follower, command.namespace)).ready);
}

test "ordered artifact inventory covers every config generation and rejects stale ordering" {
    const alloc = std.testing.allocator;
    var txn = TestTxn.init();
    defer txn.deinit();
    const initial = try catalogs(&txn);
    const command: Command = .{ .namespace = @splat(1), .binding = .{ .epoch = 1, .digest = initial.digest(), .semantic_digest = try initial.semanticDigest(alloc) }, .catalogs = initial };
    try stageOrdered(alloc, &txn, command, 9);
    try stageOrdered(alloc, &txn, command, 9);
    inline for (.{ index_key, enrichment_key, resolver_key }) |key| {
        try txn.put(key, "config-or-generation-change");
        try refresh(&txn);
        try std.testing.expectError(error.ArtifactCatalogDrift, requireReady(alloc, &txn, command.namespace, command.binding));
        try txn.delete(key);
        try refresh(&txn);
    }
    var next = command;
    next.binding.epoch = 2;
    next.previous = command.binding;
    try std.testing.expectError(error.ArtifactCatalogEpochChanged, stageOrdered(alloc, &txn, next, 8));
    try stageOrdered(alloc, &txn, next, 10);
    try std.testing.expectError(error.ArtifactCatalogEpochChanged, stageOrdered(alloc, &txn, command, 11));
    try txn.put(index_key, "untracked-corruption");
    try std.testing.expectError(error.ArtifactCatalogDrift, local(&txn));
}
