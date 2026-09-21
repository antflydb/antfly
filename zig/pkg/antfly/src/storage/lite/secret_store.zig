// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Scope-bound encrypted secrets in the native file's private metadata catalog.
//! Host authorization selects the scope; the file/index is a trusted boundary.
const std = @import("std");
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const sync = @import("antfly_platform").sync;
const contract = @import("../../common/secret_contract.zig");
const record = @import("../../common/secret_record.zig");
const Allocator = std.mem.Allocator;
const namespace = native.secret_catalog_prefix;

pub const Store = struct {
    allocator: Allocator,
    docs: *docstore.Store,
    scope: []u8,
    provider: record.KeyProvider,
    prefix: [namespace.len + 64 + 1]u8,

    /// Both docs and provider must outlive this adapter and its borrowed interfaces.
    /// Keep the adapter at a stable address after obtaining source/nativeStore.
    pub fn init(allocator: Allocator, docs: *docstore.Store, scope: []const u8, provider: record.KeyProvider) !Store {
        try contract.validateName(scope);
        return .{
            .allocator = allocator,
            .docs = docs,
            .scope = try allocator.dupe(u8, scope),
            .provider = provider,
            .prefix = namespace.* ++ digest(scope) ++ "/".*,
        };
    }

    pub fn deinit(self: *Store) void {
        self.allocator.free(self.scope);
        self.* = undefined;
    }

    pub fn source(self: *Store) contract.Source {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .list_metadata = listMetadata, .refresh = refresh } };
    }

    /// Unsynced handles cannot promise durable mutation success.
    pub fn nativeStore(self: *Store) ?contract.NativeStore {
        if (self.docs.read_only or self.docs.file.no_sync) return null;
        return .{ .source = self.source(), .writer = .{ .ptr = self, .vtable = &.{ .put = put, .remove_override = removeOverride } } };
    }

    fn authorize(self: *Store, scope: []const u8) !void {
        if (!std.mem.eql(u8, self.scope, scope)) return error.Unauthorized;
    }

    // All catalog access and the uncertainty fence are protected by docs.mutex.
    fn head(self: *Store) !u64 {
        if (self.docs.file.checkpoint_publication_uncertain) self.docs.secret_store_uncertain = true;
        if (self.docs.secret_store_uncertain) return error.OutcomeUnknown;
        const bytes = try self.docs.file.getCatalogRecordAlloc(self.allocator, &(self.prefix ++ "head".*)) orelse return 0;
        defer self.allocator.free(bytes);
        if (bytes.len != 8) return error.CorruptInput;
        const revision = std.mem.readInt(u64, bytes[0..8], .little);
        if (revision == 0) return error.CorruptInput;
        return revision;
    }

    fn entryKey(self: *Store, key: []const u8) [namespace.len + 64 + 1 + 8 + 64]u8 {
        return self.prefix ++ "entries/".* ++ digest(key);
    }

    fn resolve(ptr: *anyopaque, alloc: Allocator, scope: []const u8, key: []const u8, options: contract.ReadOptions) !contract.Lookup {
        const self: *Store = @ptrCast(@alignCast(ptr));
        try self.authorize(scope);
        const snapshot = blk: {
            const io = self.docs.file.runtime();
            self.docs.generation_lock.lockSharedUncancelable(io);
            defer self.docs.generation_lock.unlockShared(io);
            sync.lockYielding(&self.docs.mutex);
            defer self.docs.mutex.unlock();
            const revision = try self.head();
            if (revision < options.min_revision) return error.Unavailable;
            break :blk .{ .revision = revision, .bytes = try self.docs.file.getCatalogRecordAlloc(alloc, &self.entryKey(key)) };
        };
        const bytes = snapshot.bytes orelse return .{ .revision = snapshot.revision };
        defer alloc.free(bytes);
        const entry = try decodeEntry(bytes, snapshot.revision);
        if (!std.mem.eql(u8, key, entry.key)) return error.CorruptInput;
        const plaintext = try record.open(alloc, self.provider, .{ .scope = scope, .key = key, .revision = entry.revision }, entry.envelope);
        return .{ .revision = snapshot.revision, .value = .{ .revision = entry.revision, .secret = plaintext } };
    }

    fn listMetadata(ptr: *anyopaque, alloc: Allocator, scope: []const u8, options: contract.ReadOptions) !contract.Listing {
        const self: *Store = @ptrCast(@alignCast(ptr));
        try self.authorize(scope);
        const io = self.docs.file.runtime();
        self.docs.generation_lock.lockSharedUncancelable(io);
        defer self.docs.generation_lock.unlockShared(io);
        sync.lockYielding(&self.docs.mutex);
        defer self.docs.mutex.unlock();
        const revision = try self.head();
        if (revision < options.min_revision) return error.Unavailable;
        var rows = try self.docs.file.metadataCatalogCursor(self.docs.file.activeCheckpoint(), &(self.prefix ++ "entries/".*));
        defer rows.deinit();
        var entries: std.ArrayList(contract.Metadata) = .empty;
        errdefer {
            for (entries.items) |entry| alloc.free(entry.key);
            entries.deinit(alloc);
        }
        while (try rows.nextRecordAlloc(alloc)) |row| {
            defer alloc.free(row.key);
            defer alloc.free(row.value);
            const entry = try decodeEntry(row.value, revision);
            if (!std.mem.eql(u8, row.key, &self.entryKey(entry.key))) return error.CorruptInput;
            const view = try record.decode(entry.envelope);
            if (!(contract.Identity{ .scope = scope, .key = entry.key, .revision = entry.revision }).eql(view.identity)) return error.CorruptInput;
            const key = try alloc.dupe(u8, entry.key);
            errdefer alloc.free(key);
            try entries.append(alloc, .{ .key = key, .revision = entry.revision });
        }
        std.mem.sort(contract.Metadata, entries.items, {}, struct {
            fn less(_: void, a: contract.Metadata, b: contract.Metadata) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.less);
        return .{ .revision = revision, .entries = try entries.toOwnedSlice(alloc) };
    }

    fn refresh(ptr: *anyopaque, scope: []const u8) !contract.Health {
        const self: *Store = @ptrCast(@alignCast(ptr));
        try self.authorize(scope);
        const io = self.docs.file.runtime();
        self.docs.generation_lock.lockSharedUncancelable(io);
        defer self.docs.generation_lock.unlockShared(io);
        sync.lockYielding(&self.docs.mutex);
        defer self.docs.mutex.unlock();
        return .{ .revision = try self.head() };
    }

    fn put(ptr: *anyopaque, scope: []const u8, key: []const u8, value: []const u8, expected: contract.ExpectedRevision) !contract.Mutation {
        const self: *Store = @ptrCast(@alignCast(ptr));
        return self.mutate(scope, key, value, expected);
    }

    fn removeOverride(ptr: *anyopaque, scope: []const u8, key: []const u8, expected: contract.ExpectedRevision) !contract.Mutation {
        const self: *Store = @ptrCast(@alignCast(ptr));
        return self.mutate(scope, key, null, expected);
    }

    fn mutate(self: *Store, scope: []const u8, key: []const u8, value: ?[]const u8, expected: contract.ExpectedRevision) !contract.Mutation {
        try self.authorize(scope);
        if (self.docs.read_only or self.docs.file.no_sync) return error.UnsupportedOperation;
        try self.docs.reserveWriterSlotYielding();
        defer self.docs.releaseWriterSlot();
        const entry_key = self.entryKey(key);
        const previous = blk: {
            sync.lockYielding(&self.docs.mutex);
            defer self.docs.mutex.unlock();
            const revision = try self.head();
            const bytes = try self.docs.file.getCatalogRecordAlloc(self.allocator, &entry_key);
            defer if (bytes) |b| self.allocator.free(b);
            const entry = if (bytes) |b| try decodeEntry(b, revision) else null;
            if (entry) |e| if (!std.mem.eql(u8, e.key, key)) return error.CorruptInput;
            try expected.check(if (entry) |e| e.revision else null);
            if (value == null and entry == null) return .{ .revision = revision, .changed = false };
            break :blk revision;
        };
        const revision = std.math.add(u64, previous, 1) catch return error.Unavailable;
        // Retain the writer reservation, but do not block readers during key-provider I/O.
        const encoded = if (value) |plaintext| try record.seal(self.allocator, self.docs.file.runtime(), self.provider, .{ .scope = scope, .key = key, .revision = revision }, plaintext) else null;
        defer if (encoded) |bytes| self.allocator.free(bytes);
        const indexed = if (encoded) |bytes| try encodeEntry(self.allocator, key, revision, bytes) else null;
        defer if (indexed) |bytes| self.allocator.free(bytes);
        var head_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &head_bytes, revision, .little);
        sync.lockYielding(&self.docs.mutex);
        defer self.docs.mutex.unlock();
        // Index writers can publish while wrapping; use their latest checkpoint.
        if (try self.head() != previous) return error.Conflict;
        self.docs.file.putCatalogBatch(&.{
            .{ .key = &entry_key, .value = indexed orelse "", .is_delete = value == null },
            .{ .key = &(self.prefix ++ "head".*), .value = &head_bytes },
        }) catch {
            self.docs.secret_store_uncertain = true;
            return error.OutcomeUnknown;
        };
        return .{ .revision = revision };
    }
};

fn digest(bytes: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return std.fmt.bytesToHex(hash, .lower);
}

const Entry = struct { revision: u64, key: []const u8, envelope: []const u8 };

fn decodeEntry(bytes: []const u8, head: u64) !Entry {
    if (bytes.len < 10 or bytes.len > 10 + contract.max_identity_bytes + record.max_record_bytes) return error.CorruptInput;
    const revision = std.mem.readInt(u64, bytes[0..8], .little);
    const key_len = std.mem.readInt(u16, bytes[8..10], .little);
    if (revision == 0 or revision > head or key_len > bytes.len - 10) return error.CorruptInput;
    const key = bytes[10..][0..key_len];
    contract.validateName(key) catch return error.CorruptInput;
    return .{ .revision = revision, .key = key, .envelope = bytes[10 + key_len ..] };
}

fn encodeEntry(alloc: Allocator, key: []const u8, revision: u64, envelope: []const u8) ![]u8 {
    const bytes = try alloc.alloc(u8, 10 + key.len + envelope.len);
    std.mem.writeInt(u64, bytes[0..8], revision, .little);
    std.mem.writeInt(u16, bytes[8..10], @intCast(key.len), .little);
    @memcpy(bytes[10..][0..key.len], key);
    @memcpy(bytes[10 + key.len ..], envelope);
    return bytes;
}

const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;
const DataKey = record.DataKey;
const Identity = record.Identity;
const KeyProvider = record.KeyProvider;
const WrappedKey = record.WrappedKey;
const TestProvider = struct {
    key: DataKey = [_]u8{7} ** 32,
    unavailable: bool = false,
    unwrap_calls: usize = 0,
    index_docs: ?*docstore.Store = null,
    fn provider(self: *@This()) KeyProvider {
        return .{ .ptr = self, .vtable = &.{ .wrap = wrap, .unwrap = unwrap } };
    }
    fn wrap(ptr: *anyopaque, alloc: std.mem.Allocator, identity: Identity, key: *const DataKey) !WrappedKey {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.unavailable) return error.Unavailable;
        if (self.index_docs) |docs| {
            sync.lockYielding(&docs.mutex);
            defer docs.mutex.unlock();
            try docs.file.putIndexCatalogRecord("index/progress", "wrapped");
        }
        const id = try alloc.dupe(u8, "test-key-1");
        errdefer alloc.free(id);
        const bytes = try alloc.alloc(u8, 24 + 32 + 16);
        errdefer alloc.free(bytes);
        try std.Options.debug_io.randomSecure(bytes[0..24]);
        var tag: [16]u8 = undefined;
        Aead.encrypt(bytes[24..56], &tag, key, identity.scope, bytes[0..24].*, self.key);
        @memcpy(bytes[56..72], &tag);
        return .{ .key_id = id, .bytes = bytes };
    }
    fn unwrap(ptr: *anyopaque, identity: Identity, key_id: []const u8, bytes: []const u8, key: *DataKey) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.unwrap_calls += 1;
        if (self.unavailable) return error.Unavailable;
        if (!std.mem.eql(u8, key_id, "test-key-1") or bytes.len != 72) return error.CorruptInput;
        Aead.decrypt(key, bytes[24..56], bytes[56..72].*, identity.scope, bytes[0..24].*, self.key) catch return error.CorruptInput;
    }
};

fn testPath(alloc: Allocator, tmp: std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secrets.aflite", .{tmp.sub_path});
}

fn expectValue(store: *Store, key: []const u8, expected: []const u8, revision: u64) !void {
    var value = try store.source().resolve(std.testing.allocator, store.scope, key, .{});
    defer value.deinit(std.testing.allocator);
    try std.testing.expectEqual(revision, value.value.?.revision);
    try std.testing.expectEqualSlices(u8, expected, value.value.?.secret.bytes);
}

test "lite secrets embedding host retains live resolver through rotation snapshot and reopen" {
    const alloc = std.testing.allocator;
    const Handle = @import("backend.zig").Handle;
    const resolver = @import("../../common/secrets.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}.backup.aflite", .{path});
    defer alloc.free(snapshot_path);
    var keys = TestProvider{};
    var reference = (try resolver.SecretValue.initConfig(alloc, "${secret:provider.api_key}")).?;
    defer reference.deinit(alloc);
    {
        var handle = try Handle.create(alloc, path, true);
        defer handle.deinit();
        var native_store = try handle.secretStore(alloc, "host-scope", keys.provider());
        defer native_store.deinit();
        var facade = try resolver.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{
            // Embedding hosts supply the native capability directly.
            .native = .{ .backend = .distributed, .scope = "host-scope", .keyring_path = "host-provider" },
            .environment = false,
        });
        defer facade.deinit();
        const native_handle = native_store.nativeStore().?;
        facade.attachNative(native_handle.source, native_handle.writer);
        for ([_][]const u8{ "initial-host-credential", "rotated-host-credential" }) |expected| {
            var metadata = try facade.put(alloc, "provider.api_key", expected);
            defer metadata.deinit(alloc);
            const actual = (try reference.resolveOwned(alloc, &facade)).?;
            defer alloc.free(actual);
            try std.testing.expectEqualStrings(expected, actual);
        }
        keys.unavailable = true;
        try std.testing.expectError(error.Unavailable, reference.resolveOwned(alloc, &facade));
        keys.unavailable = false;
        _ = try handle.copyStableSnapshot(snapshot_path, false);
    }
    for ([_][]const u8{ path, snapshot_path }) |reopen_path| {
        var reopened = try Handle.open(alloc, reopen_path, .{ .read_only = true });
        defer reopened.deinit();
        var native_store = try reopened.secretStore(alloc, "host-scope", keys.provider());
        defer native_store.deinit();
        try std.testing.expect(native_store.nativeStore() == null);
        var facade = try resolver.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{
            .native = .{ .backend = .distributed, .scope = "host-scope", .keyring_path = "host-provider" },
            .environment = false,
        });
        defer facade.deinit();
        facade.attachNative(native_store.source(), null);
        const actual = (try reference.resolveOwned(alloc, &facade)).?;
        defer alloc.free(actual);
        try std.testing.expectEqualStrings("rotated-host-credential", actual);
        try std.testing.expectError(error.WriteUnavailable, facade.put(alloc, "provider.api_key", "denied"));
        const raw = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, reopen_path, alloc, .limited(16 * 1024 * 1024));
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "rotated-host-credential") == null);
    }
}

test "lite secrets persist encrypted scoped values alongside documents through vacuum and reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    const snapshot_path = try std.fmt.allocPrint(alloc, "{s}.snapshot.aflite", .{path});
    defer alloc.free(snapshot_path);
    var provider = TestProvider{};
    const large = try alloc.alloc(u8, 16384);
    defer alloc.free(large);
    @memset(large, 0xad);
    {
        var handle = try @import("backend.zig").Handle.create(alloc, path, true);
        defer handle.deinit();
        var store = try handle.secretStore(alloc, "tenant/a", provider.provider());
        defer store.deinit();
        var other = try handle.secretStore(alloc, "tenant/b", provider.provider());
        defer other.deinit();
        const writer = store.nativeStore().?.writer;
        _ = try writer.put(store.scope, "token", "unique-secret-plaintext-that-must-not-persist", .absent);
        _ = try writer.put(store.scope, "empty", "", .absent);
        _ = try writer.put(store.scope, "large", large, .absent);
        _ = try other.nativeStore().?.writer.put(other.scope, "token", "other-tenant", .absent);
        try std.testing.expectError(error.Unauthorized, store.source().resolve(alloc, other.scope, "token", .{}));
        try std.testing.expectError(error.Unauthorized, writer.put(other.scope, "token", "bad", .any));
        var runtime = try handle.native_docstore.?.runtimeStore(alloc);
        defer runtime.deinit();
        var batch = try runtime.beginBatch();
        try batch.put("token", "ordinary document");
        try batch.commit();
        _ = try handle.native_docstore.?.vacuum();
        _ = try handle.copyStableSnapshot(snapshot_path, false);
        try expectValue(&other, "token", "other-tenant", 1);
        const raw = try std.Io.Dir.cwd().readFileAlloc(std.Options.debug_io, path, alloc, .limited(16 * 1024 * 1024));
        defer alloc.free(raw);
        try std.testing.expect(std.mem.indexOf(u8, raw, "unique-secret-plaintext-that-must-not-persist") == null);
        try std.testing.expect(std.mem.indexOf(u8, raw, large) == null);
    }
    {
        var snapshot = try docstore.Store.open(alloc, snapshot_path, true);
        defer snapshot.close();
        var secrets = try Store.init(alloc, &snapshot, "tenant/a", provider.provider());
        defer secrets.deinit();
        try expectValue(&secrets, "large", large, 3);
    }
    var docs = try docstore.Store.open(alloc, path, true);
    defer docs.close();
    var fresh_provider = TestProvider{};
    var store = try Store.init(alloc, &docs, "tenant/a", fresh_provider.provider());
    defer store.deinit();
    try std.testing.expect(store.nativeStore() == null);
    try expectValue(&store, "token", "unique-secret-plaintext-that-must-not-persist", 1);
    try expectValue(&store, "empty", "", 2);
    try expectValue(&store, "large", large, 3);
    var listing = try store.source().listMetadata(alloc, store.scope, .{ .min_revision = 3 });
    defer listing.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), listing.entries.len);
    try std.testing.expectEqualStrings("empty", listing.entries[0].key);
    var runtime = try docs.runtimeStore(alloc);
    defer runtime.deinit();
    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("ordinary document", try read.get("token"));
    try std.testing.expectError(error.NotFound, read.get("large"));
}

test "lite secrets CAS deletion recreation freshness and provider failure" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var provider = TestProvider{};
    {
        var docs = try docstore.Store.create(alloc, path, true);
        defer docs.close();
        var store = try Store.init(alloc, &docs, "scope", provider.provider());
        defer store.deinit();
        const writer = store.nativeStore().?.writer;
        try std.testing.expect(!(try writer.removeOverride("scope", "key", .any)).changed);
        try std.testing.expectEqual(@as(u64, 1), (try writer.put("scope", "key", "one", .absent)).revision);
        try std.testing.expectError(error.Conflict, writer.put("scope", "key", "bad", .absent));
        try std.testing.expectError(error.Conflict, writer.removeOverride("scope", "key", .{ .exact = 2 }));
        provider.unavailable = true;
        try std.testing.expectError(error.Unavailable, writer.put("scope", "key", "bad", .{ .exact = 1 }));
        try std.testing.expectError(error.Unavailable, store.source().resolve(alloc, "scope", "key", .{}));
        // Metadata does not require unwrapping a key or returning any value.
        var listing = try store.source().listMetadata(alloc, "scope", .{});
        defer listing.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), listing.revision);
        provider.unavailable = false;
        try std.testing.expectEqual(@as(u64, 2), (try writer.removeOverride("scope", "key", .{ .exact = 1 })).revision);
        try std.testing.expectError(error.Unavailable, store.source().resolve(alloc, "scope", "missing", .{ .min_revision = 3 }));
        try std.testing.expectError(error.Unavailable, store.source().listMetadata(alloc, "scope", .{ .min_revision = 3 }));
    }
    var docs = try docstore.Store.open(alloc, path, false);
    defer docs.close();
    var store = try Store.init(alloc, &docs, "scope", provider.provider());
    defer store.deinit();
    var missing = try store.source().resolve(alloc, "scope", "key", .{ .min_revision = 2 });
    defer missing.deinit(alloc);
    try std.testing.expect(missing.value == null);
    try std.testing.expectEqual(@as(u64, 2), missing.revision);
    try std.testing.expectEqual(@as(u64, 3), (try store.nativeStore().?.writer.put("scope", "key", "new\x00\xff", .absent)).revision);
    try std.testing.expectError(error.Conflict, store.nativeStore().?.writer.put("scope", "key", "stale", .{ .exact = 1 }));
    try expectValue(&store, "key", "new\x00\xff", 3);
    provider.key[0] ^= 1;
    try std.testing.expectError(error.CorruptInput, store.source().resolve(alloc, "scope", "key", .{}));
}

test "lite secrets unsynced and read-only handles reject writes even with forged writer" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var provider = TestProvider{};
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .exclusive = true, .no_sync = true });
        defer docs.close();
        var store = try Store.init(alloc, &docs, "scope", provider.provider());
        defer store.deinit();
        try std.testing.expect(store.nativeStore() == null);
        try std.testing.expectError(error.UnsupportedOperation, Store.put(&store, "scope", "key", "value", .any));
    }
    var docs = try docstore.Store.open(alloc, path, true);
    defer docs.close();
    var store = try Store.init(alloc, &docs, "scope", provider.provider());
    defer store.deinit();
    try std.testing.expect(store.nativeStore() == null);
    try std.testing.expectError(error.UnsupportedOperation, Store.removeOverride(&store, "scope", "key", .any));
}

test "lite secrets competing adapters serialize conditional writers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var docs = try docstore.Store.create(alloc, path, true);
    defer docs.close();
    var provider = TestProvider{};
    var first = try Store.init(alloc, &docs, "scope", provider.provider());
    defer first.deinit();
    var second = try Store.init(alloc, &docs, "scope", provider.provider());
    defer second.deinit();
    _ = try first.nativeStore().?.writer.put("scope", "key", "initial", .absent);
    const Worker = struct {
        store: *Store,
        result: anyerror!contract.Mutation = error.Unavailable,
        fn run(self: *@This()) void {
            self.result = self.store.nativeStore().?.writer.put("scope", "key", "winner", .{ .exact = 1 });
        }
    };
    var a = Worker{ .store = &first };
    var b = Worker{ .store = &second };
    const thread_a = try std.Thread.spawn(.{}, Worker.run, .{&a});
    const thread_b = std.Thread.spawn(.{}, Worker.run, .{&b}) catch |err| {
        thread_a.join();
        return err;
    };
    thread_a.join();
    thread_b.join();
    var successes: usize = 0;
    for ([_]anyerror!contract.Mutation{ a.result, b.result }) |result| {
        if (result) |mutation| {
            successes += 1;
            try std.testing.expectEqual(@as(u64, 2), mutation.revision);
        } else |err| try std.testing.expectEqual(error.Conflict, err);
    }
    try std.testing.expectEqual(@as(usize, 1), successes);
    try expectValue(&first, "key", "winner", 2);
}

test "lite secrets reject tampering mismatched indexes and revision exhaustion" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var docs = try docstore.Store.create(alloc, path, true);
    defer docs.close();
    var provider = TestProvider{};
    var store = try Store.init(alloc, &docs, "scope", provider.provider());
    defer store.deinit();
    _ = try store.nativeStore().?.writer.put("scope", "key", "value", .absent);
    const key = store.entryKey("key");
    const bytes = (try docs.file.getCatalogRecordAlloc(alloc, &key)).?;
    defer alloc.free(bytes);
    bytes[bytes.len - 1] ^= 1;
    try docs.file.putCatalogRecord(&key, bytes);
    try std.testing.expectError(error.CorruptInput, store.source().resolve(alloc, "scope", "key", .{}));
    std.mem.writeInt(u64, bytes[0..8], 2, .little);
    try docs.file.putCatalogRecord(&key, bytes);
    try std.testing.expectError(error.CorruptInput, store.source().listMetadata(alloc, "scope", .{}));
    var max: [8]u8 = undefined;
    std.mem.writeInt(u64, &max, std.math.maxInt(u64), .little);
    try docs.file.putCatalogRecord(&(store.prefix ++ "head".*), &max);
    try std.testing.expectError(error.Unavailable, store.nativeStore().?.writer.put("scope", "new", "bad", .absent));
}

const SyncFault = struct {
    var remaining: usize = 0;
    fn fileSync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
        if (remaining != 0) {
            remaining -= 1;
            if (remaining == 0) return error.InputOutput;
        }
        return std.Options.debug_io.vtable.fileSync(userdata, file);
    }
};

test "lite secrets uncertain publication fences all adapters and recovers atomically" {
    const alloc = std.testing.allocator;
    var vtable = std.Options.debug_io.vtable.*;
    vtable.fileSync = SyncFault.fileSync;
    const io = std.Io{ .userdata = std.Options.debug_io.userdata, .vtable = &vtable };
    for (1..7) |scenario| {
        const fail_sync = (scenario - 1) % 3 + 1;
        const removing = scenario > 3;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try testPath(alloc, tmp);
        defer alloc.free(path);
        var provider = TestProvider{};
        {
            var docs = try docstore.Store.createWithOptions(alloc, path, .{ .exclusive = true, .io = io });
            defer docs.close();
            var store = try Store.init(alloc, &docs, "scope", provider.provider());
            defer store.deinit();
            var other = try Store.init(alloc, &docs, "scope", provider.provider());
            defer other.deinit();
            _ = try store.nativeStore().?.writer.put("scope", "key", "before", .absent);
            SyncFault.remaining = fail_sync;
            defer SyncFault.remaining = 0;
            const writer = store.nativeStore().?.writer;
            try std.testing.expectError(error.OutcomeUnknown, if (removing) writer.removeOverride("scope", "key", .{ .exact = 1 }) else writer.put("scope", "key", "after", .{ .exact = 1 }));
            try std.testing.expectEqual(@as(usize, 0), SyncFault.remaining);
            try std.testing.expectError(error.OutcomeUnknown, other.source().resolve(alloc, "scope", "key", .{}));
            try std.testing.expectError(error.OutcomeUnknown, other.source().listMetadata(alloc, "scope", .{}));
            try std.testing.expectError(error.OutcomeUnknown, other.source().refresh("scope"));
            try std.testing.expectError(error.OutcomeUnknown, other.nativeStore().?.writer.removeOverride("scope", "key", .any));
        }
        var docs = try docstore.Store.open(alloc, path, false);
        defer docs.close();
        var store = try Store.init(alloc, &docs, "scope", provider.provider());
        defer store.deinit();
        var result = try store.source().resolve(alloc, "scope", "key", .{});
        defer result.deinit(alloc);
        try std.testing.expect(result.revision == 1 or result.revision == 2);
        if (removing and result.revision == 2) {
            try std.testing.expect(result.value == null);
        } else {
            try std.testing.expectEqual(result.revision, result.value.?.revision);
            try std.testing.expectEqualStrings(if (result.revision == 1) "before" else "after", result.value.?.secret.bytes);
        }
    }
}

test "lite secrets preserve index commits made during wrapping and reject stale read-only snapshots" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var docs = try docstore.Store.create(alloc, path, true);
    defer docs.close();
    var provider = TestProvider{ .index_docs = &docs };
    var store = try Store.init(alloc, &docs, "scope", provider.provider());
    defer store.deinit();
    _ = try store.nativeStore().?.writer.put("scope", "key", "one", .absent);
    var readonly = try docstore.Store.open(alloc, path, true);
    defer readonly.close();
    var reader = try Store.init(alloc, &readonly, "scope", provider.provider());
    defer reader.deinit();
    _ = try store.nativeStore().?.writer.put("scope", "key", "two", .{ .exact = 1 });
    try expectValue(&store, "key", "two", 2);
    try std.testing.expectEqual(@as(u64, 1), (try reader.source().refresh("scope")).revision);
    try std.testing.expectError(error.Unavailable, reader.source().resolve(alloc, "scope", "key", .{ .min_revision = 2 }));
    const index = (try docs.file.getIndexCatalogRecordAlloc(alloc, "index/progress")).?;
    defer alloc.free(index);
    try std.testing.expectEqualStrings("wrapped", index);
}

test "lite secrets portable import rejects live secrets and retained scope revisions" {
    const alloc = std.testing.allocator;
    const LiteDb = @import("connection.zig").Connection;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    const target_path = try std.fmt.allocPrint(alloc, "{s}.target.aflite", .{path});
    defer alloc.free(target_path);
    var source = try LiteDb.create(alloc, path, true);
    defer source.close();
    try source.db.batch(.{ .writes = &.{.{ .key = "document", .value = "{}" }} });
    var portable: std.ArrayList(u8) = .empty;
    defer portable.deinit(alloc);
    try @import("../portable_backup.zig").exportPortable(alloc, source.db.core.store, &portable);
    var target = try LiteDb.create(alloc, target_path, true);
    defer target.close();
    var provider = TestProvider{};
    var store = try target.backend.secretStore(alloc, "scope", provider.provider());
    defer store.deinit();
    _ = try store.nativeStore().?.writer.put("scope", "key", "credential", .absent);
    try std.testing.expect(try target.db.isPortableImportTargetEmpty(alloc));
    try std.testing.expectError(error.LiteImportTargetNotEmpty, @import("restore_staging.zig").importPortableIntoLiteDb(alloc, &target.db, &target.backend, portable.items));
    var value = try store.source().resolve(alloc, "scope", "key", .{});
    defer value.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), value.revision);
    try std.testing.expect(value.value != null);
    _ = try store.nativeStore().?.writer.removeOverride("scope", "key", .{ .exact = 1 });
    // A head with no live entries is still durable state and must not reset.
    try std.testing.expectError(error.LiteImportTargetNotEmpty, @import("restore_staging.zig").importPortableIntoLiteDb(alloc, &target.db, &target.backend, portable.items));
    try std.testing.expectEqual(@as(u64, 2), (try store.source().refresh("scope")).revision);
    try std.testing.expectEqual(@as(u64, 3), (try store.nativeStore().?.writer.put("scope", "key", "recreated", .absent)).revision);
    try expectValue(&store, "key", "recreated", 3);
}

test "lite secret metadata listing seeks its scope without loading unrelated catalogs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp);
    defer alloc.free(path);
    var docs = try docstore.Store.create(alloc, path, true);
    defer docs.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const mutations = try arena.allocator().alloc(native.CatalogMutation, 2048);
    for (mutations, 0..) |*mutation, i| mutation.* = .{
        .key = try std.fmt.allocPrint(arena.allocator(), "unrelated-{d:0>6}", .{i}),
        .value = "unrelated-metadata-value",
    };
    try docs.file.putCatalogBatch(mutations);
    var provider = TestProvider{};
    var store = try Store.init(alloc, &docs, "scope", provider.provider());
    defer store.deinit();
    _ = try store.nativeStore().?.writer.put("scope", "token", "secret", .absent);
    const before = docs.file.test_page_reads.load(.monotonic);
    try std.testing.expect(try docs.file.hasSecretState());
    var listing = try store.source().listMetadata(alloc, "scope", .{});
    defer listing.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), listing.entries.len);
    try std.testing.expectEqualStrings("token", listing.entries[0].key);
    try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - before <= 24);
}
