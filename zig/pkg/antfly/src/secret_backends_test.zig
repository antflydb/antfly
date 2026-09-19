// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
const collection = @import("common/secret_collection.zig");
const contract = @import("common/secret_contract.zig");
const record = @import("common/secret_record.zig");
const serverless = @import("serverless/secret_store.zig");
const distributed = @import("metadata/secret_store.zig");
const raft_store = @import("metadata/storage/raft_apply_store.zig");
const raft_sm = @import("raft/state_machine/mod.zig");
const objects = @import("storage/object_storage.zig");
const alloc = std.testing.allocator;
const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;
const DataKey = record.DataKey;
const Identity = record.Identity;
const KeyProvider = record.KeyProvider;
const WrappedKey = record.WrappedKey;
const TestProvider = struct {
    key: DataKey = [_]u8{7} ** 32,
    unavailable: bool = false,
    unwrap_calls: usize = 0,
    fn provider(self: *@This()) KeyProvider {
        return .{ .ptr = self, .vtable = &.{ .wrap = wrap, .unwrap = unwrap } };
    }
    fn wrap(ptr: *anyopaque, allocator: std.mem.Allocator, identity: Identity, key: *const DataKey) !WrappedKey {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.unavailable) return error.Unavailable;
        const id = try allocator.dupe(u8, "test-key-1");
        errdefer allocator.free(id);
        const bytes = try allocator.alloc(u8, 24 + 32 + 16);
        errdefer allocator.free(bytes);
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

fn checkContract(store: anytype, keys: *TestProvider) !void {
    const writer = store.nativeStore().writer;
    try std.testing.expect(!(try writer.removeOverride("scope", "token", .any)).changed);
    try std.testing.expectEqual(@as(u64, 1), (try writer.put("scope", "token", "first", .absent)).revision);
    try std.testing.expectError(error.Conflict, writer.put("scope", "token", "bad", .absent));
    try std.testing.expectError(error.Unauthorized, writer.put("other", "token", "bad", .any));
    try std.testing.expectError(error.Unauthorized, store.source().resolve(alloc, "other", "token", .{}));
    _ = try writer.put("scope", "empty", "", .absent);
    var value = try store.source().resolve(alloc, "scope", "token", .{ .min_revision = 2 });
    defer value.deinit(alloc);
    try std.testing.expectEqualStrings("first", value.value.?.secret.bytes);
    try std.testing.expectEqual(@as(u64, 1), value.value.?.revision);
    try std.testing.expectError(error.Unavailable, store.source().resolve(alloc, "scope", "missing", .{ .min_revision = 3 }));
    keys.unavailable = true;
    try std.testing.expectError(error.Unavailable, writer.put("scope", "token", "bad", .any));
    try std.testing.expectError(error.Unavailable, store.source().resolve(alloc, "scope", "token", .{}));
    var listing = try store.source().listMetadata(alloc, "scope", .{});
    defer listing.deinit(alloc);
    try std.testing.expectEqualStrings("empty", listing.entries[0].key);
    try std.testing.expectEqual(@as(u64, 2), listing.revision);
    keys.unavailable = false;
    _ = try writer.removeOverride("scope", "token", .{ .exact = 1 });
    _ = try writer.removeOverride("scope", "empty", .{ .exact = 2 });
    try std.testing.expectEqual(@as(u64, 4), (try store.source().refresh("scope")).revision);
    try std.testing.expectEqual(@as(u64, 5), (try writer.put("scope", "token", "recreated", .absent)).revision);
    try std.testing.expectError(error.Conflict, writer.removeOverride("scope", "token", .{ .exact = 1 }));
}

test "secret backend serverless conditional persistence scope isolation and restart" {
    var objects_memory = objects.MemoryObjectStorage.init(alloc);
    defer objects_memory.deinit();
    var client = objects_memory.client();
    try client.makeBucket("secrets");
    var keys = TestProvider{};
    const backend = serverless.Backend{ .client = client, .bucket = "secrets", .prefix = "deployment", .consistency = .linearizable_cas };
    {
        var store = try serverless.Store.init(alloc, std.testing.io, "scope", keys.provider(), backend);
        defer store.deinit();
        try checkContract(&store, &keys);
    }
    var reopened = try serverless.Store.init(alloc, std.testing.io, "scope", keys.provider(), backend);
    defer reopened.deinit();
    var value = try reopened.source().resolve(alloc, "scope", "token", .{});
    defer value.deinit(alloc);
    try std.testing.expectEqualStrings("recreated", value.value.?.secret.bytes);
    const key = try backend.objectKey(alloc, "scope");
    defer alloc.free(key);
    var raw = try client.getObject("secrets", key, .{});
    defer raw.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, raw.body, "recreated") == null);
    var other = try serverless.Store.init(alloc, std.testing.io, "other", keys.provider(), backend);
    defer other.deinit();
    var absent = try other.source().resolve(alloc, "other", "token", .{});
    defer absent.deinit(alloc);
    try std.testing.expect(absent.value == null);
    var unsupported = backend;
    unsupported.consistency = .unsupported;
    try std.testing.expectError(error.UnsupportedOperation, unsupported.read(alloc, "scope"));
    var missing_bucket = backend;
    missing_bucket.bucket = "missing";
    try std.testing.expectError(error.Unavailable, missing_bucket.read(alloc, "scope"));
    var cas = backend;
    var old = try cas.read(alloc, "scope");
    defer old.deinit(alloc);
    _ = try reopened.nativeStore().writer.put("scope", "token", "new", .any);
    try std.testing.expectError(error.Conflict, cas.publish(alloc, "scope", old, raw.body));
    raw.body[raw.body.len - 1] ^= 1;
    var write = try client.putObject("secrets", key, raw.body, .{});
    defer write.deinit(alloc);
    try std.testing.expectError(error.CorruptInput, reopened.source().resolve(alloc, "scope", "token", .{}));
}

const RaftBackend = struct {
    store: *raft_store.RaftApplyStore,
    index: *u64,
    pub fn read(self: *@This(), a: std.mem.Allocator, scope: []const u8) !collection.Snapshot {
        return .{ .bytes = try self.store.getSecretCollection(a, 1, scope) };
    }
    pub fn publish(self: *@This(), a: std.mem.Allocator, scope: []const u8, previous: collection.Snapshot, bytes: []const u8) !void {
        var before = try collection.decode(a, scope, previous.bytes);
        defer before.deinit(a);
        const command = try distributed.encodePublication(a, before.revision, bytes);
        defer a.free(command);
        try apply(self.store, self.index, command);
        const after = (try self.store.getSecretCollection(a, 1, scope)).?;
        defer a.free(after);
        if (!std.mem.eql(u8, after, bytes)) return error.Conflict;
    }
};

fn apply(store: *raft_store.RaftApplyStore, index: *u64, command: []const u8) !void {
    const encoded = try raft_store.encodeTransitionCommand(alloc, .{ .publish_secret_collection = command });
    defer alloc.free(encoded);
    index.* += 1;
    const entries = try raft_sm.encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = index.*, .entry_type = .normal, .data = encoded }});
    defer alloc.free(entries);
    try store.snapshotBuilder().applyBatch(.{ .group_id = 1, .commit_index = index.*, .entries_bytes = entries });
}

test "secret backend raft atomic CAS durable reopen snapshot and stale command replay" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const follower_root = try std.fs.path.join(alloc, &.{ root, "follower" });
    defer alloc.free(follower_root);
    var keys = TestProvider{};
    var index: u64 = 0;
    const Store = collection.Store(RaftBackend);
    {
        var db = try raft_store.RaftApplyStore.init(alloc, .{ .root_dir = root });
        defer db.deinit();
        var store = try Store.init(alloc, std.testing.io, "scope", keys.provider(), .{ .store = &db, .index = &index });
        defer store.deinit();
        try checkContract(&store, &keys);
        const bytes = (try db.getSecretCollection(alloc, 1, "scope")).?;
        defer alloc.free(bytes);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "recreated") == null);
        // A losing proposal is committed as a deterministic no-op, not an apply failure.
        const stale = try distributed.encodePublication(alloc, 4, bytes);
        defer alloc.free(stale);
        try apply(&db, &index, stale);
        var prepared = (try db.snapshotBuilder().prepareSnapshot(1, index)).?;
        defer prepared.deinit();
        var materialized = try prepared.materialize(alloc);
        defer materialized.deinit(alloc);
        var follower = try raft_store.RaftApplyStore.init(alloc, .{ .root_dir = follower_root });
        defer follower.deinit();
        try std.testing.expect(try follower.snapshotBuilder().installSnapshot(alloc, 1, index, materialized.bytes));
        const replicated = (try follower.getSecretCollection(alloc, 1, "scope")).?;
        defer alloc.free(replicated);
        try std.testing.expectEqualSlices(u8, bytes, replicated);
    }
    var reopened = try raft_store.RaftApplyStore.init(alloc, .{ .root_dir = root });
    defer reopened.deinit();
    var store = try Store.init(alloc, std.testing.io, "scope", keys.provider(), .{ .store = &reopened, .index = &index });
    defer store.deinit();
    var found = try store.source().resolve(alloc, "scope", "token", .{});
    defer found.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 5), found.revision);
    try std.testing.expectEqualStrings("recreated", found.value.?.secret.bytes);
}

const secrets = @import("common/secrets.zig");
const delivery = @import("common/secret_delivery.zig");
const http = @import("common/http/http_common.zig");
const DeliveryExecutor = struct {
    store: *secrets.FileStore,
    replay: ?[]const u8 = null,
    capture: ?[]u8 = null,
    unavailable: bool = false,
    fn execute(ptr: *anyopaque, a: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.unavailable) return error.Unavailable;
        if (self.replay) |bytes| return .{ .status = 200, .body = try a.dupe(u8, bytes) };
        const bytes = delivery.serve(a, self.store, req.header("X-Antfly-Secret-Grant").?, req.body) catch |err| return .{ .status = if (err == error.Unauthorized) 403 else 503 };
        if (self.capture) |old| a.free(old);
        self.capture = try a.dupe(u8, bytes);
        return .{ .status = 200, .body = bytes };
    }
};

test "secret backend runtime facade encrypted delivery grants replay freshness and API" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reader.key", .data = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n" });
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, "reader.key", alloc);
    defer alloc.free(path);
    var memory = objects.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("secrets");
    var keys = TestProvider{};
    var backend = try serverless.Store.init(alloc, std.testing.io, "scope", keys.provider(), .{ .client = client, .bucket = "secrets", .prefix = "cluster", .consistency = .linearizable_cas });
    defer backend.deinit();
    var facade = try secrets.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{ .native = .{
        .backend = .distributed,
        .scope = "scope",
        .keyring_path = "bootstrap-only",
        .grants = &.{.{ .name = "data-1", .credential_path = path, .keys = &.{"token"} }},
    }, .environment = false });
    defer facade.deinit();
    const native_handle = backend.nativeStore();
    facade.attachNative(native_handle.source, native_handle.writer);
    var put = try facade.put(alloc, "token", "first");
    defer put.deinit(alloc);
    try std.testing.expect(put.managed);
    try std.testing.expectEqual(@as(?u64, 1), put.revision);
    _ = try native_handle.writer.put("scope", "private", "do-not-deliver", .any);
    var executor = DeliveryExecutor{ .store = &facade };
    defer if (executor.capture) |bytes| alloc.free(bytes);
    var remote = delivery.Remote{ .alloc = alloc, .io = std.testing.io, .scope = "scope", .config = .{ .name = "data-1", .credential_path = path, .urls = &.{"http://metadata"} }, .executor = .{ .ptr = &executor, .vtable = &.{ .execute = DeliveryExecutor.execute } } };
    var result = try remote.source().resolve(alloc, "scope", "token", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("first", result.value.?.secret.bytes);
    try std.testing.expect(std.mem.indexOf(u8, executor.capture.?, "first") == null);
    const old_reply = try alloc.dupe(u8, executor.capture.?);
    defer alloc.free(old_reply);
    executor.replay = old_reply;
    try std.testing.expectError(error.Unauthorized, remote.source().resolve(alloc, "scope", "token", .{}));
    executor.replay = null;
    try std.testing.expectError(error.Unauthorized, remote.source().resolve(alloc, "scope", "private", .{}));
    try std.testing.expectError(error.Unauthorized, remote.source().resolve(alloc, "other", "token", .{}));
    var list = try remote.source().listMetadata(alloc, "scope", .{});
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), list.entries.len);
    try std.testing.expectEqualStrings("token", list.entries[0].key);
    try std.testing.expectError(error.Unavailable, remote.source().resolve(alloc, "scope", "token", .{ .min_revision = 99 }));
    try std.testing.expect(try facade.delete("token"));
    var absent = try remote.source().resolve(alloc, "scope", "token", .{ .min_revision = 3 });
    defer absent.deinit(alloc);
    try std.testing.expect(absent.value == null);
    executor.unavailable = true;
    try std.testing.expectError(error.Unavailable, remote.source().resolve(alloc, "scope", "token", .{}));
    executor.unavailable = false;
    const api_mod = @import("serverless_http_server.zig");
    const Handler = struct {
        pub fn handle(_: *@This(), _: @import("serverless/api/http_types.zig").HttpRequest) !@import("serverless/api/http_types.zig").HttpResponse {
            return error.UnexpectedCall;
        }
    };
    var handler = Handler{};
    var api = api_mod.ServerlessHttpServer.init(alloc, .{ .secret_store = &facade, .secret_admin_token = "admin-token-at-least-32-bytes-long" }, &handler);
    var http_runtime = try api_mod.HttpxRuntime.start(alloc, std.testing.io, &api);
    defer http_runtime.deinit();
    var wire = @import("common/http/std_http_executor.zig").StdHttpExecutor.init(alloc, .{});
    defer wire.deinit();
    const token_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/secrets/token", .{http_runtime.base_uri});
    defer alloc.free(token_uri);
    const list_uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/secrets", .{http_runtime.base_uri});
    defer alloc.free(list_uri);
    var denied = try wire.executor().execute(alloc, .{ .method = .PUT, .uri = token_uri, .body = "{\"value\":\"rotated\"}", .timeout_ms = 10_000 });
    defer denied.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 401), denied.status);
    try std.testing.expectEqualStrings("application/json", denied.content_type.?);
    try std.testing.expectEqualStrings("no-store", denied.header("Cache-Control").?);
    var accepted = try wire.executor().execute(alloc, .{ .method = .PUT, .uri = token_uri, .body = "{\"value\":\"rotated\"}", .authorization = "Bearer admin-token-at-least-32-bytes-long", .timeout_ms = 10_000 });
    defer accepted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), accepted.status);
    try std.testing.expectEqualStrings("application/json", accepted.content_type.?);
    try std.testing.expectEqualStrings("no-store", accepted.header("Cache-Control").?);
    try std.testing.expect(std.mem.indexOf(u8, accepted.body, "rotated") == null);
    var listed = try wire.executor().execute(alloc, .{ .method = .GET, .uri = list_uri, .authorization = "Bearer admin-token-at-least-32-bytes-long", .timeout_ms = 10_000 });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), listed.status);
    try std.testing.expectEqualStrings("application/json", listed.content_type.?);
    try std.testing.expectEqualStrings("no-store", listed.header("Cache-Control").?);
    var parsed = try std.json.parseFromSlice(struct { secrets: []struct { key: []const u8 }, writable: bool }, alloc, listed.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.writable);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.secrets.len);
    try std.testing.expect(std.mem.indexOf(u8, listed.body, "rotated") == null);
    try std.testing.expect(std.mem.indexOf(u8, listed.body, "do-not-deliver") == null);
    var rotated = try remote.source().resolve(alloc, "scope", "token", .{ .min_revision = 4 });
    defer rotated.deinit(alloc);
    try std.testing.expectEqualStrings("rotated", rotated.value.?.secret.bytes);
    var data_facade = try secrets.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{ .native = .{ .backend = .distributed, .scope = "scope", .reader = remote.config }, .environment = true });
    defer data_facade.deinit();
    data_facade.attachNative(remote.source(), null);
    try std.testing.expectError(error.WriteUnavailable, data_facade.put(alloc, "token", "bad"));
    executor.unavailable = true;
    try std.testing.expectError(error.Unavailable, data_facade.getOwned(alloc, "token"));
}

test "secret backend mounted keyring rotation retained decrypt keys and identity authentication" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first = "{\"active\":\"one\",\"keys\":[{\"id\":\"one\",\"key\":\"1111111111111111111111111111111111111111111111111111111111111111\"}]}";
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "keys.json", .data = first });
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, "keys.json", alloc);
    defer alloc.free(path);
    var keyring = @import("common/secret_keyring.zig").Keyring{ .alloc = alloc, .io = std.testing.io, .path = path };
    const identity = record.Identity{ .scope = "scope", .key = "token", .revision = 1 };
    const encoded = try record.seal(alloc, std.testing.io, keyring.provider(), identity, "secret");
    defer alloc.free(encoded);
    const rotated = "{\"active\":\"two\",\"keys\":[{\"id\":\"one\",\"key\":\"1111111111111111111111111111111111111111111111111111111111111111\"},{\"id\":\"two\",\"key\":\"2222222222222222222222222222222222222222222222222222222222222222\"}]}";
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "keys.json", .data = rotated });
    var opened = try record.open(alloc, keyring.provider(), identity, encoded);
    defer opened.deinit(alloc);
    try std.testing.expectEqualStrings("secret", opened.bytes);
    const new_record = try record.seal(alloc, std.testing.io, keyring.provider(), identity, "new");
    defer alloc.free(new_record);
    try std.testing.expectEqualStrings("two", (try record.decode(new_record)).key_id);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "keys.json", .data = first });
    try std.testing.expectError(error.Unavailable, record.open(alloc, keyring.provider(), identity, new_record));
}

test "secret backend live metadata API commits Raft and authenticated data delivery" {
    const runtime = @import("metadata/runtime.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const replicas = try std.fs.path.join(alloc, &.{ root, "replicas" });
    defer alloc.free(replicas);
    const catalog = try std.fs.path.join(alloc, &.{ root, "catalog" });
    defer alloc.free(catalog);
    const snapshots = try std.fs.path.join(alloc, &.{ root, "snapshots" });
    defer alloc.free(snapshots);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "reader.key", .data = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" });
    const credential_path = try tmp.dir.realPathFileAlloc(std.testing.io, "reader.key", alloc);
    defer alloc.free(credential_path);
    var facade = try secrets.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{ .native = .{ .backend = .distributed, .scope = "scope", .keyring_path = "host-provider", .grants = &.{.{ .name = "data-1", .credential_path = credential_path, .keys = &.{"token"} }} }, .environment = false });
    defer facade.deinit();
    var keys = TestProvider{};
    var native: ?distributed.Store = null;
    defer if (native) |*store| store.deinit();
    const service_secret = "internal-service-secret-32-bytes-minimum";
    var server = try runtime.Server.init(alloc, .{
        .replica_root_dir = replicas,
        .replica_catalog_path = catalog,
        .snapshot_root_dir = snapshots,
        .local_node_id = 1,
        .metadata_group_id = 1,
        .bind_port = 0,
        .admin_bind_port = 0,
        .secret_store = &facade,
        .api_server_cfg = .{ .auth_enabled = false, .secret_store = &facade, .internal_service_secret = service_secret, .internal_service_issuer = "secret-test" },
    });
    defer server.deinit();
    native = try distributed.Store.init(alloc, std.testing.io, "scope", keys.provider(), .{ .service = server.server.svc });
    const native_handle = native.?.nativeStore();
    facade.attachNative(native_handle.source, native_handle.writer);
    try server.start();
    try server.bootstrapLocal(1, 1);
    const base = try server.adminBaseUri(alloc);
    defer alloc.free(base);
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/secrets/token", .{base});
    defer alloc.free(uri);
    var executor = @import("common/http/std_http_executor.zig").StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var response = try executor.executor().execute(alloc, .{ .method = .PUT, .uri = uri, .body = "{\"value\":\"from-api\"}", .content_type = "application/json", .timeout_ms = 10_000 });
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    const CheckedDelivery = struct {
        inner: http.RequestExecutor,
        checked: bool = false,
        fn execute(ptr: *anyopaque, a: std.mem.Allocator, req: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var reply = try self.inner.execute(a, req);
            errdefer reply.deinit(a);
            if (reply.status == 200) {
                try std.testing.expectEqualStrings("application/octet-stream", reply.content_type.?);
                try std.testing.expectEqualStrings("no-store", reply.header("Cache-Control").?);
                self.checked = true;
            }
            return reply;
        }
    };
    var checked_delivery = CheckedDelivery{ .inner = executor.executor() };
    var remote = delivery.Remote{ .alloc = alloc, .io = std.testing.io, .scope = "scope", .config = .{ .name = "data-1", .credential_path = credential_path, .urls = &.{base} }, .executor = .{ .ptr = &checked_delivery, .vtable = &.{ .execute = CheckedDelivery.execute } }, .internal_service = .{ .secret = service_secret, .issuer = "secret-test" } };
    var read = try remote.source().resolve(alloc, "scope", "token", .{ .min_revision = 1 });
    defer read.deinit(alloc);
    try std.testing.expect(checked_delivery.checked);
    try std.testing.expectEqualStrings("from-api", read.value.?.secret.bytes);
    try std.testing.expectError(error.Unauthorized, remote.source().resolve(alloc, "scope", "forbidden", .{}));
    remote.internal_service = null;
    try std.testing.expectError(error.Unauthorized, remote.source().resolve(alloc, "scope", "token", .{}));
}

test "secret backend GCS uses observed generation for conditional upload" {
    const Gcs = objects.Gcs;
    const State = struct {
        generation: u64 = 10,
        body: ?[]u8 = null,
        writes: usize = 0,
        fn request(ptr: ?*anyopaque, a: std.mem.Allocator, method: Gcs.HttpMethod, url: []const u8, _: []const Gcs.HeaderPair, body: ?[]const u8, _: ?[]const u8, _: ?usize, _: ?objects.CancellationToken) !Gcs.TransportResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            if (method == .GET) {
                if (std.mem.indexOf(u8, url, "alt=media") == null) return .{ .status = 200, .body = try a.dupe(u8, "{}") };
                if (self.body == null) return .{ .status = 404, .body = try a.dupe(u8, "") };
                return .{ .status = 200, .body = try a.dupe(u8, self.body.?), .generation = try std.fmt.allocPrint(a, "{d}", .{self.generation}) };
            }
            const expected = try std.fmt.allocPrint(a, "ifGenerationMatch={d}", .{if (self.body == null) @as(u64, 0) else self.generation});
            defer a.free(expected);
            if (!std.mem.endsWith(u8, url, expected)) return .{ .status = 412, .body = try a.dupe(u8, "") };
            const next = try a.dupe(u8, body.?);
            if (self.body) |old| a.free(old);
            self.body = next;
            self.generation += 1;
            self.writes += 1;
            return .{ .status = 200, .body = try a.dupe(u8, "{}") };
        }
    };
    var state = State{};
    defer if (state.body) |bytes| alloc.free(bytes);
    var gcs = Gcs.JsonApiClient.initWithRequestFn(alloc, try Gcs.jsonApiClientConfigAlloc(alloc), &state, State.request);
    defer gcs.deinit();
    var keys = TestProvider{};
    const backend = serverless.Backend{ .client = gcs.client(), .gcs_client = &gcs, .bucket = "bucket", .prefix = "native", .consistency = .linearizable_cas };
    var store = try serverless.Store.init(alloc, std.testing.io, "scope", keys.provider(), backend);
    defer store.deinit();
    _ = try store.nativeStore().writer.put("scope", "token", "one", .absent);
    var persistence = backend;
    var old = try persistence.read(alloc, "scope");
    defer old.deinit(alloc);
    _ = try store.nativeStore().writer.put("scope", "token", "two", .{ .exact = 1 });
    try std.testing.expectError(error.Conflict, persistence.publish(alloc, "scope", old, old.bytes.?));
    try std.testing.expectEqual(@as(usize, 2), state.writes);
    var result = try store.source().resolve(alloc, "scope", "token", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("two", result.value.?.secret.bytes);
}

const RacingBackend = struct {
    delegate: serverless.Backend,
    competitor: ?*serverless.Store = null,
    unknown_after_write: bool = false,
    calls: usize = 0,
    pub fn read(self: *@This(), a: std.mem.Allocator, scope: []const u8) !collection.Snapshot {
        return self.delegate.read(a, scope);
    }
    pub fn publish(self: *@This(), a: std.mem.Allocator, scope: []const u8, prior: collection.Snapshot, bytes: []const u8) !void {
        self.calls += 1;
        if (self.competitor) |other| {
            self.competitor = null;
            _ = try other.nativeStore().writer.put(scope, "concurrent", "other-writer", .any);
        }
        try self.delegate.publish(a, scope, prior, bytes);
        if (self.unknown_after_write) return error.OutcomeUnknown;
    }
};

test "secret backend retries known CAS races and never replays uncertain publication" {
    var memory = objects.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("secrets");
    var keys = TestProvider{};
    const backend = serverless.Backend{ .client = client, .bucket = "secrets", .prefix = "native", .consistency = .linearizable_cas };
    var competitor = try serverless.Store.init(alloc, std.testing.io, "scope", keys.provider(), backend);
    defer competitor.deinit();
    var racing = try collection.Store(RacingBackend).init(alloc, std.testing.io, "scope", keys.provider(), .{ .delegate = backend, .competitor = &competitor });
    defer racing.deinit();
    try std.testing.expectEqual(@as(u64, 2), (try racing.nativeStore().writer.put("scope", "token", "our-write", .absent)).revision);
    try std.testing.expectEqual(@as(usize, 2), racing.backend.calls);
    var other = try competitor.source().resolve(alloc, "scope", "concurrent", .{});
    defer other.deinit(alloc);
    try std.testing.expectEqualStrings("other-writer", other.value.?.secret.bytes);
    racing.backend.unknown_after_write = true;
    try std.testing.expectError(error.OutcomeUnknown, racing.nativeStore().writer.put("scope", "token", "committed-but-unacknowledged", .{ .exact = 2 }));
    try std.testing.expectEqual(@as(usize, 3), racing.backend.calls);
    var committed = try competitor.source().resolve(alloc, "scope", "token", .{});
    defer committed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), committed.revision);
    try std.testing.expectEqualStrings("committed-but-unacknowledged", committed.value.?.secret.bytes);
}

test "secret backend startup keeps operational references before native attachment" {
    const config = @import("common/config.zig");
    var facade = try secrets.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{ .native = .{ .backend = .distributed, .keyring_path = "bootstrap-keyring" }, .environment = false });
    defer facade.deinit();
    var cfg = try config.Config.parseFromSliceWithSecrets(alloc,
        \\{"secrets":{"native":{"backend":"distributed","keyring_path":"bootstrap-keyring"},"environment":false},
        \\ "inference":{"api_url":"http://localhost:8080","api_key":"${secret:provider.key}","s3_credentials":{"access_key_id":"${secret:s3.id}","secret_access_key":"${secret:s3.key}"}},
        \\ "connections":{"provider":{"kind":"inference","capabilities":["models.generate"],"inference":{"provider":"openai","api_key":"${secret:provider.key}"}},"search":{"kind":"web_search","provider":"exa","capabilities":["web.search"],"web_search":{"api_key":"${secret:search.key}"}}}}
    , &facade);
    defer cfg.deinit();
    try std.testing.expectEqualStrings("${secret:provider.key}", cfg.inference.api_key.?);
    try std.testing.expectEqualStrings("${secret:s3.key}", cfg.inference.s3_credentials.?.secret_access_key.?);
    try std.testing.expectEqualStrings("${secret:search.key}", cfg.connections.get("search").?.web_search.?.api_key.?);
    try std.testing.expectEqualStrings("${secret:provider.key}", cfg.connections.get("provider").?.inference.?.api_key.?);
    try std.testing.expectError(error.Unavailable, facade.list(alloc));
    try std.testing.expectError(error.InvalidConfig, secrets.Config.validate(.{ .native = .{ .backend = .distributed } }));
    try std.testing.expectError(error.InvalidConfig, secrets.Config.validate(.{ .native = .{ .backend = .distributed, .keyring_path = "${secret:cyclic}" } }));
}

test "secret backend publication identities distinguish competing deletes and read legacy collections" {
    var empty = try collection.decode(alloc, "scope", null);
    defer empty.deinit(alloc);
    var keys = TestProvider{};
    const envelope = try record.seal(alloc, std.testing.io, keys.provider(), .{ .scope = "scope", .key = "token", .revision = 1 }, "value");
    defer alloc.free(envelope);
    const initial = try collection.replace(alloc, std.testing.io, "scope", empty, "token", envelope);
    defer alloc.free(initial);
    var previous = try collection.decode(alloc, "scope", initial);
    defer previous.deinit(alloc);
    const first = try collection.replace(alloc, std.testing.io, "scope", previous, "token", null);
    defer alloc.free(first);
    const second = try collection.replace(alloc, std.testing.io, "scope", previous, "token", null);
    defer alloc.free(second);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    // AFSC v1 has the same fixed fields but no publication identity.
    const legacy = try std.mem.concat(alloc, u8, &.{ first[0..20], first[36..] });
    defer alloc.free(legacy);
    std.mem.writeInt(u16, legacy[4..6], 1, .little);
    var decoded = try collection.decode(alloc, "scope", legacy);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), decoded.revision);
    try std.testing.expectEqualStrings("scope", try collection.storedScope(legacy));
}

test "secret backend S3 opening uses refreshable bootstrap credential sources" {
    const support = @import("serverless/object_store_support.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "credentials", .data = "[native-test]\naws_access_key_id = native-access\naws_secret_access_key = native-secret\naws_session_token = native-session\n" });
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, "credentials", alloc);
    defer alloc.free(path);
    var opened = try support.OpenedObjectStore.initS3UriWithS3AndOpenOptions(alloc, "secrets", "prefix", .{
        .endpoint = "http://127.0.0.1:1",
        .credential_source = .{ .profile = .{ .name = "native-test", .shared_credentials_file = path } },
    }, .{ .ensure_bucket = false });
    defer opened.deinit();
    // The exact constructor used by native startup must also install a
    // provider when no explicit options or static environment keys exist.
    var defaults = try support.OpenedObjectStore.initRemoteUriWithS3AndOpenOptions(alloc, "s3://secrets/prefix", "unused", null, .{ .ensure_bucket = false });
    defer defaults.deinit();
    try std.testing.expect(defaults.s3_client.?.cfg.credential_provider != null);
    const provider = opened.s3_client.?.cfg.credential_provider orelse return error.TestUnexpectedResult;
    var credentials = try provider.get(alloc);
    defer credentials.deinit(alloc);
    try std.testing.expectEqualStrings("native-access", credentials.access_key_id);
    try std.testing.expectEqualStrings("native-session", credentials.session_token.?);
    var static = try support.OpenedObjectStore.initS3UriWithS3AndOpenOptions(alloc, "secrets", "prefix", .{
        .endpoint = "http://127.0.0.1:1",
        .access_key_id = "explicit",
        .secret_access_key = "explicit-secret",
    }, .{ .ensure_bucket = false });
    defer static.deinit();
    try std.testing.expect(static.s3_client.?.cfg.credential_provider == null);
}

test "secret backend follower API forwards encrypted PUT and DELETE through Raft" {
    const runtime = @import("metadata/runtime.zig");
    const time = @import("antfly_platform").time;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    var servers: [2]runtime.Server = undefined;
    var facades: [2]secrets.FileStore = undefined;
    var natives: [2]distributed.Store = undefined;
    var keys: [2]TestProvider = .{ .{}, .{} };
    var initialized: usize = 0;
    defer for (0..initialized) |i| {
        servers[i].deinit();
        natives[i].deinit();
        facades[i].deinit();
    };
    // Membership must be known to the descriptor factory before replicas open.
    // Replace the placeholder transport addresses after binding ephemeral ports.
    var peers = [_]runtime.MetadataClusterPeer{
        .{ .node_id = 1, .raft_url = "http://127.0.0.1:1" },
        .{ .node_id = 2, .raft_url = "http://127.0.0.1:1" },
    };
    for (0..2) |i| {
        facades[i] = try secrets.FileStore.initConfiguredWithIo(alloc, std.testing.io, .{ .native = .{ .backend = .distributed, .scope = "scope", .keyring_path = "host-provider" }, .environment = false });
        errdefer facades[i].deinit();
        servers[i] = try runtime.Server.init(alloc, .{
            .replica_root_dir = try std.fmt.allocPrint(a, "{s}/{d}/replicas", .{ root, i }),
            .replica_catalog_path = try std.fmt.allocPrint(a, "{s}/{d}/catalog", .{ root, i }),
            .snapshot_root_dir = try std.fmt.allocPrint(a, "{s}/{d}/snapshots", .{ root, i }),
            .local_node_id = i + 1,
            .metadata_group_id = 1,
            .metadata_cluster_peers = &peers,
            .secret_store = &facades[i],
            .api_server_cfg = .{ .auth_enabled = false, .secret_store = &facades[i] },
        });
        errdefer servers[i].deinit();
        natives[i] = try distributed.Store.init(alloc, std.testing.io, "scope", keys[i].provider(), .{ .service = servers[i].server.svc });
        const handle = natives[i].nativeStore();
        facades[i].attachNative(handle.source, handle.writer);
        initialized += 1;
    }
    for (0..2) |i| {
        try servers[i].start();
        peers[i] = .{ .node_id = i + 1, .raft_url = try servers[i].baseUri(a), .orchestration_url = try servers[i].adminBaseUri(a) };
    }
    for (0..2) |i| try servers[i].bootstrapCluster(1, i + 1, &peers);
    const Progress = struct {
        servers: *[2]runtime.Server,
        stop: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            while (!self.stop.load(.acquire)) {
                for (self.servers) |*server| server.runRaftRoundOnly() catch {
                    self.failed.store(true, .release);
                    return;
                };
                time.sleepNs(10 * std.time.ns_per_ms);
            }
        }
    };
    try servers[0].server.svc.campaignMetadataGroup();
    var progress = Progress{ .servers = &servers };
    const thread = try std.Thread.spawn(.{}, Progress.run, .{&progress});
    defer {
        progress.stop.store(true, .release);
        thread.join();
    }
    const deadline = time.monotonicNs() + 10 * std.time.ns_per_s;
    while (servers[0].server.svc.localMetadataLeadershipTerm() == null and time.monotonicNs() < deadline) time.sleepNs(10 * std.time.ns_per_ms);
    try std.testing.expect(servers[0].server.svc.localMetadataLeadershipTerm() != null);
    try std.testing.expect(servers[1].server.svc.localMetadataLeadershipTerm() == null);
    const uri = try std.fmt.allocPrint(a, "{s}/db/v1/secrets/token", .{peers[1].orchestration_url.?});
    var executor = @import("common/http/std_http_executor.zig").StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var put = try executor.executor().execute(alloc, .{ .method = .PUT, .uri = uri, .body = "{\"value\":\"follower-write\"}", .content_type = "application/json", .timeout_ms = 10_000 });
    defer put.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), put.status);
    var found = try natives[0].source().resolve(alloc, "scope", "token", .{ .min_revision = 1 });
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("follower-write", found.value.?.secret.bytes);
    var deleted = try executor.executor().execute(alloc, .{ .method = .DELETE, .uri = uri, .timeout_ms = 10_000 });
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), deleted.status);
    var absent = try natives[0].source().resolve(alloc, "scope", "token", .{ .min_revision = 2 });
    defer absent.deinit(alloc);
    try std.testing.expect(absent.value == null);
    try std.testing.expect(servers[1].server.svc.localMetadataLeadershipTerm() == null);
    try std.testing.expect(!progress.failed.load(.acquire));
}
