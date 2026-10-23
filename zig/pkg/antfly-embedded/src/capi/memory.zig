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

const h = @import("handles.zig");
const std = h.std;
const engine = @import("../memory/engine.zig");
const memory = engine.service;

pub const Request = struct {
    action: memory.Action,
    id: []const u8 = "",
    idempotency_key: []const u8 = "",
    principal: []const u8 = "embedded",
    body: std.json.Value = .{ .object = .empty },
};

fn clockMs() i64 {
    return @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
}

const ResourceReader = struct {
    // The guard is rebound when inference resumes; a captured Handle can be
    // retired and freed by another connection while the lease is released.
    guard: *@import("db.zig").HandleGuard,
    fn read(ptr: *anyopaque, a: std.mem.Allocator, reference: memory.types.provenance.Resource) !memory.types.provenance.Snapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, reference.database, "default") or !std.mem.eql(u8, reference.namespace, "public")) return .{ .state = .denied };
        const current = self.guard.handle;
        const db = @import("tables.zig").get(current.parent_handle orelse current, reference.table) catch |err| switch (err) {
            error.UndefinedTable, error.TableNotFound => return .{ .state = .missing },
            else => return err,
        };
        if (reference.table_id != 0 and db.core.identity_namespace.table_id != reference.table_id) return .{ .state = .missing };
        var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer scratch.deinit();
        const row = (try db.getDocument(scratch.allocator(), reference.id, .{})) orelse return .{ .state = .missing };
        var snapshot = try memory.types.provenance.snapshot(a, reference, db.core.identity_namespace.table_id, row.version orelse try db.getTimestamp(a, reference.id), row.json, row.expected_content_digest);
        snapshot.storage_name = try std.fmt.allocPrint(a, "table:{d}", .{db.core.identity_namespace.table_id});
        return snapshot;
    }
};

/// The local database owner is the authority for embedded callers. There are
/// no HTTP credentials or caller-supplied server permissions in this contract.
pub fn execute(guard: *@import("db.zig").HandleGuard, handle_ptr: ?*anyopaque, alloc: std.mem.Allocator, space: []const u8, request_json: []const u8) ![]u8 {
    const handle = guard.handle;
    if (request_json.len > 4 * 1024 * 1024) return error.MemoryInputTooLarge;
    try memory.types.validateId(space);
    if (std.mem.indexOfAny(u8, space, "/.\\%") != null) return error.InvalidMemoryId;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(Request, a, request_json, .{ .ignore_unknown_fields = false, .parse_numbers = false });
    const table = try std.fmt.allocPrint(a, "memory_{s}", .{space});
    const tables = @import("tables.zig");
    if (memory.mutates(request.action) and (request.idempotency_key.len == 0 or request.idempotency_key.len > 256)) return error.MemoryIdempotencyKeyRequired;
    if (request.action == .create) {
        const config = try std.json.parseFromSliceLeaky(memory.types.Config, a, try @import("antfly-json").stringifyValueAlloc(a, request.body), .{ .ignore_unknown_fields = false });
        try config.validate();
    }
    if (memory.mutates(request.action) and !h.liteOpenModeCanWrite(handle.open_mode)) return error.SqlReadOnlyTransaction;
    if (request.action == .create) try tables.create(handle.parent_handle orelse handle, table, "{}", true);
    const database = try tables.get(handle.parent_handle orelse handle, table);
    const context: memory.Context = .{ .table_id = database.core.identity_namespace.table_id, .space = space, .principal = request.principal, .admin = true, .clock_ms_fn = clockMs, .now_ms = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) };
    var resources: ResourceReader = .{ .guard = guard };
    var adapter: engine.Native(h.db_mod.DB) = .{ .db = database, .context = context, .resource_ptr = &resources, .resource_fn = ResourceReader.read };
    // Provider lifetime is independent of the storage generation. Retain it
    // across every model call and writer-lease release in this operation.
    var provider_owner = handle;
    while (provider_owner.parent_handle) |parent| provider_owner = parent;
    const provider_service = provider_owner.lite_inference;
    if (provider_service) |provider| provider.retain();
    defer if (provider_service) |provider| provider.release();
    const runtime = try @import("../asset_producer_runtime.zig").Runtime.createOwned(a, h.handleLockIo(), .{ .antfly_provider = handle.liteAntflyProvider(), .source_table = table });
    defer runtime.deinit();
    const Hooks = struct {
        guard: *@import("db.zig").HandleGuard,
        handle_ptr: ?*anyopaque,
        adapter: *engine.Native(h.db_mod.DB),
        table: []const u8,
        table_id: u64,
        access: @import("db.zig").HandleAccess,
        legacy_suspended: bool = false,
        fn before(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.guard.connection != null) self.guard.suspendQuery() else {
                // Registry admission still pins the directory handle while
                // releasing only its API lock around the owned model plan.
                if (self.access == .read) self.guard.lock_handle.api_lock.unlockShared(h.handleLockIo()) else self.guard.lock_handle.api_lock.unlock(h.handleLockIo());
                self.legacy_suspended = true;
            }
        }
        fn after(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.legacy_suspended) {
                if (self.access == .read) self.guard.lock_handle.api_lock.lockSharedUncancelable(h.handleLockIo()) else self.guard.lock_handle.api_lock.lockUncancelable(h.handleLockIo());
                self.legacy_suspended = false;
            }
            if (!self.guard.resumeAccess(self.handle_ptr, self.access)) return error.MemoryRevisionConflict;
            if (self.guard.entry_error != null) return error.MemoryRevisionConflict;
            const current = self.guard.handle;
            const db = try @import("tables.zig").get(current.parent_handle orelse current, self.table);
            if (db.core.identity_namespace.table_id != self.table_id) return error.MemorySpaceBindingChanged;
            self.adapter.db = db;
        }
    };
    var hooks: Hooks = .{ .guard = guard, .handle_ptr = handle_ptr, .adapter = &adapter, .table = table, .table_id = context.table_id, .access = guard.access };
    var bridge: @import("../memory/models.zig").Bridge = .{ .base = adapter.backend(), .producer = runtime.producer(), .alloc = a, .inference_ptr = &hooks, .before_inference = Hooks.before, .after_inference = Hooks.after, .embedding_options = .{ .antfly_provider = handle.liteAntflyProvider(), .io = h.handleLockIo(), .source_table = table } };
    return (memory.Service{ .backend = bridge.backend(), .context = context }).execute(alloc, request.action, request.id, try @import("antfly-json").stringifyValueAlloc(a, request.body), request.idempotency_key);
}

test "capi native memory resource reader follows a resumed storage generation" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("memory-resume-generation");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_create(path, &id));
    defer api.antfly_db_close(id);
    {
        const setup = api.enterHandle(id, .exclusive) orelse return error.TestUnexpectedResult;
        defer setup.leave();
        const root = setup.handle.parent_handle orelse setup.handle;
        try @import("tables.zig").create(root, "business", "{}", true);
        const db = try @import("tables.zig").get(root, "business");
        try db.batch(.{ .writes = &.{.{ .key = "approval", .value = "{\"message\":\"BEFORE\"}" }}, .sync_level = .write });
    }
    var guard = api.enterHandle(id, .read) orelse return error.TestUnexpectedResult;
    defer guard.leave();
    const prior_address = @intFromPtr(guard.handle);
    var reader: ResourceReader = .{ .guard = &guard };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const reference: memory.types.provenance.Resource = .{ .table = "business", .table_id = 0, .kind = .record, .id = "approval", .revision = 0 };
    const before = try ResourceReader.read(&reader, alloc, reference);
    try std.testing.expect(std.mem.indexOf(u8, before.json, "BEFORE") != null);
    // Exercise the inference lease boundary with an independent native writer,
    // as another process would. Shared in-process connections can mutate the
    // same cached runtime without forcing a generation replacement.
    guard.suspendQuery();
    {
        // Exclude this process's maintenance publications while the fixture
        // acts as an independent writer. They take the same gate before the
        // kernel lease, so contention cannot make this handoff timing-dependent.
        const gate = guard.connection.?.gate;
        gate.mutex.lockUncancelable(h.handleLockIo());
        defer gate.mutex.unlock(h.handleLockIo());
        var lease = try @import("../storage/lite/native.zig").lockWriterPathWithIo(a, h.handleLockIo(), path);
        defer lease.close();
        const writer = try api.openLiteHandleAllocWithSharedInference(a, path, .{ .no_sync = true, .profile = .hosted, .externally_locked = true }, false, h.handleLockIo(), null, null);
        defer h.closeHandle(writer);
        const db = try @import("tables.zig").get(writer, "business");
        try db.batch(.{ .writes = &.{.{ .key = "approval", .value = "{\"message\":\"AFTER\"}" }}, .sync_level = .write });
    }
    try std.testing.expect(guard.resumeQuery(id));
    try std.testing.expect(guard.entry_error == null);
    try std.testing.expect(@intFromPtr(guard.handle) != prior_address);
    const after = try ResourceReader.read(&reader, alloc, reference);
    try std.testing.expectEqual(memory.types.provenance.ResourceState.current, after.state);
    try std.testing.expect(std.mem.indexOf(u8, after.json, "AFTER") != null);
    try std.testing.expect(after.revision != before.revision);
}

test "capi native memory ANN projections use shared native indexes and routed neighbor decoding" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("memory-ann-native");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_create(path, &id));
    defer api.antfly_db_close(id);
    var guard = api.enterHandle(id, .exclusive) orelse return error.TestUnexpectedResult;
    defer guard.leave();
    const root = guard.handle.parent_handle orelse guard.handle;
    try @import("tables.zig").create(root, "memory_ann", "{}", true);
    const db = try @import("tables.zig").get(root, "memory_ann");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const context: memory.Context = .{ .principal = "owner", .admin = true, .now_ms = 1, .table_id = db.core.identity_namespace.table_id, .space = "ann" };
    var native: engine.Native(h.db_mod.DB) = .{ .db = db, .context = context };
    const Fixture = struct {
        fn embed(_: *anyopaque, allocator: std.mem.Allocator, model: memory.types.Model, _: []const u8) ![]const f32 {
            const vector = try allocator.alloc(f32, model.dimensions.?);
            @memset(vector, 0);
            vector[0] = 1;
            return vector;
        }
        fn remoteNeighbors(ptr: *anyopaque, allocator: std.mem.Allocator, request: memory.NeighborRequest) ![]const memory.Neighbor {
            const owner: *engine.Native(h.db_mod.DB) = @ptrCast(@alignCast(ptr));
            var routed: engine.Routed = .{ .table = "memory_ann", .reads = .{ .ptr = owner.db, .vtable = &.{ .lookup = lookup, .scan = scan, .query = query } }, .writes = undefined, .context = owner.context };
            const remote = routed.backend();
            return remote.neighbors.?(remote.ptr, allocator, request);
        }
        fn remoteGetMany(ptr: *anyopaque, allocator: std.mem.Allocator, keys: []const []const u8) ![]const ?memory.Row {
            const owner: *engine.Native(h.db_mod.DB) = @ptrCast(@alignCast(ptr));
            var routed: engine.Routed = .{ .table = "memory_ann", .reads = .{ .ptr = owner.db, .vtable = &.{ .lookup = lookup, .scan = scan, .query = query } }, .writes = undefined, .context = owner.context };
            const remote = routed.backend();
            return remote.get_many.?(remote.ptr, allocator, keys);
        }
        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: h.db_mod.types.LookupOptions, _: @import("../storage/read_consistency.zig").ReadConsistency) anyerror!?@import("../api/table_read_source.zig").LookupResponse {
            return error.UnexpectedLookup;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: h.db_mod.types.ScanOptions, _: @import("../storage/read_consistency.zig").ReadConsistency) anyerror!?@import("../api/table_read_source.zig").ScanResponse {
            return error.UnexpectedScan;
        }
        fn query(ptr: *anyopaque, allocator: std.mem.Allocator, _: []const u8, request: h.db_mod.types.SearchRequest, _: @import("../storage/read_consistency.zig").ReadConsistency) anyerror!?@import("../api/query.zig").QueryResponse {
            const database: *h.db_mod.DB = @ptrCast(@alignCast(ptr));
            var result = try database.search(allocator, request);
            defer result.deinit();
            return try @import("../api/query.zig").encodeQueryResponses(allocator, "memory_ann", request, .{}, result);
        }
    };
    var backend = native.backend();
    backend.embed = Fixture.embed;
    backend.embed_document = Fixture.embed;
    const service: memory.Service = .{ .backend = backend, .context = context };
    const dimensions = if (std.c.getenv("ANTFLY_MEMORY_QUALIFY_DIMS")) |raw| try std.fmt.parseInt(u32, std.mem.span(raw), 10) else @as(u32, 2);
    if (dimensions == 0 or dimensions > 1536) return error.InvalidMemoryRequest;
    const config: memory.types.Config = .{ .models = .{ .embedder = .{ .provider = "fixture", .model = "role-test", .revision = "v1", .dimensions = dimensions } }, .semantic_discovery = .{ .enabled = true } };
    _ = try service.execute(alloc, .create, "", try std.json.Stringify.valueAlloc(alloc, config, .{}), "create");
    _ = try service.execute(alloc, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"A knowledge graph is a KG\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"kg\",\"name\":\"KG\",\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
    _ = try service.execute(alloc, .advance, "", "{}", "backfill");
    const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqual(.indexed, page.semantic_discovery);
    try std.testing.expectEqual(@as(usize, 1), page.items.len);
    try std.testing.expectEqualStrings("kg", page.items[0].entity.entity.id);
    const projection_rows = try backend.scan(backend.ptr, alloc, "entity-semantic/", null, 8);
    try std.testing.expectEqual(@as(usize, 4), projection_rows.rows.len);
    var routed: engine.Routed = .{ .table = "memory_ann", .reads = .{ .ptr = db, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } }, .writes = undefined, .context = context };
    const remote = routed.backend();
    const hydration_keys: []const []const u8 = &.{ "control", "missing", projection_rows.rows[0].key, "control" };
    const native_rows = try backend.get_many.?(backend.ptr, alloc, hydration_keys);
    const remote_rows = try remote.get_many.?(remote.ptr, alloc, hydration_keys);
    try std.testing.expect(native_rows[1] == null and remote_rows[1] == null);
    for (native_rows, remote_rows) |local, distant| {
        if (local) |row| {
            try std.testing.expectEqualStrings(row.key, distant.?.key);
            try std.testing.expectEqualStrings(row.json, distant.?.json);
        }
    }
    for (projection_rows.rows) |row| {
        const rest = row.key["entity-semantic/".len..];
        const slash = std.mem.indexOfScalar(u8, rest, '/').?;
        const request: memory.NeighborRequest = .{ .index = rest[0..slash], .vector = (try std.json.parseFromSliceLeaky(memory.types.EntityProjection, alloc, row.json, .{})).embedding, .limit = 4 };
        const local_neighbors = try backend.neighbors.?(backend.ptr, alloc, request);
        const remote_neighbors = try remote.neighbors.?(remote.ptr, alloc, request);
        try std.testing.expectEqualDeep(local_neighbors, remote_neighbors);
    }
    var remote_service = service;
    remote_service.backend.neighbors = Fixture.remoteNeighbors;
    remote_service.backend.get_many = Fixture.remoteGetMany;
    const remote_page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try remote_service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
    try std.testing.expectEqualDeep(page, remote_page);
    try qualifyAnnScale(alloc, &native, service, projection_rows.rows);
}

test "capi native memory semantic deadlines survive reopen and refresh both validity boundaries" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("memory-semantic-timers");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    const Fixture = struct {
        fn embed(_: *anyopaque, allocator: std.mem.Allocator, _: memory.types.Model, _: []const u8) ![]const f32 {
            return allocator.dupe(f32, &.{ 1, 0 });
        }
    };
    for ([_]i64{ 1, 200, 400 }, 0..) |now, phase| {
        var id: ?*anyopaque = null;
        const status = if (phase == 0) api.antfly_lite_create(path, &id) else api.antfly_lite_open(path, &id);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, status);
        defer api.antfly_db_close(id);
        var guard = api.enterHandle(id, .exclusive) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        const root = guard.handle.parent_handle orelse guard.handle;
        if (phase == 0) try @import("tables.zig").create(root, "memory_timers", "{}", true);
        const db = try @import("tables.zig").get(root, "memory_timers");
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const context: memory.Context = .{ .principal = "owner", .admin = true, .now_ms = now, .table_id = db.core.identity_namespace.table_id, .space = "timers" };
        var native: engine.Native(h.db_mod.DB) = .{ .db = db, .context = context };
        var backend = native.backend();
        backend.embed = Fixture.embed;
        backend.embed_document = Fixture.embed;
        const service: memory.Service = .{ .backend = backend, .context = context };
        if (phase == 0) {
            const config: memory.types.Config = .{ .models = .{ .embedder = .{ .provider = "fixture", .model = "timer", .revision = "v1", .dimensions = 2 } }, .semantic_discovery = .{ .enabled = true } };
            _ = try service.execute(alloc, .create, "", try std.json.Stringify.valueAlloc(alloc, config, .{}), "create");
            _ = try service.execute(alloc, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"description\",\"value\":\"graph\",\"statement\":\"Timed graph description\",\"valid_from\":100,\"valid_to\":300,\"evidence\":[{\"id\":\"e\"}]}]}", "observe");
        }
        // Primary hydration must match point reads for temporal facts and
        // internal scheduler rows, including immediately after reopening.
        const raw = try backend.scan(backend.ptr, alloc, "", null, 64);
        const paths = try alloc.alloc([]const u8, raw.rows.len);
        for (raw.rows, paths) |row, *key| key.* = row.key;
        if (paths.len != 0) {
            const batch = try backend.get_many.?(backend.ptr, alloc, paths);
            for (paths, batch) |key, row| {
                const point = (try backend.get(backend.ptr, alloc, key)).?;
                try std.testing.expect(row != null);
                try std.testing.expectEqualStrings(point.json, row.?.json);
            }
        }
        for (0..16) |i| {
            _ = try service.execute(alloc, .advance, "", "{}", try std.fmt.allocPrint(alloc, "phase-{d}-{d}", .{ phase, i }));
            const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
            if (page.semantic_discovery == .indexed) {
                try std.testing.expectEqual(phase == 1, std.mem.indexOf(u8, page.seed_view.description, "Timed graph") != null);
                break;
            }
        } else return error.TestUnexpectedResult;
        const timers = try backend.scan(backend.ptr, alloc, "entity-semantic-due/", null, 8);
        try std.testing.expectEqual(@as(usize, if (phase == 2) 0 else 1), timers.rows.len);
    }
}

test "capi native memory projection manifests survive reopen and retire curation entries" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("memory-semantic-manifests");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    const Fixture = struct {
        fn embed(_: *anyopaque, allocator: std.mem.Allocator, _: memory.types.Model, _: []const u8) ![]const f32 {
            return allocator.dupe(f32, &.{ 1, 0 });
        }
    };
    for (0..3) |phase| {
        var id: ?*anyopaque = null;
        const status = if (phase == 0) api.antfly_lite_create(path, &id) else api.antfly_lite_open(path, &id);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, status);
        defer api.antfly_db_close(id);
        var guard = api.enterHandle(id, .exclusive) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        const root = guard.handle.parent_handle orelse guard.handle;
        if (phase == 0) {
            try @import("tables.zig").create(root, "business", "{}", true);
            const business = try @import("tables.zig").get(root, "business");
            try business.batch(.{ .writes = &.{.{ .key = "approval", .value = "{\"message\":\"Approved deployment\"}" }}, .sync_level = .write });
            try @import("tables.zig").create(root, "memory_manifests", "{}", true);
        }
        const db = try @import("tables.zig").get(root, "memory_manifests");
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const context: memory.Context = .{ .principal = "owner", .admin = true, .now_ms = if (phase == 0) 1 else @intCast(40000 * phase), .table_id = db.core.identity_namespace.table_id, .space = "manifests" };
        var reader: ResourceReader = .{ .guard = &guard };
        var native: engine.Native(h.db_mod.DB) = .{ .db = db, .context = context, .resource_ptr = &reader, .resource_fn = ResourceReader.read };
        var backend = native.backend();
        backend.embed = Fixture.embed;
        backend.embed_document = Fixture.embed;
        const service: memory.Service = .{ .backend = backend, .context = context };
        if (phase == 0) {
            const config: memory.types.Config = .{ .models = .{ .embedder = .{ .provider = "fixture", .model = "manifest", .revision = "v1", .dimensions = 2 } }, .semantic_discovery = .{ .enabled = true } };
            _ = try service.execute(alloc, .create, "", try std.json.Stringify.valueAlloc(alloc, config, .{}), "create");
            _ = try service.execute(alloc, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"},{\"id\":\"source\",\"resource_selector\":{\"table\":\"business\",\"id\":\"approval\"}}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"kg\",\"name\":\"KG\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"d\",\"subject\":\"graph\",\"predicate\":\"description\",\"value\":\"graph\",\"statement\":\"Source-backed description\",\"evidence\":[{\"id\":\"source\"}]}]}", "observe");
        } else {
            const bindings = try backend.scan(backend.ptr, alloc, "entity-semantic-binding/", null, 8);
            try std.testing.expectEqual(@as(usize, if (phase == 1) 2 else 1), bindings.rows.len);
            try std.testing.expectEqual(@as(usize, 1), (try backend.scan(backend.ptr, alloc, "dependency/semantic-resource/", null, 8)).rows.len);
            if (phase == 1) {
                const state = try std.json.parseFromSliceLeaky(memory.types.State, alloc, (try backend.get(backend.ptr, alloc, "control")).?.json, .{});
                _ = try service.execute(alloc, .curate_entity, "", try std.json.Stringify.valueAlloc(alloc, .{ .expected_revision = state.revision, .action = "suppress", .from = "kg" }, .{}), "suppress");
            } else {
                const business = try @import("tables.zig").get(root, "business");
                try business.batch(.{ .writes = &.{.{ .key = "approval", .value = "{\"message\":\"Changed source\"}" }}, .sync_level = .write });
                const stale = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
                try std.testing.expectEqual(.not_ready, stale.semantic_discovery);
            }
        }
        for (0..32) |i| {
            _ = try service.execute(alloc, .advance, "", "{}", try std.fmt.allocPrint(alloc, "phase-{d}-{d}", .{ phase, i }));
            const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
            if (page.semantic_discovery == .indexed) {
                try std.testing.expectEqual(phase != 2, std.mem.indexOf(u8, page.seed_view.description, "Source-backed") != null);
                break;
            }
        } else return error.TestUnexpectedResult;
        const projections = try backend.scan(backend.ptr, alloc, "entity-semantic/", null, 8);
        try std.testing.expectEqual(@as(usize, if (phase == 0) 4 else 2), projections.rows.len);
        for (projections.rows) |row| {
            const rest = row.key["entity-semantic/".len..];
            const slash = std.mem.indexOfScalar(u8, rest, '/').?;
            const neighbors = try backend.neighbors.?(backend.ptr, alloc, .{ .index = rest[0..slash], .vector = &.{ 1, 0 }, .limit = 4 });
            try std.testing.expectEqual(@as(usize, if (phase == 0) 2 else 1), neighbors.len);
            if (phase != 0) for (neighbors) |neighbor| {
                const projection = try std.json.parseFromSliceLeaky(memory.types.EntityProjection, alloc, (try backend.get(backend.ptr, alloc, neighbor.key)).?.json, .{});
                try std.testing.expectEqualStrings("graph", projection.entity_id);
            };
        }
    }
}

test "capi native memory internal assertion refresh survives reopen and preserves facts" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("memory-semantic-assertions");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    const Fixture = struct {
        fn embed(_: *anyopaque, allocator: std.mem.Allocator, _: memory.types.Model, _: []const u8) ![]const f32 {
            return allocator.dupe(f32, &.{ 1, 0 });
        }
    };
    var original_revision: u64 = 0;
    for (0..3) |phase| {
        var id: ?*anyopaque = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, if (phase == 0) api.antfly_lite_create(path, &id) else api.antfly_lite_open(path, &id));
        defer api.antfly_db_close(id);
        var guard = api.enterHandle(id, .exclusive) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        const root = guard.handle.parent_handle orelse guard.handle;
        if (phase == 0) try @import("tables.zig").create(root, "memory_internal", "{}", true);
        const db = try @import("tables.zig").get(root, "memory_internal");
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const context: memory.Context = .{ .principal = "owner", .admin = true, .now_ms = @intCast(phase + 1), .table_id = db.core.identity_namespace.table_id, .space = "internal" };
        var native: engine.Native(h.db_mod.DB) = .{ .db = db, .context = context };
        var backend = native.backend();
        backend.embed = Fixture.embed;
        backend.embed_document = Fixture.embed;
        const service: memory.Service = .{ .backend = backend, .context = context };
        if (phase == 0) {
            const config: memory.types.Config = .{ .models = .{ .embedder = .{ .provider = "fixture", .model = "internal", .revision = "v1", .dimensions = 2 } }, .semantic_discovery = .{ .enabled = true } };
            _ = try service.execute(alloc, .create, "", try std.json.Stringify.valueAlloc(alloc, config, .{}), "create");
            const admitted = try std.json.parseFromSliceLeaky(memory.types.OperationStatus, alloc, try service.execute(alloc, .observe, "", "{\"evidence\":[{\"id\":\"e\",\"source_id\":\"s\",\"source_version\":\"1\",\"text\":\"Graph\"}],\"entities\":[{\"id\":\"graph\",\"name\":\"knowledge graph\",\"evidence\":[{\"id\":\"e\"}]}],\"assertions\":[{\"id\":\"root\",\"subject\":\"other\",\"predicate\":\"p\",\"value\":\"root\",\"statement\":\"Root premise\",\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"middle\",\"subject\":\"other\",\"predicate\":\"p\",\"value\":\"middle\",\"statement\":\"Middle premise\",\"depends_on\":[\"root\"],\"evidence\":[{\"id\":\"e\"}]},{\"id\":\"description\",\"subject\":\"graph\",\"predicate\":\"p\",\"value\":\"graph\",\"statement\":\"Derived graph description\",\"depends_on\":[\"middle\"],\"evidence\":[{\"id\":\"e\"}]}]}", "observe"), .{});
            original_revision = admitted.revision;
        } else {
            try std.testing.expectEqual(@as(usize, 3), (try backend.scan(backend.ptr, alloc, "dependency/semantic-assertion/", null, 8)).rows.len);
            const claims = try backend.scan(backend.ptr, alloc, "assertion/", null, 8);
            for (claims.rows) |row| {
                const claim = try std.json.parseFromSliceLeaky(memory.types.StoredAssertion, alloc, row.json, .{});
                if (!std.mem.eql(u8, claim.assertion.id, "root")) continue;
                const correction: memory.types.Correction = .{ .id = "root", .expected_revision = claim.revision, .lifecycle = if (phase == 1) .retracted else .active, .reason = "Review premise", .evidence = &.{.{ .id = "e" }} };
                _ = try service.execute(alloc, .correct, "", try std.json.Stringify.valueAlloc(alloc, correction, .{}), try std.fmt.allocPrint(alloc, "correct-{d}", .{phase}));
            }
        }
        for (0..24) |i| {
            _ = try service.execute(alloc, .advance, "", "{}", try std.fmt.allocPrint(alloc, "phase-{d}-{d}", .{ phase, i }));
            const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try service.execute(alloc, .entity_candidates, "graph", "{}", ""), .{});
            if (page.semantic_discovery == .indexed) {
                try std.testing.expectEqual(phase != 1, std.mem.indexOf(u8, page.seed_view.description, "Derived graph") != null);
                break;
            }
        } else return error.TestUnexpectedResult;
        const claims = try backend.scan(backend.ptr, alloc, "assertion/", null, 8);
        try std.testing.expectEqual(@as(usize, 3), claims.rows.len);
        for (claims.rows) |row| {
            const claim = try std.json.parseFromSliceLeaky(memory.types.StoredAssertion, alloc, row.json, .{});
            if (std.mem.eql(u8, claim.assertion.id, "root")) continue;
            try std.testing.expectEqual(original_revision, claim.revision);
            try std.testing.expectEqual(memory.types.Lifecycle.active, claim.lifecycle);
        }
    }
}

// This qualification isolates retrieval cost from model latency and import.
// Projection fixtures use the exact native view contract and real vector indexes.
fn qualifyAnnScale(allocator: std.mem.Allocator, native: *engine.Native(h.db_mod.DB), service: memory.Service, prototypes: []const memory.Row) !void {
    // Keep routine C ABI checks small; qualification explicitly selects 10k.
    const count = if (std.c.getenv("ANTFLY_MEMORY_QUALIFY_SCALE")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else @as(usize, 128);
    if (count == 0 or count > 10000) return error.InvalidMemoryRequest;
    const baseline = native.backend();
    var projections: [2]memory.types.EntityProjection = undefined;
    var index_names: [2][]const u8 = undefined;
    for (prototypes) |row| {
        const projection = try std.json.parseFromSliceLeaky(memory.types.EntityProjection, allocator, row.json, .{});
        const kind = @backingInt(projection.kind);
        projections[kind] = projection;
        const rest = row.key["entity-semantic/".len..];
        index_names[kind] = rest[0..std.mem.indexOfScalar(u8, rest, '/').?];
    }
    const Helpers = struct {
        var neighbor_calls: usize = 0;
        var primary_scans: usize = 0;
        fn digest(a: std.mem.Allocator, id: []const u8) ![]const u8 {
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(try std.json.Stringify.valueAlloc(a, id, .{}), &hash, .{});
            return a.dupe(u8, &std.fmt.bytesToHex(hash, .lower));
        }
        fn neighbors(ptr: *anyopaque, a: std.mem.Allocator, input: memory.NeighborRequest) ![]const memory.Neighbor {
            neighbor_calls += 1;
            const owner: *engine.Native(h.db_mod.DB) = @ptrCast(@alignCast(ptr));
            const backend = owner.backend();
            return backend.neighbors.?(ptr, a, input);
        }
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, prefix: []const u8, cursor: ?[]const u8, limit: u32) !memory.Page {
            if (std.mem.eql(u8, prefix, "entity/")) primary_scans += 1;
            const owner: *engine.Native(h.db_mod.DB) = @ptrCast(@alignCast(ptr));
            return owner.backend().scan(ptr, a, prefix, cursor, limit);
        }
    };
    var changes: std.ArrayList(memory.Change) = .empty;
    for (0..count) |i| {
        const id = try std.fmt.allocPrint(allocator, "scale-{d}", .{i});
        const entity: memory.types.EntityRecord = .{ .entity = .{ .id = id, .name = id, .evidence = &.{.{ .id = "e" }} }, .owner = service.context.principal, .revision = 3, .recorded_at = 1 };
        try changes.append(allocator, .{ .key = try std.fmt.allocPrint(allocator, "entity/{s}", .{try Helpers.digest(allocator, id)}), .json = try std.json.Stringify.valueAlloc(allocator, entity, .{}) });
    }
    var control = (try baseline.get(baseline.ptr, allocator, "control")).?;
    try changes.append(allocator, .{ .key = "control", .json = control.json });
    try baseline.commit(baseline.ptr, allocator, control, changes.items);
    changes.clearRetainingCapacity();
    for (0..count) |i| {
        const id = try std.fmt.allocPrint(allocator, "scale-{d}", .{i});
        const view = try std.json.parseFromSliceLeaky(memory.types.EntityView, allocator, try service.execute(allocator, .entity_view, id, "{}", ""), .{});
        inline for (.{ memory.types.ProjectionKind.names, memory.types.ProjectionKind.view }) |kind| {
            const slot = @backingInt(kind);
            var projection = projections[slot];
            projection.entity_id = id;
            projection.entity_revision = 3;
            projection.text = if (kind == .names) id else view.description;
            projection.fingerprint = view.fingerprint;
            projection.dependencies = view.dependencies;
            try changes.append(allocator, .{ .key = try std.fmt.allocPrint(allocator, "entity-semantic/{s}/{s}", .{ index_names[slot], try Helpers.digest(allocator, id) }), .json = try std.json.Stringify.valueAlloc(allocator, projection, .{}) });
            var query_projection = projection;
            query_projection.embedding = projection.embedding;
            try changes.append(allocator, .{ .key = try std.fmt.allocPrint(allocator, "entity-semantic-query/{s}/{s}", .{ index_names[slot], try Helpers.digest(allocator, id) }), .json = try std.json.Stringify.valueAlloc(allocator, query_projection, .{}) });
        }
    }
    control = (try baseline.get(baseline.ptr, allocator, "control")).?;
    try changes.append(allocator, .{ .key = "control", .json = control.json });
    try baseline.commit(baseline.ptr, allocator, control, changes.items);
    const stats = try native.db.diagnosticStats(allocator);
    defer h.db_mod.types.freeDBStats(allocator, stats);
    var observed: usize = 0;
    for (stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, index_names[0]) and !std.mem.eql(u8, index.name, index_names[1])) continue;
        try std.testing.expect(index.serving_snapshot_ready);
        try std.testing.expectEqual(@as(u64, count + 2), index.doc_count);
        observed += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), observed);
    var measured = service;
    measured.backend.neighbors = Helpers.neighbors;
    measured.backend.scan = Helpers.scan;
    Helpers.neighbor_calls = 0;
    Helpers.primary_scans = 0;
    const start = @import("antfly_platform").time.monotonicNs();
    for (0..count) |i| {
        var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const id = try std.fmt.allocPrint(a, "scale-{d}", .{i});
        const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, a, try measured.execute(a, .entity_candidates, id, "{}", ""), .{});
        try std.testing.expectEqual(.indexed, page.semantic_discovery);
    }
    const elapsed = @import("antfly_platform").time.monotonicNs() - start;
    try std.testing.expectEqual(@as(usize, 2 * count), Helpers.neighbor_calls);
    try std.testing.expectEqual(@as(usize, 0), Helpers.primary_scans);
    std.debug.print("memory ANN scale: {d} seeds, {d} bounded neighbor calls, {d} primary scans, {d} ms\n", .{ count, Helpers.neighbor_calls, Helpers.primary_scans, elapsed / std.time.ns_per_ms });
}

test "capi native memory Codex semantic quality with a qualified managed embedder" {
    const api = @import("db.zig");
    const models_dir = std.mem.span(std.c.getenv("ANTFLY_MEMORY_QUALIFY_MODELS_DIR") orelse return error.SkipZigTest);
    if (!api.localInferenceRuntimeAvailable()) return error.RequiredInferenceRuntimeUnavailable;
    const model_name = if (std.c.getenv("ANTFLY_MEMORY_QUALIFY_MODEL")) |name| std.mem.span(name) else "embeddinggemma2";
    const dimensions = if (std.c.getenv("ANTFLY_MEMORY_QUALIFY_DIMENSIONS")) |raw| try std.fmt.parseInt(u32, std.mem.span(raw), 10) else @as(u32, 768);
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("codex-ann-quality");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/db.aflite", .{directory.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_local_runtime_configured, .models_dir = .fromSlice(models_dir), .inference_host_budget_mb = 16384, .inference_backend_budget_mb = 16384, .inference_combined_budget_mb = 24576, .inference_scratch_budget_mb = 16384, .inference_process_memory_budget_mb = 32768 };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const managed = @import("../inference/managed_embedder.zig");
    const revision = blk: {
        var guard = api.enterHandle(id, .read) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        const indexes = try std.json.Stringify.valueAlloc(alloc, .{ .memory = .{ .type = "embeddings", .field = "text", .dimension = dimensions, .embedder = .{ .provider = "antfly", .model = model_name } } }, .{});
        var embedder = try managed.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes, .{ .antfly_provider = guard.handle.liteAntflyProvider(), .io = h.handleLockIo() });
        defer embedder.deinit();
        const capabilities = try embedder.denseInterface().capabilities(alloc, "memory");
        break :blk try alloc.dupe(u8, &std.fmt.bytesToHex(capabilities.model_revision orelse return error.MemoryModelRevisionUnavailable, .lower));
    };
    const Call = struct {
        fn execute(handle: ?*anyopaque, allocator: std.mem.Allocator, action: memory.Action, identifier: []const u8, body: anytype, token: []const u8) ![]const u8 {
            const request = try std.json.Stringify.valueAlloc(allocator, .{ .action = action, .id = identifier, .body = body, .idempotency_key = token, .principal = "codex-eval" }, .{});
            var output: h.capi.Buffer = .{};
            defer @import("db.zig").antfly_buffer_free(&output);
            const code = @import("db.zig").antfly_db_memory_json(handle, .fromSlice("evaluation"), .fromSlice(request), &output);
            const bytes = if (output.ptr) |ptr| ptr[0..output.len] else "";
            if (code != .ok) {
                std.debug.print("memory quality {s}: code {s}: {s}\n", .{ @tagName(action), @tagName(code), bytes });
                return error.QualificationOperationFailed;
            }
            return allocator.dupe(u8, bytes);
        }
    };
    const config: memory.types.Config = .{ .models = .{ .embedder = .{ .provider = "antfly", .model = model_name, .revision = revision, .dimensions = dimensions } }, .semantic_discovery = .{ .enabled = true }, .max_output_items = 256, .max_input_bytes = 1048576 };
    _ = try Call.execute(id, alloc, .create, "", config, "create");
    const Case = struct { a: []const u8, b: []const u8, same: bool, a_description: []const u8, b_description: []const u8 };
    var cases: std.ArrayList(Case) = .empty;
    var names: std.StringHashMapUnmanaged(usize) = .empty;
    var entities: std.ArrayList(memory.types.Entity) = .empty;
    var claims: std.ArrayList(memory.types.Assertion) = .empty;
    const corpus = @embedFile("../memory/fixtures/codex-a2d59cfb01/concept_merge.jsonl");
    var lines = std.mem.tokenizeScalar(u8, corpus, '\n');
    while (lines.next()) |line| {
        const row = try std.json.parseFromSliceLeaky(Case, alloc, line, .{ .ignore_unknown_fields = true });
        try cases.append(alloc, row);
        inline for (.{ .{ row.a, row.a_description }, .{ row.b, row.b_description } }) |concept| {
            const entry = try names.getOrPut(alloc, concept[0]);
            if (!entry.found_existing) {
                entry.value_ptr.* = entities.items.len;
                const entity_id = try std.fmt.allocPrint(alloc, "concept-{d}", .{entities.items.len});
                try entities.append(alloc, .{ .id = entity_id, .name = concept[0], .evidence = &.{.{ .id = "fixture" }} });
                try claims.append(alloc, .{ .id = entity_id, .subject = entity_id, .predicate = "description", .value = concept[0], .statement = concept[1], .evidence = &.{.{ .id = "fixture" }} });
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 84), entities.items.len);
    _ = try Call.execute(id, alloc, .observe, "", .{ .evidence = &[_]memory.types.Evidence{.{ .id = "fixture", .source_id = "codex-a2d59cfb01", .source_version = "1", .text = corpus }}, .entities = entities.items, .assertions = claims.items }, "fixture");
    for (0..64) |i| {
        _ = try Call.execute(id, alloc, .advance, "", .{}, try std.fmt.allocPrint(alloc, "backfill-{d}", .{i}));
        const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try Call.execute(id, alloc, .entity_candidates, entities.items[0].id, .{ .limit = @as(u16, 64) }, ""), .{});
        if (page.semantic_discovery == .indexed) break;
    } else return error.SemanticBackfillNotReady;
    var discovered: [3][84][84]bool = @splat(@splat(@splat(false)));
    var top5: [3][84][84]bool = @splat(@splat(@splat(false)));
    for (entities.items, 0..) |entity, i| {
        var cursor: ?[]const u8 = null;
        var unique: std.StringHashMapUnmanaged(memory.types.EntityCandidate) = .empty;
        while (true) {
            const page = try std.json.parseFromSliceLeaky(memory.types.EntityCandidatePage, alloc, try Call.execute(id, alloc, .entity_candidates, entity.id, .{ .cursor = cursor, .limit = @as(u16, 64) }, ""), .{});
            try std.testing.expectEqual(.indexed, page.semantic_discovery);
            try std.testing.expectEqual(@as(usize, 0), page.omitted);
            for (page.items) |hit| {
                var merged = hit;
                if (unique.get(hit.entity.entity.id)) |prior| merged.semantic_discovered = merged.semantic_discovered or prior.semantic_discovered;
                try unique.put(alloc, hit.entity.entity.id, merged);
            }
            cursor = page.next_cursor;
            if (cursor == null) break;
        }
        for (0..3) |mode| {
            var hits: std.ArrayList(memory.types.EntityCandidate) = .empty;
            var values = unique.valueIterator();
            while (values.next()) |hit| {
                const semantic = hit.semantic_discovered;
                if (mode == 0 and hit.match_score <= 0 or mode == 1 and !semantic) continue;
                try hits.append(alloc, hit.*);
            }
            std.mem.sort(memory.types.EntityCandidate, hits.items, mode, struct {
                fn less(selected: usize, left: memory.types.EntityCandidate, right: memory.types.EntityCandidate) bool {
                    const lhs = if (selected == 1) @max(left.name_similarity orelse 0, left.view_similarity orelse 0) else left.match_score;
                    const rhs = if (selected == 1) @max(right.name_similarity orelse 0, right.view_similarity orelse 0) else right.match_score;
                    if (lhs != rhs) return lhs > rhs;
                    if (selected != 1 and left.description_score != right.description_score) return left.description_score > right.description_score;
                    return std.mem.lessThan(u8, left.entity.entity.id, right.entity.entity.id);
                }
            }.less);
            for (hits.items, 0..) |hit, position| {
                const j = try std.fmt.parseInt(usize, hit.entity.entity.id["concept-".len..], 10);
                discovered[mode][i][j] = true;
                if (position < 5) top5[mode][i][j] = true;
            }
        }
    }
    var recalls: [3]usize = @splat(0);
    for (0..3) |mode| {
        var recalled_top5: usize = 0;
        var non_synonyms: usize = 0;
        var total: usize = 0;
        for (discovered[mode]) |seed| for (seed) |found| {
            if (found) total += 1;
        };
        for (cases.items) |row| {
            const i = names.get(row.a).?;
            const j = names.get(row.b).?;
            const found = discovered[mode][i][j] or discovered[mode][j][i];
            if (row.same) {
                if (found) recalls[mode] += 1;
                if (top5[mode][i][j] or top5[mode][j][i]) recalled_top5 += 1;
            } else if (found) non_synonyms += 1;
        }
        std.debug.print("Codex {s}: model={s} revision={s}, {d}/18 synonyms, {d}/18 top5, {d}/46 labelled non-synonyms, {d:.3} candidates/seed\n", .{ ([_][]const u8{ "lexical", "ann", "union" })[mode], model_name, revision, recalls[mode], recalled_top5, non_synonyms, @as(f64, @floatFromInt(total)) / 84 });
    }
    try std.testing.expect(recalls[2] >= recalls[0]);
    inline for (.{ .{ "knowledge graph", "KG" }, .{ "Antfly", "AntflyDB" } }) |pair| {
        const i = names.get(pair[0]).?;
        const j = names.get(pair[1]).?;
        try std.testing.expect(discovered[2][i][j] or discovered[2][j][i]);
    }
}
