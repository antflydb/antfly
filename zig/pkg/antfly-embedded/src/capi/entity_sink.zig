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

//! Cross-table promotion under the embedded database's writer fence. Resolver
//! workers are driven by the connection owner, so catalog access and the local
//! transaction coordinator share the same lifetime and serialization as SQL.
const h = @import("handles.zig");
const std = h.std;
const tables = @import("tables.zig");
const types = h.db_mod.types;
const contract = h.antfly.capi_dependencies.api_distributed_txn_contract;
const callbacks = @import("../storage/db/runtime_callbacks.zig");

pub fn attach(handle: *h.Handle, db: *h.db_mod.DB) void {
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return;
    db.setResolutionCandidateSource(candidateSource(handle));
    db.setEntitySink(.{ .ptr = handle, .vtable = &.{ .upsert = upsert, .upsert_batch = upsertBatch, .supports_curation = supportsCuration } });
}

/// Read-only candidate seam also used by ephemeral query resolution.
pub fn candidateSource(handle: *h.Handle) callbacks.CandidateSource {
    return .{ .ptr = handle.parent_handle orelse handle, .vtable = &.{ .get = candidateGet, .scan_prefix = candidateScan, .nearest = candidateNearest, .bound_table = boundTable, .embed_mention = embedMention, .embed_mentions = embedMentions, .adjudicate = adjudicate, .adjudicate_batch = adjudicateBatch, .adjudicator_model_revision = adjudicatorModelRevision, .get_description = getDescription, .revisions = candidateRevisions, .embedding_cache_prepare = embeddingCachePrepare, .embedding_cache_flush = embeddingCacheFlush, .embedding_cache_get = embeddingCacheGet, .embedding_cache_put = embeddingCachePut, .load_vector = loadVector } };
}

fn candidateRevisions(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, index: []const u8, embedding: []const u8) anyerror!callbacks.CandidateSource.Revisions {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    // Directory handles permit concurrent writes; without a pinned candidate
    // snapshot they must refetch. Native calls own the database path gate.
    const database = try tables.get(handle, table);
    const physical = try boundTable(ptr, table);
    const index_config = database.core.index_manager.get(index);
    var schema = database.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    const time_dependent = if (schema) |view| view.visibilityTtlDurationNs() != 0 else false;
    const hash = @import("antfly_resolver").fingerprint;
    const embedder = database.resolution_embedder orelse if (database.enrichment_runtime) |runtime| runtime.config.dense_embedder else null;
    const model_revision: ?[32]u8 = if (embedder) |provider| blk: {
        if (embedding.len == 0) break :blk null;
        const revision = provider.cache_revision_fn orelse break :blk null;
        break :blk try revision(provider.ptr, a, embedding);
    } else null;
    var applied = try database.core.loadAppliedSequence(a, index);
    if (database.executor.appliedSequence(index)) |live| applied = @max(applied, live);
    return .{
        .candidates = if (time_dependent or handle.lite_profile != .native) null else try hash(a, .{ physical, database.core.nextDerivedSequence(), database.core.nextEnrichmentSequence(), index_config, applied }),
        .embedding = if (model_revision) |revision| try hash(a, .{revision}) else null,
    };
}

const embedding_cache_prefix = "\x00antfly/cache/mention_embeddings/v2/";
const embedding_cache_bytes_key = "\x00antfly/cache/mention_embeddings/bytes_v2";
const embedding_cache_limit: usize = 256 * 1024 * 1024;
var embedding_cache_limit_for_test: ?usize = null;
const embedding_cache_pending_limit: usize = 16 * 1024 * 1024;
const embedding_cache_retention_ns: i64 = 30 * std.time.ns_per_day;
const embedding_cache_sweep_key = "\x00antfly/cache/mention_embeddings/last_sweep_ns";

fn embeddingCacheKey(a: std.mem.Allocator, digest: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ embedding_cache_prefix, std.fmt.bytesToHex(digest, .lower) });
}

fn decodeCachedVector(a: std.mem.Allocator, raw: []const u8) !?[]const f32 {
    if (raw.len < 48 or !std.mem.eql(u8, raw[0..4], "ECF2")) return null;
    const now: i64 = @intCast(std.Io.Clock.real.now(h.handleLockIo()).nanoseconds);
    const created = std.mem.readInt(i64, raw[40..48], .little);
    if (now >= created and now - created > embedding_cache_retention_ns) return null;
    const count = std.mem.readInt(u32, raw[4..8], .little);
    if (count == 0 or raw.len != 48 + @as(usize, count) * 4) return null;
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[48..], &hash, .{});
    if (!std.mem.eql(u8, &hash, raw[8..40])) return null;
    const vector = try a.alloc(f32, count);
    for (vector, 0..) |*value, i| {
        value.* = @bitCast(std.mem.readInt(u32, raw[48 + i * 4 ..][0..4], .little));
        if (!std.math.isFinite(value.*)) {
            a.free(vector);
            return null;
        }
    }
    return vector;
}

fn embeddingCacheGet(ptr: *anyopaque, a: std.mem.Allocator, digest: [32]u8) anyerror!?[]const f32 {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    lockCache(&root.embedding_cache_mutex);
    if (root.embedding_cache_pending.get(digest)) |raw| {
        defer root.embedding_cache_mutex.unlock();
        return decodeCachedVector(a, raw);
    }
    root.embedding_cache_mutex.unlock();
    const key = try embeddingCacheKey(a, digest);
    defer a.free(key);
    var read = try root.db.core.store.beginReadTxn();
    defer read.abort();
    const raw = read.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return decodeCachedVector(a, raw);
}

fn lockCache(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
}

fn embeddingCachePut(ptr: *anyopaque, digest: [32]u8, vector: []const f32) anyerror!void {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    if (!h.liteOpenModeCanWrite(root.open_mode) or vector.len == 0 or vector.len > 4 * 1024 * 1024) return;
    const raw = try root.alloc.alloc(u8, 48 + vector.len * 4);
    errdefer root.alloc.free(raw);
    @memcpy(raw[0..4], "ECF2");
    std.mem.writeInt(u32, raw[4..8], @intCast(vector.len), .little);
    std.mem.writeInt(i64, raw[40..48], @intCast(std.Io.Clock.real.now(h.handleLockIo()).nanoseconds), .little);
    for (vector, 0..) |value, i| {
        if (!std.math.isFinite(value)) {
            root.alloc.free(raw);
            return;
        }
        std.mem.writeInt(u32, raw[48 + i * 4 ..][0..4], @bitCast(value), .little);
    }
    std.crypto.hash.sha2.Sha256.hash(raw[48..], raw[8..40], .{});
    lockCache(&root.embedding_cache_mutex);
    defer root.embedding_cache_mutex.unlock();
    if (root.embedding_cache_pending.contains(digest) or root.embedding_cache_pending_bytes + raw.len > embedding_cache_pending_limit) {
        root.alloc.free(raw);
        return;
    }
    try root.embedding_cache_pending.put(root.alloc, digest, raw);
    root.embedding_cache_pending_bytes += raw.len;
}

const PreparedEmbeddingCache = struct {
    root: *h.Handle,
    arena: std.heap.ArenaAllocator,
    writes: []const callbacks.CandidateSource.CacheBatch.Write,
    deletes: []const []const u8,
    keys: []const [32]u8,
    fn finish(ptr: *anyopaque, committed: bool) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const root = self.root;
        if (committed) {
            lockCache(&root.embedding_cache_mutex);
            for (self.keys) |key| if (root.embedding_cache_pending.fetchRemove(key)) |entry| {
                root.embedding_cache_pending_bytes -= entry.value.len;
                root.alloc.free(entry.value);
            };
            root.embedding_cache_mutex.unlock();
        }
        self.arena.deinit();
        root.alloc.destroy(self);
        root.embedding_cache_commit_mutex.unlock();
    }
};

fn prepareEmbeddingCache(root: *h.Handle) !callbacks.CandidateSource.CacheBatch {
    lockCache(&root.embedding_cache_commit_mutex);
    errdefer root.embedding_cache_commit_mutex.unlock();
    const state = try root.alloc.create(PreparedEmbeddingCache);
    errdefer root.alloc.destroy(state);
    state.* = .{ .root = root, .arena = std.heap.ArenaAllocator.init(root.alloc), .writes = &.{}, .deletes = &.{}, .keys = &.{} };
    errdefer state.arena.deinit();
    const a = state.arena.allocator();
    var writes: std.ArrayList(callbacks.CandidateSource.CacheBatch.Write) = .empty;
    var deletes: std.ArrayList([]const u8) = .empty;
    var keys: std.ArrayList([32]u8) = .empty;
    var read = try root.db.core.store.beginReadTxn();
    defer read.abort();
    const old_usage = read.get(embedding_cache_bytes_key) catch "0";
    var usage = std.fmt.parseInt(usize, old_usage, 10) catch 0;
    lockCache(&root.embedding_cache_mutex);
    {
        defer root.embedding_cache_mutex.unlock();
        var it = root.embedding_cache_pending.iterator();
        while (it.next()) |entry| {
            const key = try embeddingCacheKey(a, entry.key_ptr.*);
            const old = read.get(key) catch "";
            usage -|= if (old.len > 0) old.len + key.len else 0;
            usage +|= entry.value_ptr.*.len + key.len;
            try writes.append(a, .{ .key = key, .value = try a.dupe(u8, entry.value_ptr.*) });
            try keys.append(a, entry.key_ptr.*);
        }
    }
    // Bound the complete shared cache, independent of mention/document/table
    // count. Eviction touches only cache keys; live references may recompute.
    var cursor = try read.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(embedding_cache_prefix);
    const limit = if (@import("builtin").is_test) embedding_cache_limit_for_test orelse embedding_cache_limit else embedding_cache_limit;
    while (usage > limit) {
        const kv = row orelse break;
        if (!std.mem.startsWith(u8, kv.key, embedding_cache_prefix)) break;
        var replaced = false;
        for (writes.items) |write| if (std.mem.eql(u8, write.key, kv.key)) {
            replaced = true;
            break;
        };
        if (!replaced) {
            try deletes.append(a, try a.dupe(u8, kv.key));
            usage -|= kv.key.len + kv.value.len;
        }
        row = try cursor.next();
    }
    while (usage > limit and writes.items.len > 0) {
        const dropped = writes.pop().?;
        usage -|= dropped.key.len + dropped.value.len;
        if (read.get(dropped.key)) |old| {
            try deletes.append(a, dropped.key);
            _ = old;
        } else |_| {}
    }
    try writes.append(a, .{ .key = embedding_cache_bytes_key, .value = try std.fmt.allocPrint(a, "{d}", .{usage}) });
    state.writes = writes.items;
    state.deletes = deletes.items;
    state.keys = keys.items;
    return .{ .ptr = state, .writes = state.writes, .deletes = state.deletes, .finish = PreparedEmbeddingCache.finish };
}

fn embeddingCachePrepare(ptr: *anyopaque, _: std.mem.Allocator, store: *anyopaque) anyerror!?callbacks.CandidateSource.CacheBatch {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    if (store != @as(*anyopaque, @ptrCast(root.db.core.store))) return null;
    lockCache(&root.embedding_cache_mutex);
    const pending = root.embedding_cache_pending.count();
    root.embedding_cache_mutex.unlock();
    if (pending == 0) return null;
    return try prepareEmbeddingCache(root);
}

fn embeddingCacheFlush(ptr: *anyopaque) anyerror!void {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    lockCache(&root.embedding_cache_mutex);
    const pending = root.embedding_cache_pending.count();
    root.embedding_cache_mutex.unlock();
    if (pending == 0) return;
    const batch = try prepareEmbeddingCache(root);
    var committed = false;
    defer batch.finish(batch.ptr, committed);
    var txn = try root.db.core.store.beginWriteTxn();
    errdefer txn.abort();
    for (batch.deletes) |key| try txn.delete(key);
    for (batch.writes) |write| try txn.put(write.key, write.value);
    try txn.commit();
    committed = true;
}

fn loadVector(ptr: *anyopaque, a: std.mem.Allocator, ref: @import("antfly_resolver").VectorReference) anyerror!?[]const f32 {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    const current = (try candidateRevisions(ptr, a, ref.table, ref.index_name, "")).candidates orelse return null;
    if (!std.mem.eql(u8, &current, &ref.index_revision)) return null;
    const database = try tables.get(root, ref.table);
    if (ref.vector_id) |id| {
        if ((try database.core.index_manager.lookupDenseVectorId(database.core.store, ref.index_name, ref.key)) != id) return null;
        const dense = database.core.index_manager.denseIndex(ref.index_name) orelse return null;
        var txn = try dense.index.beginReadTxn();
        defer txn.abort();
        const vector = try a.alloc(f32, ref.dimensions);
        const loaded = dense.index.getVectorInto(&txn, id, vector) catch {
            a.free(vector);
            return null;
        };
        if (loaded.len != ref.dimensions) {
            a.free(vector);
            return null;
        }
        return vector;
    }
    const raw = try candidateGet(ptr, a, ref.table, ref.key) orelse return null;
    defer a.free(raw);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const field = parsed.value.object.get(ref.field) orelse return null;
    if (field != .array or field.array.items.len != ref.dimensions) return null;
    const vector = try a.alloc(f32, ref.dimensions);
    for (field.array.items, vector) |value, *out| switch (value) {
        .float => |n| out.* = @floatCast(n),
        .integer => |n| out.* = @floatFromInt(n),
        else => {
            a.free(vector);
            return null;
        },
    };
    return vector;
}

const embedding_cache_cursor_key = "\x00antfly/cache/mention_embeddings/sweep_cursor";

/// Incremental expiration scans cache keys only, never documents/artifacts.
fn sweepEmbeddingCachePage(root: *h.Handle) !void {
    const now: i64 = @intCast(std.Io.Clock.real.now(h.handleLockIo()).nanoseconds);
    lockCache(&root.embedding_cache_commit_mutex);
    defer root.embedding_cache_commit_mutex.unlock();
    var read = try root.db.core.store.beginReadTxn();
    defer read.abort();
    const resume_key = read.get(embedding_cache_cursor_key) catch "";
    const stamp = read.get(embedding_cache_sweep_key) catch "0";
    const previous = std.fmt.parseInt(i64, stamp, 10) catch 0;
    if (resume_key.len == 0 and now >= previous and now - previous < 15 * std.time.ns_per_min) return;
    var usage = std.fmt.parseInt(usize, read.get(embedding_cache_bytes_key) catch "0", 10) catch 0;
    const prefix = "\x00antfly/cache/mention_embeddings/v";
    var cursor = try read.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(if (resume_key.len == 0) prefix else resume_key);
    if (row != null and resume_key.len != 0 and std.mem.eql(u8, row.?.key, resume_key)) row = try cursor.next();
    var write = try root.db.core.store.beginWriteTxn();
    errdefer write.abort();
    var count: usize = 0;
    var last: []const u8 = "";
    defer root.alloc.free(last);
    while (row) |kv| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, prefix)) {
            row = null;
            break;
        }
        if (count == 256) break;
        count += 1;
        const owned_key = try root.alloc.dupe(u8, kv.key);
        root.alloc.free(last);
        last = owned_key;
        const current = std.mem.startsWith(u8, kv.key, embedding_cache_prefix);
        const expired = !current or kv.value.len < 48 or !std.mem.eql(u8, kv.value[0..4], "ECF2") or
            now -| std.mem.readInt(i64, kv.value[40..48], .little) > embedding_cache_retention_ns;
        if (expired) {
            try write.delete(kv.key);
            if (current) usage -|= kv.key.len + kv.value.len;
        }
    }
    if (row == null) {
        try write.delete(embedding_cache_cursor_key);
        const text = try std.fmt.allocPrint(root.alloc, "{d}", .{now});
        defer root.alloc.free(text);
        try write.put(embedding_cache_sweep_key, text);
    } else try write.put(embedding_cache_cursor_key, last);
    const bytes = try std.fmt.allocPrint(root.alloc, "{d}", .{usage});
    defer root.alloc.free(bytes);
    try write.put(embedding_cache_bytes_key, bytes);
    try write.commit();
}

/// Opportunistic periodic mark/sweep. Artifacts own references, never cache
/// entries; races with an unfinished artifact only cause harmless recomputation.
pub fn collectEmbeddingCache(root: *h.Handle, force: bool) !void {
    if (!h.liteOpenModeCanWrite(root.open_mode)) return;
    if (!force) return sweepEmbeddingCachePage(root);
    try embeddingCacheFlush(root);
    lockCache(&root.embedding_cache_commit_mutex);
    defer root.embedding_cache_commit_mutex.unlock();
    const now = std.math.cast(i64, std.Io.Clock.real.now(h.handleLockIo()).nanoseconds) orelse return;
    if (!force) {
        var read = try root.db.core.store.beginReadTxn();
        defer read.abort();
        const last = read.get(embedding_cache_sweep_key) catch |err| switch (err) {
            error.NotFound => "0",
            else => return err,
        };
        const timestamp = std.fmt.parseInt(i64, last, 10) catch 0;
        if (now >= timestamp and now - timestamp < 15 * std.time.ns_per_min) return;
    }
    var live: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer live.deinit(root.alloc);
    const Mark = struct {
        fn table(a: std.mem.Allocator, database: *h.db_mod.DB, keys: *std.AutoHashMapUnmanaged([32]u8, void)) !void {
            var read = try database.core.store.beginReadTxn();
            defer read.abort();
            var cursor = try read.openCursor();
            defer cursor.close();
            var row = try cursor.seekAtOrAfter("");
            while (row) |kv| : (row = try cursor.next()) {
                if (!@import("../storage/internal_keys.zig").isResolutionArtifactKey(kv.key)) continue;
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const parsed = std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), kv.value, .{}) catch continue;
                if (parsed != .object) continue;
                const work = parsed.object.get("_resolution_work") orelse continue;
                if (work != .object) continue;
                const mentions = work.object.get("mentions") orelse continue;
                if (mentions != .array) continue;
                for (mentions.array.items) |mention| {
                    if (mention != .object) continue;
                    const value = mention.object.get("embedding_key") orelse continue;
                    const key = std.json.parseFromValueLeaky(?[32]u8, arena.allocator(), value, .{}) catch continue;
                    if (key) |digest| try keys.put(a, digest, {});
                }
            }
        }
    };
    try Mark.table(root.alloc, &root.db, &live);
    var tables_it = root.embedded_tables.valueIterator();
    while (tables_it.next()) |table| try Mark.table(root.alloc, &table.*.db, &live);
    var read = try root.db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var write = try root.db.core.store.beginWriteTxn();
    errdefer write.abort();
    var deleted: usize = 0;
    var retained_bytes: usize = 0;
    var row = try cursor.seekAtOrAfter(embedding_cache_prefix);
    while (row) |kv| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, embedding_cache_prefix)) break;
        var digest: [32]u8 = undefined;
        const parsed = if (kv.key.len == embedding_cache_prefix.len + 64) std.fmt.hexToBytes(&digest, kv.key[embedding_cache_prefix.len..]) catch null else null;
        if (parsed != null and live.contains(digest)) {
            retained_bytes += kv.key.len + kv.value.len;
            continue;
        }
        try write.delete(kv.key);
        deleted += 1;
        if (deleted % 1024 == 0) {
            try write.commit();
            write = try root.db.core.store.beginWriteTxn();
        }
    }
    const stamp = try std.fmt.allocPrint(root.alloc, "{d}", .{now});
    defer root.alloc.free(stamp);
    try write.put(embedding_cache_sweep_key, stamp);
    const usage = try std.fmt.allocPrint(root.alloc, "{d}", .{retained_bytes});
    defer root.alloc.free(usage);
    try write.put(embedding_cache_bytes_key, usage);
    try write.commit();
}

fn getDescription(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, artifact: []const u8) anyerror!?[]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const database = try tables.get(handle, table);
    const field = try std.fmt.allocPrint(a, "_artifacts.{s}", .{artifact});
    defer a.free(field);
    var row = (try database.lookup(a, key, .{ .fields = &.{field}, .include_all_fields = false })) orelse return null;
    defer row.deinit(a);
    return @import("../functions/resolver_adjudication.zig").projectedDescription(a, row.json, artifact);
}

fn adjudicationProducer(handle: *h.Handle) !@import("../storage/db/enrichment/asset_producer.zig").Producer {
    const database = handle.database();
    const io = database.backend_runtime.io() orelse return error.MissingAdjudicator;
    handle.query_producer_mutex.lockUncancelable(io);
    const cached = if (handle.query_producer) |runtime| runtime.producer() else null;
    handle.query_producer_mutex.unlock(io);
    if (cached) |producer| return producer;
    if (database.enrichment_runtime) |runtime| if (runtime.config.asset_producer) |producer| return producer;
    handle.query_producer_mutex.lockUncancelable(io);
    defer handle.query_producer_mutex.unlock(io);
    if (handle.query_producer == null) handle.query_producer = try @import("../asset_producer_runtime.zig").Runtime.createOwned(handle.alloc, io, .{ .antfly_provider = handle.liteAntflyProvider() });
    return handle.query_producer.?.producer();
}

fn adjudicate(ptr: *anyopaque, a: std.mem.Allocator, request: @import("antfly_resolver").adjudication.Request) anyerror![]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const io = handle.database().backend_runtime.io() orelse return error.MissingAdjudicator;
    const deadline = @import("antfly_platform").time.monotonicNs() + @as(u64, request.timeout_ms) * std.time.ns_per_ms;
    const producer = try adjudicationProducer(handle);
    return producer.withInvocationContext(.{ .io = io, .deadline_ns = deadline }).produce(a, .{ .producer_type = .decision, .config_json = request.specification_json, .source_text = request.input, .source_fingerprint = request.source_fingerprint });
}

fn adjudicatorModelRevision(ptr: *anyopaque, a: std.mem.Allocator, specification: []const u8, timeout_ms: u32) anyerror!?[32]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    return @import("../functions/resolver_adjudication.zig").modelRevision(a, try adjudicationProducer(handle), .{ .io = handle.database().backend_runtime.io() orelse return error.MissingAdjudicator }, specification, timeout_ms);
}

fn adjudicateBatch(ptr: *anyopaque, a: std.mem.Allocator, requests: []const @import("antfly_resolver").adjudication.Request) anyerror![]@import("antfly_resolver").adjudication.Result {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const io = handle.database().backend_runtime.io() orelse return error.MissingAdjudicator;
    const producer = try adjudicationProducer(handle);
    return @import("../functions/resolver_adjudication.zig").executeBatch(a, producer, .{ .io = io }, requests);
}

fn embedMentions(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, embedding_name: []const u8, texts: []const []const u8, dims: u32) ![]const []const f32 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const database = try tables.get(handle, table);
    if (database.resolution_embedder) |embedder| return embedder.embedDenseBatch(a, embedding_name, texts, dims);
    if (database.enrichment_runtime) |runtime| if (runtime.config.dense_embedder) |embedder| return embedder.embedDenseBatch(a, embedding_name, texts, dims);
    const config = try @import("db.zig").liteMergedIndexesForDatabaseJsonAlloc(handle.alloc, database);
    defer handle.alloc.free(config);
    var embedder = try h.managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(handle.alloc, config, .{ .antfly_provider = handle.liteAntflyProvider(), .io = database.backend_runtime.io(), .source_table = table });
    defer embedder.deinit();
    return embedder.denseInterface().embedDenseBatch(a, embedding_name, texts, dims);
}

fn embedMention(ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const database = try tables.get(handle, table);
    // The destination owns the canonical embedding space. Reuse its configured
    // provider for mention generation too, including injected runtime providers.
    if (database.resolution_embedder) |embedder|
        return embedder.embedDense(allocator, embedding_name, text, dims);
    if (database.enrichment_runtime) |runtime| if (runtime.config.dense_embedder) |embedder|
        return embedder.embedDense(allocator, embedding_name, text, dims);
    const config = try @import("db.zig").liteMergedIndexesForDatabaseJsonAlloc(handle.alloc, database);
    defer handle.alloc.free(config);
    var embedder = try h.managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(handle.alloc, config, .{ .antfly_provider = handle.liteAntflyProvider(), .io = database.backend_runtime.io(), .source_table = table });
    defer embedder.deinit();
    return embedder.denseInterface().embedDense(allocator, embedding_name, text, dims);
}

fn upsert(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, json: []const u8) anyerror!void {
    return upsertBatch(ptr, a, &.{.{ .table = table, .key = key, .doc_json = json }});
}

const Row = struct {
    key: []const u8,
    json: ?[]const u8,
    version: u64,
    digest: ?[32]u8,
};
const Participant = struct {
    name: []const u8,
    db: *h.db_mod.DB,
    rows: std.StringArrayHashMapUnmanaged(Row) = .empty,
};

fn merge(a: std.mem.Allocator, current: ?[]const u8, incoming: []const u8, key: []const u8) ![]const u8 {
    return @import("../storage/db/entity_alias_curation.zig").merge(a, current, incoming, key);
}

fn supportsCuration(_: *anyopaque) anyerror!bool {
    return true;
}

fn upsertBatch(ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const callbacks.EntityUpsert) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    if (!h.liteOpenModeCanWrite(handle.open_mode)) return error.ReadOnly;
    if (entries.len == 0) return;
    const admission = if (handle.owned_lite_backend) |backend|
        if (backend.native_docstore) |store| store.writer_admission else null
    else
        null;
    // A detached promoter must retain the database fence across the entire
    // local two-phase commit. Between individual native writes, a refreshed
    // coordinator could otherwise mistake its active preparation for an
    // abandoned transaction and recover/abort it. Nested writes borrow this
    // admission; extraction and candidate computation have already finished.
    if (admission) |guard| try guard.enter(guard.ptr, .primary);
    defer if (admission) |guard| guard.leave(guard.ptr);
    // Source-table workers share one destination coordinator. Serialize only
    // canonical read/merge/commit, never extraction, embedding or candidate work.
    const coordinator = @import("sql_commit.zig").acquire(handle);
    defer coordinator.release();
    try coordinator.recover();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var participants: std.StringArrayHashMapUnmanaged(Participant) = .empty;
    // Resolve every destination before publishing anything. Missing tables
    // leave the source promotion receipt pending and can be created later.
    for (entries) |entry| {
        if (entry.storage_table) |physical| {
            if (!std.mem.eql(u8, physical, (try boundTable(ptr, entry.table)).?)) return error.EntityPromotionUnavailable;
        }
        const slot = try participants.getOrPut(a, entry.table);
        if (!slot.found_existing) slot.value_ptr.* = .{ .name = entry.table, .db = try tables.get(handle, entry.table) };
        const participant = slot.value_ptr;
        const row = try participant.rows.getOrPut(a, entry.key);
        if (!row.found_existing) {
            const old = try participant.db.lookup(a, entry.key, .{ .include_primary_digest = true });
            row.value_ptr.* = .{ .key = entry.key, .json = if (old) |value| value.json else null, .version = if (old) |value| value.version orelse 0 else 0, .digest = if (old) |value| value.expected_content_digest else null };
        }
        if (entry.skip_if_missing and row.value_ptr.json == null) continue;
        row.value_ptr.json = if (entry.delete) null else try merge(a, row.value_ptr.json, entry.doc_json, entry.key);
    }
    const requests = try a.alloc(contract.TableCommitRequest, participants.count());
    for (participants.values(), requests) |*participant, *request| {
        var writes: std.ArrayList(types.TransactionWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
        for (participant.rows.values()) |row| {
            if (row.json) |json| try writes.append(a, .{ .key = row.key, .value = json }) else try deletes.append(a, row.key);
            try predicates.append(a, .{ .key = row.key, .expected_version = row.version, .expected_content_digest = row.digest });
        }
        request.* = .{ .table_name = participant.name, .writes = writes.items, .deletes = deletes.items, .predicates = predicates.items };
    }
    var transaction: ?types.TxnId = null;
    const outcome = try coordinator.commit(requests, &transaction);
    if (outcome != .committed) return error.EntityPromotionUnavailable;
}

/// Promotion can enqueue enrichments in any table, including one already
/// drained earlier in the sweep. Repeat until a complete sweep creates no
/// new replay work; a recursive taxonomy that does not converge fails boundedly.
pub fn runUntilIdle(handle: *h.Handle) !void {
    const root = handle.parent_handle orelse handle;
    if (root.embedded_path == null) return drainDatabase(root, handle.database());
    try tables.loadAvailable(root);
    for (0..1024) |_| {
        const before = replaySequence(root);
        try drainDatabase(root, &root.db);
        var iterator = root.embedded_tables.valueIterator();
        while (iterator.next()) |table| try drainDatabase(root, &table.*.db);
        if (before == replaySequence(root)) {
            collectEmbeddingCache(root, false) catch {};
            return;
        }
    }
    return error.RunUntilIdleDidNotConverge;
}

fn drainDatabase(root: *h.Handle, database: *h.db_mod.DB) !void {
    var arena = std.heap.ArenaAllocator.init(root.alloc);
    defer arena.deinit();
    // Manual maintenance uses the same one-projection-per-turn pull as the
    // native owner loop. A drain is explicitly allowed to repeat these turns.
    for (0..4096) |_| {
        if (!try @import("resolver_authority.zig").pullProjection(root, database, arena.allocator())) break;
    } else return error.RunUntilIdleDidNotConverge;
    if (root.lite_connection_context) |connection|
        return database.runUntilIdleWithGroupSourceState(try connection.owner.groupSourceState(database));
    return database.runUntilIdle();
}

fn replaySequence(handle: *h.Handle) u128 {
    var sequence: u128 = handle.db.core.nextDerivedSequence();
    sequence += handle.db.core.nextEnrichmentSequence();
    var iterator = handle.embedded_tables.valueIterator();
    while (iterator.next()) |table| {
        sequence += table.*.db.core.nextDerivedSequence();
        sequence += table.*.db.core.nextEnrichmentSequence();
    }
    return sequence;
}

fn boundTable(ptr: *anyopaque, name: []const u8) anyerror!?[]const u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    _ = try tables.get(handle, name);
    if (std.mem.eql(u8, name, "default")) return "embedded/tables/1";
    return handle.embedded_tables.get(name).?.namespace;
}

fn candidateGet(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, key: []const u8) anyerror!?[]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    const row = (try db.lookup(a, key, .{})) orelse return null;
    return row.json;
}

fn candidateScan(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, prefix: []const u8, options: callbacks.CandidateSource.ScanOptions, ctx: *anyopaque, consume: callbacks.CandidateSource.Consume) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    const upper = try a.dupe(u8, prefix);
    defer a.free(upper);
    var end = upper.len;
    while (end > 0 and upper[end - 1] == 255) end -= 1;
    if (end > 0) upper[end - 1] += 1;
    var rows = try db.scan(a, prefix, if (end == 0) "" else upper[0..end], .{ .include_documents = true, .limit = @intCast(@min(options.limit, std.math.maxInt(u32))) });
    defer rows.deinit(a);
    for (rows.documents) |row| {
        if (!std.mem.startsWith(u8, row.id, prefix)) continue;
        try consume(ctx, row.id, row.json);
    }
}

fn candidateNearest(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, query: callbacks.CandidateSource.NearestQuery, ctx: *anyopaque, consume: callbacks.CandidateSource.Consume) anyerror!void {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    const db = try tables.get(handle, name);
    var result = try db.search(a, .{ .index_name = query.index_name, .query = .{ .dense_knn = .{ .vector = query.embedding, .k = @intCast(query.k) } }, .limit = @intCast(query.k), .include_stored = true, .fields = query.fields, .include_all_fields = query.fields.len == 0 });
    defer result.deinit();
    if (query.vector_fields.len == 0) {
        for (result.hits) |hit| if (hit.stored_data) |json| try consume(ctx, hit.id, json);
        return;
    }
    const dense = db.core.index_manager.denseIndex(query.index_name) orelse return error.IndexNotFound;
    var txn = try dense.index.beginReadTxn();
    defer txn.abort();
    const scratch = try a.alloc(f32, query.embedding.len);
    defer a.free(scratch);
    for (result.hits) |hit| if (hit.stored_data) |json| {
        // Generated vectors live in the index, not in the primary JSON.
        // Scorer-selected candidate fields receive the canonical vector that
        // ANN used, including after a reopen.
        const vector_id = try db.core.index_manager.lookupDenseVectorId(db.core.store, query.index_name, hit.id) orelse {
            // A candidate may disappear between ANN and hydration. Keep the
            // remaining candidates and let missing-value scoring handle it.
            try consume(ctx, hit.id, json);
            continue;
        };
        const vector = dense.index.getVectorInto(&txn, vector_id, scratch) catch |err| switch (err) {
            error.NotFound, error.VectorNotFound => {
                try consume(ctx, hit.id, json);
                continue;
            },
            else => return err,
        };
        const hydrated = try hydrateCandidate(a, json, query.vector_fields, vector);
        defer if (hydrated) |bytes| a.free(bytes);
        const revision = if (query.cache_vectors) (try candidateRevisions(ptr, a, name, query.index_name, "")).candidates else null;
        if (revision) |epoch| {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, hydrated orelse json, .{});
            defer parsed.deinit();
            const arena = parsed.arena.allocator();
            const original = try std.json.parseFromSliceLeaky(std.json.Value, arena, json, .{});
            var refs: std.ArrayList(@import("antfly_resolver").VectorReference) = .empty;
            for (query.vector_fields) |field| if (parsed.value.object.get(field)) |value| {
                if (value != .array) continue;
                try refs.append(arena, .{ .field = field, .table = name, .index_name = query.index_name, .key = hit.id, .vector_id = if (original.object.contains(field)) null else vector_id, .index_revision = epoch, .dimensions = value.array.items.len });
            };
            const wire = try std.json.Stringify.valueAlloc(arena, refs.items, .{});
            try parsed.value.object.put(arena, "_resolution_vector_refs", try std.json.parseFromSliceLeaky(std.json.Value, arena, wire, .{}));
            const referenced = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
            defer a.free(referenced);
            try consume(ctx, hit.id, referenced);
        } else try consume(ctx, hit.id, hydrated orelse json);
    };
}

// Null means use the original bytes. Missing vectors remain missing values
// for the scorer; text-only scorers incur no parsing/serialization cost.
fn hydrateCandidate(a: std.mem.Allocator, json: []const u8, fields: []const []const u8, vector: ?[]const f32) !?[]u8 {
    if (fields.len == 0 or vector == null) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEntityPromotionDocument;
    const arena = parsed.arena.allocator();
    var values: std.array_list.Managed(std.json.Value) = .init(arena);
    for (vector.?) |value| try values.append(.{ .float = value });
    var changed = false;
    for (fields) |field| {
        if (parsed.value.object.contains(field)) continue;
        try parsed.value.object.put(arena, field, .{ .array = values });
        changed = true;
    }
    if (!changed) return null;
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

test "capi ANN hydration keeps missing vectors local and honors scorer fields" {
    const a = std.testing.allocator;
    // Even non-JSON bytes bypass parsing when the scorer needs no vector.
    try std.testing.expect((try hydrateCandidate(a, "unparsed", &.{}, &.{ 1, 0 })) == null);
    try std.testing.expect((try hydrateCandidate(a, "unparsed", &.{"canonical_vector"}, null)) == null);
    const hydrated = (try hydrateCandidate(a, "{\"canonical_name\":\"Reviewer\"}", &.{"canonical_vector"}, &.{ 1, 0 })).?;
    defer a.free(hydrated);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, hydrated, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("name_embedding") == null);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("canonical_vector").?.array.items.len);
    const stored = "{\"canonical_vector\":[0,1],\"other_vector\":null}";
    try std.testing.expect((try hydrateCandidate(a, stored, &.{ "canonical_vector", "other_vector" }, &.{ 1, 0 })) == null);
    const mixed = (try hydrateCandidate(a, stored, &.{ "canonical_vector", "missing_vector" }, &.{ 1, 0 })).?;
    defer a.free(mixed);
    const mixed_parsed = try std.json.parseFromSlice(std.json.Value, a, mixed, .{});
    defer mixed_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), mixed_parsed.value.object.get("canonical_vector").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 1), mixed_parsed.value.object.get("missing_vector").?.array.items[0].integer);
}

test "capi Lite promotion retains writer admission across coordinator and participant commits" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("promotion-admission");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    try @import("sql_commit.zig").recover(handle);
    const Admission = struct {
        handle: *h.Handle,
        depth: usize = 0,
        acquisitions: usize = 0,
        fn enter(ptr: *anyopaque, _: @import("../storage/lite/docstore.zig").PublicationScope) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.depth += 1;
            errdefer self.depth -= 1;
            if (self.depth != 1) return;
            self.acquisitions += 1;
            // A new path owner may recover this SQL record. Inject refusal
            // if the promoter drops admission while its commit is active;
            // nested writes under the retained fence remain admissible.
            if (self.handle.sql_commit_recovery_needed) return error.PromotionWriterFenceReleased;
        }
        fn leave(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.depth > 0);
            self.depth -= 1;
        }
    };
    var admission: Admission = .{ .handle = handle };
    const store = handle.owned_lite_backend.?.native_docstore.?;
    store.writer_admission = .{ .ptr = &admission, .enter = Admission.enter, .leave = Admission.leave };
    defer store.writer_admission = null;
    try handle.db.entity_sink.?.upsertBatch(a, &.{.{ .table = "entities", .key = "person/ada", .doc_json = "{\"canonical_name\":\"Ada\",\"aliases\":[\"Ada\"]}" }});
    try std.testing.expectEqual(@as(usize, 0), admission.depth);
    try std.testing.expectEqual(@as(usize, 1), admission.acquisitions);
    var row = (try (try tables.get(handle, "entities")).lookup(a, "person/ada", .{})) orelse return error.TestUnexpectedResult;
    defer row.deinit(a);
    try @import("antfly-json").testing.expectSubsetJsonText(a, "{\"canonical_name\":\"Ada\"}", row.json);
    try std.testing.expect(!handle.sql_commit_recovery_needed);
}

test "capi Lite promotion atomically merges across tables and survives reopen" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("capi");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    var handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    try tables.create(handle, "events", "{}", false);
    const sink = handle.db.entity_sink.?;
    const entries = [_]callbacks.EntityUpsert{
        .{ .table = "entities", .key = "person/ada", .doc_json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada\",\"aliases\":[\"Ada\"]}" },
        .{ .table = "events", .key = "event/write", .doc_json = "{\"entity_type\":\"event\",\"canonical_name\":\"Write\",\"aliases\":[\"Write\"]}" },
        .{ .table = "entities", .key = "person/ada", .doc_json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada Lovelace\",\"aliases\":[\"Ada Lovelace\"]}" },
    };
    try sink.upsertBatch(a, &entries);
    try sink.upsertBatch(a, &entries);
    try runUntilIdle(handle);
    const entities = try tables.get(handle, "entities");
    var row = (try entities.lookup(a, "person/ada", .{})).?;
    defer row.deinit(a);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, row.json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("Ada", parsed.value.object.get("canonical_name").?.string);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("aliases").?.array.items.len);
    var event = (try (try tables.get(handle, "events")).lookup(a, "event/write", .{})).?;
    event.deinit(a);
    const unavailable = [_]callbacks.EntityUpsert{
        .{ .table = "entities", .key = "person/missing", .doc_json = entries[0].doc_json },
        .{ .table = "absent", .key = "event/missing", .doc_json = entries[1].doc_json },
    };
    try std.testing.expectError(error.UndefinedTable, sink.upsertBatch(a, &unavailable));
    try std.testing.expect((try entities.lookup(a, "person/missing", .{})) == null);
    h.closeHandle(handle);
    handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, false, null, null);
    var reopened = (try (try tables.get(handle, "entities")).lookup(a, "person/ada", .{})).?;
    defer reopened.deinit(a);
    try std.testing.expectEqualStrings(row.json, reopened.json);
}

test "capi Lite promotion label routes extraction graphs into entity and event documents" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("promotion-graph");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    try tables.create(handle, "entities", "{}", false);
    try tables.create(handle, "events", "{}", false);
    const handle_id = try api.publishHandle(handle);
    defer api.antfly_db_close(handle_id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg_v1","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg_v1","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"metrics":{"ppr":{"kind":"pagerank","enabled":true,"damping":0.85}},"edge_types":[{"name":"mentions"},{"name":"works_at"},{"name":"participates_in"},{"name":"uses"}],"resolvers":[{"name":"events","table":"events","source_artifact":"kg_v1","resolution_artifact":"events_resolution_v1","key_template":"event/{{ slug _entity.text }}","labels":["event"],"config_generation":1},{"name":"entities","table":"entities","source_artifact":"kg_v1","resolution_artifact":"entities_resolution_v1","key_template":"entity/{{ slug _entity.text }}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice(config)));
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true });
    try handle.db.batch(.{ .writes = &.{.{ .key = "doc:a", .value =
        \\{"kg":{"entities":[{"id":"e0","label":"person","text":"Ada Lovelace"},{"id":"e1","label":"org","text":"Antfly"},{"id":"v0","label":"event","text":"Write program","predicate":"write"}],"relations":[{"type":"works_at","source":"e0","target":"e1"},{"type":"participates_in","source":"e0","target":"v0"},{"type":"uses","source":{"text":"Ada Lovelace"},"target":{"text":"Antfly"}},{"type":"uses","source":{"text":"missing"},"target":{"text":"Antfly"}}]}}
    }}, .sync_level = .write });
    try runUntilIdle(handle);
    var entity = (try (try tables.get(handle, "entities")).lookup(a, "entity/ada_lovelace", .{})) orelse return error.TestExpectedEqual;
    defer entity.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, entity.json, "Ada Lovelace") != null);
    var event = (try (try tables.get(handle, "events")).lookup(a, "event/write_program", .{})) orelse return error.TestExpectedEqual;
    defer event.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, event.json, "Write program") != null);
    try std.testing.expect((try (try tables.get(handle, "entities")).lookup(a, "entity/write_program", .{})) == null);
    try std.testing.expect(!handle.db.pendingWorkStats().promotion.catch_up_required);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const edges = try graph.getEdges(a, "entity/ada_lovelace", "uses", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("entity/antfly", edges[0].target);
    const document_edges = try graph.getEdges(a, "doc:a", "uses", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, document_edges);
    try std.testing.expectEqual(@as(usize, 0), document_edges.len);
    // Same numeric error code must never reuse another operation's detail.
    var child: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(handle_id, .fromSlice("entities"), &child));
    defer api.antfly_db_close(child);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"invalid\"}")));
    try std.testing.expectEqualStrings("MissingIndexType", std.mem.span(api.antfly_last_error_detail()));
    var session: u64 = 0;
    const sessions = @import("sql_session.zig");
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, sessions.antfly_db_sql_session_open(child, &session));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_last_error_code());
    const detail = std.mem.span(api.antfly_last_error_detail());
    try std.testing.expect(std.mem.indexOf(u8, detail, "antfly_db_sql_session_open") != null);
    try std.testing.expect(std.mem.indexOf(u8, detail, "MissingIndexType") == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_open(handle_id, &session));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_last_error_code());
    try std.testing.expectEqualStrings("", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sessions.antfly_db_sql_session_close(handle_id, session));
    var named_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&named_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(child, .fromSlice("{\"full_text_search\":{\"match_all\":{}},\"limit\":10}"), &named_response));
    const named_json = try std.json.parseFromSlice(std.json.Value, a, named_response.ptr.?[0..named_response.len], .{});
    defer named_json.deinit();
    try std.testing.expect(std.mem.indexOf(u8, named_response.ptr.?[0..named_response.len], "\"table\":\"entities\"") != null);
}

test "capi Lite promotion graph metrics flow through shared concepts in both directions" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("lite-undirected-ppr");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    const handle_id = try api.publishHandle(handle);
    defer api.antfly_db_close(handle_id);
    try handle.db.addIndex(.{ .name = "knowledge", .kind = .graph, .config_json =
        \\{"metrics":{"ppr":{"kind":"pagerank","enabled":true}},"edge_types":[{"name":"about","field":"about"}]}
    });
    try handle.db.addIndex(.{ .name = "claim_text", .kind = .full_text, .config_json = "{\"field\":\"search_text\"}" });
    try handle.db.batch(.{ .writes = &.{
        .{ .key = "claim:a", .value = "{\"about\":[\"concept/shared\"],\"search_text\":\"embedder\"}" },
        .{ .key = "claim:b", .value = "{\"about\":[\"concept/shared\"]}" },
        .{ .key = "claim:unrelated", .value = "{\"about\":[\"concept/other\"]}" },
    }, .sync_level = .write });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    var directed: [3]?f64 = undefined;
    var undirected: [3]?f64 = undefined;
    const keys = &.{ "concept/shared", "claim:a", "claim:b" };
    _ = try graph.personalizedPageRankColumnIntoWithControls(a, "ppr", &.{"concept/shared"}, null, keys, &directed, .{});
    _ = try graph.personalizedPageRankColumnIntoWithControls(a, "ppr", &.{"concept/shared"}, null, keys, &undirected, .{ .direction = .both });
    try std.testing.expectEqual(@as(f64, 0), directed[1].?);
    try std.testing.expect(undirected[1].? > 0);
    try std.testing.expectApproxEqAbs(undirected[1].?, undirected[2].?, 1e-10);
    var response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&response);
    const body =
        \\{"graph_metric":{"index":"knowledge","metric":"ppr","metric_freshness":"fresh","seed_nodes":["concept/shared"],"direction":"both","top_k":3}}
    ;
    try std.testing.expect(api.requestLooksLikePublicQueryJson(body));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(handle_id, .fromSlice(body), &response));
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "claim:a") != null);
    api.antfly_buffer_free(&response);
    const traversal =
        \\{"full_text_search":{"match":{"field":"search_text","text":"embedder"}},"limit":5,"graph_queries":{"q":{"index":"knowledge","traverse":{"start":{"result_ref":"$query_results","limit":5},"direction":"both","edge_types":["about"],"max_depth":2,"include_documents":true,"include_paths":true,"limit":20}}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(handle_id, .fromSlice(traversal), &response));
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "claim:b") != null);
    // Verbatim #1089 request: default deduplication, both directions, two hops,
    // document hydration and paths all survive the real C entry point.
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "\"path\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "\"document\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.ptr.?[0..response.len], "path") != null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"public_graph\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"supersedes\",\"field\":\"supersedes\"}]}")));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"invalid\"}")));
    try std.testing.expectEqualStrings("MissingIndexType", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(handle_id, .fromSlice("missing-document"), &response));
    try std.testing.expectEqualStrings("NotFound", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_last_error_code());
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"public_sparse\",\"type\":\"embeddings\",\"sparse\":true,\"external\":true,\"field\":\"sparse\"}")));
    try std.testing.expect(handle.database().core.index_manager.sparseIndex("public_sparse") != null);
    try std.testing.expect(handle.database().core.index_manager.denseIndex("public_sparse") == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(handle_id, .fromSlice("{\"name\":\"success\",\"type\":\"graph\"}")));
    try std.testing.expectEqualStrings("", std.mem.span(api.antfly_last_error_detail()));
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_search_json(null, .fromSlice(body), &response));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(api.antfly_last_error_detail()), "antfly_db_search_json") != null);
}

// The generator is deterministic so this exercises the full recursive replay
// topology without making the test depend on an external inference provider.
test "capi Lite promotion recursively generates a taxonomy in the destination table" {
    const api = @import("db.zig");
    const producer = @import("../storage/db/enrichment/asset_producer.zig");
    const a = std.testing.allocator;
    const Generator = struct {
        calls: usize = 0,
        saw_neighbors: bool = false,
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const producer.Request) !@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .data_uri, .fixed_bytes = 16 << 20, .allocator_limit_bytes = 16 << 20, .max_result_bytes_per_item = 8 << 20, .max_result_bytes = 8 << 20 };
        }
        fn produce(ptr: *anyopaque, alloc: std.mem.Allocator, request: producer.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.saw_neighbors = self.saw_neighbors or std.mem.indexOf(u8, request.source_text, "neighbors") != null;
            const source = request.source_text[0 .. std.mem.indexOfScalar(u8, request.source_text, '\n') orelse request.source_text.len];
            if (std.mem.eql(u8, source, "fail")) return error.UnsupportedGlinerBoundaryRuntime;
            const parent = if (std.mem.eql(u8, source, "Compiler")) "Software" else if (std.mem.eql(u8, source, "Software")) "Computing" else return alloc.dupe(u8, "{\"entities\":[],\"relations\":[]}");
            return std.fmt.allocPrint(alloc, "{{\"entities\":[{{\"id\":\"self\",\"label\":\"concept\",\"text\":{f}}},{{\"id\":\"parent\",\"label\":\"concept\",\"text\":{f}}}],\"relations\":[{{\"type\":\"is_a\",\"source\":\"self\",\"target\":\"parent\"}}]}}", .{ std.json.fmt(source, .{}), std.json.fmt(parent, .{}) });
        }
    };
    var generator: Generator = .{};
    var tmp = try api.TestDirectory.init("recursive-taxonomy");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    defer h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    const entities = try tables.get(handle, "entities");
    try entities.addIndex(.{ .name = "entity_context", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"context\",\"field\":\"context\"}]}" });
    try entities.addEnrichment(.{ .name = "taxonomy", .kind = .asset, .field = "canonical_name", .neighbor_context = .{ .graph_index = "entity_context", .edge_types = &.{"context"}, .direction = .out }, .content_type = "application/json", .producer_json = "{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}" });
    try entities.addIndex(.{ .name = "taxonomy_graph", .kind = .graph, .config_json =
        \\{"sources":[{"artifact":"taxonomy","format":"extraction_graph","mention_edge_type":"mentions"}],"edge_types":[{"name":"mentions"},{"name":"is_a"},{"name":"supersedes","field":"supersedes"}]}
    });
    try entities.addResolver(.{ .name = "parents", .table = "entities", .source_artifact = "taxonomy", .resolution_artifact = "parents_resolved", .key_template = "concept/{{ slug _entity.text }}", .config_generation = 1 });
    try entities.reconfigureEnrichmentRuntime(.{ .asset_producer = .{ .ptr = &generator, .vtable = &.{ .produce = Generator.produce, .invocation_memory_for_requests = Generator.memory } } });
    try entities.batch(.{ .graph_writes = &.{.{ .index_name = "entity_context", .source = "concept/compiler", .target = "concept/programming", .edge_type = "context" }} });
    try handle.db.entity_sink.?.upsert(a, "entities", "concept/compiler", "{\"entity_type\":\"concept\",\"canonical_name\":\"Compiler\",\"aliases\":[\"Compiler\"]}");
    try runUntilIdle(handle);
    for ([_][]const u8{ "concept/compiler", "concept/software", "concept/computing" }) |key| {
        var row = (try entities.lookup(a, key, .{})) orelse return error.TestExpectedEqual;
        row.deinit(a);
    }
    try std.testing.expect(generator.calls >= 3);
    try std.testing.expect(generator.saw_neighbors);
    try std.testing.expect(!entities.pendingWorkStats().promotion.catch_up_required);
    const before = generator.calls;
    try runUntilIdle(handle);
    try std.testing.expectEqual(before, generator.calls);
    const initial_graph = &@constCast(entities.core.graphIndex("taxonomy_graph").?).index;
    const initial_taxonomy = try initial_graph.getEdges(a, "concept/compiler", "is_a", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, initial_taxonomy);
    try std.testing.expect(initial_taxonomy.len > 0);
    // Field and artifact contributions share an index across updates/deletes.
    try entities.batch(.{ .writes = &.{.{ .key = "concept/compiler", .value = "{\"canonical_name\":\"Compiler\",\"supersedes\":[\"concept/old\"]}" }} });
    try runUntilIdle(handle);
    const graph = &@constCast(entities.core.graphIndex("taxonomy_graph").?).index;
    const field_edges = try graph.getEdges(a, "concept/compiler", "supersedes", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, field_edges);
    try std.testing.expectEqual(@as(usize, 1), field_edges.len);
    try entities.batch(.{ .writes = &.{.{ .key = "concept/compiler", .value = "{\"canonical_name\":\"Compiler\",\"supersedes\":[]}" }} });
    try runUntilIdle(handle);
    const cleared = try graph.getEdges(a, "concept/compiler", "supersedes", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, cleared);
    try std.testing.expectEqual(@as(usize, 0), cleared.len);
    const taxonomy = try graph.getEdges(a, "concept/compiler", "is_a", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, taxonomy);
    try std.testing.expect(taxonomy.len > 0);

    try entities.batch(.{ .writes = &.{.{ .key = "concept/failure", .value = "{\"canonical_name\":\"fail\"}" }} });
    runUntilIdle(handle) catch {};
    var stats = try api.pendingWorkStatsJson(handle);
    defer api.antfly_buffer_free(&stats);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, stats.ptr.?[0..stats.len], .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("tables").?.object.contains("entities"));
    const enrichment = parsed.value.object.get("enrichment").?.object;
    try std.testing.expect(enrichment.get("enabled").?.bool);
    try std.testing.expect(enrichment.get("error_count").?.integer > 0);
    try std.testing.expectEqualStrings("UnsupportedGlinerBoundaryRuntime", enrichment.get("last_error_code").?.string);
}

test {
    _ = @import("../storage/db/graph_endpoint_resolutions.zig");
}

test "capi Lite promotion resolves ANN and explicit aliases through stored entity tables" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("lite-ann-resolution");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("entities"), .fromSlice("{}")));
    const entities = try tables.get(handle, "entities");
    try entities.addIndex(.{ .name = "names", .kind = .dense_vector, .config_json = "{\"field\":\"canonical_vector\",\"dims\":3,\"embedding_name\":\"entity_names\",\"external\":true}" });
    // Exercise promotion -> generated embedding -> ANN visibility, with no
    // hand-written canonical row or vector. A deterministic provider makes
    // this production drain path run in ordinary CI without model downloads.
    const Embedder = struct {
        model_revision: u8 = 1,
        mention_calls: usize = 0,
        entity_calls: usize = 0,
        forbid_model_load: bool = false,
        fn revision(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?[32]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as([32]u8, @splat(self.model_revision));
        }
        fn embed(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, _: []const u8, dims: u32) anyerror![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.forbid_model_load) return error.UnexpectedModelLoad;
            if (std.mem.eql(u8, name, "mention_names")) self.mention_calls += 1 else self.entity_calls += 1;
            if (dims != 3) return error.InvalidArgument;
            return try alloc.dupe(f32, &.{ 1, 0, 0 });
        }
    };
    var embed_context: Embedder = .{};
    try entities.addEnrichment(.{ .name = "entity_names", .kind = .embedding, .field = "canonical_name", .vector_space = "names", .expected_dims = 3 });
    try entities.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = .{ .ptr = &embed_context, .dense_embed_fn = Embedder.embed, .cache_revision_fn = Embedder.revision } });
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ slug _entity.text }}","candidate_search":"ann","candidate_ann_index":"names","candidate_limit":1,"name_embedding":"mention_names","name_embedding_dims":3,"scorer_json":"{\"comparisons\":[{\"name\":\"embedding\",\"left\":\"name_embedding\",\"right\":\"canonical_embedding\",\"levels\":[{\"when\":\"cosine > 0.9\",\"weight\":8.0},{\"else\":true,\"weight\":-6.0}]}],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    const fake_embedder: @import("../storage/db/enrichment/embedder.zig").DenseEmbedder = .{ .ptr = &embed_context, .dense_embed_fn = Embedder.embed, .cache_revision_fn = Embedder.revision };
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = fake_embedder });
    handle.db.setResolutionEmbedder(fake_embedder);
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:seed", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Approval reviewer\"}]}}" }} });
    try runUntilIdle(handle);
    var promoted = (try entities.lookup(a, "concept/approval_reviewer", .{})).?;
    defer promoted.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, promoted.json, "Approval reviewer") != null);

    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:ann", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"},{\"id\":\"bad\",\"label\":\"concept\",\"text\":\"!!!\"}]}}" }} });
    try runUntilIdle(handle);
    const graph = &@constCast(handle.db.core.graphIndex("knowledge").?).index;
    const edges = try graph.getEdges(a, "claim:ann", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", edges[0].target);
    try std.testing.expect((try entities.lookup(a, "concept/laya_reviewer", .{})) == null);
    const before_duplicate = embed_context.mention_calls;
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:duplicate", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"}]}}" }} });
    try runUntilIdle(handle);
    try std.testing.expectEqual(before_duplicate, embed_context.mention_calls);
    try tables.create(handle, "second_source", "{}", false);
    var second_id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("second_source"), &second_id));
    defer api.antfly_db_close(second_id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(second_id, .fromSlice(config)));
    const second = try tables.get(handle, "second_source");
    try second.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = fake_embedder });
    second.setResolutionEmbedder(fake_embedder);
    try second.batch(.{ .writes = &.{.{ .key = "claim:other_table", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"}]}}" }} });
    try runUntilIdle(handle);
    try std.testing.expectEqual(before_duplicate, embed_context.mention_calls);
    const private_key = try @import("../storage/internal_keys.zig").resolutionArtifactKeyAlloc(a, "claim:duplicate", "resolved");
    defer a.free(private_key);
    const private_bytes = try handle.db.core.store.get(a, private_key);
    defer a.free(private_bytes);
    var private_arena = std.heap.ArenaAllocator.init(a);
    defer private_arena.deinit();
    const private_json = try std.json.parseFromSliceLeaky(std.json.Value, private_arena.allocator(), private_bytes, .{});
    const work_json = private_json.object.get("_resolution_work").?;
    const mention = work_json.object.get("mentions").?.array.items[0];
    try std.testing.expect(mention.object.get("embedding").? == .null);
    const embedding_key = (try std.json.parseFromValueLeaky(?[32]u8, private_arena.allocator(), mention.object.get("embedding_key").?, .{})).?;
    try std.testing.expectEqual(@as(usize, 0), handle.embedding_cache_pending.count());
    const persisted_cache_key = try embeddingCacheKey(a, embedding_key);
    defer a.free(persisted_cache_key);
    const persisted_vector = try handle.db.core.store.get(a, persisted_cache_key);
    defer a.free(persisted_vector);
    try std.testing.expect(std.mem.startsWith(u8, persisted_vector, "ECF2"));
    const wire_candidate = work_json.object.get("candidates").?.array.items[0];
    const refs = try std.json.parseFromValueLeaky([]const @import("antfly_resolver").VectorReference, private_arena.allocator(), wire_candidate.object.get("vector_refs").?, .{});
    try std.testing.expect(refs.len > 0 and refs[0].vector_id != null);
    for (wire_candidate.object.get("record").?.object.get("fields").?.array.items) |field| {
        if (std.mem.eql(u8, field.object.get("name").?.string, "canonical_embedding")) try std.testing.expect(field.object.get("value").?.object.contains("none"));
    }
    var current_ref = refs[0];
    current_ref.index_revision = (try candidateRevisions(handle, a, "entities", "names", "")).candidates.?;
    const loaded_vector = (try loadVector(handle, a, current_ref)).?;
    defer a.free(loaded_vector);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0, 0 }, loaded_vector);
    var stale_ref = current_ref;
    stale_ref.index_revision[0] ^= 1;
    try std.testing.expect((try loadVector(handle, a, stale_ref)) == null);
    stale_ref = current_ref;
    stale_ref.vector_id = std.math.maxInt(u64);
    try std.testing.expect((try loadVector(handle, a, stale_ref)) == null);
    const orphan: [32]u8 = @splat(255);
    try embeddingCachePut(handle, orphan, &.{ 1, 0, 0 });
    try collectEmbeddingCache(handle, true);
    try std.testing.expect((try embeddingCacheGet(handle, a, orphan)) == null);
    const retained_embedding = (try embeddingCacheGet(handle, a, embedding_key)).?;
    defer a.free(retained_embedding);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0, 0 }, retained_embedding);

    // Public query resolution uses the stored ANN index and custom scorer
    // vector field, but must not publish a query document or any derived work.
    const sequence = handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    var query_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice(
        "{\"extraction\":{\"entities\":[{\"id\":\"q0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"},{\"id\":\"bad\",\"label\":\"concept\",\"text\":\"!!!\"}]}}",
    ), &query_result));
    const query_json = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer query_json.deinit();
    const query_resolution = query_json.value.object.get("resolution").?.object;
    try std.testing.expect(!query_resolution.contains("_resolution_work"));
    const resolved = query_resolution.get("entities").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), resolved.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", resolved[0].object.get("doc_ref").?.object.get("key").?.string);
    try std.testing.expectEqualStrings("entities", resolved[0].object.get("doc_ref").?.object.get("table").?.string);
    try std.testing.expectEqualStrings("match", resolved[0].object.get("decision").?.string);
    try std.testing.expectEqual(@as(i64, 1), query_resolution.get("skipped_mentions").?.integer);
    try std.testing.expectEqual(sequence, handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    // A copy-only artifact cannot silently masquerade as a text extractor.
    var invalid_result: h.capi.Buffer = .{};
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"A reviewer\"}"), &invalid_result));
    try std.testing.expectEqual(@as(usize, 0), invalid_result.len);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"x\",\"extraction\":{\"entities\":[]}}"), &invalid_result));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_resolve_query_json(id, .fromSlice("missing"), .fromSlice("{\"extraction\":{\"entities\":[]}}"), &invalid_result));
    try std.testing.expect(embed_context.mention_calls >= 3);
    try std.testing.expect(embed_context.entity_calls >= 1);
    // No vector on the alias row: ANN cannot return it as a candidate.
    try handle.db.entity_sink.?.upsert(a, "entities", "concept/the_decider", "{\"entity_type\":\"concept\",\"canonical_name\":\"The decider\",\"merged_into\":\"concept/approval_reviewer\"}");
    try handle.db.batch(.{ .writes = &.{.{ .key = "claim:alias", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"The decider\"}]}}" }} });
    try runUntilIdle(handle);
    const alias_edges = try graph.getEdges(a, "claim:alias", "mentions", .out);
    defer @import("../graph/graph.zig").GraphIndex.freeEdges(a, alias_edges);
    try std.testing.expectEqual(@as(usize, 1), alias_edges.len);
    try std.testing.expectEqualStrings("concept/approval_reviewer", alias_edges[0].target);

    // Reapply a public inline config with a changed scorer. A real config
    // replay must reuse mention embeddings, including across canonical writes.
    const before_replay = embed_context.mention_calls;
    const before_entity_replay = embed_context.entity_calls;
    embed_context.forbid_model_load = true;
    var updated = try std.json.parseFromSlice(std.json.Value, a, config, .{});
    defer updated.deinit();
    try updated.value.object.put(a, "name", .{ .string = "knowledge_updated" });
    const scorer_value = updated.value.object.get("resolvers").?.array.items[0].object.getPtr("scorer_json").?;
    const scorer = try std.mem.replaceOwned(u8, a, scorer_value.string, "0.9", "0.8");
    defer a.free(scorer);
    scorer_value.* = .{ .string = scorer };
    const new_config = try std.json.Stringify.valueAlloc(a, updated.value, .{});
    defer a.free(new_config);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(new_config)));
    try runUntilIdle(handle);
    try std.testing.expectEqual(before_replay, embed_context.mention_calls);
    try std.testing.expectEqual(before_entity_replay, embed_context.entity_calls);
    embed_context.forbid_model_load = false;
    // A candidate/index revision changes independently of the embedding model.
    const before_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    try entities.batch(.{ .writes = &.{.{ .key = "concept/new", .value = "{\"canonical_name\":\"New candidate\"}" }} });
    const after_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    try std.testing.expect(!std.mem.eql(u8, &before_revision.candidates.?, &after_revision.candidates.?));
    try std.testing.expectEqualSlices(u8, &before_revision.embedding.?, &after_revision.embedding.?);

    try entities.batch(.{ .deletes = &.{"concept/new"} });
    const deleted_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    try std.testing.expect(!std.mem.eql(u8, &after_revision.candidates.?, &deleted_revision.candidates.?));
    const compacted = try handle.owned_lite_backend.?.vacuum();
    _ = compacted;
    const compacted_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    // Compaction cannot roll back the deletion fence or resurrect candidates.
    try std.testing.expectEqualSlices(u8, &deleted_revision.candidates.?, &compacted_revision.candidates.?);
    const absent = try entities.get(a, "concept/new");
    try std.testing.expect(absent == null);
    embed_context.model_revision = 2;
    const upgraded_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    try std.testing.expect(!std.mem.eql(u8, &deleted_revision.embedding.?, &upgraded_revision.embedding.?));
    embed_context.model_revision = 1;
    // Logical TTL visibility may change before physical cleanup appends a
    // replay record. Until cleanup, a candidate snapshot cannot be reused.
    try entities.setSchema(.{ .version = 1, .ttl_duration_ns = std.time.ns_per_s });
    const expiring_revision = try candidateRevisions(handle, a, "entities", "names", "mention_names");
    try std.testing.expect(expiring_revision.candidates == null);
    try std.testing.expect(expiring_revision.embedding != null);
    try entities.setSchema(.{ .version = 2, .ttl_duration_ns = 0 });

    // The cache shares the artifact's portable lifetime. Exercise export ->
    // import -> close -> reopen before the next real scorer change.
    try runUntilIdle(handle);
    {
        const runtime = handle.db.resolution_runtime.?;
        try std.testing.expect(runtime.catch_up_mutex.tryLock());
        defer runtime.catch_up_mutex.unlock();
        var deleted: bool = true;
        try std.testing.expectEqual(h.capi.ErrorCode.busy, api.antfly_db_delete_index(id, .fromSlice("knowledge"), &deleted));
        try std.testing.expect(!deleted);
        try std.testing.expect(handle.db.core.index_manager.get("knowledge") != null);
    }
    const resolution_key = try @import("../storage/internal_keys.zig").resolutionArtifactKeyAlloc(a, "claim:ann", "resolved");
    defer a.free(resolution_key);
    const cached = try handle.db.core.store.get(a, resolution_key);
    defer a.free(cached);
    var backup: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&backup);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_backup(id, &backup));
    const destination_path = try std.fmt.allocPrint(a, "{s}.restored.aflite", .{tmp.path()});
    defer a.free(destination_path);
    const destination = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, destination_path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const destination_id = try api.publishHandle(destination);
    {
        defer api.antfly_db_close(destination_id);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_import_backup(destination_id, .{ .ptr = backup.ptr, .len = backup.len }));
        const imported = try destination.db.core.store.get(a, resolution_key);
        defer a.free(imported);
        try std.testing.expectEqualStrings(cached, imported);
    }
    const reopened = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, destination_path, .{ .profile = .native, .externally_locked = true }, false, null, null);
    const reopened_id = try api.publishHandle(reopened);
    defer api.antfly_db_close(reopened_id);
    const reopened_cache = try reopened.db.core.store.get(a, resolution_key);
    defer a.free(reopened_cache);
    try std.testing.expectEqualStrings(cached, reopened_cache);
    const reopened_entities = try tables.get(reopened, "entities");
    try reopened_entities.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = fake_embedder });
    try reopened.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .dense_embedder = fake_embedder });
    reopened.db.setResolutionEmbedder(fake_embedder);
    // Explicitly install the same provider on the destination used by mentions.
    reopened_entities.setResolutionEmbedder(fake_embedder);
    const before_reopen_replay = embed_context.mention_calls;
    try updated.value.object.put(a, "name", .{ .string = "knowledge_restored" });
    const restored_scorer = try std.mem.replaceOwned(u8, a, scorer, "0.8", "0.7");
    defer a.free(restored_scorer);
    scorer_value.* = .{ .string = restored_scorer };
    const restored_config = try std.json.Stringify.valueAlloc(a, updated.value, .{});
    defer a.free(restored_config);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(reopened_id, .fromSlice(restored_config)));
    try runUntilIdle(reopened);
    try std.testing.expect(embed_context.mention_calls > before_reopen_replay);
    // Once the restored cache is populated, another new document reuses it.
    const after_restore = embed_context.mention_calls;
    try reopened.db.batch(.{ .writes = &.{.{ .key = "claim:restored-duplicate", .value = "{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Laya reviewer\"}]}}" }} });
    try runUntilIdle(reopened);
    try std.testing.expectEqual(after_restore, embed_context.mention_calls);
}

test "capi Lite models_dir loads a real extractor for graph enrichment" {
    const api = @import("db.zig");
    const required = std.c.getenv("ANTFLY_LITE_REQUIRE_EXTRACTOR_TEST") != null;
    if (!api.localInferenceRuntimeAvailable()) return if (required) error.RequiredInferenceRuntimeUnavailable else error.SkipZigTest;
    const model_dir = std.mem.span(std.c.getenv("ANTFLY_LITE_EXTRACTOR_MODEL_DIR") orelse return if (required) error.RequiredExtractorModelMissing else error.SkipZigTest);
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("explicit-models-dir");
    defer tmp.cleanup();
    const models_dir = try std.fmt.allocPrint(a, "{s}/models", .{tmp.path()});
    defer a.free(models_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, models_dir);
    const link = try std.fmt.allocPrint(a, "{s}/explicit-base", .{models_dir});
    defer a.free(link);
    const encoder_dir = try std.fmt.allocPrint(a, "{s}/encoder_config", .{link});
    defer a.free(encoder_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, encoder_dir);
    for ([_][]const u8{ "config.json", "model.safetensors", "model_manifest.json", "tokenizer.json", "tokenizer_config.json", "encoder_config/config.json" }) |file| {
        const original = try std.fmt.allocPrint(a, "{s}/{s}", .{ model_dir, file });
        defer a.free(original);
        const destination = try std.fmt.allocPrint(a, "{s}/{s}", .{ link, file });
        defer a.free(destination);
        try std.Io.Dir.cwd().hardLink(original, .cwd(), destination, std.testing.io, .{});
    }
    const path = try std.fmt.allocPrintSentinel(a, "{s}/graph.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{
        .storage_kind = h.capi.storage_kind_lite,
        .profile = h.capi.profile_native,
        .flags = h.capi.open_flag_local_runtime_configured,
        .models_dir = .fromSlice(models_dir),
        .inference_host_budget_mb = 16384,
        .inference_backend_budget_mb = 16384,
        .inference_combined_budget_mb = 24576,
        .inference_scratch_budget_mb = 16384,
        .inference_process_memory_budget_mb = 32768,
    };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("entities"), .fromSlice("{}")));
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"antfly","model":"explicit-base","schema":{"entities":["company","person","product","location"]}}}},"resolvers":[{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}],"edge_types":[{"name":"mentions"}],"metrics":{"ppr":{"kind":"pagerank","enabled":true}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("queries"), .fromSlice("{}")));
    var queries: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("queries"), &queries));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(queries, .fromSlice(config)));
    api.antfly_db_close(queries);
    var batch_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&batch_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:example\":{\"body\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}},\"sync_level\":\"write\"}"), &batch_response));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    var edge_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&edge_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("claim:example"), .fromSlice("mentions"), 0, &edge_response));
    const parsed_edges = try std.json.parseFromSlice(std.json.Value, a, edge_response.ptr.?[0..edge_response.len], .{});
    defer parsed_edges.deinit();
    try std.testing.expect(parsed_edges.value.array.items.len > 0);
    const encoded_target = parsed_edges.value.array.items[0].object.get("target_b64").?.string;
    const decoder = std.base64.standard.Decoder;
    const target = try a.alloc(u8, try decoder.calcSizeForSlice(encoded_target));
    defer a.free(target);
    try decoder.decode(target, encoded_target);
    var entities: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("entities"), &entities));
    defer api.antfly_db_close(entities);
    var entity_response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&entity_response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(entities, .fromSlice(target), &entity_response));
    try std.testing.expect(entity_response.len > 0);
    const query = try std.fmt.allocPrint(a, "{{\"graph_metric\":{{\"index\":\"knowledge\",\"metric\":\"ppr\",\"metric_freshness\":\"fresh\",\"seed_nodes\":[{f}],\"direction\":\"both\",\"top_k\":20}}}}", .{std.json.fmt(target, .{})});
    defer a.free(query);
    var response: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&response);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_search_json(id, .fromSlice(query), &response));
    const parsed_response = try std.json.parseFromSlice(std.json.Value, a, response.ptr.?[0..response.len], .{});
    defer parsed_response.deinit();
    const metrics = parsed_response.value.object.get("responses").?.array.items[0].object.get("graph_metric_results").?.object;
    var values = metrics.iterator();
    var found = false;
    while (values.next()) |metric| for (metric.value_ptr.object.get("scores").?.array.items) |score| {
        if (!std.mem.eql(u8, score.object.get("node").?.string, "claim:example")) continue;
        const rank = score.object.get("score").?;
        try std.testing.expect(if (rank == .float) rank.float > 0 else rank.integer > 0);
        found = true;
    };
    try std.testing.expect(found);
    // Quiesce the automatic maintenance worker before checking query side
    // effects. Inference releases the writer gate, so renewal of unrelated
    // job leases/status observations would otherwise race this assertion.
    // Foreground draining still completes all actual setup/replay work.
    const owner = blk: {
        const guard = api.enterHandle(id, .read).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    owner.stopping.store(true, .release);
    owner.wake.set(h.handleLockIo());
    if (owner.worker) |worker| worker.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    // The ad-hoc text route runs the configured extractor and resolves its
    // real, non-empty mentions against the entities promoted above.
    var query_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&query_result);
    const native = @import("../storage/lite/native.zig");
    const baseline = blk: {
        const before = api.enterHandle(id, .read).?;
        defer before.leave();
        const file = &before.handle.owned_lite_backend.?.native_docstore.?.file;
        const documents = try file.snapshotDocumentsAlloc(a);
        errdefer native.NativeFile.freeSnapshotDocuments(a, documents);
        const catalog = try file.snapshotCatalogRecordsAlloc(a);
        errdefer native.NativeFile.freeSnapshotCatalogRecords(a, catalog);
        const indexes = try file.snapshotIndexCatalogRecordsAlloc(a);
        break :blk .{ .documents = documents, .catalog = catalog, .indexes = indexes };
    };
    defer native.NativeFile.freeSnapshotDocuments(a, baseline.documents);
    defer native.NativeFile.freeSnapshotCatalogRecords(a, baseline.catalog);
    defer native.NativeFile.freeSnapshotCatalogRecords(a, baseline.indexes);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    const query_json = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer query_json.deinit();
    const resolved = query_json.value.object.get("resolution").?.object.get("entities").?.array.items;
    try std.testing.expect(resolved.len > 0);
    for (resolved) |entity| {
        try std.testing.expectEqualStrings("entities", entity.object.get("doc_ref").?.object.get("table").?.string);
        // A scorer-free resolver deterministically renders keys and reports
        // `new` on ingestion too, even when those keys already exist.
        try std.testing.expectEqualStrings("new", entity.object.get("decision").?.string);
        api.antfly_buffer_free(&entity_response);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_lookup_json(entities, .fromSlice(entity.object.get("doc_ref").?.object.get("key").?.string), &entity_response));
    }
    {
        const after = api.enterHandle(id, .read).?;
        defer after.leave();
        const file = &after.handle.owned_lite_backend.?.native_docstore.?.file;
        // Candidate readers publish artifact leases, advancing the physical
        // checkpoint. Query resolution must preserve every document, index,
        // and non-lease metadata record, including derived-work state.
        const documents = try file.snapshotDocumentsAlloc(a);
        defer native.NativeFile.freeSnapshotDocuments(a, documents);
        try std.testing.expectEqualDeep(baseline.documents, documents);
        const indexes = try file.snapshotIndexCatalogRecordsAlloc(a);
        defer native.NativeFile.freeSnapshotCatalogRecords(a, indexes);
        try std.testing.expectEqualDeep(baseline.indexes, indexes);
        const catalog = try file.snapshotCatalogRecordsAlloc(a);
        defer native.NativeFile.freeSnapshotCatalogRecords(a, catalog);
        var position: usize = 0;
        for (baseline.catalog) |expected| {
            if (std.mem.startsWith(u8, expected.key, native.NativeFile.artifact_lease_key_prefix)) continue;
            while (position < catalog.len and std.mem.startsWith(u8, catalog[position].key, native.NativeFile.artifact_lease_key_prefix)) : (position += 1) {}
            try std.testing.expect(position < catalog.len);
            try std.testing.expectEqualDeep(expected, catalog[position]);
            position += 1;
        }
        while (position < catalog.len and std.mem.startsWith(u8, catalog[position].key, native.NativeFile.artifact_lease_key_prefix)) : (position += 1) {}
        try std.testing.expectEqual(catalog.len, position);
    }
    api.antfly_db_close(entities);
    entities = null;
    api.antfly_db_close(id);
    id = null;
    var readonly = options;
    readonly.open_mode = h.capi.open_mode_readonly;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &readonly, &id));
    api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    const cached_producer, const readonly_sequence = blk: {
        const readonly_guard = api.enterHandle(id, .read).?;
        defer readonly_guard.leave();
        break :blk .{ readonly_guard.handle.query_producer orelse return error.TestExpectedEqual, readonly_guard.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence };
    };
    api.antfly_buffer_free(&query_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}"), &query_result));
    {
        const repeated_guard = api.enterHandle(id, .read).?;
        defer repeated_guard.leave();
        try std.testing.expectEqual(cached_producer, repeated_guard.handle.query_producer.?);
        try std.testing.expectEqual(readonly_sequence, repeated_guard.handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    }
    var readonly_table: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("queries"), &readonly_table));
    defer api.antfly_db_close(readonly_table);
    var table_query: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&table_query);
    const text_request = "{\"text\":\"Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(readonly_table, .fromSlice("concepts"), .fromSlice(text_request), &table_query));
    {
        const child, const slot = api.handleRegistry().enter(readonly_table).?;
        defer api.HandleRegistry.leave(slot);
        try std.testing.expect(child.query_producer == null);
        try std.testing.expect(child.parent_handle.?.query_producer != null);
    }
    var writer: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &options, &writer));
    defer api.antfly_db_close(writer);
    var write_result: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&write_result);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(writer, .fromSlice("{\"inserts\":{\"refresh_probe\":{\"body\":\"snapshot changed\"}}}"), &write_result));
    // Entering any read refreshes the owner, invalidating child caches before
    // freeing the old provider. No stale producer survives to the next query.
    {
        const refreshed = api.enterHandle(readonly_table, .read).?;
        defer refreshed.leave();
        try std.testing.expect(refreshed.entry_error == null);
        try std.testing.expect(refreshed.handle.query_producer == null);
        try std.testing.expect(refreshed.handle.parent_handle.?.query_producer == null);
    }
    api.antfly_buffer_free(&table_query);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(readonly_table, .fromSlice("concepts"), .fromSlice(text_request), &table_query));
    try std.testing.expect(table_query.len > 0);
    const readonly_query = try std.json.parseFromSlice(std.json.Value, a, query_result.ptr.?[0..query_result.len], .{});
    defer readonly_query.deinit();
    const readonly_entities = readonly_query.value.object.get("resolution").?.object.get("entities").?.array.items;
    try std.testing.expectEqual(resolved.len, readonly_entities.len);
    for (readonly_entities) |entity| try std.testing.expectEqualStrings("new", entity.object.get("decision").?.string);
}

test "capi resolver REVIEW adjudication publishes promotes queries and reopens durable evidence" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    const producer = @import("../storage/db/enrichment/asset_producer.zig");
    const Fixture = struct {
        calls: usize = 0,
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const producer.Request) !@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .data_uri, .fixed_bytes = 1 << 20, .allocator_limit_bytes = 1 << 20, .max_result_bytes_per_item = 1 << 20, .max_result_bytes = 1 << 20 };
        }
        fn produce(ptr: *anyopaque, alloc: std.mem.Allocator, request: producer.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.producer_type == .copy) return alloc.dupe(u8, request.source_text);
            try std.testing.expectEqual(producer.ProducerType.decision, request.producer_type);
            try std.testing.expect(std.mem.indexOf(u8, request.source_text, "Antfly database") != null);
            self.calls += 1;
            return alloc.dupe(u8,
                \\{"model":"fixture@revision","answers":[{"name":"same","type":"choice","decision_method":"typed","choice":"same","probabilities":[{"value":"same","probability":0.98},{"value":"different","probability":0.02}]}],"usage":{"input_tokens":8,"output_tokens":0}}
            );
        }
    };
    var fixture: Fixture = .{};
    var tmp = try api.TestDirectory.init("review-adjudication");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    var opened = true;
    defer if (opened) h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    const entities = try tables.get(handle, "entities");
    try entities.addEnrichment(.{ .name = "description_text", .kind = .asset, .field = "description", .content_type = "text/plain" });
    try entities.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true });
    try entities.batch(.{ .writes = &.{.{ .key = "concept/antfly", .value = "{\"entity_type\":\"concept\",\"canonical_name\":\"Antfly\",\"description\":\"A database\"}" }} });
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true, .asset_producer = .{ .ptr = &fixture, .vtable = &.{ .produce = Fixture.produce, .invocation_memory_for_requests = Fixture.memory } } });
    const policy =
        \\{"decider":{"provider":"antfly","model":"fixture"},"template":"A: {{m.text}} {{m.description}} B: {{c.canonical_name}} {{c.description}}","question":{"type":"choice","name":"same","instructions":"Do these refer to the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9},"description_inputs":{"candidate_artifact":"description_text"}}
    ;
    const declaration = try std.fmt.allocPrint(a,
        \\{{"name":"concepts","table":"entities","source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{{{ uslug _entity.text }}}}","candidate_search":"prefix","scorer":{{"comparisons":[],"combine":{{"bias":1}},"decision":{{"match":0.95,"review":0.6}}}},"adjudicate":{s}}}
    , .{policy});
    defer a.free(declaration);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, declaration, .{});
    defer parsed.deinit();
    const config = try h.db_mod.ResolverConfig.parseValue(a, parsed.value);
    defer config.deinit();
    try handle.db.addResolver(config.value);
    try handle.db.addEnrichment(.{ .name = "kg", .kind = .asset, .field = "kg", .content_type = "application/json" });
    try handle.db.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"kg\":{\"entities\":[{\"id\":\"e\",\"label\":\"concept\",\"text\":\"Antfly database\",\"description\":\"An indexed database\"}]}}" }} });
    try runUntilIdle(handle);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    const description = (try candidateSource(handle).getDescription(a, "entities", "concept/antfly", "description_text")) orelse return error.TestUnexpectedResult;
    defer a.free(description);
    try std.testing.expectEqualStrings("\"A database\"", description);
    try std.testing.expect((try entities.lookup(a, "concept/antfly_database", .{})) == null);
    var canonical = (try entities.lookup(a, "concept/antfly", .{})).?;
    defer canonical.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, canonical.json, "Antfly database") != null); // promoted alias
    const query = try @import("../storage/db/resolution_runtime.zig").resolveQueryExtractionAlloc(a, &.{config.value}, &config.value,
        \\{"entities":[{"id":"q","label":"concept","text":"Antfly database"}]}
    , candidateSource(handle), null);
    defer a.free(query);
    try std.testing.expect(std.mem.indexOf(u8, query, "concept/antfly\"") != null);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    const catalog = @import("../storage/db/catalog/resolver_catalog.zig");
    try std.testing.expectEqual(catalog.adjudication_reader_version, handle.db.core.index_manager.minimum_reader_version.load(.acquire));
    const resolution_key = try @import("../storage/internal_keys.zig").resolutionArtifactKeyAlloc(a, "source", "resolved");
    defer a.free(resolution_key);
    const before = try handle.db.core.store.get(a, resolution_key);
    defer a.free(before);
    h.closeHandle(handle);
    opened = false;
    const reopened = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, false, null, null);
    defer h.closeHandle(reopened);
    try runUntilIdle(reopened);
    const after = try reopened.db.core.store.get(a, resolution_key);
    defer a.free(after);
    try std.testing.expectEqualStrings(before, after);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
}

test "capi resolver render and merge rekey existing edges without entity promotion" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-public-keys");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, "{s}.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_generated_enrichment_replay };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"},{"name":"uses"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","aliases":[{"key":"concept/qwen3","target":"concept/qwen3_embedding"}],"config_generation":1},{"name":"events","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"events_resolved","key_template":"event/{{ uslug _entity.text }}","labels":["event"],"config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Launch\",\"label\":\"event\"}"), &out));
    try std.testing.expect(out.ptr == null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("events"), .fromSlice("{\"text\":\"Launch\",\"label\":\"event\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "event/launch") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"e\",\"text\":\"Launch\",\"label\":\"event\"}]}}"), &out));
    const excluded_query = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer excluded_query.deinit();
    try std.testing.expectEqual(@as(usize, 0), excluded_query.value.object.get("resolution").?.object.get("entities").?.array.items.len);
    api.antfly_buffer_free(&out);

    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Qwen3\",\"label\":\"concept\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "concept/qwen3_embedding") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:a\":{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Qwen3\"},{\"id\":\"e1\",\"label\":\"concept\",\"text\":\"Antfly\"}],\"relations\":[{\"type\":\"uses\",\"source\":\"e0\",\"target\":\"e1\"}]}}}}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.not_found, api.antfly_db_lookup_json(id, .fromSlice("concept/qwen3_embedding"), &out));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3_embedding\",\"target\":\"concept/qwen\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("claim:a"), .fromSlice("mentions"), 0, &out));
    var result = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer result.deinit();
    const edges = result.value.array.items;
    try std.testing.expectEqual(@as(usize, 2), edges.len);
    var found_canonical = false;
    for (edges) |edge| {
        const encoded = edge.object.get("target_b64").?.string;
        const decoded = try a.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(encoded));
        defer a.free(decoded);
        try std.base64.standard.Decoder.decode(decoded, encoded);
        if (std.mem.eql(u8, decoded, "concept/qwen")) found_canonical = true;
        try std.testing.expect(!std.mem.eql(u8, decoded, "concept/qwen3_embedding"));
    }
    try std.testing.expect(found_canonical);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const merged_relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer merged_relations.deinit();
    try std.testing.expectEqual(@as(usize, 1), merged_relations.value.array.items.len);
    // Recreate the index using the original declarative config, which omits
    // the API-created merge. Resolver identity state outlives index configs.
    var deleted = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_delete_index(id, .fromSlice("knowledge"), &deleted));
    try std.testing.expect(deleted);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer relations.deinit();
    try std.testing.expectEqual(@as(usize, 1), relations.value.array.items.len);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen3_embedding"), .fromSlice("uses"), 0, &out));
    const old_relations = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer old_relations.deinit();
    try std.testing.expectEqual(@as(usize, 0), old_relations.value.array.items.len);

    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen\",\"target\":\"concept/qwen3\"}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3_embedding\",\"target\":\"concept/qwen\"}")));
    api.antfly_db_close(id);
    id = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &options, &id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(id, .fromSlice("knowledge"), .fromSlice("concept/qwen"), .fromSlice("uses"), 0, &out));
    const replayed_edges = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer replayed_edges.deinit();
    try std.testing.expectEqual(@as(usize, 1), replayed_edges.value.array.items.len);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Qwen3\",\"label\":\"concept\"}"), &out));
    const rendered = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer rendered.deinit();
    try std.testing.expectEqualStrings("concept/qwen", rendered.value.object.get("key").?.string);
    try std.testing.expectEqual(@as(i64, 5), rendered.value.object.get("config_generation").?.integer);
    // Read-only handles can query aliases without canonical documents and
    // observe the same durable generation after reopening.
    api.antfly_db_close(id);
    id = null;
    var readonly = options;
    readonly.open_mode = h.capi.open_mode_readonly;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &readonly, &id));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"q0\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}"), &out));
    const queried = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer queried.deinit();
    const resolution = queried.value.object.get("resolution").?.object;
    try std.testing.expectEqualStrings("concept/qwen", resolution.get("entities").?.array.items[0].object.get("doc_ref").?.object.get("key").?.string);
    try std.testing.expectEqualStrings("match", resolution.get("entities").?.array.items[0].object.get("decision").?.string);
    try std.testing.expectEqual(rendered.value.object.get("config_generation").?.integer, resolution.get("config_generation").?.integer);
}

test "capi resolver config merge config advances revisions and protects key namespaces" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-revision");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, "{s}.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_generated_enrichment_replay };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice("{\"inserts\":{\"claim:a\":{\"kg\":{\"entities\":[{\"id\":\"e0\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}}}"), &out));
    const Check = struct {
        fn bump(handle_id: ?*anyopaque, generation: u64) !void {
            const guard = api.enterHandle(handle_id, .exclusive).?;
            defer guard.leave();
            const configs = try guard.handle.database().listResolvers(std.testing.allocator);
            defer {
                for (configs) |*cfg| cfg.deinit(std.testing.allocator);
                std.testing.allocator.free(configs);
            }
            var cfg = configs[0];
            cfg.config_generation = generation;
            _ = try guard.handle.database().upsertResolverWithResultOptions(cfg, .{ .drain_backfill = false });
        }
        fn storedRevision(handle_id: ?*anyopaque, revision: u64) !void {
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle_id));
            const guard = api.enterHandle(handle_id, .read).?;
            defer guard.leave();
            const key = try @import("../storage/internal_keys.zig").resolutionArtifactKeyAlloc(std.testing.allocator, "claim:a", "resolved");
            defer std.testing.allocator.free(key);
            var txn = try guard.handle.database().core.store.beginReadTxn();
            defer txn.abort();
            const bytes = try txn.get(key);
            const expected = try std.fmt.allocPrint(std.testing.allocator, "\"config_generation\":{d}", .{revision});
            defer std.testing.allocator.free(expected);
            try std.testing.expect(std.mem.indexOf(u8, bytes, expected) != null);
        }
    };
    try Check.bump(id, 2);
    try Check.storedRevision(id, 2);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_merge_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/qwen3\",\"target\":\"concept/qwen\"}")));
    try Check.storedRevision(id, 3);
    try Check.bump(id, 3);
    try Check.storedRevision(id, 4);
    {
        const guard = api.enterHandle(id, .exclusive).?;
        defer guard.leave();
        const configs = try guard.handle.database().listResolvers(a);
        defer {
            for (configs) |*cfg| cfg.deinit(a);
            a.free(configs);
        }
        var metadata_only = configs[0];
        metadata_only.merge_generation += 100;
        try std.testing.expectEqual(@import("../storage/db/catalog/index_manager.zig").IndexManager.ResolverUpsertResult.updated_no_backfill, try guard.handle.database().upsertResolverWithResultOptions(metadata_only, .{ .drain_backfill = false }));
        var changed = configs[0];
        changed.table = "other";
        try std.testing.expectError(error.ResolverMergeNamespaceChange, guard.handle.database().upsertResolverWithResultOptions(changed, .{ .drain_backfill = false }));
        changed = configs[0];
        changed.key_template = "other/{{ uslug _entity.text }}";
        try std.testing.expectError(error.ResolverMergeNamespaceChange, guard.handle.database().upsertResolverWithResultOptions(changed, .{ .drain_backfill = false }));
    }
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolve_query_json(id, .fromSlice("concepts"), .fromSlice("{\"extraction\":{\"entities\":[{\"id\":\"q\",\"label\":\"concept\",\"text\":\"Qwen3\"}]}}"), &out));
    const queried = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
    defer queried.deinit();
    const resolution = queried.value.object.get("resolution").?.object;
    try std.testing.expectEqual(@as(i64, 4), resolution.get("config_generation").?.integer);
    try std.testing.expectEqualStrings("concept/qwen", resolution.get("entities").?.array.items[0].object.get("doc_ref").?.object.get("key").?.string);
}

test "capi native text queries overlap writer commits and retain retired providers" {
    try testNativeTextQueryConcurrentCommit(false, false);
}

test "capi native text queries return busy after a concurrent merge" {
    try testNativeTextQueryConcurrentCommit(true, false);
}

test "capi resolver REVIEW adjudication queries overlap writers" {
    try testNativeTextQueryConcurrentCommit(false, true);
}
test "capi resolver REVIEW adjudication queries reject concurrent merges" {
    try testNativeTextQueryConcurrentCommit(true, true);
}

fn testNativeTextQueryConcurrentCommit(change_config: bool, adjudication_mode: bool) !void {
    const api = @import("db.zig");
    const extracting = @import("antfly_extracting");
    const managed = @import("../inference/managed_embedder.zig");
    const a = std.testing.allocator;
    const State = struct {
        entered: std.atomic.Value(u32) = .init(0),
        both_entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        committed: std.Io.Event = .unset,
        pub fn extract(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: extracting.Request) !extracting.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.entered.fetchAdd(1, .acq_rel) == 1) self.both_entered.set(h.handleLockIo());
            try self.release.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } });
            return .{ .allocator = alloc, .json = try alloc.dupe(u8, "{\"object\":\"extraction\",\"model\":\"test-extractor\",\"data\":[{\"entities\":[{\"text\":\"Antfly\",\"label\":\"company\"}],\"relations\":[]}]}") };
        }
        fn decide(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: ?managed.RequestContext) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.entered.fetchAdd(1, .acq_rel) == 1) self.both_entered.set(h.handleLockIo());
            try self.release.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } });
            return alloc.dupe(u8,
                \\{"model":"test-decision","answers":[{"name":"same","type":"choice","decision_method":"typed","choice":"same","confidence":0.98,"confidence_method":"max_probability","probabilities":[{"value":"same","probability":0.98},{"value":"different","probability":0.02}]}],"usage":{"input_tokens":8,"output_tokens":0}}
            );
        }
        fn embedDense(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }
        fn embedSparse(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]@import("../storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }
    };
    const Query = struct {
        id: ?*anyopaque,
        adjudication: bool = false,
        code: h.capi.ErrorCode = .internal,
        done: std.Io.Event = .unset,
        response: [2048]u8 = undefined,
        response_len: usize = 0,
        fn run(self: *@This()) void {
            defer self.done.set(h.handleLockIo());
            var result: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&result);
            const request = if (self.adjudication) "{\"extraction\":{\"entities\":[{\"id\":\"q\",\"label\":\"company\",\"text\":\"Antfly database\"}]}}" else "{\"text\":\"Antfly\"}";
            self.code = api.antfly_db_resolve_query_json(self.id, .fromSlice("concepts"), .fromSlice(request), &result);
            if (result.ptr) |bytes| {
                self.response_len = @min(result.len, self.response.len);
                @memcpy(self.response[0..self.response_len], bytes[0..self.response_len]);
            }
        }
    };
    const Writer = struct {
        id: ?*anyopaque,
        state: *State,
        change_config: bool,
        code: h.capi.ErrorCode = .internal,
        fn run(self: *@This()) void {
            var result: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&result);
            self.code = if (self.change_config)
                api.antfly_db_merge_resolver_keys_json(self.id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/antfly\",\"target\":\"concept/canonical\"}"))
            else
                api.antfly_db_batch_json(self.id, .fromSlice("{\"inserts\":{\"probe\":{\"body\":\"committed\"}}}"), &result);
            self.state.committed.set(h.handleLockIo());
        }
    };
    var tmp = try api.TestDirectory.init("resolver-query-concurrency");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, "{s}.aflite", .{tmp.path()}, 0);
    defer a.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native };
    var writer: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &writer));
    defer api.antfly_db_close(writer);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"body"},"content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"antfly","model":"test-extractor","schema":{"entities":["company"]}}}},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}]}
    ;
    const admitted_config = if (adjudication_mode) blk: {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var value = try std.json.parseFromSliceLeaky(std.json.Value, scratch, config, .{});
        const resolver = &value.object.getPtr("resolvers").?.array.items[0];
        try resolver.object.put(scratch, "table", .{ .string = "entities" });
        try resolver.object.put(scratch, "candidate_search", .{ .string = "prefix" });
        try resolver.object.put(scratch, "scorer", try std.json.parseFromSliceLeaky(std.json.Value, scratch,
            \\{"comparisons":[],"combine":{"bias":1},"decision":{"match":0.95,"review":0.6}}
        , .{}));
        try resolver.object.put(scratch, "adjudicate", try std.json.parseFromSliceLeaky(std.json.Value, scratch,
            \\{"decider":{"provider":"antfly","model":"test-decision"},"template":"{{m.text}} / {{c.canonical_name}}","question":{"type":"choice","name":"same","instructions":"Are these the same entity?","choices":[{"value":"same"},{"value":"different"}]},"accept":{"choice":"same","min_probability":0.9}}
        , .{}));
        break :blk try std.json.Stringify.valueAlloc(a, value, .{});
    } else try a.dupe(u8, config);
    defer a.free(admitted_config);
    var entities: ?*anyopaque = null;
    defer api.antfly_db_close(entities);
    if (adjudication_mode) {
        // Canonical candidates are not source documents. Keeping them in their
        // own table also prevents background extraction retries from refreshing
        // the provider snapshot before the concurrent queries have entered it.
        try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(writer, .fromSlice("entities"), .fromSlice("{}")));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(writer, .fromSlice("entities"), &entities));
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(writer, .fromSlice(admitted_config)));
    if (adjudication_mode) {
        var result: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&result);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(entities, .fromSlice("{\"inserts\":{\"concept/antfly\":{\"entity_type\":\"company\",\"canonical_name\":\"Antfly\"}}}"), &result));
    }
    var sql_output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&sql_output);
    for ([_][]const u8{
        \\{"statement":"CREATE TABLE numbers (n BIGINT)"}
        ,
        \\{"statement":"INSERT INTO numbers (_id,n) VALUES ('row',7)"}
        ,
    }) |request| {
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(writer, .fromSlice(request), &sql_output));
        api.antfly_buffer_free(&sql_output);
    }
    // Complete setup publications before installing a producer on the pinned
    // reader root; background setup work must not replace that test double.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(writer));
    var readonly = options;
    readonly.open_mode = h.capi.open_mode_readonly;
    var reader: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_open_with_options(path, &readonly, &reader));
    defer api.antfly_db_close(reader);
    const sql_cursor = @import("sql_cursor.zig");
    var cursor: u64 = 0;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sql_cursor.antfly_db_sql_open_cursor_json(reader, .fromSlice("{\"statement\":\"SELECT n FROM numbers\"}"), &cursor, &sql_output));
    api.antfly_buffer_free(&sql_output);
    defer _ = sql_cursor.antfly_db_sql_close_cursor(reader, cursor);
    var state: State = .{};
    var cursor_snapshot: *h.Handle = undefined;
    const snapshot = blk: {
        const setup = api.enterHandle(reader, .read).?;
        defer setup.leave();
        try std.testing.expectEqual(@as(?h.capi.ErrorCode, null), setup.entry_error);
        cursor_snapshot = setup.handle.sql_cursors.get(cursor).?.adapter.handle.?;
        const snapshot = setup.handle;
        snapshot.query_producer = try @import("../asset_producer_runtime.zig").Runtime.createOwned(snapshot.alloc, snapshot.database().backend_runtime.io().?, .{
            .antfly_provider = managed.AntflyProvider{ .ptr = &state, .owns_invocation_admission = true, .embed_dense_texts = State.embedDense, .embed_sparse_texts = State.embedSparse, .extract = State.extract, .decide_json = State.decide },
        });
        break :blk snapshot;
    };
    var first: Query = .{ .id = reader, .adjudication = adjudication_mode };
    var second: Query = .{ .id = reader, .adjudication = adjudication_mode };
    const first_thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Query.run, .{&first});
    defer first_thread.join();
    defer state.release.set(h.handleLockIo());
    const second_thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Query.run, .{&second});
    defer second_thread.join();
    defer state.release.set(h.handleLockIo());
    state.both_entered.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } }) catch |err| {
        std.debug.print("query provider calls: {d}\n", .{state.entered.load(.acquire)});
        if (first.done.isSet()) std.debug.print("first query: {s} {s}\n", .{ @tagName(first.code), first.response[0..first.response_len] });
        if (second.done.isSet()) std.debug.print("second query: {s} {s}\n", .{ @tagName(second.code), second.response[0..second.response_len] });
        return err;
    };
    var write: Writer = .{ .id = writer, .state = &state, .change_config = change_config };
    const writer_thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Writer.run, .{&write});
    defer writer_thread.join();
    defer state.release.set(h.handleLockIo());
    // A bounded wait fails instead of deadlocking if extraction retains the path lock.
    try state.committed.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    try std.testing.expectEqual(h.capi.ErrorCode.ok, write.code);
    {
        const refreshed = api.enterHandle(reader, .read).?;
        defer refreshed.leave();
        try std.testing.expect(refreshed.handle != snapshot);
        try std.testing.expectEqual(@as(usize, 2), snapshot.query_borrows);
        try std.testing.expect(snapshot.query_producer != null);
    }
    state.release.set(h.handleLockIo());
    try first.done.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    try second.done.waitTimeout(h.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
    try std.testing.expectEqual(if (change_config) h.capi.ErrorCode.busy else h.capi.ErrorCode.ok, first.code);
    try std.testing.expectEqual(if (change_config) h.capi.ErrorCode.busy else h.capi.ErrorCode.ok, second.code);
    {
        const pinned = api.enterHandle(reader, .read).?;
        defer pinned.leave();
        // resumeQuery collected while this registry was attached to the new root.
        // A background publication may have refreshed the root between cursor
        // creation and query setup. Pinning follows the cursor's actual root,
        // independently of which root supplied the query producer.
        try std.testing.expectEqual(@as(usize, 1), pinned.connection.?.owner.retired.items.len);
        try std.testing.expect(pinned.connection.?.owner.retired.items[0] == cursor_snapshot);
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sql_cursor.antfly_db_sql_fetch_cursor_json(reader, cursor, 10, &sql_output));
    try std.testing.expect(std.mem.indexOf(u8, sql_output.ptr.?[0..sql_output.len], "7") != null);
    api.antfly_buffer_free(&sql_output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, sql_cursor.antfly_db_sql_close_cursor(reader, cursor));
    const finished = api.enterHandle(reader, .read).?;
    defer finished.leave();
    try std.testing.expectEqual(@as(usize, 0), finished.connection.?.owner.retired.items.len);
}

test "capi inline resolver ownership migrates legacy declarations and preserves independent definitions" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-ownership");
    defer tmp.cleanup();
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{ uslug _entity.text }}","config_generation":1}]}
    ;
    const Case = struct { independent: bool, replay: bool };
    for ([_]Case{ .{ .independent = false, .replay = false }, .{ .independent = false, .replay = true }, .{ .independent = true, .replay = true } }, 0..) |case, ordinal| {
        const independent = case.independent;
        const path = try std.fmt.allocPrint(a, "{s}-{d}.aflite", .{ tmp.path(), ordinal });
        defer a.free(path);
        const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
        const id = try api.publishHandle(handle);
        defer api.antfly_db_close(id);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
        const configs = try handle.db.listResolvers(a);
        defer {
            for (configs) |*cfg| cfg.deinit(a);
            a.free(configs);
        }
        if (independent) {
            try std.testing.expect(try handle.db.removeResolver("concepts"));
            try handle.db.addResolver(configs[0]);
        } else {
            var legacy = configs[0];
            legacy.inline_managed = null;
            try handle.db.restoreResolverIdentityWithoutDrain("concepts", legacy);
        }
        // Replaying the existing index claims a pre-ownership inline resolver,
        // while an independently registered definition remains independent.
        if (case.replay) {
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
            try std.testing.expectEqual(!independent, handle.db.core.index_manager.getResolver("concepts").?.inline_managed.?);
        }
        var deleted = false;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_delete_index(id, .fromSlice("knowledge"), &deleted));
        try std.testing.expect(deleted);
        var out: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&out);
        try std.testing.expectEqual(if (independent) h.capi.ErrorCode.ok else h.capi.ErrorCode.not_found, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Antfly\",\"label\":\"concept\"}"), &out));
    }
}

test "capi resolver reader version is checked before opening catalog" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("reader-epoch");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    const catalog = @import("../storage/db/catalog/resolver_catalog.zig");
    var version_buffer: [20]u8 = undefined;
    const unsupported_version = try std.fmt.bufPrint(&version_buffer, "{d}", .{catalog.supported_reader_version + 1});
    try handle.db.core.store.put(catalog.minimum_reader_key, unsupported_version);
    api.antfly_db_close(id);
    try std.testing.expectError(error.UnsupportedDatabaseReaderVersion, api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, false, null, null));
}

test "capi resolver sparse model revision uses sparse capability discovery" {
    try h.managed_embedder.testSparseCacheRevision();
}

test "capi resolver concurrent source promotions preserve shared destination aliases" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("concurrent-promotion");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const id = try api.publishHandle(root);
    defer api.antfly_db_close(id);
    try tables.create(root, "entities", "{}", false);
    var ready = std.atomic.Value(u32).init(0);
    const State = struct {
        root: *h.Handle,
        ready: *std.atomic.Value(u32),
        json: []const u8,
        failure: ?anyerror = null,
        fn run(state: *@This()) void {
            _ = state.ready.fetchAdd(1, .acq_rel);
            while (state.ready.load(.acquire) < 2) std.atomic.spinLoopHint();
            upsert(state.root, std.heap.c_allocator, "entities", "entity/ada", state.json) catch |err| {
                state.failure = err;
            };
        }
    };
    var first: State = .{ .root = root, .ready = &ready, .json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada\",\"aliases\":[\"Ada\"]}" };
    var second: State = .{ .root = root, .ready = &ready, .json = "{\"entity_type\":\"person\",\"canonical_name\":\"ADA\",\"aliases\":[\"ADA\"]}" };
    const one = try std.Thread.spawn(.{ .stack_size = 16 * 1024 * 1024 }, State.run, .{&first});
    const two = try std.Thread.spawn(.{ .stack_size = 16 * 1024 * 1024 }, State.run, .{&second});
    one.join();
    two.join();
    if (first.failure) |err| return err;
    if (second.failure) |err| return err;
    const destination = try tables.get(root, "entities");
    var row = (try destination.lookup(a, "entity/ada", .{})).?;
    defer row.deinit(a);
    var json = try std.json.parseFromSlice(std.json.Value, a, row.json, .{});
    defer json.deinit();
    try std.testing.expectEqual(@as(usize, 2), json.value.object.get("aliases").?.array.items.len);
    const canonical = json.value.object.get("canonical_name").?.string;
    try std.testing.expect(std.mem.eql(u8, canonical, "Ada") or std.mem.eql(u8, canonical, "ADA"));
}

test "capi resolver embedding cache batches writes bounds size and expires pages" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("mention-cache-pages");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    const sequence = handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    for (0..300) |i| {
        var digest: [32]u8 = @splat(0);
        std.mem.writeInt(u64, digest[0..8], i, .little);
        try embeddingCachePut(handle, digest, &.{1});
    }
    try std.testing.expectEqual(sequence, handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    try embeddingCacheFlush(handle);
    try std.testing.expectEqual(sequence + 1, handle.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    try std.testing.expectEqual(@as(usize, 0), handle.embedding_cache_pending.count());
    {
        var read = try handle.db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var txn = try handle.db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var row = try cursor.seekAtOrAfter(embedding_cache_prefix);
        while (row) |kv| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, embedding_cache_prefix)) break;
            const expired = try a.dupe(u8, kv.value);
            defer a.free(expired);
            std.mem.writeInt(i64, expired[40..48], 0, .little);
            try txn.put(kv.key, expired);
        }
        try txn.commit();
    }
    try collectEmbeddingCache(handle, false);
    {
        var read = try handle.db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect((try read.get(embedding_cache_cursor_key)).len > 0);
        const bytes = try read.get(embedding_cache_bytes_key);
        try std.testing.expect(try std.fmt.parseInt(usize, bytes, 10) > 0);
    }
    try collectEmbeddingCache(handle, false);
    embedding_cache_limit_for_test = 512;
    defer embedding_cache_limit_for_test = null;
    for (0..10) |i| try embeddingCachePut(handle, @splat(@intCast(i)), &.{ 1, 2, 3 });
    try embeddingCacheFlush(handle);
    var read = try handle.db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(embedding_cache_prefix);
    var total: usize = 0;
    while (row) |kv| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, kv.key, embedding_cache_prefix)) break;
        total += kv.key.len + kv.value.len;
    }
    try std.testing.expect(total <= 512);
    try std.testing.expectEqual(total, try std.fmt.parseInt(usize, try read.get(embedding_cache_bytes_key), 10));
}

test "capi resolver failed replacement permits portable backup and cancellation" {
    const api = @import("db.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("failed-shadow-backup");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    const old = "{\"name\":\"old\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}";
    const replacement = "{\"name\":\"new\",\"type\":\"graph\",\"replaces\":\"old\",\"edge_types\":[{\"name\":\"R\",\"field\":\"next_links\"}]}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(old)));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var rows: std.ArrayList(h.db_mod.types.BatchWrite) = .empty;
    for (0..300) |i| try rows.append(arena.allocator(), .{ .key = try std.fmt.allocPrint(arena.allocator(), "doc:{d}", .{i}), .value = "{\"links\":[\"old\"],\"next_links\":[\"new\"]}" });
    try handle.db.batch(.{ .writes = rows.items });
    try runUntilIdle(handle);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(replacement)));
    _ = try handle.db.advanceGraphReplacement();
    const jobs = try handle.db.graphReplacementStatusAlloc(a);
    defer jobs.deinit();
    try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
    var job = jobs.value[0];
    job.phase = .failed;
    job.last_error = "InjectedReplacementBuildFailure";
    try handle.db.core.index_manager.failGraphReplacement(handle.db.core.store, job);
    try runUntilIdle(handle);
    try std.testing.expectEqual(@as(u64, 999), try handle.db.core.index_manager.clampReplayForGraphReplacementBuilds(handle.db.core.store, 999));
    var archive: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&archive);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_backup(id, &archive));
    try @import("database_backup.zig").validate(a, archive.ptr.?[0..archive.len]);
    var cancelled = false;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_cancel_replacement(id, .fromSlice("new"), &cancelled));
    try std.testing.expect(cancelled);
    try runUntilIdle(handle);
    try std.testing.expect(handle.db.core.index_manager.get("old") != null);
    try std.testing.expect(handle.db.core.index_manager.get("new") == null);
}

test "capi resolver curation repoints restores suppresses and reopens promotion state" {
    const api = @import("db.zig");
    const curation = @import("../storage/db/resolver_curation.zig");
    const a = std.testing.allocator;
    var tmp = try api.TestDirectory.init("resolver-curation-lifecycle");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    var handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, true, null, null);
    var opened = true;
    defer if (opened) h.closeHandle(handle);
    try tables.create(handle, "entities", "{}", false);
    const seeded_entities = try tables.get(handle, "entities");
    try seeded_entities.batch(.{ .writes = &.{ .{ .key = "concept/beta", .value = "{\"canonical_name\":\"Beta\",\"entity_type\":\"concept\",\"aliases\":[\"Beta\"]}" }, .{ .key = "concept/gamma", .value = "{\"canonical_name\":\"Gamma\",\"entity_type\":\"concept\",\"aliases\":[\"Gamma\"]}" } } });
    try handle.db.reconfigureEnrichmentRuntime(.{ .enable_without_producers = true });
    try handle.db.addEnrichment(.{ .name = "kg", .kind = .asset, .field = "kg", .content_type = "application/json" });
    try handle.db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}" });
    try handle.db.batch(.{ .writes = &.{.{ .key = "source", .value = "{\"kg\":{\"entities\":[{\"id\":\"e\",\"label\":\"concept\",\"text\":\"Alpha\"}]}}" }} });
    try runUntilIdle(handle);
    const Actions = struct {
        fn apply(root: *h.Handle, request: []const u8) !void {
            const result = try curation.mutate(&root.db, std.testing.allocator, "concepts", request);
            defer std.testing.allocator.free(result);
            try runUntilIdle(root);
        }
        fn document(root: *h.Handle, key: []const u8) ![]u8 {
            const destination = try tables.get(root, "entities");
            var row = (try destination.lookup(std.testing.allocator, key, .{})) orelse return error.TestUnexpectedResult;
            defer row.deinit(std.testing.allocator);
            return std.testing.allocator.dupe(u8, row.json);
        }
    };
    try Actions.apply(handle, "{\"action\":\"merge\",\"from\":\"concept/alpha\",\"to\":\"concept/beta\"}");
    try Actions.apply(handle, "{\"action\":\"merge\",\"from\":\"concept/alpha\",\"to\":\"concept/gamma\",\"replace\":true}");
    const beta = try Actions.document(handle, "concept/beta");
    defer a.free(beta);
    try std.testing.expect(std.mem.indexOf(u8, beta, "merged_into") == null);
    try std.testing.expect(std.mem.indexOf(u8, beta, "Alpha") == null);
    try Actions.apply(handle, "{\"action\":\"unmerge\",\"from\":\"concept/alpha\",\"pin_distinct\":true}");
    const restored = try Actions.document(handle, "concept/alpha");
    defer a.free(restored);
    try std.testing.expect(std.mem.indexOf(u8, restored, "merged_into") == null);
    try Actions.apply(handle, "{\"action\":\"suppress\",\"key\":\"concept/alpha\"}");
    const suppressed = try Actions.document(handle, "concept/alpha");
    defer a.free(suppressed);
    try std.testing.expect(std.mem.indexOf(u8, suppressed, "resolver_suppressed_by") != null);
    try Actions.apply(handle, "{\"action\":\"unsuppress\",\"key\":\"concept/alpha\"}");
    const before = try curation.list(&handle.db, a, "concepts");
    defer a.free(before);
    h.closeHandle(handle);
    opened = false;
    handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .hosted, .externally_locked = true }, false, null, null);
    opened = true;
    try runUntilIdle(handle);
    const after = try curation.list(&handle.db, a, "concepts");
    defer a.free(after);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, after, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 5), parsed.value.object.get("curation_revision").?.integer);
    const final = try Actions.document(handle, "concept/alpha");
    defer a.free(final);
    try std.testing.expect(std.mem.indexOf(u8, final, "merged_into") == null);
}

test "capi resolver curation public operations expose durable state reviews and entity fanout" {
    const api = @import("db.zig");
    var tmp = try api.TestDirectory.init("resolver-curation-public");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const id = try api.publishHandle(root);
    defer api.antfly_db_close(id);
    try root.db.addResolver(.{ .name = "concepts", .table = "default", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false });
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_entity_merge_json(id, .fromSlice("default"), .fromSlice("concept/a"), .fromSlice("{\"into\":\"other/b\"}"), &out));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_merge_json(id, .fromSlice("default"), .fromSlice("concept/a"), .fromSlice("{\"into\":\"concept/b\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"revision\":1") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_unmerge_json(id, .fromSlice("default"), .fromSlice("concept/a"), .fromSlice("{\"pin_distinct\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_suppress_json(id, .fromSlice("default"), .fromSlice("concept/noise"), .fromSlice("{}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_render_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"text\":\"Noise\",\"label\":\"concept\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"suppressed\":true") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_unsuppress_json(id, .fromSlice("default"), .fromSlice("concept/noise"), .fromSlice("{}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolver_curation_json(id, .fromSlice("concepts"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"curation_revision\":4") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_resolver_reviews_json(id, .fromSlice("{\"operation\":\"reviews\",\"resolver\":\"concepts\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"reviews\":[]") != null);
}

test "capi resolver curation suppression replays mention and relation edge contributions" {
    const api = @import("db.zig");
    var tmp = try api.TestDirectory.init("resolver-curation-edges");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}.aflite", .{tmp.path()}, 0);
    defer std.testing.allocator.free(path);
    const options: h.capi.OpenOptions = .{ .storage_kind = h.capi.storage_kind_lite, .profile = h.capi.profile_native, .flags = h.capi.open_flag_generated_enrichment_replay };
    var id: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_create_with_options(path, &options, &id));
    defer api.antfly_db_close(id);
    const config =
        \\{"name":"knowledge","type":"graph","source":{"artifact":"kg","format":"extraction_graph","mention_edge_type":"mentions"},"artifact":{"name":"kg","kind":"asset","source":{"type":"field","value":"kg"},"content_type":"application/json"},"edge_types":[{"name":"mentions"},{"name":"uses"}],"resolvers":[{"name":"concepts","table":"default","promote":false,"source_artifact":"kg","resolution_artifact":"resolved","key_template":"concept/{{uslug _entity.text}}"}]}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_index_json(id, .fromSlice(config)));
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    const batch =
        \\{"inserts":{"source":{"kg":{"entities":[{"id":"a","label":"concept","text":"Alpha"},{"id":"b","label":"concept","text":"Beta"}],"relations":[{"type":"uses","source":"a","target":"b"}]}}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_batch_json(id, .fromSlice(batch), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    const Edges = struct {
        fn count(db: ?*anyopaque, key: []const u8, edge_type: []const u8, expected: usize) !void {
            var buffer: h.capi.Buffer = .{};
            defer api.antfly_buffer_free(&buffer);
            try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_edges_json(db, .fromSlice("knowledge"), .fromSlice(key), .fromSlice(edge_type), 0, &buffer));
            const result = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, buffer.ptr.?[0..buffer.len], .{});
            defer result.deinit();
            try std.testing.expectEqual(expected, result.value.array.items.len);
        }
    };
    try Edges.count(id, "source", "mentions", 2);
    try Edges.count(id, "concept/alpha", "uses", 1);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_suppress_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"add\":{\"keys\":[\"concept/alpha\"]}}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try Edges.count(id, "source", "mentions", 1);
    try Edges.count(id, "concept/alpha", "uses", 0);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_unsuppress_resolver_key_json(id, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/alpha\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    try Edges.count(id, "source", "mentions", 2);
    try Edges.count(id, "concept/alpha", "uses", 1);
}

test "capi resolver curation native authority commits decisions once and replays shared projections" {
    const api = @import("db.zig");
    const authority = @import("resolver_authority.zig");
    const pure = @import("antfly_resolver").curation;
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-curation-native-records");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const id = try api.publishHandle(root);
    var closed = false;
    defer if (!closed) api.antfly_db_close(id);
    for ([_][]const u8{ "concepts", "concepts-secondary" }) |name| try root.db.addResolver(.{ .name = name, .table = "default", .source_artifact = name, .resolution_artifact = try std.fmt.allocPrint(scratch, "{s}-resolved", .{name}), .key_template = "concept/{{uslug _entity.text}}", .promote = false });
    try root.db.addResolver(.{ .name = "global", .table = "default", .source_artifact = "global", .resolution_artifact = "global-resolved", .key_template = "{{_entity.text}}", .curation_revision = 5, .promote = false });
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    const create = "{\"action\":\"merge\",\"key\":\"concept/a\",\"into\":\"concept/b\",\"actor\":\"screen\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(id, .fromSlice("default"), .fromSlice(create), &out));
    const proposal = try std.json.parseFromSliceLeaky(pure.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    api.antfly_buffer_free(&out);
    const queue = try authority.queueHead(&root.db, scratch);
    try std.testing.expect(queue.receipt_scope != null);
    try std.testing.expectEqual(@as(u32, 1), queue.count);
    try std.testing.expectError(error.NotFound, root.db.core.store.get(scratch, pure.proposal_store_key));
    // A proposal already transfers ownership, before derived replay catches up.
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_suppress_resolver_keys_json(id, .fromSlice("concepts"), .fromSlice("{\"add\":{\"keys\":[\"concept/a\"]}}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(id, .fromSlice("default"), .fromSlice(proposal.id), .fromSlice("{\"accept\":true,\"actor\":\"curator\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"atomicity\":\"namespace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"state\":\"accepted\"") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(@as(?u64, 1), try authority.revision(&root.db, scratch, "concept/{{uslug _entity.text}}"));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_redirects_json(id, .fromSlice("default"), .fromSlice("concept/a"), .fromSlice("{}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"canonical_key\":\"concept/b\"") != null);
    api.antfly_buffer_free(&out);
    // Replay is derived and consumes one shared namespace revision for both.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(id));
    const configs = try root.db.core.index_manager.listResolverIdentities(scratch);
    for (configs) |cfg| {
        const expected: u64 = if (std.mem.eql(u8, cfg.name, "global")) 6 else 1;
        try std.testing.expectEqual(expected, cfg.namespace_curation_generation);
        try std.testing.expectEqual(@as(usize, 1), cfg.merges.len);
        try std.testing.expectEqualStrings("concept/b", cfg.merges[0].target);
    }
    try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, api.antfly_db_entity_suppress_json(id, .fromSlice("default"), .fromSlice("concept/b"), .fromSlice("{}"), &out));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_suppress_json(id, .fromSlice("default"), .fromSlice("concept/b"), .fromSlice("{\"absorbed\":\"suppress\",\"actor\":\"user\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"disposition\":\"suppress\"") != null);
    api.antfly_buffer_free(&out);
    const revision = try authority.revision(&root.db, scratch, "concept/{{uslug _entity.text}}");
    // Receipt replay cannot reapply the merge over the subsequent suppression.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(id, .fromSlice("default"), .fromSlice(proposal.id), .fromSlice("{\"accept\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(revision, try authority.revision(&root.db, scratch, "concept/{{uslug _entity.text}}"));
    api.antfly_db_close(id);
    closed = true;
    const reopened = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const reopened_id = try api.publishHandle(reopened);
    defer api.antfly_db_close(reopened_id);
    try std.testing.expectEqual(revision, try authority.revision(&reopened.db, scratch, "concept/{{uslug _entity.text}}"));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_curation_proposal_json(reopened_id, .fromSlice("default"), .fromSlice(proposal.id), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"state\":\"accepted\"") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(reopened_id, .fromSlice("default"), .fromSlice("{\"action\":\"suppress\",\"key\":\"concept/noise\"}"), &out));
    const noise = try std.json.parseFromSliceLeaky(pure.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(reopened_id, .fromSlice("default"), .fromSlice(noise.id), .fromSlice("{\"accept\":false}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(reopened_id, .fromSlice("default"), .fromSlice("{\"action\":\"suppress\",\"key\":\"concept/noise\"}"), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"state\":\"rejected\"") != null);
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_list_curation_proposals_json(reopened_id, .fromSlice("default"), .fromSlice("{\"state\":\"all\",\"limit\":1}"), &out));
    const page = try std.json.parseFromSliceLeaky(pure.ProposalPage, scratch, out.ptr.?[0..out.len], .{});
    try std.testing.expectEqual(@as(usize, 1), page.proposals.len);
    try std.testing.expect(page.next_cursor != null);
}

test "capi resolver curation recreated destination applies a fresh proposal with the same ID" {
    const api = @import("db.zig");
    const table_ops = @import("tables.zig");
    const pure = @import("antfly_resolver").curation;
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-queue-incarnation");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    try table_ops.create(root, "entities", "{}", false);
    try root.db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false });
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    const request = "{\"action\":\"suppress\",\"key\":\"concept/x\"}";
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(handle, .fromSlice("entities"), .fromSlice(request), &out));
    const proposal = try std.json.parseFromSliceLeaky(pure.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    api.antfly_buffer_free(&out);
    const original = try @import("resolver_authority.zig").queueHead(try table_ops.get(root, "entities"), scratch);
    try std.testing.expect(original.receipt_scope != null);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("entities"), .fromSlice(proposal.id), .fromSlice("{\"accept\":true}"), &out));
    api.antfly_buffer_free(&out);
    try table_ops.drop(root, "entities", false);
    try table_ops.create(root, "entities", "{}", false);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_unsuppress_resolver_key_json(handle, .fromSlice("concepts"), .fromSlice("{\"key\":\"concept/x\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(handle, .fromSlice("entities"), .fromSlice(request), &out));
    const fresh = try std.json.parseFromSliceLeaky(pure.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    try std.testing.expectEqualStrings(proposal.id, fresh.id);
    api.antfly_buffer_free(&out);
    const current = try @import("resolver_authority.zig").queueHead(try table_ops.get(root, "entities"), scratch);
    try std.testing.expect(!pure.sameReceiptScope(original.receipt_scope, current.receipt_scope));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("entities"), .fromSlice(fresh.id), .fromSlice("{\"accept\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    const configs = try root.db.core.index_manager.listResolverIdentities(scratch);
    const state = try pure.parseState(scratch, configs[0].curation_json);
    try std.testing.expectEqual(@as(usize, 1), state.suppress.keys.len);
    try std.testing.expectEqualStrings("concept/x", state.suppress.keys[0]);
    try std.testing.expect(pure.sameReceiptScope(current.receipt_scope, state.receipt_scope));
}

test "capi resolver curation finishes legacy frozen decisions before native adoption" {
    const api = @import("db.zig");
    const pure = @import("antfly_resolver").curation;
    const authority = @import("resolver_authority.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-native-legacy-adoption");
    defer tmp.cleanup();
    const root = try api.openDefaultDirectoryHandle(std.mem.span(tmp.path().ptr));
    const id = try api.publishHandle(root);
    defer api.antfly_db_close(id);
    try root.db.addResolver(.{ .name = "concepts", .table = "default", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false });
    const receipt_scope = std.fmt.bytesToHex(@as([16]u8, @splat(7)), .lower);
    const origin: pure.Origin = .{ .actor = "legacy-curator", .created_at_ms = 1 };
    const proposed = try pure.propose(scratch, .{ .receipt_scope = receipt_scope }, &.{}, .{ .action = .merge, .key = "concept/a", .into = "concept/b" }, origin);
    const proposal = proposed.state.proposals[0];
    const manifest: pure.ProposalManifest = .{ .queue = proposed.state, .pending_id = proposal.id, .pending_decision = .{ .accept = true, .actor = "legacy-curator" }, .pending_origin = origin };
    try root.db.core.store.putBatch(&.{ .{ .key = pure.proposal_store_key, .value = try std.json.Stringify.valueAlloc(scratch, manifest, .{}) }, .{ .key = @import("../storage/db/catalog/resolver_catalog.zig").minimum_reader_key, .value = "11" } }, &.{});
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(id, .fromSlice("default"), .fromSlice(proposal.id), .fromSlice("{\"accept\":true,\"actor\":\"legacy-curator\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expect(!try authority.enabled(&root.db, scratch));
    const recovered = try pure.parseProposalManifest(scratch, try root.db.core.store.get(scratch, pure.proposal_store_key));
    try std.testing.expect(recovered.pending_id == null);
    try std.testing.expectEqual(pure.ProposalState.accepted, recovered.queue.proposals[0].state);
    // The first new mutation imports the completed receipt and legacy policy.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(id, .fromSlice("default"), .fromSlice("{\"action\":\"suppress\",\"key\":\"concept/later\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_redirects_json(id, .fromSlice("default"), .fromSlice("concept/a"), .fromSlice("{}"), &out));
    try std.testing.expect(try authority.enabled(&root.db, scratch));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"canonical_key\":\"concept/b\"") != null);
    api.antfly_buffer_free(&out);
    const head = try authority.queueHead(&root.db, scratch);
    try std.testing.expect(pure.sameReceiptScope(receipt_scope, head.receipt_scope));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_get_curation_proposal_json(id, .fromSlice("default"), .fromSlice(proposal.id), &out));
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"state\":\"accepted\"") != null);
}

test "capi resolver curation native migration and consumer seeks isolate unrelated tables" {
    const api = @import("db.zig");
    const table_ops = @import("tables.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try api.TestDirectory.init("resolver-native-consumer-seek");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const root = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const handle = try api.publishHandle(root);
    defer api.antfly_db_close(handle);
    for ([_][]const u8{ "old", "new", "unrelated" }) |name| try table_ops.create(root, name, "{}", false);
    try root.db.addResolver(.{ .name = "moving", .table = "old", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "entity/{{_entity.text}}", .curation_revision = 100, .promote = false });
    try root.db.addResolver(.{ .name = "seed", .table = "new", .source_artifact = "seed-kg", .resolution_artifact = "seed-resolved", .key_template = "entity/{{_entity.text}}", .promote = false });
    var out: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_suppress_json(handle, .fromSlice("old"), .fromSlice("entity/noise"), .fromSlice("{}"), &out));
    api.antfly_buffer_free(&out);
    // An unadopted namespace on an already-native destination must use the
    // reverse bindings without opening every unrelated table.
    try root.db.addResolver(.{ .name = "unadopted", .table = "old", .source_artifact = "other-kg", .resolution_artifact = "other-resolved", .key_template = "other/{{_entity.text}}", .promote = false });
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    table_ops.closeAll(root);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_redirects_json(handle, .fromSlice("old"), .fromSlice("other/a"), .fromSlice("{}"), &out));
    try std.testing.expectEqual(@as(usize, 1), root.embedded_tables.count());
    try std.testing.expect(root.embedded_tables.contains("old"));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(handle, .fromSlice("new"), .fromSlice("{\"action\":\"suppress\",\"key\":\"entity/other\"}"), &out));
    const proposal = try std.json.parseFromSliceLeaky(@import("antfly_resolver").curation.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_suppress_json(handle, .fromSlice("new"), .fromSlice("entity/preexisting"), .fromSlice("{}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_set_resolver_namespace_json(handle, .fromSlice("moving"), .fromSlice("{\"table\":\"new\",\"key_template\":\"entity/{{_entity.text}}\",\"reset_merges\":true,\"reset_curation\":true}")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    const configs = try root.db.core.index_manager.listResolverIdentities(scratch);
    const destination = try table_ops.get(root, "new");
    for (configs) |cfg| {
        if (!std.mem.eql(u8, cfg.table, "new")) continue;
        try std.testing.expectEqual(@as(u64, 1), cfg.namespace_curation_generation);
        try std.testing.expectEqual(destination.core.identity_namespace.table_id, cfg.namespace_curation_table_id);
    }
    table_ops.closeAll(root);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_entity_merge_json(handle, .fromSlice("new"), .fromSlice("entity/a"), .fromSlice("{\"into\":\"entity/b\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(@as(usize, 1), root.embedded_tables.count());
    try std.testing.expect(root.embedded_tables.contains("new"));
    try table_ops.create(root, "review-source", "{}", false);
    try (try table_ops.get(root, "review-source")).addResolver(.{ .name = "related", .table = "new", .source_artifact = "related-kg", .resolution_artifact = "related-resolved", .key_template = "entity/{{_entity.text}}", .promote = false });
    // Complete genuine DDL recovery before measuring warm queue admission.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    table_ops.closeAll(root);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("new"), .fromSlice(proposal.id), .fromSlice("{\"accept\":false}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(@as(usize, 1), root.embedded_tables.count());
    try std.testing.expect(root.embedded_tables.contains("new"));
    // The durable queue-only witness keeps receipt replay point-scoped too.
    table_ops.closeAll(root);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("new"), .fromSlice(proposal.id), .fromSlice("{\"accept\":false}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(@as(usize, 1), root.embedded_tables.count());
    // A rejected distinct pin must retain consumer observation when the retry
    // omits pin_distinct; caller flags cannot reclassify its committed effect.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(handle, .fromSlice("new"), .fromSlice("{\"action\":\"merge\",\"key\":\"entity/c\",\"into\":\"entity/d\"}"), &out));
    const pinned = try std.json.parseFromSliceLeaky(@import("antfly_resolver").curation.Proposal, scratch, out.ptr.?[0..out.len], .{ .allocate = .alloc_always });
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("new"), .fromSlice(pinned.id), .fromSlice("{\"accept\":false,\"pin_distinct\":true}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_decide_curation_proposal_json(handle, .fromSlice("new"), .fromSlice(pinned.id), .fromSlice("{\"accept\":false}"), &out));
    const replay = try std.json.parseFromSliceLeaky(std.json.Value, scratch, out.ptr.?[0..out.len], .{});
    try std.testing.expect(replay.object.get("results").?.array.items.len != 0);
    api.antfly_buffer_free(&out);
    try table_ops.drop(root, "new", false);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    for (try root.db.core.index_manager.listResolverIdentities(scratch)) |cfg| {
        if (!std.mem.eql(u8, cfg.table, "new")) continue;
        try std.testing.expect(cfg.namespace_curation_missing);
        try std.testing.expectEqual(@as(usize, 0), cfg.merges.len);
        try std.testing.expect(std.mem.indexOf(u8, cfg.curation_json, "preexisting") == null);
    }
    try table_ops.create(root, "new", "{}", false);
    const fresh_id = (try table_ops.get(root, "new")).core.identity_namespace.table_id;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    for (try root.db.core.index_manager.listResolverIdentities(scratch)) |cfg| {
        if (!std.mem.eql(u8, cfg.table, "new")) continue;
        try std.testing.expect(cfg.namespace_curation_missing);
        try std.testing.expectEqual(fresh_id, cfg.namespace_curation_table_id);
        try std.testing.expectEqual(@as(u64, 0), cfg.namespace_curation_generation);
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_propose_curation_json(handle, .fromSlice("new"), .fromSlice("{\"action\":\"suppress\",\"key\":\"entity/fresh\"}"), &out));
    api.antfly_buffer_free(&out);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_run_until_idle(handle));
    for (try root.db.core.index_manager.listResolverIdentities(scratch)) |cfg| {
        if (!std.mem.eql(u8, cfg.table, "new")) continue;
        try std.testing.expect(!cfg.namespace_curation_missing);
        try std.testing.expectEqual(@as(u64, 0), cfg.namespace_curation_generation);
    }
}
