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

//! Immutable relational index definitions, independent of rebuild progress.
//! Prepare/encode outside the serialized commit section; stage the immutable
//! blob and CAS head in the SAME single-writer transaction as schema/outbox
//! metadata. This module never commits or publishes a query-ready generation.
//! Only publish a prebuilt execution snapshot after the caller commits.

const std = @import("std");
const schema = @import("../schema.zig");
const native = @import("../relational_index.zig");
const registry = @import("schema_registry.zig");
const Allocator = std.mem.Allocator;
const Digest = [std.crypto.hash.Blake3.digest_length]u8;

pub const head_key = "\x00\x00__metadata__:relational_index_head";
pub const blob_prefix = "\x00\x00__metadata__:relational_index_definitions:";
pub const format_version: u32 = 1;
pub const max_payload_bytes: usize = 4 * 1024 * 1024;
pub const max_index_count: usize = 4096;
const blob_header_len = 48;
const head_len = 84;

fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn blobKey(hash: Digest) [blob_prefix.len + 32]u8 {
    var key: [blob_prefix.len + 32]u8 = undefined;
    @memcpy(key[0..blob_prefix.len], blob_prefix);
    @memcpy(key[blob_prefix.len..], &hash);
    return key;
}

pub const Head = struct {
    revision: u64,
    schema_version: u32,
    blob_digest: Digest,

    pub fn encode(self: Head) [head_len]u8 {
        var out: [head_len]u8 = undefined;
        @memcpy(out[0..4], "AIDH");
        std.mem.writeInt(u32, out[4..8], format_version, .little);
        std.mem.writeInt(u64, out[8..16], self.revision, .little);
        std.mem.writeInt(u32, out[16..20], self.schema_version, .little);
        @memcpy(out[20..52], &self.blob_digest);
        @memcpy(out[52..84], &digest(out[0..52]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Head {
        if (bytes.len != head_len or !std.mem.eql(u8, bytes[0..4], "AIDH")) return error.InvalidRelationalIndexCatalog;
        if (std.mem.readInt(u32, bytes[4..8], .little) != format_version) return error.UnsupportedRelationalIndexCatalogVersion;
        if (!std.mem.eql(u8, bytes[52..84], &digest(bytes[0..52]))) return error.RelationalIndexCatalogChecksumMismatch;
        const revision = std.mem.readInt(u64, bytes[8..16], .little);
        if (revision == 0) return error.InvalidRelationalIndexCatalog;
        return .{ .revision = revision, .schema_version = std.mem.readInt(u32, bytes[16..20], .little), .blob_digest = bytes[20..52].* };
    }

    pub fn eql(self: Head, other: Head) bool {
        return self.revision == other.revision and self.schema_version == other.schema_version and
            std.mem.eql(u8, &self.blob_digest, &other.blob_digest);
    }
};

pub const Entry = struct {
    generation: u64,
    slot: u32,
    definition: native.RelationalIndexDefinition,

    pub fn id(self: Entry) native.RelationalIndexId {
        return .{ .generation = self.generation, .slot = self.slot };
    }
};

pub const Loaded = struct {
    head: Head,
    schema_digest: Digest,
    parsed: std.json.Parsed([]Entry),

    pub fn deinit(self: *Loaded) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn entries(self: *const Loaded) []const Entry {
        return self.parsed.value;
    }

    fn find(self: *const Loaded, name: []const u8) ?Entry {
        var lo: usize = 0;
        var hi = self.entries().len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.entries()[mid].definition.name, name)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return self.entries()[mid],
            }
        }
        return null;
    }
};

/// All strings and arrays in a decoded catalog are owned, not borrowed from a
/// transaction or its blob buffer. Reject noncanonical encodings at this cold
/// boundary so a definition has one physical identity.
pub fn decode(alloc: Allocator, head: Head, blob: []const u8) !Loaded {
    if (head.revision == 0 or blob.len < blob_header_len or blob.len - blob_header_len > max_payload_bytes)
        return error.InvalidRelationalIndexCatalog;
    if (!std.mem.eql(u8, &digest(blob), &head.blob_digest)) return error.RelationalIndexCatalogChecksumMismatch;
    if (!std.mem.eql(u8, blob[0..4], "AIDF")) return error.InvalidRelationalIndexCatalog;
    if (std.mem.readInt(u32, blob[4..8], .little) != format_version) return error.UnsupportedRelationalIndexCatalogVersion;
    if (std.mem.readInt(u32, blob[8..12], .little) != head.schema_version or
        std.mem.readInt(u32, blob[44..48], .little) != blob.len - blob_header_len)
        return error.InvalidRelationalIndexCatalog;
    const payload = blob[blob_header_len..];
    try preflight(alloc, payload);
    var parsed = try std.json.parseFromSlice([]Entry, alloc, payload, .{
        .allocate = .alloc_always,
        .max_value_len = max_payload_bytes,
    });
    errdefer parsed.deinit();
    try validateEntries(alloc, parsed.value, head.revision);
    const canonical = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, payload)) return error.NoncanonicalRelationalIndexCatalog;
    return .{ .head = head, .schema_digest = blob[12..44].*, .parsed = parsed };
}

/// Bound recursive expression/config parsing before the typed JSON decoder can
/// descend or allocate an unbounded number of index records.
fn preflight(alloc: Allocator, payload: []const u8) !void {
    var scanner = std.json.Scanner.initCompleteInput(alloc, payload);
    defer scanner.deinit();
    var depth: usize = 0;
    var index_count: usize = 0;
    while (true) {
        switch (try scanner.next()) {
            .object_begin, .array_begin => |tag| {
                _ = tag;
                depth += 1;
                if (depth > 64) return error.RelationalIndexCatalogLimitExceeded;
                if (depth == 2) {
                    index_count += 1;
                    if (index_count > max_index_count) return error.RelationalIndexCatalogLimitExceeded;
                }
            },
            .object_end, .array_end => depth -= 1,
            .end_of_document => break,
            else => {},
        }
    }
}

fn validateEntries(alloc: Allocator, entries: []const Entry, revision: u64) !void {
    if (entries.len > max_index_count) return error.RelationalIndexCatalogLimitExceeded;
    var identities = std.AutoHashMapUnmanaged(u128, void).empty;
    defer identities.deinit(alloc);
    for (entries, 0..) |entry, i| {
        const definition = entry.definition;
        if (entry.generation == 0 or entry.generation > revision or entry.slot >= max_index_count or definition.name.len == 0 or
            !std.unicode.utf8ValidateSlice(definition.name))
            return error.InvalidRelationalIndexCatalog;
        if (i != 0 and !std.mem.lessThan(u8, entries[i - 1].definition.name, definition.name))
            return error.DuplicateOrUnorderedRelationalIndex;
        if ((try identities.getOrPut(alloc, entry.id().mapKey())).found_existing)
            return error.DuplicateRelationalIndexId;
    }
}

pub const Prepared = struct {
    alloc: Allocator,
    expected: ?Head,
    head: Head,
    schema_digest: Digest,
    blob: []u8,
    retired: []native.RelationalIndexId,

    /// Index names are sorted once. An unchanged definition retains its physical
    /// generation; changed/new/reintroduced definitions use the new catalog
    /// revision, so dropping an index never permits its generation to be reused.
    /// A schema change conservatively creates new generations until access-
    /// method-specific dependency validation can prove a generation reusable.
    pub fn init(
        alloc: Allocator,
        view: registry.SchemaView,
        previous: ?*const Loaded,
        definitions: []const native.RelationalIndexDefinition,
    ) !Prepared {
        if (view.storageMode() != .relational) return error.InvalidRelationalIndexCatalog;
        if (definitions.len > max_index_count) return error.RelationalIndexCatalogLimitExceeded;
        const revision = try std.math.add(u64, if (previous) |p| p.head.revision else 0, 1);
        const schema_bytes = try schema.serializeSchema(alloc, view.tableSchema().*);
        defer alloc.free(schema_bytes);
        const schema_hash = digest(schema_bytes);
        const items = try alloc.alloc(Entry, definitions.len);
        defer alloc.free(items);
        for (definitions, items) |definition, *item| {
            item.* = .{ .generation = revision, .slot = 0, .definition = definition };
            if (previous) |p| {
                if (std.mem.eql(u8, &p.schema_digest, &schema_hash)) {
                    if (p.find(definition.name)) |old| {
                        const a = try std.json.Stringify.valueAlloc(alloc, definition, .{});
                        defer alloc.free(a);
                        const b = try std.json.Stringify.valueAlloc(alloc, old.definition, .{});
                        defer alloc.free(b);
                        if (std.mem.eql(u8, a, b)) {
                            item.generation = old.generation;
                            item.slot = old.slot;
                        }
                    }
                }
            }
        }
        std.mem.sort(Entry, items, {}, struct {
            fn less(_: void, a: Entry, b: Entry) bool {
                return std.mem.lessThan(u8, a.definition.name, b.definition.name);
            }
        }.less);
        for (items, 0..) |*item, i| if (item.generation == revision) {
            item.slot = @intCast(i);
        };
        try validateEntries(alloc, items, revision);
        const payload = try std.json.Stringify.valueAlloc(alloc, items, .{});
        defer alloc.free(payload);
        if (payload.len > max_payload_bytes) return error.RelationalIndexCatalogLimitExceeded;
        try preflight(alloc, payload);
        const blob = try alloc.alloc(u8, blob_header_len + payload.len);
        errdefer alloc.free(blob);
        @memcpy(blob[0..4], "AIDF");
        std.mem.writeInt(u32, blob[4..8], format_version, .little);
        std.mem.writeInt(u32, blob[8..12], view.version(), .little);
        @memcpy(blob[12..44], &schema_hash);
        std.mem.writeInt(u32, blob[44..48], @intCast(payload.len), .little);
        @memcpy(blob[blob_header_len..], payload);
        var head = Head{ .revision = revision, .schema_version = view.version(), .blob_digest = digest(blob) };
        if (previous) |p| if (std.mem.eql(u8, &p.head.blob_digest, &head.blob_digest)) {
            head = p.head; // A reordered/no-op request must not rewrite the head.
        };
        var retired = std.ArrayList(native.RelationalIndexId).empty;
        defer retired.deinit(alloc);
        var active = std.AutoHashMapUnmanaged(u128, void).empty;
        defer active.deinit(alloc);
        for (items) |item| try active.put(alloc, item.id().mapKey(), {});
        if (previous) |p| for (p.entries()) |entry| {
            if (!active.contains(entry.id().mapKey())) try retired.append(alloc, entry.id());
        };
        return .{ .alloc = alloc, .expected = if (previous) |p| p.head else null, .head = head, .schema_digest = schema_hash, .blob = blob, .retired = try retired.toOwnedSlice(alloc) };
    }

    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.blob);
        self.alloc.free(self.retired);
        self.* = undefined;
    }

    /// Stage under the backend's single-writer transaction. The caller owns
    /// commit/abort and may add schema/catalog/outbox mutations to that same
    /// transaction. On ANY error it must abort, not commit a partial stage.
    /// No in-memory state or readiness is published here.
    pub fn stage(self: *const Prepared, txn: anytype) !bool {
        const current_bytes = txn.get(head_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const current = if (current_bytes) |bytes| try Head.decode(bytes) else null;
        // Validate the target schema even on idempotent retries.
        const schema_bytes = txn.get(schema.schema_key) catch |err| switch (err) {
            error.NotFound => return error.RelationalIndexCatalogSchemaMismatch,
            else => return err,
        };
        if (!std.mem.eql(u8, &digest(schema_bytes), &self.schema_digest))
            return error.RelationalIndexCatalogSchemaMismatch;
        const unchanged = if (current) |head| head.eql(self.head) else false;
        if (unchanged) {
            const key = blobKey(self.head.blob_digest);
            const existing = txn.get(&key) catch |err| switch (err) {
                error.NotFound => return error.MissingRelationalIndexDefinitions,
                else => return err,
            };
            if (!std.mem.eql(u8, existing, self.blob)) return error.ImmutableRelationalIndexDefinitionConflict;
            return false;
        }
        if (self.expected) |expected| {
            if (current == null or !current.?.eql(expected)) return error.PreparedGenerationChanged;
        } else if (current != null) return error.PreparedGenerationChanged;
        const key = blobKey(self.head.blob_digest);
        const existing = txn.get(&key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing) |bytes| {
            if (!std.mem.eql(u8, bytes, self.blob)) return error.ImmutableRelationalIndexDefinitionConflict;
        } else try txn.put(&key, self.blob);
        const encoded_head = self.head.encode();
        try txn.put(head_key, &encoded_head);
        // The transaction that stops selecting an ID also queues its cleanup.
        const gc = @import("relational_index_gc.zig");
        for (self.retired) |id| try txn.put(&gc.key(id), &gc.initial(id));
        // Loaded views own their definitions and MVCC retains old roots.
        if (self.expected) |old| if (!std.mem.eql(u8, &old.blob_digest, &self.head.blob_digest))
            try txn.delete(&blobKey(old.blob_digest));
        return true;
    }
};

/// Read head, immutable blob, and schema from ONE read/probe transaction. Missing
/// definitions behind an existing head or a schema mismatch are hard errors.
pub fn load(alloc: Allocator, txn: anytype) !?Loaded {
    const head_bytes = txn.get(head_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const head = try Head.decode(head_bytes);
    const key = blobKey(head.blob_digest);
    const blob = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return error.MissingRelationalIndexDefinitions,
        else => return err,
    };
    var loaded = try decode(alloc, head, blob);
    errdefer loaded.deinit();
    const schema_bytes = txn.get(schema.schema_key) catch |err| switch (err) {
        error.NotFound => return error.RelationalIndexCatalogSchemaMismatch,
        else => return err,
    };
    if (!std.mem.eql(u8, &digest(schema_bytes), &loaded.schema_digest))
        return error.RelationalIndexCatalogSchemaMismatch;
    return loaded;
}

const plans = @import("relational_index_plan.zig");
const docstore = @import("../docstore.zig");

/// Executable ordered-key compilation. The durable vocabulary is intentionally
/// broader than the currently implemented executor. Do not lower a partial,
/// covering, expression, or constraint index to an unconditional plain key.
pub fn bindWritePlan(alloc: Allocator, view: registry.SchemaView, loaded: *const Loaded) !plans.View {
    const schema_bytes = try schema.serializeSchema(alloc, view.tableSchema().*);
    defer alloc.free(schema_bytes);
    if (view.version() != loaded.head.schema_version or !std.mem.eql(u8, &digest(schema_bytes), &loaded.schema_digest))
        return error.RelationalIndexCatalogSchemaMismatch;
    const definitions = try alloc.alloc(plans.Definition, loaded.entries().len);
    defer alloc.free(definitions);
    for (loaded.entries(), definitions) |entry, *target| {
        const source = entry.definition;
        if (source.access_method != .ordered_tuple or source.unique or
            source.owner_kind != .table or !std.mem.eql(u8, source.owner_name, native.relational_table_index_owner_name) or
            source.method_config_json != null or source.expressions.len != 0 or
            source.include_columns.len != 0 or source.where.len != 0 or source.where_expressions.len != 0)
            return error.UnsupportedRelationalIndexExecution;
        if (source.keys.len == 0) return error.InvalidRelationalIndexDefinition;
        // Redundant column summaries must agree with the executable key order.
        if (source.columns.len != 0) {
            if (source.columns.len != source.keys.len) return error.InvalidRelationalIndexDefinition;
            for (source.columns, source.keys) |column, key|
                if (!std.mem.eql(u8, column, key.column)) return error.InvalidRelationalIndexDefinition;
        }
        target.* = .{ .name = source.name, .generation = entry.generation, .slot = entry.slot, .keys = source.keys };
    }
    return try plans.View.init(alloc, view, definitions);
}

pub const PreparedPublication = struct {
    owner: *Controller,
    metadata: Prepared,
    plan: plans.View,

    pub fn deinit(self: *PreparedPublication) void {
        self.plan.release();
        self.metadata.deinit();
        self.* = undefined;
    }
};

pub const WriteSnapshot = struct {
    head: Head,
    plan: plans.View,

    pub fn deinit(self: *WriteSnapshot) void {
        self.plan.release();
        self.* = undefined;
    }
};

/// Copy the active definition root to an unpublished destination after its
/// immutable schema history has been copied. Retired blobs are not needed to
/// interpret physical ownership records and are deliberately omitted.
pub fn copyToUnpublished(alloc: Allocator, source: *docstore.DocStore, destination: *docstore.DocStore) !void {
    var read = try source.beginReadTxn();
    defer read.abort();
    var loaded = (try load(alloc, &read)) orelse return;
    defer loaded.deinit();
    const key = blobKey(loaded.head.blob_digest);
    const blob = try read.get(&key);
    var txn = try destination.beginWriteTxn();
    errdefer txn.abort();
    const schema_bytes = try txn.get(schema.schema_key);
    if (!std.mem.eql(u8, &digest(schema_bytes), &loaded.schema_digest)) return error.RelationalIndexCatalogSchemaMismatch;
    try txn.put(&key, blob);
    try txn.put(head_key, &loaded.head.encode());
    // Physical range copying also carries retired reverse companions. Carry
    // their cleanup authority, but restart each cursor in the new owner range.
    const gc = @import("relational_index_gc.zig");
    var cursor = try read.openCursor();
    defer cursor.close();
    var pending = try cursor.seekAtOrAfter(gc.prefix);
    while (pending) |entry| : (pending = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, gc.prefix)) break;
        const id = try native.RelationalIndexId.decode(entry.key[gc.prefix.len..]);
        _ = try gc.decode(id, entry.value);
        try txn.put(entry.key, &gc.initial(id));
    }
    try txn.commit();
}

/// Owns a published WRITE-preparation snapshot, not a query-readiness proof.
/// Schema/restore callers must hold their existing DB mutation fence while
/// committing catalog changes. The small publication mutex protects retain/swap
/// against request acquisition; compilation and encoding happen outside it.
/// One acquire per worker/batch suffices; there is no per-row lock.
pub const Controller = struct {
    alloc: Allocator,
    io: std.Io,
    store: *docstore.DocStore,
    mutex: std.Io.Mutex = .init,
    current: ?WriteSnapshot = null,

    /// Also used against an unpublished restore store. All allocations and
    /// schema/definition validation happen before the publication boundary.
    pub fn loadSnapshot(alloc: Allocator, store: *docstore.DocStore, view: ?registry.SchemaView) !?WriteSnapshot {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        if (try load(alloc, &txn)) |value| {
            var loaded = value;
            defer loaded.deinit();
            const pinned = view orelse return error.RelationalIndexCatalogSchemaMismatch;
            return .{ .head = loaded.head, .plan = try bindWritePlan(alloc, pinned, &loaded) };
        }
        return null;
    }

    pub fn init(alloc: Allocator, io: std.Io, store: *docstore.DocStore, view: ?registry.SchemaView) !Controller {
        return .{ .alloc = alloc, .io = io, .store = store, .current = try loadSnapshot(alloc, store, view) };
    }

    /// Stop acquisition before destroying the controller. Already acquired
    /// batches/snapshots remain valid through their independent references.
    pub fn deinit(self: *Controller) void {
        if (self.current) |*current| current.deinit();
        self.* = undefined;
    }

    pub fn acquire(self: *Controller) ?WriteSnapshot {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const current = self.current orelse return null;
        return .{ .head = current.head, .plan = current.plan.clone() };
    }

    pub fn isCurrent(self: *Controller, pinned: ?WriteSnapshot) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const current = self.current orelse return pinned == null;
        const expected = pinned orelse return false;
        return current.head.eql(expected.head) and current.plan.snapshot == expected.plan.snapshot;
    }

    /// Transfer an already validated snapshot under the caller's publication
    /// fence. Rebind even an unchanged durable head to its newly published
    /// schema epoch; head equality alone does not establish runtime identity.
    pub fn replacePrepared(self: *Controller, next: *?WriteSnapshot) void {
        self.mutex.lockUncancelable(self.io);
        var retired = self.current;
        self.current = next.*;
        next.* = null;
        self.mutex.unlock(self.io);
        if (retired) |*snapshot| snapshot.deinit();
    }

    pub fn publishCommitted(self: *Controller, candidate: *const PreparedPublication) void {
        std.debug.assert(candidate.owner == self);
        var next: ?WriteSnapshot = .{ .head = candidate.metadata.head, .plan = candidate.plan.clone() };
        self.replacePrepared(&next);
    }

    /// Recompile the durable definitions against a proposed schema before
    /// taking apply-exclusive. Does not create a catalog for unindexed tables.
    pub fn prepareSchemaChange(self: *Controller, view: registry.SchemaView) !?PreparedPublication {
        var previous = blk: {
            var txn = try self.store.beginReadTxn();
            defer txn.abort();
            break :blk (try load(self.alloc, &txn)) orelse return null;
        };
        defer previous.deinit();
        const definitions = try self.alloc.alloc(native.RelationalIndexDefinition, previous.entries().len);
        defer self.alloc.free(definitions);
        for (previous.entries(), definitions) |entry, *definition| definition.* = entry.definition;
        var metadata = try Prepared.init(self.alloc, view, &previous, definitions);
        errdefer metadata.deinit();
        var next = try decode(self.alloc, metadata.head, metadata.blob);
        defer next.deinit();
        return .{ .owner = self, .metadata = metadata, .plan = try bindWritePlan(self.alloc, view, &next) };
    }

    pub fn validateExtraMetadata(writes: []const docstore.KVPair, deletes: []const []const u8) !void {
        for (writes) |write| if (isReservedMetadataKey(write.key)) return error.ReservedRelationalIndexMetadataKey;
        for (deletes) |key| if (isReservedMetadataKey(key)) return error.ReservedRelationalIndexMetadataKey;
    }

    pub fn isReservedMetadataKey(key: []const u8) bool {
        return std.mem.eql(u8, key, head_key) or std.mem.startsWith(u8, key, blob_prefix) or
            std.mem.eql(u8, key, @import("relational_constraint_jobs.zig").progress_key) or
            std.mem.startsWith(u8, key, @import("relational_index_jobs.zig").progress_prefix) or
            std.mem.startsWith(u8, key, @import("relational_index_gc.zig").prefix);
    }

    pub fn prepare(
        self: *Controller,
        view: registry.SchemaView,
        definitions: []const native.RelationalIndexDefinition,
    ) !PreparedPublication {
        var previous = blk: {
            var txn = try self.store.beginReadTxn();
            defer txn.abort();
            break :blk try load(self.alloc, &txn);
        };
        defer if (previous) |*loaded| loaded.deinit();
        var metadata = try Prepared.init(self.alloc, view, if (previous) |*loaded| loaded else null, definitions);
        errdefer metadata.deinit();
        var next = try decode(self.alloc, metadata.head, metadata.blob);
        defer next.deinit();
        return .{ .owner = self, .metadata = metadata, .plan = try bindWritePlan(self.alloc, view, &next) };
    }

    /// Extra writes (e.g. HA/outbox) share the catalog transaction. Reserved
    /// definition/schema keys cannot be overwritten through this argument.
    /// Idempotent retries preserve the existing snapshot and do not replay
    /// side effects. A failed transaction leaves the published snapshot intact.
    pub fn commit(self: *Controller, candidate: *const PreparedPublication, extra_writes: []const docstore.KVPair) !bool {
        if (candidate.owner != self) return error.PreparedGenerationChanged;
        try validateExtraMetadata(extra_writes, &.{});
        for (extra_writes) |write| {
            if (std.mem.eql(u8, write.key, schema.schema_key))
                return error.ReservedRelationalIndexMetadataKey;
        }
        var changed = false;
        const retired = blk: {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            var txn = try self.store.beginWriteTxn();
            errdefer txn.abort();
            changed = try candidate.metadata.stage(&txn);
            if (changed) {
                for (extra_writes) |write| try txn.put(write.key, write.value);
                try txn.commit();
            } else txn.abort();
            if (self.current) |current| if (current.head.eql(candidate.metadata.head)) return changed;
            const old = self.current;
            // Both the plan and all its ownership are prepared before commit.
            self.current = .{ .head = candidate.metadata.head, .plan = candidate.plan.clone() };
            break :blk old;
        };
        // Potentially large retired definitions are freed outside acquisition.
        if (retired) |value| {
            var old = value;
            old.deinit();
        }
        return changed;
    }
};

fn testSchemaParticipant(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    _ = try schema.saveSchema(&store, alloc, test_schema);
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var controller = try Controller.init(alloc, std.testing.io, &store, view);
    defer controller.deinit();
    var initial = try controller.prepare(view, test_definitions[0..1]);
    defer initial.deinit();
    try std.testing.expect(try controller.commit(&initial, &.{}));
    var pinned = controller.acquire().?;
    defer pinned.deinit();
    var next_schema = test_schema;
    next_schema.version += 1;
    var next_schemas = try registry.Registry.initCloned(alloc, std.testing.io, next_schema);
    defer next_schemas.deinit();
    var next_view = next_schemas.acquire().?;
    defer next_view.release();
    var candidate = (try controller.prepareSchemaChange(next_view)).?;
    defer candidate.deinit();
    const encoded = try schema.serializeSchema(alloc, next_schema);
    defer alloc.free(encoded);
    const outbox = "\x00\x00__metadata__:test_schema_index_outbox";
    const FailAfterStage = struct {
        metadata: *const Prepared,
        pub fn stage(self: @This(), txn: anytype) !bool {
            _ = try self.metadata.stage(txn);
            return error.InjectedSchemaCatalogFailure;
        }
    };
    try std.testing.expectError(error.InjectedSchemaCatalogFailure, schema.saveEncodedSchemaWithMetadataAndStage(
        &store,
        alloc,
        next_schema.version,
        encoded,
        &.{.{ .key = outbox, .value = "pending" }},
        &.{},
        FailAfterStage{ .metadata = &candidate.metadata },
    ));
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var durable = (try load(alloc, &txn)).?;
        defer durable.deinit();
        try std.testing.expectEqual(view.version(), durable.head.schema_version);
        try std.testing.expectError(error.NotFound, txn.get(outbox));
        const version_key = try schema.schemaVersionKeyAlloc(alloc, next_schema.version);
        defer alloc.free(version_key);
        try std.testing.expectError(error.NotFound, txn.get(version_key));
    }
    try std.testing.expect(controller.isCurrent(pinned));
    try std.testing.expect(try schema.saveEncodedSchemaWithMetadataAndStage(
        &store,
        alloc,
        next_schema.version,
        encoded,
        &.{.{ .key = outbox, .value = "pending" }},
        &.{},
        &candidate.metadata,
    ));
    controller.publishCommitted(&candidate);
    try std.testing.expect(!controller.isCurrent(pinned));
    var current = controller.acquire().?;
    defer current.deinit();
    try std.testing.expectEqual(next_view.epoch, current.plan.schemaView().epoch);
    try std.testing.expectEqual(view.epoch, pinned.plan.schemaView().epoch);
    // A fresh registry with identical durable bytes still needs a new retained
    // runtime plan. Otherwise exact-epoch prepared rows would fail after restore.
    var replacement = try registry.Registry.initCloned(alloc, std.testing.io, next_schema);
    defer replacement.deinit();
    var replacement_view = replacement.acquire().?;
    defer replacement_view.release();
    var rebind = (try controller.prepareSchemaChange(replacement_view)).?;
    defer rebind.deinit();
    try std.testing.expect(rebind.metadata.head.eql(current.head));
    try std.testing.expect(!try schema.saveEncodedSchemaWithMetadataAndStage(
        &store,
        alloc,
        next_schema.version,
        encoded,
        &.{},
        &.{},
        &rebind.metadata,
    ));
    controller.publishCommitted(&rebind);
    try std.testing.expect(!controller.isCurrent(current));
    var restored = try Controller.loadSnapshot(alloc, &store, replacement_view);
    defer if (restored) |*snapshot| snapshot.deinit();
    try std.testing.expectEqual(replacement_view.epoch, restored.?.plan.schemaView().epoch);
    try std.testing.expectError(error.RelationalIndexCatalogSchemaMismatch, Controller.loadSnapshot(alloc, &store, view));
    try std.testing.expectError(error.RelationalIndexCatalogSchemaMismatch, Controller.loadSnapshot(alloc, &store, null));
}

test "relational index catalog shares schema commit rollback and rebinds identical durable epochs" {
    try testSchemaParticipant(@import("../mem_backend.zig").Backend);
    try testSchemaParticipant(@import("../lsm_backend.zig").Backend);
}

const test_columns = [_]schema.RelationalColumn{
    .{ .name = "id", .path = "id", .column_type = .integer },
    .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
};
const test_schema = schema.TableSchema{ .version = 7, .storage_mode = .relational, .relational_columns = &test_columns };
const test_definitions = [_]native.RelationalIndexDefinition{
    .{ .name = "a_id", .owner_kind = .table, .owner_name = native.relational_table_index_owner_name, .access_method = .ordered_tuple, .keys = &.{.{ .column = "id" }} },
    .{ .name = "b_label", .owner_kind = .table, .owner_name = native.relational_table_index_owner_name, .access_method = .ordered_tuple, .unique = true, .include_columns = &.{"id"}, .keys = &.{.{ .column = "label", .direction = .desc, .nulls = .last, .collation = "ci" }}, .where = &.{.{ .field = "label", .op = .is_not_null }} },
};

fn testCodecAllocations(alloc: Allocator) !void {
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var prepared = try Prepared.init(alloc, view, null, &test_definitions);
    defer prepared.deinit();
    var decoded = try decode(alloc, prepared.head, prepared.blob);
    defer decoded.deinit();
    var retry = try Prepared.init(alloc, view, &decoded, &.{ test_definitions[1], test_definitions[0] });
    defer retry.deinit();
    try std.testing.expect(prepared.head.eql(retry.head));
    try std.testing.expectEqual(@as(usize, 2), decoded.entries().len);
    const second = decoded.entries()[1].definition;
    try std.testing.expect(second.unique);
    try std.testing.expectEqualStrings("id", second.include_columns[0]);
    try std.testing.expectEqual(native.RelationalIndexKeyDirection.desc, second.keys[0].direction);
    try std.testing.expectEqual(native.RelationalIndexKeyNulls.last, second.keys[0].nulls);
    try std.testing.expectEqualStrings("ci", second.keys[0].collation.?);
    try std.testing.expectEqual(native.UniquePredicateOp.is_not_null, second.where[0].op);
}

test "relational index catalog preserves full definitions and cleans every allocation failure" {
    try testCodecAllocations(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testCodecAllocations, .{});
}

test "relational index catalog definitions exclude mutable generation progress" {
    const alloc = std.testing.allocator;
    var index = native.RelationalIndex{
        .name = "a_id",
        .owner_kind = .table,
        .owner_name = native.relational_table_index_owner_name,
        .access_method = .ordered_tuple,
        .keys = &.{.{ .column = "id" }},
        .lifecycle = .building,
        .generation = 3,
        .generation_record = .{ .generation = 3, .lifecycle = .building, .lag = 100 },
    };
    const before = try std.json.Stringify.valueAlloc(alloc, native.RelationalIndexDefinition.fromIndex(index), .{});
    defer alloc.free(before);
    index.generation_record.?.lag = 0;
    index.generation_record.?.rebuild_cursor = "page-200";
    index.generation_record.?.lifecycle = .ready;
    index.lifecycle = .ready;
    const after = try std.json.Stringify.valueAlloc(alloc, native.RelationalIndexDefinition.fromIndex(index), .{});
    defer alloc.free(after);
    try std.testing.expectEqualStrings(before, after);
}

test "relational index catalog rejects corruption noncanonical bytes and unbounded nesting" {
    const alloc = std.testing.allocator;
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var prepared = try Prepared.init(alloc, view, null, &test_definitions);
    defer prepared.deinit();
    const corrupt = try alloc.dupe(u8, prepared.blob);
    defer alloc.free(corrupt);
    corrupt[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.RelationalIndexCatalogChecksumMismatch, decode(alloc, prepared.head, corrupt));
    const spaced = try std.mem.concat(alloc, u8, &.{ prepared.blob, " " });
    defer alloc.free(spaced);
    std.mem.writeInt(u32, spaced[44..48], @intCast(spaced.len - blob_header_len), .little);
    var alternate = prepared.head;
    alternate.blob_digest = digest(spaced);
    try std.testing.expectError(error.NoncanonicalRelationalIndexCatalog, decode(alloc, alternate, spaced));
    try std.testing.expectError(error.RelationalIndexCatalogLimitExceeded, preflight(alloc, "[" ** 65 ++ "]" ** 65));
    try std.testing.expectError(error.DuplicateOrUnorderedRelationalIndex, Prepared.init(alloc, view, null, &.{ test_definitions[0], test_definitions[0] }));
    const encoded_head = prepared.head.encode();
    try std.testing.expect(prepared.head.eql(try Head.decode(&encoded_head)));
    var corrupt_head = encoded_head;
    corrupt_head[8] ^= 1;
    try std.testing.expectError(error.RelationalIndexCatalogChecksumMismatch, Head.decode(&corrupt_head));
    for (0..encoded_head.len) |len|
        try std.testing.expectError(error.InvalidRelationalIndexCatalog, Head.decode(encoded_head[0..len]));
}

fn testCatalogTransaction(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    const DocStore = @import("../docstore.zig").DocStore;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    _ = try schema.saveSchema(&store, alloc, test_schema);
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var prepared = try Prepared.init(alloc, view, null, &test_definitions);
    defer prepared.deinit();
    var stale = try Prepared.init(alloc, view, null, test_definitions[0..1]);
    defer stale.deinit();
    const outbox_key = "\x00\x00__metadata__:test_index_outbox";

    {
        // A failure after the blob put but before the head put must still be
        // aborted as one transaction, never exposed as a partial catalog.
        const FailingTxn = struct {
            txn: *DocStore.Txn,
            puts: usize = 0,
            pub fn delete(self: *@This(), item: []const u8) !void {
                try self.txn.delete(item);
            }

            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                return self.txn.get(key);
            }

            pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
                self.puts += 1;
                if (self.puts == 2) return error.OutOfMemory;
                try self.txn.put(key, value);
            }
        };
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var failing = FailingTxn{ .txn = &txn };
        try std.testing.expectError(error.OutOfMemory, prepared.stage(&failing));
        try std.testing.expectEqual(@as(usize, 2), failing.puts);
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect((try load(alloc, &txn)) == null);
        const key = blobKey(prepared.head.blob_digest);
        try std.testing.expectError(error.NotFound, txn.get(&key));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expect(try prepared.stage(&txn));
        try txn.put(outbox_key, "pending");
        // Abandon everything, including the immutable blob, before publication.
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expect((try load(alloc, &txn)) == null);
        try std.testing.expectError(error.NotFound, txn.get(outbox_key));
        const key = blobKey(prepared.head.blob_digest);
        try std.testing.expectError(error.NotFound, txn.get(&key));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try prepared.stage(&txn));
        try txn.put(outbox_key, "pending");
        try txn.commit();
    }
    var loaded = blk: {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualStrings("pending", try txn.get(outbox_key));
        break :blk (try load(alloc, &txn)).?;
    };
    defer loaded.deinit();
    // Strings remain valid after their source transaction has closed.
    try std.testing.expectEqualStrings("b_label", loaded.entries()[1].definition.name);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.PreparedGenerationChanged, stale.stage(&txn));
        try std.testing.expect(!(try prepared.stage(&txn)));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(schema.schema_key, "different schema with the same version");
        try std.testing.expectError(error.RelationalIndexCatalogSchemaMismatch, prepared.stage(&txn));
    }
}

test "relational index catalog CAS and outbox staging are atomic on memory and LSM backends" {
    try testCatalogTransaction(@import("../mem_backend.zig").Backend);
    try testCatalogTransaction(@import("../lsm_backend.zig").Backend);
}

test "relational index catalog generation identity survives changes drop and reintroduction" {
    const alloc = std.testing.allocator;
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var first = try Prepared.init(alloc, view, null, &test_definitions);
    defer first.deinit();
    var original = try decode(alloc, first.head, first.blob);
    defer original.deinit();
    var definitions = test_definitions;
    definitions[0].keys = &.{.{ .column = "id", .direction = .desc }};
    var second = try Prepared.init(alloc, view, &original, &definitions);
    defer second.deinit();
    var changed = try decode(alloc, second.head, second.blob);
    defer changed.deinit();
    try std.testing.expectEqual(@as(u64, 2), changed.entries()[0].generation);
    try std.testing.expectEqual(@as(u64, 1), changed.entries()[1].generation);
    var drop = try Prepared.init(alloc, view, &changed, &.{});
    defer drop.deinit();
    var empty = try decode(alloc, drop.head, drop.blob);
    defer empty.deinit();
    var readd = try Prepared.init(alloc, view, &empty, &test_definitions);
    defer readd.deinit();
    var added = try decode(alloc, readd.head, readd.blob);
    defer added.deinit();
    try std.testing.expectEqual(@as(u64, 4), added.head.revision);
    for (added.entries()) |entry| try std.testing.expectEqual(@as(u64, 4), entry.generation);
}

test "relational index catalog physical identities survive name ordering and reject aliases" {
    const alloc = std.testing.allocator;
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var first = try Prepared.init(alloc, view, null, &test_definitions);
    defer first.deinit();
    var original = try decode(alloc, first.head, first.blob);
    defer original.deinit();
    try std.testing.expectEqual(@as(u32, 0), original.entries()[0].slot);
    try std.testing.expectEqual(@as(u32, 1), original.entries()[1].slot);
    var earlier = test_definitions[0];
    earlier.name = "0_before_existing";
    var next = try Prepared.init(alloc, view, &original, &.{ test_definitions[1], earlier, test_definitions[0] });
    defer next.deinit();
    var loaded = try decode(alloc, next.head, next.blob);
    defer loaded.deinit();
    for (original.entries()) |entry| {
        const retained = loaded.find(entry.definition.name).?;
        try std.testing.expectEqual(entry.id().mapKey(), retained.id().mapKey());
        try std.testing.expectEqual(entry.id(), try native.RelationalIndexId.decode(&entry.id().encode()));
    }
    try std.testing.expectEqual(@as(u64, 2), loaded.entries()[0].generation);
    try std.testing.expectEqual(@as(u32, 0), loaded.entries()[0].slot);
    // The new and first old entry share a slot, but never a full generation ID.
    var aliases = [_]Entry{ original.entries()[0], original.entries()[1] };
    aliases[1].slot = aliases[0].slot;
    try std.testing.expectError(error.DuplicateRelationalIndexId, validateEntries(alloc, &aliases, original.head.revision));
    try std.testing.expectError(error.InvalidRelationalIndexId, native.RelationalIndexId.decode(&([_]u8{0} ** 12)));
}

fn testPublication(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    _ = try schema.saveSchema(&store, alloc, test_schema);
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var controller = try Controller.init(alloc, std.testing.io, &store, view);
    defer controller.deinit();
    try std.testing.expect(controller.acquire() == null);
    try std.testing.expectError(error.UnsupportedRelationalIndexExecution, controller.prepare(view, &test_definitions));
    try std.testing.expect(controller.acquire() == null);
    var first = try controller.prepare(view, test_definitions[0..1]);
    defer first.deinit();
    var stale = try controller.prepare(view, &.{});
    defer stale.deinit();
    // No publication before the durable transaction.
    try std.testing.expect(controller.acquire() == null);
    const outbox = "\x00\x00__metadata__:test_publication_outbox";
    try std.testing.expect(try controller.commit(&first, &.{.{ .key = outbox, .value = "once" }}));
    var pinned = controller.acquire().?;
    defer pinned.deinit();
    var batch = plans.Batch.init(alloc, pinned.plan);
    defer batch.deinit();
    try std.testing.expect(pinned.head.eql(first.metadata.head));
    try std.testing.expectError(error.PreparedGenerationChanged, controller.commit(&stale, &.{}));
    try std.testing.expect(!(try controller.commit(&first, &.{.{ .key = outbox, .value = "must-not-repeat" }})));
    {
        var retry = controller.acquire().?;
        defer retry.deinit();
        try std.testing.expect(batch.isForPlan(retry.plan));
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualStrings("once", try txn.get(outbox));
    }
    var definitions = [_]native.RelationalIndexDefinition{test_definitions[0]};
    definitions[0].keys = &.{ .{ .column = "id" }, .{ .column = "label", .direction = .desc } };
    var second = try controller.prepare(view, &definitions);
    defer second.deinit();
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, controller.commit(&second, &.{.{ .key = head_key, .value = "bad" }}));
    try std.testing.expect(try controller.commit(&second, &.{}));
    var current = controller.acquire().?;
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 2), current.head.revision);
    try std.testing.expectEqual(@as(u64, 1), pinned.head.revision);
    try std.testing.expect(!batch.isForPlan(current.plan));
    var prepared_row = try @import("document_mapper.zig").PreparedRelationalWrite.init(alloc, "row", "{\"id\":7,\"label\":\"a\"}", null, view.tableSchema().*, view.physicalLayout());
    defer prepared_row.deinit(alloc);
    // The retired snapshot remains usable for its owner, but is fenced out
    // from committing by the current-plan identity above.
    _ = try batch.appendPrepared(&prepared_row);
    try std.testing.expectEqual(@as(usize, 9), (try batch.key(0, 0)).bytes.len);
    var reopened = try Controller.init(alloc, std.testing.io, &store, view);
    defer reopened.deinit();
    var recovered = reopened.acquire().?;
    defer recovered.deinit();
    try std.testing.expect(recovered.head.eql(current.head));
    try std.testing.expectEqualSlices(u8, &current.plan.fingerprint(), &recovered.plan.fingerprint());
    try std.testing.expectError(error.PreparedGenerationChanged, reopened.commit(&second, &.{}));
}

test "relational index catalog publishes prepared snapshots only after atomic commit" {
    try testPublication(@import("../mem_backend.zig").Backend);
    try testPublication(@import("../lsm_backend.zig").Backend);
}

fn testPublicationAllocations(alloc: Allocator, store: *docstore.DocStore, view: registry.SchemaView) !void {
    var controller = try Controller.init(alloc, std.testing.io, store, view);
    defer controller.deinit();
    var prepared = try controller.prepare(view, test_definitions[0..1]);
    defer prepared.deinit();
    try std.testing.expect(controller.acquire() == null);
}

test "relational index catalog prepared publication cleans every allocation failure" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    _ = try schema.saveSchema(&store, alloc, test_schema);
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    try std.testing.checkAllAllocationFailures(alloc, testPublicationAllocations, .{ &store, view });
    var txn = try store.beginReadTxn();
    defer txn.abort();
    try std.testing.expect((try load(alloc, &txn)) == null);
}

test "relational index catalog survives LSM reopen and rejects incomplete durable state" {
    const alloc = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    const DocStore = @import("../docstore.zig").DocStore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var schemas = try registry.Registry.initCloned(alloc, std.testing.io, test_schema);
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var prepared = try Prepared.init(alloc, view, null, &test_definitions);
    defer prepared.deinit();
    const outbox_key = "\x00\x00__metadata__:test_index_outbox";
    {
        var backend = try Backend.open(alloc, path, .{ .flush_threshold = 2, .wal_enabled = true });
        defer backend.close();
        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();
        var store = try DocStore.openRuntime(alloc, &runtime);
        defer store.close();
        _ = try schema.saveSchema(&store, alloc, test_schema);
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try prepared.stage(&txn));
        try txn.put(outbox_key, "committed-with-definitions");
        try txn.commit();
    }
    var backend = try Backend.open(alloc, path, .{ .flush_threshold = 2, .wal_enabled = true });
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var restored = (try load(alloc, &txn)).?;
        defer restored.deinit();
        try std.testing.expect(restored.head.eql(prepared.head));
        try std.testing.expectEqualStrings("committed-with-definitions", try txn.get(outbox_key));
    }
    const key = blobKey(prepared.head.blob_digest);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.delete(&key);
        try std.testing.expectError(error.MissingRelationalIndexDefinitions, load(alloc, &txn));
        try std.testing.expectError(error.MissingRelationalIndexDefinitions, prepared.stage(&txn));
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(&key, "corrupt");
        try std.testing.expectError(error.ImmutableRelationalIndexDefinitionConflict, prepared.stage(&txn));
    }
}
