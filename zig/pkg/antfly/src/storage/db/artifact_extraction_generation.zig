// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Immutable named outputs over the shared staged-generation lifecycle.
//! Name and ordinal indexes commit with payload progress. Enumeration and GC
//! read metadata only; point lookup reads one index entry and one payload.
//! Producer authorization and semantic output validation belong to the ordered
//! caller. This module alone is not permission to publish an extraction head.
const std = @import("std");
const generations = @import("artifact_chunk_generation.zig");
const scopes = @import("artifact_generation_scope.zig");
const manifest = @import("artifact_chunk_manifest.zig");
const publication = @import("artifact_publication.zig");
const keys = @import("../internal_keys.zig");
const Digest = publication.Digest;

pub const Entry = struct { name: []const u8, value: []const u8 };
const max_name_bytes = 1024 * 1024;

/// Typed directory names cannot alias roots, navigation blocks, or unit IDs
/// containing separators. The immutable scope already owns doc/producer IDs.
pub fn unitNameAlloc(alloc: std.mem.Allocator, unit: []const u8) ![]u8 {
    if (unit.len == 0 or unit.len >= max_name_bytes) return error.InvalidBatchRequest;
    const name = try alloc.alloc(u8, unit.len + 1);
    name[0] = 1;
    @memcpy(name[1..], unit);
    return name;
}

pub fn headKeyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    const head = try scopes.extractionKeyAlloc(alloc, document, producer);
    head[keys.findComponentTerminator(head, 1).? + 2] = keys.extraction_generation_head_kind;
    return head;
}

/// Resolve units through the selected generation, including authoritative
/// absence. Only a missing head permits physical-row fallback. Returned bytes
/// borrow the pinned transaction; the input owns its head guard key.
pub fn captureInput(alloc: std.mem.Allocator, txn: anytype, logical: []const u8) !generations.Input {
    const ids = @import("artifact_ids.zig");
    var ref = (try ids.decodeArtifactRefAlloc(alloc, logical)) orelse return generations.captureInput(alloc, txn, logical);
    defer ref.deinit(alloc);
    if (ref.kind != .asset or ref.unit_id == null or ref.chunk_id != null) return generations.captureInput(alloc, txn, logical);
    var input: generations.Input = .{ .alloc = alloc, .require_absence_proof = true };
    errdefer input.deinit();
    input.head_key = try headKeyAlloc(alloc, ref.document_id, ref.name);
    input.head_value = txn.get(input.head_key.?) catch |err| if (err == error.NotFound) null else return err;
    if (input.head_value) |raw| {
        const scope = try scopes.extractionKeyAlloc(alloc, ref.document_id, ref.name);
        defer alloc.free(scope);
        var view = try View(@TypeOf(txn.*)).fromHead(alloc, txn, scope, raw);
        defer view.deinit();
        const name = try unitNameAlloc(alloc, ref.unit_id.?);
        defer alloc.free(name);
        input.value = try view.get(alloc, name);
    } else input.value = txn.get(logical) catch |err| if (err == error.NotFound) null else return err;
    return input;
}

/// A resumable position, not proof that a producer or consumer completed.
pub const Position = struct {
    generation: Digest,
    next_ordinal: u32,
    pub fn encode(self: Position) [72]u8 {
        var raw: [72]u8 = undefined;
        @memcpy(raw[0..4], "AEP1");
        @memcpy(raw[4..36], &self.generation);
        std.mem.writeInt(u32, raw[36..40], self.next_ordinal, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0..40], raw[40..72], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Position {
        if (raw.len != 72 or !std.mem.eql(u8, raw[0..4], "AEP1")) return error.ArtifactCatalogCorrupt;
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..40], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[40..72])) return error.ArtifactCatalogCorrupt;
        return .{ .generation = raw[4..36].*, .next_ordinal = std.mem.readInt(u32, raw[36..40], .little) };
    }
};

/// The logical name participates in the output digest, not just its lookup
/// index. Reordering or renaming identical values changes the generation.
pub fn encodeEntry(alloc: std.mem.Allocator, entry: Entry) ![]u8 {
    if (entry.name.len == 0 or entry.name.len > max_name_bytes or entry.value.len > publication.max_payload_bytes or entry.name.len +| entry.value.len +| 48 > publication.max_payload_bytes) return error.InvalidBatchRequest;
    const raw = try alloc.alloc(u8, 48 + entry.name.len + entry.value.len);
    @memcpy(raw[0..4], "AER1");
    std.mem.writeInt(u32, raw[4..8], @intCast(entry.name.len), .little);
    std.mem.writeInt(u64, raw[8..16], entry.value.len, .little);
    @memcpy(raw[16 .. 16 + entry.name.len], entry.name);
    @memcpy(raw[16 + entry.name.len .. raw.len - 32], entry.value);
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    return raw;
}

pub fn decodeEntry(raw: []const u8) !Entry {
    if (raw.len < 48 or raw.len > publication.max_payload_bytes or !std.mem.eql(u8, raw[0..4], "AER1")) return error.ArtifactCatalogCorrupt;
    const name_bytes: usize = std.mem.readInt(u32, raw[4..8], .little);
    const value_bytes = std.mem.readInt(u64, raw[8..16], .little);
    if (name_bytes == 0 or name_bytes > max_name_bytes or name_bytes > raw.len - 48 or value_bytes != raw.len - 48 - name_bytes) return error.ArtifactCatalogCorrupt;
    var checksum: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    return .{ .name = raw[16 .. 16 + name_bytes], .value = raw[16 + name_bytes .. raw.len - 32] };
}

pub const Descriptor = struct {
    ordinal: u32,
    name: []const u8,
    digest: Digest,
    fn checksum(id: Digest, raw: []const u8) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:extraction-directory:v1:");
        hash.update(&id);
        hash.update(raw);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
    fn encode(self: Descriptor, alloc: std.mem.Allocator, id: Digest) ![]u8 {
        const raw = try alloc.alloc(u8, 76 + self.name.len);
        @memcpy(raw[0..4], "AED1");
        std.mem.writeInt(u32, raw[4..8], self.ordinal, .little);
        std.mem.writeInt(u32, raw[8..12], @intCast(self.name.len), .little);
        @memcpy(raw[12..44], &self.digest);
        @memcpy(raw[44 .. raw.len - 32], self.name);
        @memcpy(raw[raw.len - 32 ..], &checksum(id, raw[0 .. raw.len - 32]));
        return raw;
    }
    fn decode(raw: []const u8, id: Digest) !Descriptor {
        if (raw.len <= 76 or raw.len > 76 + max_name_bytes or !std.mem.eql(u8, raw[0..4], "AED1") or
            std.mem.readInt(u32, raw[8..12], .little) != raw.len - 76 or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(id, raw[0 .. raw.len - 32]))) return error.ArtifactCatalogCorrupt;
        return .{ .ordinal = std.mem.readInt(u32, raw[4..8], .little), .name = raw[44 .. raw.len - 32], .digest = raw[12..44].* };
    }
};

pub const Plan = struct {
    core: generations.Plan,
    names: []const u8,
    ordinals: []const u8,
    progress: []const u8,

    pub fn init(alloc: std.mem.Allocator, scope: []const u8, spec: generations.Spec) !Plan {
        if (scopes.family(scope) != .extraction) return error.InvalidBatchRequest;
        var core = try generations.Plan.init(alloc, scope, spec);
        errdefer core.deinit();
        const owned = core.arena.allocator();
        const kind = keys.findComponentTerminator(scope, 1).? + 2;
        const names = try owned.dupe(u8, core.row_prefix);
        names[kind] = keys.extraction_generation_name_kind;
        const ordinals = try owned.dupe(u8, core.row_prefix);
        ordinals[kind] = keys.extraction_generation_ordinal_kind;
        const progress = try owned.dupe(u8, core.state_key);
        progress[kind] = keys.extraction_generation_directory_kind;
        return .{ .core = core, .names = names, .ordinals = ordinals, .progress = progress };
    }
    pub fn deinit(self: *Plan) void {
        self.core.deinit();
        self.* = undefined;
    }
    fn directoryProgress(self: *const Plan, txn: anytype) !manifest.Manifest {
        const raw = txn.get(self.progress) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
        return manifest.Manifest.decode(raw);
    }
    pub fn begin(self: *const Plan, txn: anytype) !bool {
        const changed = try self.core.begin(txn);
        if (changed) try txn.put(self.progress, &manifest.Builder.init().finish().encode()) else {
            if (!std.meta.eql(try self.directoryProgress(txn), (try self.core.load(txn)).progress)) return error.ArtifactCatalogCorrupt;
        }
        return changed;
    }
    pub fn publish(self: *const Plan, txn: anytype, expected_head: ?Digest, guard: anytype) !bool {
        if (!std.meta.eql(try self.directoryProgress(txn), self.core.spec.output)) return error.ArtifactPublicationPending;
        return self.core.publish(txn, expected_head, guard);
    }
    fn nameKey(self: *const Plan, alloc: std.mem.Allocator, name: []const u8) ![]u8 {
        if (name.len == 0 or name.len > max_name_bytes) return error.InvalidBatchRequest;
        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(alloc);
        try result.appendSlice(alloc, self.names);
        try keys.appendEncodedComponent(&result, alloc, name);
        return result.toOwnedSlice(alloc);
    }
    fn ordinalKey(self: *const Plan, alloc: std.mem.Allocator, ordinal: u32) ![]u8 {
        return numberedKey(alloc, self.ordinals, ordinal);
    }
};

fn numberedKey(alloc: std.mem.Allocator, prefix: []const u8, ordinal: u32) ![]u8 {
    const key = try alloc.alloc(u8, prefix.len + 4);
    @memcpy(key[0..prefix.len], prefix);
    std.mem.writeInt(u32, key[prefix.len..][0..4], ordinal, .big);
    return key;
}

const IndexEntry = struct { name_key: []const u8, ordinal_key: []const u8, value: []const u8 };
pub const PreparedAppend = struct {
    core: generations.PreparedAppend,
    arena: std.heap.ArenaAllocator,
    entries: []const IndexEntry,

    pub fn init(alloc: std.mem.Allocator, plan: *const Plan, previous: generations.State, entries: []const Entry) !PreparedAppend {
        if (entries.len == 0 or entries.len > 128 or entries.len > plan.core.spec.output.count -| previous.progress.count) return error.InvalidBatchRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const payloads = try temporary.alloc([]const u8, entries.len);
        const indexes = try a.alloc(IndexEntry, entries.len);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var bytes: usize = 0;
        for (entries, payloads, indexes, 0..) |entry, *payload, *index, offset| {
            if ((try seen.getOrPut(temporary, entry.name)).found_existing) return error.InvalidBatchRequest;
            payload.* = try encodeEntry(temporary, entry);
            const ordinal = previous.progress.count + @as(u32, @intCast(offset));
            const descriptor: Descriptor = .{ .ordinal = ordinal, .name = entry.name, .digest = payload.*[payload.len - 32 ..][0..32].* };
            index.* = .{ .name_key = try plan.nameKey(a, entry.name), .ordinal_key = try plan.ordinalKey(a, ordinal), .value = try descriptor.encode(a, plan.core.spec.id()) };
            bytes +|= payload.len +| plan.core.row_prefix.len +| 4;
            bytes +|= index.name_key.len +| index.ordinal_key.len +| (index.value.len *| 2);
            if (bytes > (if (entries.len == 1) publication.max_payload_bytes else @as(usize, 64 * 1024))) return error.InvalidBatchRequest;
        }
        return .{ .core = try generations.PreparedAppend.init(alloc, &plan.core, previous, payloads), .arena = arena, .entries = indexes };
    }
    pub fn deinit(self: *PreparedAppend) void {
        self.core.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn stage(self: *const PreparedAppend, plan: *const Plan, txn: anytype) !bool {
        const current = try plan.core.load(txn);
        if (!std.meta.eql(try plan.directoryProgress(txn), current.progress)) return error.ArtifactCatalogCorrupt;
        const fresh = std.meta.eql(current, self.core.previous);
        for (self.entries) |entry| {
            for ([_][]const u8{ entry.name_key, entry.ordinal_key }) |key| {
                const raw = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
                if (fresh) {
                    if (raw != null) return error.InvalidBatchRequest;
                } else if (raw == null or !std.mem.eql(u8, raw.?, entry.value)) return error.EnrichmentSourceChanged;
            }
        }
        const changed = try self.core.stage(&plan.core, txn);
        if (!changed) return false;
        for (self.entries) |entry| {
            try txn.put(entry.name_key, entry.value);
            try txn.put(entry.ordinal_key, entry.value);
        }
        try txn.put(plan.progress, &self.core.next.progress.encode());
        return true;
    }
};

pub fn View(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        plan: Plan,
        pub fn open(alloc: std.mem.Allocator, txn: *Txn, scope: []const u8) !?@This() {
            if (scopes.family(scope) != .extraction) return error.InvalidBatchRequest;
            const head = try alloc.dupe(u8, scope);
            defer alloc.free(head);
            head[keys.findComponentTerminator(head, 1).? + 2] = keys.extraction_generation_head_kind;
            const raw = txn.get(head) catch |err| if (err == error.NotFound) return null else return err;
            return try fromHead(alloc, txn, scope, raw);
        }
        fn fromHead(alloc: std.mem.Allocator, txn: *Txn, scope: []const u8, raw: []const u8) !@This() {
            var plan = try Plan.init(alloc, scope, try generations.Spec.decode(raw));
            errdefer plan.deinit();
            const state = try plan.core.load(txn);
            if (state.retiring or !std.meta.eql(state.progress, plan.core.spec.output) or !std.meta.eql(try plan.directoryProgress(txn), state.progress)) return error.ArtifactCatalogCorrupt;
            return .{ .txn = txn, .plan = plan };
        }
        pub fn deinit(self: *@This()) void {
            self.plan.deinit();
            self.* = undefined;
        }
        fn findDescriptor(self: *const @This(), alloc: std.mem.Allocator, name: []const u8) !?Descriptor {
            const index = try self.plan.nameKey(alloc, name);
            defer alloc.free(index);
            const raw = self.txn.get(index) catch |err| if (err == error.NotFound) return null else return err;
            const found = try Descriptor.decode(raw, self.plan.core.spec.id());
            if (!std.mem.eql(u8, found.name, name) or found.ordinal >= self.plan.core.spec.output.count) return error.ArtifactCatalogCorrupt;
            return found;
        }
        /// Membership discovery never materializes a unit body. Accepted
        /// publication of the complete directory is a separate requirement.
        pub fn contains(self: *const @This(), alloc: std.mem.Allocator, name: []const u8) !bool {
            return try self.findDescriptor(alloc, name) != null;
        }
        pub fn get(self: *const @This(), alloc: std.mem.Allocator, name: []const u8) !?[]const u8 {
            const found = (try self.findDescriptor(alloc, name)) orelse return null;
            const key = try numberedKey(alloc, self.plan.core.row_prefix, found.ordinal);
            defer alloc.free(key);
            const payload = self.txn.get(key) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
            const entry = try decodeEntry(payload);
            if (!std.mem.eql(u8, entry.name, name) or !std.mem.eql(u8, &found.digest, payload[payload.len - 32 ..])) return error.ArtifactCatalogCorrupt;
            return entry.value;
        }
        pub const Cursor = struct {
            alloc: std.mem.Allocator,
            physical: ?Txn.CursorAdapter,
            prefix: []const u8,
            start: []const u8,
            id: Digest,
            ordinal: u32,
            count: u32,
            started: bool = false,
            pub fn position(self: *const Cursor) Position {
                return .{ .generation = self.id, .next_ordinal = self.ordinal };
            }
            pub fn deinit(self: *Cursor) void {
                if (self.physical) |*value| value.close();
                self.alloc.free(self.start);
                self.* = undefined;
            }
            /// Borrowed metadata only, valid until the next cursor operation.
            pub fn next(self: *Cursor) !?Descriptor {
                if (self.ordinal >= self.count) return null;
                const row = (if (self.started) try self.physical.?.next() else try self.physical.?.seekAtOrAfter(self.start)) orelse return error.ArtifactCatalogCorrupt;
                self.started = true;
                if (row.key.len != self.prefix.len + 4 or !std.mem.startsWith(u8, row.key, self.prefix) or std.mem.readInt(u32, row.key[self.prefix.len..][0..4], .big) != self.ordinal) return error.ArtifactCatalogCorrupt;
                const descriptor = try Descriptor.decode(row.value, self.id);
                if (descriptor.ordinal != self.ordinal) return error.ArtifactCatalogCorrupt;
                self.ordinal += 1;
                return descriptor;
            }
        };
        pub fn openCursor(self: *const @This(), alloc: std.mem.Allocator, start: u32) !Cursor {
            const at_end = start >= self.plan.core.spec.output.count;
            const key = if (at_end) "" else try self.plan.ordinalKey(alloc, start);
            errdefer alloc.free(key);
            return .{ .alloc = alloc, .physical = if (at_end) null else try self.txn.openPhysicalCursorAdapter(), .prefix = self.plan.ordinals, .start = key, .id = self.plan.core.spec.id(), .ordinal = start, .count = self.plan.core.spec.output.count };
        }
        pub fn resumeCursor(self: *const @This(), alloc: std.mem.Allocator, after: ?Position) !Cursor {
            if (after) |position| {
                if (!std.mem.eql(u8, &position.generation, &self.plan.core.spec.id())) return error.EnrichmentSourceChanged;
                if (position.next_ordinal > self.plan.core.spec.output.count) return error.InvalidBatchRequest;
            }
            return self.openCursor(alloc, if (after) |position| position.next_ordinal else 0);
        }
    };
}

/// Reconstruct the logical unit inventory from one pinned read. Once a head
/// selects a generation, its directory is authoritative even when it contains
/// no units; obsolete physical rows must not be resurrected by recovery.
/// Only keys are retained, so generation payloads are never loaded here.
pub fn unitKeysAlloc(alloc: std.mem.Allocator, txn: anytype, document: []const u8, producer: []const u8) ![]const []const u8 {
    const scope = try scopes.extractionKeyAlloc(alloc, document, producer);
    defer alloc.free(scope);
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |key| alloc.free(key);
        result.deinit(alloc);
    }
    if (try View(@TypeOf(txn.*)).open(alloc, txn, scope)) |selected| {
        var view = selected;
        defer view.deinit();
        var cursor = try view.openCursor(alloc, 0);
        defer cursor.deinit();
        while (try cursor.next()) |descriptor| {
            if (descriptor.name[0] != 1) continue;
            if (descriptor.name.len == 1) return error.ArtifactCatalogCorrupt;
            const key = try keys.documentUnitArtifactKeyAlloc(alloc, document, producer, descriptor.name[1..]);
            result.append(alloc, key) catch |err| {
                alloc.free(key);
                return err;
            };
        }
    } else {
        const prefix = try keys.artifactNamedPrefixAlloc(alloc, document, "asset", producer);
        defer alloc.free(prefix);
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        var row = try cursor.seekAtOrAfter(prefix);
        while (row) |entry| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (!keys.isDocumentUnitArtifactRecordKey(entry.key)) continue;
            const key = try alloc.dupe(u8, entry.key);
            result.append(alloc, key) catch |err| {
                alloc.free(key);
                return err;
            };
        }
    }
    return result.toOwnedSlice(alloc);
}

pub const PreparedRetirement = struct {
    core: generations.PreparedRetirement,
    arena: std.heap.ArenaAllocator,
    entries: []const IndexEntry,

    /// Directory names locate all three physical records without loading
    /// payloads. Both metadata and key bytes count toward the page budget.
    pub fn init(alloc: std.mem.Allocator, txn: anytype, plan: *const Plan, authority: publication.Authority, previous: generations.State, max_rows: u32) !PreparedRetirement {
        if (max_rows == 0 or max_rows > 128 or !previous.retiring or !std.meta.eql(previous.spec, plan.core.spec) or previous.retired_through > previous.progress.count) return error.InvalidBatchRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var entries: std.ArrayList(IndexEntry) = .empty;
        var bytes: usize = 0;
        var ordinal = previous.retired_through;
        while (ordinal < previous.progress.count and entries.items.len < max_rows) : (ordinal += 1) {
            const reverse = try plan.ordinalKey(a, ordinal);
            const raw = txn.get(reverse) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
            const descriptor = try Descriptor.decode(raw, plan.core.spec.id());
            if (descriptor.ordinal != ordinal) return error.ArtifactCatalogCorrupt;
            const forward = try plan.nameKey(a, descriptor.name);
            const size = reverse.len +| forward.len +| raw.len +| plan.core.row_prefix.len +| 4;
            if (size > publication.max_payload_bytes) return error.ArtifactCatalogCorrupt;
            if (entries.items.len != 0 and size > 64 * 1024 -| bytes) break;
            try entries.append(a, .{ .name_key = forward, .ordinal_key = reverse, .value = try a.dupe(u8, raw) });
            bytes += size;
        }
        var core = try generations.PreparedRetirement.init(alloc, &plan.core, authority, previous, @intCast(@max(1, entries.items.len)));
        errdefer core.deinit();
        // The shared layer may impose a stricter physical-key budget.
        return .{ .core = core, .arena = arena, .entries = entries.items[0..core.keys.len] };
    }
    pub fn deinit(self: *PreparedRetirement) void {
        self.core.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn stage(self: *const PreparedRetirement, plan: *const Plan, txn: anytype) !bool {
        // A repeated/advanced GC page must not require already-deleted indexes.
        if (!try self.core.stage(&plan.core, txn)) return false;
        if (!std.meta.eql(try plan.directoryProgress(txn), self.core.previous.progress)) return error.ArtifactCatalogCorrupt;
        for (self.entries) |entry| {
            for ([_][]const u8{ entry.name_key, entry.ordinal_key }) |key| {
                const raw = txn.get(key) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
                if (!std.mem.eql(u8, raw, entry.value)) return error.ArtifactCatalogCorrupt;
                try txn.delete(key);
            }
        }
        if (self.core.next.retired_through == self.core.next.progress.count) try txn.delete(plan.progress);
        return true;
    }
};

test "ordered artifact inventory named extraction directory resumes lookup enumeration and metadata-only retirement" {
    const alloc = std.testing.allocator;
    const db_mod = @import("db.zig");
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/named-extraction", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const scope = try scopes.extractionKeyAlloc(alloc, "doc", "extract");
    defer alloc.free(scope);
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    const unit_name = try unitNameAlloc(alloc, "unit\x00\xff");
    defer alloc.free(unit_name);
    const unit_key = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "extract", "unit\x00\xff");
    defer alloc.free(unit_key);
    const stale_key = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "extract", "stale");
    defer alloc.free(stale_key);
    const other_key = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "other", "unit\x00\xff");
    defer alloc.free(other_key);
    const entries = [_]Entry{ .{ .name = unit_name, .value = "first payload" }, .{ .name = "root", .value = "manifest" }, .{ .name = "unit\x00\xfe", .value = "third payload" } };
    var builder = manifest.Builder.init();
    for (entries, 0..) |entry, ordinal| {
        const raw = try encodeEntry(alloc, entry);
        defer alloc.free(raw);
        try builder.append(@intCast(ordinal), raw);
    }
    var plan = try Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(3), builder.finish(), 1));
    defer plan.deinit();
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try @import("../source_authority.zig").bind(&txn, .native, authority.namespace);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.put(unit_key, "obsolete physical row");
        try txn.put(stale_key, "obsolete extra unit");
        try txn.put(other_key, "unselected producer row");
        {
            const legacy_keys = try unitKeysAlloc(alloc, &txn, "doc", "extract");
            defer {
                for (legacy_keys) |key| alloc.free(key);
                alloc.free(legacy_keys);
            }
            try std.testing.expectEqual(@as(usize, 2), legacy_keys.len);
        }
        var physical = try captureInput(alloc, &txn, unit_key);
        defer physical.deinit();
        try std.testing.expect(physical.head_value == null);
        try std.testing.expectEqualStrings("obsolete physical row", physical.value.?);
        _ = try plan.begin(&txn);
        var first = try PreparedAppend.init(alloc, &plan, try plan.core.load(&txn), entries[0..1]);
        defer first.deinit();
        _ = try first.stage(&plan, &txn);
        try std.testing.expectError(error.ArtifactPublicationPending, plan.publish(&txn, null, Guard{}));
        try txn.commit();
    }
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    const old = read: {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        break :read try plan.core.load(&txn);
    };
    const Check = struct {
        fn append(a: std.mem.Allocator, selected: *const Plan, previous: generations.State, rows: []const Entry) !void {
            var page = try PreparedAppend.init(a, selected, previous, rows);
            defer page.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.append, .{ &plan, old, @as([]const Entry, entries[1..]) });
    try std.testing.expectError(error.InvalidBatchRequest, PreparedAppend.init(alloc, &plan, old, &.{ entries[1], entries[1] }));
    {
        var duplicate = try PreparedAppend.init(alloc, &plan, old, entries[0..1]);
        defer duplicate.deinit();
        var txn = try db.core.store.beginWriteTxn();
        defer txn.abort();
        try std.testing.expectError(error.InvalidBatchRequest, duplicate.stage(&plan, &txn));
        try std.testing.expectEqualDeep(old, try plan.core.load(&txn));
    }
    var rest = try PreparedAppend.init(alloc, &plan, old, entries[1..]);
    defer rest.deinit();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try rest.stage(&plan, &txn);
        try std.testing.expect(!try rest.stage(&plan, &txn));
        _ = try plan.publish(&txn, null, Guard{});
        try txn.commit();
    }
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    const Selected = View(@import("../docstore.zig").DocStore.Txn);
    var view = (try Selected.open(alloc, &pinned, scope)).?;
    defer view.deinit();
    {
        const selected_keys = try unitKeysAlloc(alloc, &pinned, "doc", "extract");
        defer {
            for (selected_keys) |key| alloc.free(key);
            alloc.free(selected_keys);
        }
        try std.testing.expectEqual(@as(usize, 1), selected_keys.len);
        try std.testing.expectEqualStrings(unit_key, selected_keys[0]);
    }
    const InventoryAllocationCheck = struct {
        fn run(a: std.mem.Allocator, txn: *@import("../docstore.zig").DocStore.Txn) !void {
            const selected_keys = try unitKeysAlloc(a, txn, "doc", "extract");
            defer {
                for (selected_keys) |key| a.free(key);
                a.free(selected_keys);
            }
            try std.testing.expectEqual(@as(usize, 1), selected_keys.len);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, InventoryAllocationCheck.run, .{&pinned});
    const CaptureCheck = struct {
        fn run(a: std.mem.Allocator, txn: *@import("../docstore.zig").DocStore.Txn, key: []const u8) !void {
            var input = try captureInput(a, txn, key);
            defer input.deinit();
            try std.testing.expectEqualStrings("first payload", input.value.?);
            try std.testing.expectEqualStrings(input.head_key.?, input.proofKey(key));
            try std.testing.expectEqualStrings(input.head_value.?, input.proofValue().?);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, CaptureCheck.run, .{ &pinned, unit_key });
    const ids = @import("artifact_ids.zig");
    var identity = (try ids.decodeArtifactRefAlloc(alloc, unit_key)).?;
    defer identity.deinit(alloc);
    const public_id = try ids.artifactPublicIdAlloc(alloc, identity);
    defer alloc.free(public_id);
    {
        var public = (try db.getArtifact(alloc, public_id)).?;
        defer public.deinit(alloc);
        try std.testing.expectEqualStrings(entries[0].value, public.value);
    }
    var other = try captureInput(alloc, &pinned, other_key);
    defer other.deinit();
    try std.testing.expect(other.head_value == null);
    try std.testing.expectEqualStrings("unselected producer row", other.value.?);
    for (entries) |entry| try std.testing.expectEqualStrings(entry.value, (try view.get(alloc, entry.name)).?);
    try std.testing.expect(try view.get(alloc, "missing") == null);
    const LookupProbe = struct {
        txn: *@import("../docstore.zig").DocStore.Txn,
        calls: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.calls += 1;
            return self.txn.get(key);
        }
    };
    var lookup_probe: LookupProbe = .{ .txn = &pinned };
    var direct: View(LookupProbe) = .{ .txn = &lookup_probe, .plan = try Plan.init(alloc, scope, plan.core.spec) };
    defer direct.deinit();
    try std.testing.expectEqualStrings(entries[0].value, (try direct.get(alloc, entries[0].name)).?);
    try std.testing.expectEqual(@as(usize, 2), lookup_probe.calls);
    lookup_probe.calls = 0;
    try std.testing.expect(try direct.get(alloc, "absent") == null);
    try std.testing.expectEqual(@as(usize, 1), lookup_probe.calls);
    lookup_probe.calls = 0;
    try std.testing.expect(try direct.contains(alloc, entries[0].name));
    try std.testing.expectEqual(@as(usize, 1), lookup_probe.calls);
    try std.testing.expect(!try direct.contains(alloc, "absent"));
    try std.testing.expectEqual(@as(usize, 2), lookup_probe.calls);
    var cursor = try view.openCursor(alloc, 1);
    defer cursor.deinit();
    for (entries[1..], 1..) |entry, ordinal| {
        const descriptor = (try cursor.next()).?;
        try std.testing.expectEqual(ordinal, descriptor.ordinal);
        try std.testing.expectEqualStrings(entry.name, descriptor.name);
    }
    try std.testing.expect(try cursor.next() == null);
    const completed_position = cursor.position();
    var resumed = try view.resumeCursor(alloc, try Position.decode(&completed_position.encode()));
    defer resumed.deinit();
    try std.testing.expect(try resumed.next() == null);
    var empty = try Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(4), manifest.Builder.init().finish(), 2));
    defer empty.deinit();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        _ = try empty.begin(&txn);
        _ = try empty.publish(&txn, plan.core.spec.id(), Guard{});
        _ = try plan.core.retire(&txn, authority, Guard{});
        try txn.commit();
    }
    {
        var current = try db.core.store.beginReadTxn();
        defer current.abort();
        const empty_keys = try unitKeysAlloc(alloc, &current, "doc", "extract");
        defer alloc.free(empty_keys);
        try std.testing.expectEqual(@as(usize, 0), empty_keys.len);
        const pinned_keys = try unitKeysAlloc(alloc, &pinned, "doc", "extract");
        defer {
            for (pinned_keys) |key| alloc.free(key);
            alloc.free(pinned_keys);
        }
        try std.testing.expectEqual(@as(usize, 1), pinned_keys.len);
    }
    const MetadataOnly = struct {
        txn: *@import("../docstore.zig").DocStore.Txn,
        payload_prefix: []const u8,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expect(!std.mem.startsWith(u8, key, self.payload_prefix));
            return self.txn.get(key);
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            return self.txn.put(key, value);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            return self.txn.delete(key);
        }
    };
    for (0..entries.len) |_| {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const state = try plan.core.load(&txn);
        var probe: MetadataOnly = .{ .txn = &txn, .payload_prefix = plan.core.row_prefix };
        const AllocationCheck = struct {
            fn run(a: std.mem.Allocator, read: *MetadataOnly, selected: *const Plan, active: publication.Authority, previous: generations.State) !void {
                var page = try PreparedRetirement.init(a, read, selected, active, previous, 1);
                defer page.deinit();
            }
        };
        if (state.retired_through == 0) try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{ &probe, &plan, authority, state });
        var gc = try PreparedRetirement.init(alloc, &probe, &plan, authority, state, 1);
        defer gc.deinit();
        _ = try gc.stage(&plan, &probe);
        try std.testing.expect(!try gc.stage(&plan, &txn));
        try txn.commit();
    }
    for (entries) |entry| try std.testing.expectEqualStrings(entry.value, (try view.get(alloc, entry.name)).?);
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    try std.testing.expectError(error.NotFound, plan.core.load(&current));
    try std.testing.expectError(error.NotFound, current.get(plan.progress));
    for ([_][]const u8{ plan.names, plan.ordinals, plan.core.row_prefix }) |prefix| {
        var scan = try current.openPhysicalCursorAdapter();
        defer scan.close();
        if (try scan.seekAtOrAfter(prefix)) |row| try std.testing.expect(!std.mem.startsWith(u8, row.key, prefix));
    }
    var selected_empty = (try Selected.open(alloc, &current, scope)).?;
    defer selected_empty.deinit();
    try std.testing.expect(try selected_empty.get(alloc, entries[0].name) == null);
    try std.testing.expectError(error.EnrichmentSourceChanged, selected_empty.resumeCursor(alloc, completed_position));
    var absent = try captureInput(alloc, &current, unit_key);
    defer absent.deinit();
    try std.testing.expect(absent.head_value != null);
    try std.testing.expect(absent.value == null);
    try std.testing.expect(try db.getArtifact(alloc, public_id) == null);
    try std.testing.expectEqualStrings("obsolete physical row", try current.get(unit_key));
    try CaptureCheck.run(alloc, &pinned, unit_key);
    // A complete storage generation is not yet an accepted producer result.
    // Neither its bytes nor an old physical unit can authorize downstream work.
    var token: @import("artifact_producer_context.zig").Token = .{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_name = "downstream",
        .producer_kind = .enrichment,
        .producer_generation = 1,
        .artifact_name = "downstream",
        .source = .{ .document_key = "doc", .content_digest = @splat(0), .timestamp = 1, .input_position = null },
    };
    defer token.deinit();
    var observed = try captureInput(alloc, &pinned, unit_key);
    defer observed.deinit();
    try observed.observe(&token, &pinned, unit_key);
    try std.testing.expectEqual(@as(usize, 1), token.reads.items.len);
    try publication.validateArtifactSources(alloc, &pinned, authority.namespace, token.sources(), token.reads.items);
    try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateArtifactSources(alloc, &current, authority.namespace, token.sources(), token.reads.items));
    const assets = @import("artifact_asset_publication.zig");
    try std.testing.expectError(error.ArtifactPublicationPending, assets.readUpstream(alloc, &token, &pinned, unit_key));
    try std.testing.expectError(error.ArtifactPublicationPending, assets.readUpstream(alloc, &token, &current, unit_key));
    const missing = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "unpublished", "missing");
    defer alloc.free(missing);
    try std.testing.expectError(error.ArtifactPublicationPending, assets.readUpstream(alloc, &token, &current, missing));
}

test "ordered artifact inventory named extraction codecs reject corruption and charge directory bytes" {
    const alloc = std.testing.allocator;
    const position: Position = .{ .generation = @splat(1), .next_ordinal = 9 };
    var position_bytes = position.encode();
    try std.testing.expectEqualDeep(position, try Position.decode(&position_bytes));
    for (0..position_bytes.len) |offset| {
        position_bytes[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Position.decode(&position_bytes));
        position_bytes[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Position.decode(position_bytes[0..offset]));
    }
    const encoded = try encodeEntry(alloc, .{ .name = "key\x00\xff", .value = "body\x80" });
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("body\x80", (try decodeEntry(encoded)).value);
    for (0..encoded.len) |offset| {
        encoded[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeEntry(encoded));
        encoded[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeEntry(encoded[0..offset]));
    }
    const descriptor = try (Descriptor{ .name = "key\x00\xff", .ordinal = 3, .digest = encoded[encoded.len - 32 ..][0..32].* }).encode(alloc, @splat(1));
    defer alloc.free(descriptor);
    try std.testing.expectEqual(@as(u32, 3), (try Descriptor.decode(descriptor, @splat(1))).ordinal);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Descriptor.decode(descriptor, @splat(2)));
    for (0..descriptor.len) |offset| {
        descriptor[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Descriptor.decode(descriptor, @splat(1)));
        descriptor[offset] ^= 1;
    }
    const first = try alloc.alloc(u8, 20 * 1024);
    defer alloc.free(first);
    @memset(first, 'a');
    const second = try alloc.alloc(u8, first.len);
    defer alloc.free(second);
    @memset(second, 'b');
    const entries = [_]Entry{ .{ .name = first, .value = "x" }, .{ .name = second, .value = "y" } };
    var builder = manifest.Builder.init();
    for (entries, 0..) |entry, ordinal| {
        const raw = try encodeEntry(alloc, entry);
        defer alloc.free(raw);
        try builder.append(@intCast(ordinal), raw);
    }
    const scope = try scopes.extractionKeyAlloc(alloc, "doc", "producer");
    defer alloc.free(scope);
    var plan = try Plan.init(alloc, scope, try generations.Spec.init(.{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, scope, @splat(3), builder.finish(), 1));
    defer plan.deinit();
    const previous: generations.State = .{ .spec = plan.core.spec, .progress = manifest.Builder.init().finish() };
    // Payload envelopes alone fit, but their directory records do not.
    try std.testing.expect(builder.finish().payload_bytes < 64 * 1024);
    try std.testing.expectError(error.InvalidBatchRequest, PreparedAppend.init(alloc, &plan, previous, &entries));
    // One bounded large member can still make progress.
    var single = try PreparedAppend.init(alloc, &plan, previous, entries[0..1]);
    defer single.deinit();
}
