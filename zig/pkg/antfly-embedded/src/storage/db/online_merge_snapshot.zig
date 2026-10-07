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

//! Bounded logical row pages from a verified immutable source artifact. Archive
//! positions are committed with receiver row receipts; no primary scan or live
//! recapture is permitted. A replica-local cache only avoids repeated decoding
//! of the same oversized row and is never authoritative progress.
const native_platform = @import("antfly_platform");
const server_test_adapter = if (builtin.is_test) @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter else struct {};
const builtin = @import("builtin");
const std = @import("std");

const DB = @import("antfly_source_root").antfly_sources.physical_db.DB;
const source = @import("online_source.zig");
const pages = @import("merge_page_contract.zig");
const types = @import("types.zig");
const verifier = @import("../portable_source_verifier.zig");
const Certificate = @import("../source_snapshot.zig").Certificate;
const Allocator = std.mem.Allocator;
const VectorDescriptor = @import("online_vector_snapshot.zig").RawDescriptor;
const ProofDescriptor = @import("source_proof_batch.zig").Descriptor;
const VectorStream = struct {
    descriptor: VectorDescriptor,
    base: types.BatchRequest,
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    hashed: u64 = 0,
    digest: ?pages.Digest = null,
};
const ProofStream = struct {
    descriptor: ProofDescriptor,
    base: types.BatchRequest,
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    hashed: u64 = 0,
    digest: ?pages.Digest = null,
};

pub const Cache = struct {
    arena: ?std.heap.ArenaAllocator = null,
    sequence: ?u64 = null,
    position: pages.SnapshotPosition = .{ .object = 0, .offset = 0, .remaining = 0 },
    request: ?types.BatchRequest = null,
    chunks: ?pages.RowChunks(types.BatchRequest) = null,
    vector: ?VectorStream = null,
    proof: ?ProofStream = null,
    pub fn clear(self: *Cache) void {
        if (self.arena) |*arena| arena.deinit();
        self.* = .{};
    }
};

fn outputVector(alloc: Allocator, scope: source.Scope, cache: *Cache, receipt: pages.Progress, reader: *verifier.ObjectReader, cancellation: types.CancellationToken) ![]u8 {
    const vector = &cache.vector.?;
    if (vector.digest == null) {
        // One bounded hash slice per owner call; a restart recomputes from the
        // same certified descriptor and can never substitute live bytes.
        const end = @min(vector.descriptor.value_len, vector.hashed + 4 * pages.chunk_bytes);
        var scratch: [64 * 1024]u8 = undefined;
        while (vector.hashed < end) {
            try cancellation.check();
            const count: usize = @intCast(@min(scratch.len, end - vector.hashed));
            try vector.descriptor.read(reader, vector.hashed, scratch[0..count]);
            vector.hash.update(scratch[0..count]);
            vector.hashed += count;
        }
        if (vector.hashed != vector.descriptor.value_len) return output(alloc, scope, cache, receipt);
        vector.digest = vector.hash.finalResult();
    }
    const offset = if (receipt.assembly) |assembly| assembly.next_offset else 0;
    if (offset >= vector.descriptor.value_len or offset % pages.chunk_bytes != 0) return error.InvalidMergePage;
    const count: usize = @intCast(@min(pages.chunk_bytes, vector.descriptor.value_len - offset));
    const data = try alloc.alloc(u8, count);
    defer alloc.free(data);
    try cancellation.check();
    try vector.descriptor.read(reader, offset, data);
    var request = vector.base;
    request.merge_page.?.chunk = .{ .payload = .artifact, .row_key = vector.descriptor.key.?, .timestamp = 0, .total_bytes = vector.descriptor.value_len, .row_digest = vector.digest.?, .offset = offset, .data = data, .chunk_digest = @import("merge_page_chunks.zig").checksum(data) };
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    return std.json.Stringify.valueAlloc(alloc, @import("online_merge_io_contract.zig").Prepared{ .scope = scope, .request = request }, .{});
}

fn outputProof(alloc: Allocator, scope: source.Scope, cache: *Cache, receipt: pages.Progress, reader: *verifier.ObjectReader, cancellation: types.CancellationToken) ![]u8 {
    const proof = &cache.proof.?;
    if (proof.digest == null) {
        const end = @min(proof.descriptor.value_len, proof.hashed + 4 * pages.chunk_bytes);
        var scratch: [64 * 1024]u8 = undefined;
        while (proof.hashed < end) {
            try cancellation.check();
            const count: usize = @intCast(@min(scratch.len, end - proof.hashed));
            try proof.descriptor.read(reader, proof.hashed, scratch[0..count]);
            proof.hash.update(scratch[0..count]);
            proof.hashed += count;
        }
        if (proof.hashed != proof.descriptor.value_len) return output(alloc, scope, cache, receipt);
        proof.digest = proof.hash.finalResult();
    }
    const offset = if (receipt.assembly) |assembly| assembly.next_offset else 0;
    if (offset >= proof.descriptor.value_len or offset % pages.chunk_bytes != 0) return error.InvalidMergePage;
    const count: usize = @intCast(@min(pages.chunk_bytes, proof.descriptor.value_len - offset));
    const data = try alloc.alloc(u8, count);
    defer alloc.free(data);
    try cancellation.check();
    try proof.descriptor.read(reader, offset, data);
    var request = proof.base;
    request.merge_page.?.chunk = .{ .payload = .provenance, .row_key = request.merge_page.?.next, .timestamp = 0, .total_bytes = proof.descriptor.value_len, .row_digest = proof.digest.?, .offset = offset, .data = data, .chunk_digest = @import("merge_page_chunks.zig").checksum(data) };
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    return std.json.Stringify.valueAlloc(alloc, @import("online_merge_io_contract.zig").Prepared{ .scope = scope, .request = request }, .{});
}

fn output(alloc: Allocator, scope: source.Scope, cache: *Cache, receipt: pages.Progress) ![]u8 {
    const request = if (cache.chunks) |chunks| try chunks.requestAt(if (receipt.assembly) |assembly| assembly.next_offset else 0) else cache.request;
    return std.json.Stringify.valueAlloc(alloc, @import("online_merge_io_contract.zig").Prepared{ .scope = scope, .request = request }, .{});
}

fn exact(reader: *verifier.ObjectReader, object: u32, offset: u64, bytes: []u8) !void {
    var read: usize = 0;
    while (read < bytes.len) {
        const n = try reader.readAt(object, try std.math.add(u64, offset, read), bytes[read..]);
        if (n == 0) return error.SourceSnapshotCorrupt;
        read += n;
    }
}
fn number(comptime T: type, reader: *verifier.ObjectReader, object: u32, offset: u64) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    try exact(reader, object, offset, &bytes);
    return std.mem.readInt(T, &bytes, .little);
}

pub fn executeJson(db: *DB, alloc: Allocator, scope: source.Scope, receipt: pages.Progress, certificate: Certificate, cancellation: types.CancellationToken) ![]u8 {
    try @import("online_merge_io.zig").requireSnapshotIndexes(db, alloc, receipt.source, certificate);
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const shared = &db.local_execution.online_merge_reader;
    try shared.mutex.lock(io);
    defer shared.mutex.unlock(io);
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released or progress.snapshot_phase != .published or !std.mem.eql(u8, &progress.snapshot_certificate, &try certificate.digest())) return error.SourceSnapshotCutMismatch;
    if (shared.scope == null or !std.meta.eql(shared.scope.?, scope)) {
        shared.snapshot.clear();
        shared.scope = scope;
    }
    const cache = &shared.snapshot;
    if (cache.sequence == null or cache.sequence.? != receipt.sequence) {
        cache.clear();
        cache.sequence = receipt.sequence;
        cache.position = receipt.snapshot_position orelse .{ .object = 0, .offset = 0, .remaining = 0 };
    }
    if (cache.request != null) return output(alloc, scope, cache, receipt);
    const pins = @import("source_pin.zig");
    const lock_path = try pins.lockPath(alloc, db.core.path, (try pins.locate(db, scope)).slot);
    defer alloc.free(lock_path);
    var lease = try @import("native_backup_seal.zig").StoreLock.acquire(alloc, io, lock_path, cancellation);
    defer lease.deinit();
    if ((try db.onlineSourceStatus(scope)).phase == .released) return error.OnlineSourceScopeChanged;
    const root = try pins.pathAlloc(alloc, db.core.path, scope);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/source.afb2", .{root});
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    // Original donor artifacts and transferred replica artifacts use the same
    // durable verifier. Each call performs at most one bounded verifier slice.
    if (!(try verifier.step(alloc, io, file, root, scope.pin(), certificate, cancellation, .{})).complete)
        return output(alloc, scope, cache, receipt);
    var reader = try verifier.ObjectReader.open(alloc, io, file, root, scope.pin(), certificate);
    defer reader.deinit();
    if (cache.vector != null) return outputVector(alloc, scope, cache, receipt, &reader, cancellation);
    if (cache.proof != null) return outputProof(alloc, scope, cache, receipt, &reader, cancellation);
    var arena = std.heap.ArenaAllocator.init(db.alloc);
    var arena_owned = true;
    errdefer if (arena_owned) arena.deinit();
    const owned = arena.allocator();
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    var timestamps: std.ArrayList(u64) = .empty;
    var integrity: std.ArrayList(pages.IntegrityEffect) = .empty;
    var vectors: std.ArrayList(pages.IntegrityEffect) = .empty;
    var proofs: std.ArrayList(pages.IntegrityEffect) = .empty;
    var position = cache.position;
    var last_row_position: ?pages.SnapshotPosition = null;
    var bytes: usize = 0;
    var work: usize = 0;
    const vector_copy = vector_mode: {
        if (!@import("online_vector_artifacts.zig").isEnabled() or receipt.source.artifact_catalog == null) break :vector_mode false;
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const retention = (try @import("../retained_effects.zig").load(&read)) orelse return error.SourceSnapshotCutMismatch;
        // Snapshot and tail must preserve the exact same effect language,
        // including historical names absent from the current index catalog.
        break :vector_mode retention.direct_vectors;
    };
    const effect_protocol = if (receipt.source.artifact_catalog) |binding| binding.effect_protocol else 0;
    const graph_copy = vector_copy and effect_protocol == 15;
    // Even a row-only receiver must inspect the certified object kinds: a
    // protocol-14 source cannot silently discard private graph ownership.
    while (receipt.phase == .artifacts and position.object < reader.objectCount() and integrity.items.len + vectors.items.len + proofs.items.len < pages.max_rows and work < pages.max_rows) : (work += 1) {
        try cancellation.check();
        const object = try reader.object(position.object);
        if (object.kind == .source_proof_batch) {
            if (integrity.items.len != 0 or vectors.items.len != 0) break;
            const proof_batch = @import("source_proof_batch.zig");
            const descriptor = (try proof_batch.descriptor(&reader, object.size, .{ .object = position.object, .offset = position.offset, .remaining = position.remaining })) orelse {
                position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
                if (proofs.items.len != 0) break;
                continue;
            };
            var namespace: @import("artifact_publication.zig").Namespace = undefined;
            @import("doc_identity.zig").encodeNamespace(&namespace, receipt.source.namespace);
            const key_array = proof_batch.mergeKey(namespace, receipt.source.pin_digest, descriptor.digest);
            const key = try owned.dupe(u8, &key_array);
            if (proofs.items.len != 0 and key.len +| descriptor.value_len > pages.max_bytes -| bytes) break;
            if (key.len +| descriptor.value_len > pages.max_bytes) {
                const base: types.BatchRequest = .{
                    .merge_replication = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt },
                    .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = .artifacts, .after = try owned.dupe(u8, receipt.cursor), .next = key, .exhausted = false, .digest = @splat(0), .next_snapshot_position = .{ .object = descriptor.next_position.object, .offset = descriptor.next_position.offset, .remaining = descriptor.next_position.remaining } },
                };
                cache.proof = .{ .descriptor = descriptor, .base = base };
                cache.arena = arena;
                arena_owned = false;
                return outputProof(alloc, scope, cache, receipt, &reader, cancellation);
            }
            const value = try owned.alloc(u8, descriptor.value_len);
            try descriptor.read(&reader, 0, value);
            var decoded = try proof_batch.decodeValue(owned, namespace, descriptor.digest, value);
            decoded.deinit();
            if (proofs.items.len != 0 and std.mem.order(u8, proofs.items[proofs.items.len - 1].key, key) != .lt) return error.SourceSnapshotCorrupt;
            try proofs.append(owned, .{ .key = key, .value = value });
            bytes +|= key.len +| value.len;
            position = .{ .object = descriptor.next_position.object, .offset = descriptor.next_position.offset, .remaining = descriptor.next_position.remaining };
            last_row_position = position;
            if (bytes >= pages.max_bytes) break;
            continue;
        }
        const raw_graph = object.kind == .source_artifact_batch;
        if (raw_graph and !graph_copy) return error.OnlineMergeArtifactTailsUnsupported;
        if (raw_graph or (vector_copy and object.kind == .artifact_batch)) {
            if (integrity.items.len != 0 or proofs.items.len != 0) break;
            const descriptor = (if (raw_graph)
                try @import("online_vector_snapshot.zig").graphDescriptor(owned, &reader, object.size, position, effect_protocol)
            else
                try @import("online_vector_snapshot.zig").rawDescriptor(owned, &reader, object.size, position)) orelse {
                position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
                if (vectors.items.len != 0) break;
                continue;
            };
            const key = descriptor.key orelse {
                position = descriptor.next_position;
                continue;
            };
            if (vectors.items.len != 0 and key.len +| descriptor.value_len > pages.max_bytes -| bytes) break;
            if (key.len +| descriptor.value_len > pages.max_bytes) {
                var base: types.BatchRequest = .{
                    .merge_replication = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt },
                    .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = .artifacts, .after = try owned.dupe(u8, receipt.cursor), .next = key, .exhausted = false, .digest = @splat(0), .next_snapshot_position = descriptor.next_position },
                };
                // Borrowed receipt strings must survive between bounded owner
                // calls; all command identity arrays themselves are values.
                base.merge_page.?.source = receipt.source;
                cache.vector = .{ .descriptor = descriptor, .base = base };
                cache.arena = arena;
                arena_owned = false;
                return outputVector(alloc, scope, cache, receipt, &reader, cancellation);
            }
            const value = try owned.alloc(u8, descriptor.value_len);
            try descriptor.read(&reader, 0, value);
            if (raw_graph) try @import("online_graph_artifacts.zig").validate(key, value) else try @import("online_vector_artifacts.zig").validate(key, value);
            if (vectors.items.len != 0 and std.mem.order(u8, vectors.items[vectors.items.len - 1].key, key) != .lt) return error.SourceSnapshotCorrupt;
            try vectors.append(owned, .{ .key = key, .value = value });
            bytes += key.len + value.len;
            position = descriptor.next_position;
            last_row_position = position;
            continue;
        }
        if (vector_copy and (object.kind == .embedding_batch or object.kind == .sparse_batch)) {
            if (integrity.items.len != 0 or proofs.items.len != 0) break;
            var candidate = position;
            const effect = (try @import("online_vector_snapshot.zig").next(owned, &reader, object.kind == .sparse_batch, object.size, &candidate)) orelse {
                position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
                // Portable objects are grouped by index, not global key order.
                // Keep one object's ordered entries per page and commit its
                // certified position before moving to another index object.
                if (vectors.items.len != 0) break;
                continue;
            };
            const size = effect.key.len +| effect.value.?.len;
            if (vectors.items.len != 0 and size > pages.max_bytes -| bytes) break;
            if (vectors.items.len != 0 and std.mem.order(u8, vectors.items[vectors.items.len - 1].key, effect.key) != .lt) return error.SourceSnapshotCorrupt;
            try vectors.append(owned, effect);
            bytes +|= size;
            position = candidate;
            last_row_position = position;
            if (bytes >= pages.max_bytes) break;
            continue;
        }
        if (object.kind != .integrity_batch or receipt.source.integrity == null) {
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        if (vectors.items.len != 0 or proofs.items.len != 0) break;
        if (position.offset == 0) {
            position.remaining = try number(u32, &reader, position.object, 0);
            position.offset = 4;
        }
        if (position.remaining == 0) {
            if (position.offset != object.size) return error.SourceSnapshotCorrupt;
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        const key_len = try number(u32, &reader, position.object, position.offset);
        const contract = @import("relational_integrity_contract.zig");
        if (key_len != contract.key_len and key_len != contract.key_len + 32) return error.SourceSnapshotCorrupt;
        const key_offset = try std.math.add(u64, position.offset, 4);
        const length_offset = try std.math.add(u64, key_offset, key_len);
        const value_len = try number(u32, &reader, position.object, length_offset);
        if (value_len > contract.max_record_bytes + 256) return error.SourceSnapshotCorrupt;
        if (integrity.items.len != 0 and @as(usize, key_len) +| value_len > pages.max_bytes -| bytes) break;
        const value_offset = try std.math.add(u64, length_offset, 4);
        const end = try std.math.add(u64, value_offset, value_len);
        if (end > object.size) return error.SourceSnapshotCorrupt;
        const key = try owned.alloc(u8, key_len);
        try exact(&reader, position.object, key_offset, key);
        // The previous receipt can name a vector object; only the certified
        // position orders objects, while this page orders its integrity keys.
        const previous = if (integrity.items.len == 0) "" else integrity.items[integrity.items.len - 1].key;
        if (std.mem.order(u8, previous, key) != .lt) return error.SourceSnapshotCorrupt;
        const value = try owned.alloc(u8, value_len);
        try exact(&reader, position.object, value_offset, value);
        _ = try contract.validateTransferRecord(key, value);
        try integrity.append(owned, .{ .key = key, .value = value });
        bytes +|= key.len +| value.len;
        position.offset = end;
        position.remaining -= 1;
        last_row_position = position;
        if (bytes >= pages.max_bytes) break;
    }
    while (receipt.phase == .rows and position.object < reader.objectCount() and writes.items.len < pages.max_rows and work < pages.max_rows) : (work += 1) {
        try cancellation.check();
        const object = try reader.object(position.object);
        if (object.kind != .document_batch) {
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        if (position.offset == 0) {
            position.remaining = try number(u32, &reader, position.object, 0);
            position.offset = 4;
        }
        if (position.remaining == 0) {
            if (position.offset != object.size) return error.SourceSnapshotCorrupt;
            position = .{ .object = position.object + 1, .offset = 0, .remaining = 0 };
            continue;
        }
        const key_len = try number(u32, &reader, position.object, position.offset);
        if (key_len == 0 or key_len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
        const key_offset = try std.math.add(u64, position.offset, 4);
        const flags_offset = try std.math.add(u64, key_offset, key_len);
        const flags = try number(u8, &reader, position.object, flags_offset);
        if (flags != 0 and flags != 2) return error.SourceSnapshotCorrupt;
        const value_len = try number(u32, &reader, position.object, flags_offset + 1);
        if (writes.items.len != 0 and @as(usize, key_len) +| value_len > pages.max_bytes -| bytes) break;
        const value_offset = try std.math.add(u64, flags_offset, 5);
        const end = try std.math.add(u64, value_offset, @as(u64, value_len) + 8);
        if (end > object.size) return error.SourceSnapshotCorrupt;
        const key = try owned.alloc(u8, key_len);
        try exact(&reader, position.object, key_offset, key);
        const previous = if (writes.items.len == 0) receipt.cursor else writes.items[writes.items.len - 1].key;
        if (std.mem.order(u8, previous, key) != .lt) return error.SourceSnapshotCorrupt;
        const raw = try owned.alloc(u8, value_len);
        try exact(&reader, position.object, value_offset, raw);
        const timestamp = try number(u64, &reader, position.object, value_offset + value_len);
        const value = if (flags == 2) decoded: {
            const version = try @import("relational_store.zig").rowSchemaVersion(raw);
            var view = (try db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            defer view.release();
            const row = try @import("algebraic/relational_row_codec.zig").ordinalRowViewSelective(raw, view.tableSchema().*, view.physicalLayout());
            if (row.writeTimestampNs() != timestamp) return error.SourceSnapshotCorrupt;
            break :decoded try row.reconstructValueAlloc(owned);
        } else raw;
        if (writes.items.len != 0 and key.len +| value.len > pages.max_bytes -| bytes) break;
        try writes.append(owned, .{ .key = key, .value = value });
        try timestamps.append(owned, timestamp);
        bytes +|= key.len +| value.len;
        position.offset = end;
        position.remaining -= 1;
        last_row_position = position;
        if (bytes >= pages.max_bytes) break;
    }
    if ((receipt.phase == .rows or receipt.phase == .artifacts) and writes.items.len == 0 and integrity.items.len == 0 and vectors.items.len == 0 and proofs.items.len == 0 and position.object < reader.objectCount()) {
        cache.position = position;
        arena.deinit();
        arena_owned = false;
        return output(alloc, scope, cache, receipt);
    }
    const last = if (writes.items.len != 0) writes.items[writes.items.len - 1].key else if (integrity.items.len != 0) integrity.items[integrity.items.len - 1].key else if (vectors.items.len != 0) vectors.items[vectors.items.len - 1].key else if (proofs.items.len != 0) proofs.items[proofs.items.len - 1].key else "";
    var request: types.BatchRequest = .{
        .merge_replication = .{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt },
        .writes = writes.items,
        .merge_page = .{ .source = receipt.source, .sequence = try std.math.add(u64, receipt.sequence, 1), .phase = receipt.phase, .after = try owned.dupe(u8, receipt.cursor), .next = last, .exhausted = writes.items.len == 0 and integrity.items.len == 0 and vectors.items.len == 0 and proofs.items.len == 0, .digest = @splat(0), .timestamps = timestamps.items, .next_snapshot_position = last_row_position, .integrity = integrity.items, .artifact_effects = vectors.items, .provenance_effects = proofs.items },
    };
    // Artifact phase is permitted only by the row-derived schema capability
    // admission guard; authoritative graph/vector artifacts are not discarded.
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    if ((writes.items.len == 1 or proofs.items.len == 1) and bytes > pages.max_bytes) cache.chunks = try pages.RowChunks(types.BatchRequest).init(request);
    cache.request = request;
    cache.arena = arena;
    arena_owned = false;
    return output(alloc, scope, cache, receipt);
}

test "online direct vector receiver snapshot tail duplicate reply and restart preserve both projections" {
    try testVectorReceiver(true, true, false, false, .none);
}

test "online direct vector certified source proof transfers inertly but requires local adoption" {
    try testVectorReceiver(true, true, true, true, .none);
}

test "online direct vector empty certified source proof capability still requires adoption" {
    try testVectorReceiver(true, true, true, false, .none);
}

test "online direct vector authority activated after snapshot cannot cross final fence" {
    try testVectorReceiver(true, true, false, false, .before_fence);
}

test "online direct vector authority activated after pin cannot publish stale certificate" {
    try testVectorReceiver(true, true, false, false, .before_publication);
}

test "online direct vector receiver default sync journals artifacts before asynchronous projection" {
    try testVectorReceiver(true, false, false, false, .none);
}

test "online direct vector fulltext-only receiver preserves latent artifacts through snapshot tail and restart" {
    try testVectorReceiver(false, false, false, false, .none);
}

const LateAuthority = enum { none, before_publication, before_fence };

fn activateTestAuthority(db: *DB, namespace: @import("artifact_publication.zig").Namespace, catalog_digest: [32]u8) !void {
    const publication = @import("artifact_publication.zig");
    var writer = try db.core.store.beginWriteTxn();
    errdefer writer.abort();
    try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
    try writer.commit();
}

fn testVectorReceiver(active_vectors: bool, full_sync: bool, transfer_proof: bool, selected_proof: bool, late_authority: LateAuthority) !void {
    const alloc = std.testing.allocator;
    const io_contract = @import("online_merge_io_contract.zig");
    const online_io = @import("online_merge_io.zig");
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const donor_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/vector-donor", .{tmp.sub_path});
    defer alloc.free(donor_path);
    const receiver_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/vector-receiver", .{tmp.sub_path});
    defer alloc.free(receiver_path);
    const OpenOptions = @import("antfly_source_root").antfly_sources.physical_db.OpenOptions;
    const donor_options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false };
    const receiver_options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false };
    var donor = try DB.open(alloc, donor_path, donor_options);
    defer donor.close();
    var receiver = try DB.open(alloc, receiver_path, receiver_options);
    defer receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.setSchemaJson(alloc, "{}");
        if (active_vectors) {
            try db.addIndex(.{ .name = "dense", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2}" });
            try db.addIndex(.{ .name = "sparse", .kind = .sparse_vector, .config_json = "{\"field\":\"s\"}" });
        } else try db.addIndex(.{ .name = "text", .kind = .full_text, .config_json = "{}" });
    }
    try donor.updateRange(.{ .start = "a", .end = "m" });
    try receiver.updateRange(.{ .start = "m", .end = "z" });
    const raw = "{\"v\":[1,2],\"s\":{\"indices\":[1,3],\"values\":[2,4]},\"_embeddings\":{\"retired\":[7,8]}}";
    try server_test_adapter.applyOrdered(&donor, .{ .writes = &.{ .{ .key = "a", .value = raw }, .{ .key = "b", .value = raw } }, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
    try server_test_adapter.applyOrdered(&receiver, .{ .writes = &.{.{ .key = "n", .value = raw }}, .sync_level = .full_index }, .{ .term = 1, .index = 1 });
    if (active_vectors) try expectSparseSourceHits(&donor, "initial", 2);
    const keys = @import("../internal_keys.zig");
    const codec = @import("enrichment/artifact_codec.zig");
    const large_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "large_sparse");
    defer alloc.free(large_key);
    const large_digest = large_value: {
        // Historical inactive values exercise transport without manufacturing
        // millions of postings in an unrelated active projection.
        const count: usize = if (active_vectors and full_sync) @import("../retained_effects.zig").max_frame_bytes / 8 + 16 else pages.chunk_bytes / 8 + 16;
        const indices = try alloc.alloc(u32, count);
        defer alloc.free(indices);
        const values = try alloc.alloc(f32, count);
        defer alloc.free(values);
        @memset(indices, 7);
        @memset(values, 1);
        const value = try codec.encodeSparseEmbeddingAlloc(alloc, 42, indices, values);
        defer alloc.free(value);
        try donor.core.store.put(large_key, value);
        break :large_value @import("merge_page_chunks.zig").checksum(value);
    };
    const wide_key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "a", "wide_dense");
    defer alloc.free(wide_key);
    const wide_digest = wide_value: {
        const values = try alloc.alloc(f32, @as(usize, std.math.maxInt(u16)) + 1);
        defer alloc.free(values);
        @memset(values, 2);
        const value = try codec.encodeDenseEmbeddingAlloc(alloc, 43, values);
        defer alloc.free(value);
        try donor.core.store.put(wide_key, value);
        break :wide_value @import("merge_page_chunks.zig").checksum(value);
    };
    var donor_catalog = try donor.artifactInventoryCommand(alloc);
    defer donor_catalog.catalogs.deinit(alloc);
    var receiver_catalog = try receiver.artifactInventoryCommand(alloc);
    defer receiver_catalog.catalogs.deinit(alloc);
    try std.testing.expect(donor_catalog.binding.compatible(receiver_catalog.binding));
    if (transfer_proof and selected_proof) {
        const publication = @import("artifact_publication.zig");
        const provenance = @import("artifact_producer_provenance.zig");
        var namespace: publication.Namespace = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace, donor_options.identity_namespace.?);
        var read = try donor.core.store.beginReadTxn();
        const captured = try publication.capturePrimarySource(alloc, &read, namespace, "a");
        read.abort();
        defer alloc.free(captured.document_key);
        const value = try donor.core.store.get(alloc, large_key);
        defer alloc.free(value);
        const mutation = publication.Mutation{ .family = .base_vector, .key = large_key, .value = value, .source_index = 0 };
        const command: publication.Command = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = donor_catalog.binding.digest, .producer_kind = .index, .producer_name = "large_sparse", .producer_generation = 1, .producer_artifact_name = "large_sparse", .sources = (&captured)[0..1], .mutations = (&mutation)[0..1], .publication_digest = @splat(5) };
        const effect = provenance.Effect{ .family = .base_vector, .key = large_key, .source_index = 0, .value_digest = large_digest, .value_bytes = value.len };
        const logical: provenance.Proof = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = donor_catalog.binding.digest, .producer_kind = .index, .producer_name = "large_sparse", .producer_generation = 1, .producer_artifact_name = "large_sparse", .publication_digest = command.publication_digest, .input_digest = command.inputDigest(), .sources = (&captured)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
        const encoded = try provenance.encodeAlloc(alloc, logical);
        defer alloc.free(encoded);
        var indexed = try provenance.prepareDocumentReferences(alloc, command);
        defer indexed.deinit();
        var writer = try donor.core.store.beginWriteTxn();
        errdefer writer.abort();
        try @import("../source_authority.zig").bind(&writer, .raft, namespace);
        try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = donor_catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try provenance.stageIndexed(&writer, command, encoded, .{ .raft = .{ .term = 1, .index = 1 } }, &indexed);
        try writer.commit();
    } else if (transfer_proof) {
        const publication = @import("artifact_publication.zig");
        var namespace: publication.Namespace = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace, donor_options.identity_namespace.?);
        var writer = try donor.core.store.beginWriteTxn();
        errdefer writer.abort();
        try @import("../source_authority.zig").bind(&writer, .raft, namespace);
        try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = donor_catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try writer.commit();
    }
    const identity = try donor.relationalTopologyIdentity();
    const scope: source.Scope = .{ .fence = .{ .role = .merge_source, .transition_id = 77, .attempt = 1, .admission_epoch = identity.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest }, .receiver_namespace = receiver_options.identity_namespace.?, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    try server_test_adapter.applyOrdered(&donor, .{ .artifact_catalog = donor_catalog, .online_source = .{ .admit = .{ .scope = scope, .artifact_catalog = donor_catalog.binding } } }, .{ .term = 1, .index = 2 });
    const certificate = try donor.prepareOnlineSourcePublication(scope, .none);
    if (late_authority == .before_publication) {
        var namespace: @import("artifact_publication.zig").Namespace = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace, donor_options.identity_namespace.?);
        try activateTestAuthority(&donor, namespace, donor_catalog.binding.digest);
        try std.testing.expectError(error.OnlineMergeProvenanceTransferRequired, server_test_adapter.applyOrdered(&donor, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 }));
        try std.testing.expectEqual(source.SnapshotPhase.pinned, (try donor.onlineSourceStatus(scope)).snapshot_phase);
        return;
    }
    try server_test_adapter.applyOrdered(&donor, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 });
    try server_test_adapter.applyOrdered(&donor, .{ .writes = &.{.{ .key = "a", .value = "{\"v\":[3,4],\"s\":{\"indices\":[1],\"values\":[9]},\"_embeddings\":{\"retired\":[9,10]}}" }}, .sync_level = .full_index }, .{ .term = 1, .index = 4 });
    try server_test_adapter.applyOrdered(&donor, .{ .deletes = &.{"b"}, .sync_level = .full_index }, .{ .term = 1, .index = 5 });
    if (active_vectors) try expectSparseSourceHits(&donor, "updated", 1);
    const retained_head = head: {
        const status_raw = try online_io.executeJson(&donor, alloc, .{ .scope = scope, .operation = .{ .status = .donor } }, .none);
        defer alloc.free(status_raw);
        var status = try std.json.parseFromSlice(io_contract.SourceStatus, alloc, status_raw, .{});
        defer status.deinit();
        break :head status.value.retained_head;
    };
    try server_test_adapter.applyOrdered(&donor, .{ .relational_topology = .{ .fence = scope.fence, .action = .begin } }, .{ .term = 1, .index = 6 });
    if (late_authority == .before_fence) {
        var namespace: @import("artifact_publication.zig").Namespace = undefined;
        @import("doc_identity.zig").encodeNamespace(&namespace, donor_options.identity_namespace.?);
        try activateTestAuthority(&donor, namespace, donor_catalog.binding.digest);
        try std.testing.expectError(error.OnlineMergeProvenanceTransferRequired, server_test_adapter.applyOrdered(&donor, .{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = retained_head } } }, .{ .term = 1, .index = 7 }));
        try std.testing.expectEqual(source.Phase.retaining, (try donor.onlineSourceStatus(scope)).phase);
        return;
    }
    try server_test_adapter.applyOrdered(&donor, .{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = retained_head } } }, .{ .term = 1, .index = 7 });
    const source_identity: pages.Source = .{ .namespace = identity.namespace, .pin_digest = try certificate.digest(), .applied_index = certificate.cut.applied_index, .retention = .{ .epoch = 1, .after_sequence = certificate.cut.retained_start }, .artifact_catalog = donor_catalog.binding, .provenance_required = certificate.provenance_required };
    var checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .accept, .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z", .page_receiver_namespace = scope.receiver_namespace, .page_source = source_identity };
    const context: types.MergeReplicationContext = .{ .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt };
    var accept_context = context;
    accept_context.copy_attempt = .{};
    const accept: types.BatchRequest = .{ .artifact_catalog = receiver_catalog, .merge_replication = accept_context, .merge_checkpoint = checkpoint };
    try server_test_adapter.applyOrdered(&receiver, accept, .{ .term = 1, .index = 2 });
    receiver.close();
    receiver = try DB.open(alloc, receiver_path, receiver_options);
    try server_test_adapter.applyOrdered(&receiver, accept, .{ .term = 1, .index = 2 });
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = scope.copy_attempt;
    try server_test_adapter.applyOrdered(&receiver, .{ .merge_replication = context, .merge_checkpoint = checkpoint }, .{ .term = 1, .index = 3 });
    var index: u64 = 4;
    var restarted = false;
    var chunk_restarted = false;
    var vector_pages: usize = 0;
    var checked_replay_without_catalog = false;
    var complete = false;
    var final_applied_index: u64 = 0;
    var proof_pages: usize = 0;
    for (0..200) |_| {
        const status_raw = try online_io.executeJson(&receiver, alloc, .{ .scope = scope, .operation = .{ .status = .receiver } }, .none);
        defer alloc.free(status_raw);
        var status = try std.json.parseFromSlice(io_contract.ReceiverStatus, alloc, status_raw, .{});
        defer status.deinit();
        const receipt = status.value.progress orelse return error.TestUnexpectedResult;
        if (receipt.phase == .complete) {
            complete = true;
            final_applied_index = receipt.final_applied_index;
            break;
        }
        const operation: io_contract.Request = .{ .scope = scope, .operation = switch (receipt.phase) {
            .cleanup, .cleanup_integrity => .{ .cleanup = receipt },
            .rows, .artifacts => .{ .snapshot = .{ .receipt = receipt, .certificate = certificate } },
            .tail => .{ .tail = receipt },
            .complete => unreachable,
        } };
        const response = try online_io.executeJson(if (receipt.phase == .cleanup or receipt.phase == .cleanup_integrity) &receiver else &donor, alloc, operation, .none);
        defer alloc.free(response);
        var prepared = try std.json.parseFromSlice(io_contract.Prepared, alloc, response, .{});
        defer prepared.deinit();
        var request = prepared.value.request orelse continue;
        request.sync_level = if (full_sync) .full_index else .write;
        if (transfer_proof and request.merge_page.?.tail != null and request.merge_page.?.tail.? == .finish) {
            try std.testing.expect((proof_pages != 0) == selected_proof);
            try std.testing.expectError(error.OnlineMergeProvenanceAdoptionRequired, server_test_adapter.applyOrdered(&receiver, request, .{ .term = 1, .index = index }));
            var namespace: @import("artifact_publication.zig").Namespace = undefined;
            @import("doc_identity.zig").encodeNamespace(&namespace, donor_options.identity_namespace.?);
            const proof_key = @import("source_proof_batch.zig").mergeKey(namespace, source_identity.pin_digest, @splat(5));
            const witness_key = @import("source_proof_batch.zig").witnessKey(namespace, source_identity.pin_digest, @splat(5));
            if (selected_proof) {
                const imported = try receiver.core.store.get(alloc, &proof_key);
                defer alloc.free(imported);
                var decoded = try @import("source_proof_batch.zig").decodeValue(alloc, namespace, @splat(5), imported);
                defer decoded.deinit();
                try std.testing.expectEqualSlices(u8, &.{1}, decoded.bitmap);
                const witness = try receiver.core.store.get(alloc, &witness_key);
                defer alloc.free(witness);
                try std.testing.expectEqualSlices(u8, &decoded.record_digest, witness);
                var receiver_namespace: @import("artifact_publication.zig").Namespace = undefined;
                @import("doc_identity.zig").encodeNamespace(&receiver_namespace, receiver_options.identity_namespace.?);
                var receiver_read = try receiver.core.store.beginReadTxn();
                defer receiver_read.abort();
                try std.testing.expect((try @import("artifact_publication.zig").artifactRevision(&receiver_read, receiver_namespace, large_key)) != null);
            } else {
                try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, &proof_key));
                try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, &witness_key));
            }
            try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, @import("artifact_publication.zig").authority_key));
            return;
        }
        try server_test_adapter.applyOrdered(&receiver, request, .{ .term = 1, .index = index });
        const page = request.merge_page.?;
        if (page.provenance_effects.len != 0 or (if (page.chunk) |chunk| chunk.payload == .provenance and chunk.complete() else false)) proof_pages += 1;
        if (page.artifact_effects.len != 0 or (if (page.chunk) |chunk| chunk.payload == .artifact and chunk.complete() else false)) {
            const entries = try @import("derived/replay_stream.zig").iterateFrom(alloc, receiver.core.store, receiver.core.store.lastReplaySequence(0));
            defer {
                for (entries) |*entry| entry.deinit(alloc);
                alloc.free(entries);
            }
            try std.testing.expectEqual(@as(usize, 1), entries.len);
            var journal = try @import("derived/change_journal.zig").decodeRecord(alloc, entries[0].payload);
            defer journal.deinit();
            for (page.artifact_effects) |effect| if (effect.value != null) {
                const present = for (journal.record.changed_artifact_keys) |key| {
                    if (std.mem.eql(u8, key, effect.key)) break true;
                } else false;
                try std.testing.expect(present);
            };
            if (page.chunk) |chunk| {
                const present = for (journal.record.changed_artifact_keys) |key| {
                    if (std.mem.eql(u8, key, chunk.row_key)) break true;
                } else false;
                try std.testing.expect(present);
            }
        }
        if (request.merge_page.?.artifact_effects.len != 0 or request.merge_page.?.chunk != null) {
            vector_pages += 1;
            const partial_chunk = if (request.merge_page.?.chunk) |chunk| !chunk.complete() else false;
            if (!restarted or (partial_chunk and !chunk_restarted)) {
                receiver.close();
                receiver = try DB.open(alloc, receiver_path, receiver_options);
                donor.close();
                donor = try DB.open(alloc, donor_path, donor_options);
                restarted = true;
                if (partial_chunk) chunk_restarted = true;
            }
        }
        // The reply was lost; exact replay after restart must not recopy or
        // advance a receipt twice, including artifact-only pages.
        try server_test_adapter.applyOrdered(&receiver, request, .{ .term = 1, .index = index });
        index += 1;
        if (!checked_replay_without_catalog and request.merge_page.?.artifact_effects.len != 0) {
            // A delayed retry must retire its new Raft position without
            // consulting a projection catalog that may already have changed.
            var receiver_namespace: @import("artifact_publication.zig").Namespace = undefined;
            @import("doc_identity.zig").encodeNamespace(&receiver_namespace, receiver_options.identity_namespace.?);
            const revision_key = @import("artifact_publication.zig").artifactRevisionKey(receiver_namespace, request.merge_page.?.artifact_effects[0].key);
            const original_revision = try receiver.core.store.get(alloc, &revision_key);
            defer alloc.free(original_revision);
            const inventory_key = @import("artifact_inventory.zig").ordered_key;
            const inventory = try receiver.core.store.get(alloc, inventory_key);
            defer alloc.free(inventory);
            try receiver.core.store.delete(inventory_key);
            const replay = server_test_adapter.applyOrdered(&receiver, request, .{ .term = 1, .index = index });
            try receiver.core.store.put(inventory_key, inventory);
            try replay;
            const replay_revision = try receiver.core.store.get(alloc, &revision_key);
            defer alloc.free(replay_revision);
            try std.testing.expectEqualSlices(u8, original_revision, replay_revision);
            index += 1;
            checked_replay_without_catalog = true;
        }
    }
    try std.testing.expect(complete and restarted and chunk_restarted and checked_replay_without_catalog and vector_pages >= (if (active_vectors and full_sync) @as(usize, 17) else 2));
    // A complete copy is not yet the receiver's public range. Finish through
    // the same ordered checkpoints as the driver before checking visibility.
    checkpoint.kind = .bootstrap_complete;
    checkpoint.page_source = null;
    checkpoint.page_receiver_namespace = null;
    checkpoint.bootstrap_applied_index = final_applied_index;
    try server_test_adapter.applyOrdered(&receiver, .{ .merge_replication = context, .merge_checkpoint = checkpoint }, .{ .term = 1, .index = index });
    index += 1;
    checkpoint.kind = .finalize;
    try server_test_adapter.applyOrdered(&receiver, .{ .merge_replication = context, .merge_checkpoint = checkpoint }, .{ .term = 1, .index = index });
    try std.testing.expectEqualStrings("a", receiver.getRange().start);
    try std.testing.expectEqualStrings("z", receiver.getRange().end);
    for ([_]struct { key: []const u8, digest: pages.Digest }{ .{ .key = large_key, .digest = large_digest }, .{ .key = wide_key, .digest = wide_digest } }) |expected| {
        const value = try receiver.core.store.get(alloc, expected.key);
        defer alloc.free(value);
        try std.testing.expectEqualSlices(u8, &expected.digest, &@import("merge_page_chunks.zig").checksum(value));
    }
    try std.testing.expect(!receiver.hasIndex("large_sparse") and !receiver.hasIndex("wide_dense"));
    try std.testing.expect(!receiver.hasIndex("retired"));
    const historical_key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "a", "retired");
    defer alloc.free(historical_key);
    const historical_raw = try receiver.core.store.get(alloc, historical_key);
    defer alloc.free(historical_raw);
    const historical = try @import("enrichment/artifact_codec.zig").decodeDenseEmbeddingAlloc(alloc, historical_raw);
    defer alloc.free(historical);
    try std.testing.expectEqualSlices(f32, &.{ 9, 10 }, historical);
    if (!active_vectors) {
        try std.testing.expect(!receiver.hasIndex("dense") and !receiver.hasIndex("sparse"));
        return;
    }
    try receiver.waitForCurrentSyncLevel(.full_index);
    // Both owners were cold-opened mid-copy. The source's final fence freezes
    // primary writes, not asynchronous projection replay recovery.
    try donor.waitForCurrentSyncLevel(.full_index);
    for ([_]bool{ false, true }) |sparse| {
        const query: types.SearchRequest = .{ .index_name = if (sparse) "sparse" else "dense", .query = if (sparse) .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1}, .k = 10 } } else .{ .dense_knn = .{ .vector = &.{ 3, 4 }, .k = 10 } }, .limit = 10 };
        const generation = receiver.core.index_manager.coverageGenerationForIndex(query.index_name.?) orelse return error.TestUnexpectedResult;
        const outcome_key = try keys.derivedCoverageOutcomeKeyAlloc(alloc, query.index_name.?, generation, "a");
        defer alloc.free(outcome_key);
        const outcome = try receiver.core.store.get(alloc, outcome_key);
        defer alloc.free(outcome);
        try std.testing.expectEqualStrings("produced", outcome);
        var result = try receiver.search(alloc, query);
        defer result.deinit();
        var original = try donor.search(alloc, query);
        defer original.deinit();
        try std.testing.expectEqual(@as(usize, 1), original.hits.len);
        try std.testing.expectEqual(@as(usize, 2), result.hits.len);
        for (result.hits) |hit| {
            try std.testing.expect(std.mem.eql(u8, hit.id, "a") or std.mem.eql(u8, hit.id, "n"));
            if (std.mem.eql(u8, hit.id, "a")) try std.testing.expectApproxEqAbs(original.hits[0].score.?, hit.score.?, @as(f32, 0.0001));
        }
    }
}

fn expectSparseSourceHits(db: *DB, stage: []const u8, expected: usize) !void {
    _ = stage;
    try db.waitForCurrentSyncLevel(.full_index);
    var result = try db.search(std.testing.allocator, .{ .index_name = "sparse", .query = .{ .sparse_knn = .{ .indices = &.{1}, .values = &.{1}, .k = 10 } }, .limit = 10 });
    defer result.deinit();
    try std.testing.expectEqual(expected, result.hits.len);
}

test "relational index system online snapshot locator resumes immutable rows and chunk receipts after owner reopen" {
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |relational| {
        var tmp = native_platform.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/snapshot", .{tmp.sub_path});
        defer alloc.free(path);
        const options: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var large = try alloc.alloc(u8, 2 * pages.max_bytes + 16);
        defer alloc.free(large);
        @memset(large, 'x');
        @memcpy(large[0..6], "{\"v\":\"");
        @memcpy(large[large.len - 2 ..], "\"}");
        var scope: source.Scope = undefined;
        var certificate: Certificate = undefined;
        {
            var db = try DB.open(alloc, path, options);
            defer db.close();
            if (relational) try db.setSchemaJson(alloc,
                \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"v":{"type":"string"}},"additionalProperties":false}}}}
            );
            try server_test_adapter.applyOrdered(&db, .{ .timestamp_ns = 111, .writes = &.{ .{ .key = "a", .value = large }, .{ .key = "b", .value = "{\"v\":\"second\"}" } } }, .{ .term = 1, .index = 1 });
            const identity = try db.relationalTopologyIdentity();
            scope = .{ .fence = .{ .role = .merge_source, .transition_id = 77, .attempt = 1, .admission_epoch = identity.next_epoch, .peer_group_id = 3, .owner_group_id = 2, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
            try server_test_adapter.applyOrdered(&db, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
            certificate = try db.prepareOnlineSourcePublication(scope, .none);
            try server_test_adapter.applyOrdered(&db, .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } }, .{ .term = 1, .index = 3 });
            try server_test_adapter.applyOrdered(&db, .{ .timestamp_ns = 222, .writes = &.{.{ .key = "a", .value = "{\"v\":\"changed\"}" }} }, .{ .term = 1, .index = 4 });
        }
        var db = try DB.open(alloc, path, options);
        defer db.close();
        var receipt: pages.Progress = .{ .version = 2, .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .receiver_namespace = scope.receiver_namespace, .attempt = scope.copy_attempt, .source = .{ .namespace = scope.fence.namespace, .pin_digest = try certificate.digest(), .applied_index = certificate.cut.applied_index, .retention = .{ .epoch = 1, .after_sequence = certificate.cut.retained_start } }, .phase = .rows, .tail_sequence = certificate.cut.retained_start };
        var receipt_arena = std.heap.ArenaAllocator.init(alloc);
        defer receipt_arena.deinit();
        var chunks: usize = 0;
        var rows: usize = 0;
        var loops: usize = 0;
        var restarted = false;
        while (receipt.phase == .rows and loops < 200) : (loops += 1) {
            const raw = try executeJson(&db, alloc, scope, receipt, certificate, .none);
            defer alloc.free(raw);
            var parsed = try std.json.parseFromSlice(@import("online_merge_io_contract.zig").Prepared, alloc, raw, .{});
            defer parsed.deinit();
            const request = parsed.value.request orelse continue;
            try pages.validateRequest(request);
            const old_position = receipt.snapshot_position;
            if (request.merge_page.?.chunk) |chunk| {
                try std.testing.expectEqualStrings("a", chunk.row_key);
                try std.testing.expectEqual(@as(u64, 111), chunk.timestamp);
                try std.testing.expectEqualSlices(u8, large[@intCast(chunk.offset)..][0..chunk.data.len], chunk.data);
                chunks += 1;
            } else {
                for (request.writes) |row| {
                    try std.testing.expectEqualStrings("b", row.key);
                    try std.testing.expectEqualStrings("{\"v\":\"second\"}", row.value);
                    rows += 1;
                }
            }
            const next = (try pages.plan(receipt, request)).apply;
            if (request.merge_page.?.chunk) |chunk| if (!chunk.complete()) try std.testing.expectEqualDeep(old_position, next.snapshot_position);
            const encoded = try pages.encode(receipt_arena.allocator(), next);
            receipt = try std.json.parseFromSliceLeaky(pages.Progress, receipt_arena.allocator(), encoded, .{ .allocate = .alloc_always });
            if (chunks == 1 and !restarted) {
                // Drop all replica-local row/session caches midassembly. The next
                // request locates the same immutable row using the durable receipt.
                db.close();
                db = try DB.open(alloc, path, options);
                restarted = true;
            }
        }
        try std.testing.expect(loops < 200);
        try std.testing.expect(chunks > 1);
        try std.testing.expectEqual(@as(usize, 1), rows);
        try std.testing.expectEqual(.artifacts, receipt.phase);
        try std.testing.expect(receipt.snapshot_position == null);
    }
}
