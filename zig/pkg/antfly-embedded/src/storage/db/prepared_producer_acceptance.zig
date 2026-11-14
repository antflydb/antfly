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

//! Receiver-owned acceptance for an atomic, closed producer result. Incremental
//! worker streams retain their ordinary receipts; this certificate covers only
//! generated requirements executed against the submitted source postimage.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const provenance = @import("artifact_producer_provenance.zig");
const Observation = @import("artifact_stream_observation.zig").Observation;
const commit_hooks = @import("../commit_participant.zig");
const keys = @import("../internal_keys.zig");
const requests = @import("enrichment/enrichment_types.zig");
const Manager = @import("catalog/index_manager.zig").IndexManager;
const prefix = "\x00\x00__artifact_publication__:atomic-producer:v1:";

const Key = [prefix.len + 24 + 8 + 16 + 64]u8;
const record_len = 167;
fn selectedKey(a: std.mem.Allocator, authority: publication.Authority, root: u128, requirement: [32]u8, document: []const u8) ![]u8 {
    const key = try a.alloc(u8, @sizeOf(Key));
    @memcpy(key[0..prefix.len], prefix);
    @memcpy(key[prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, key[prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, key[prefix.len + 32 ..][0..16], root, .big);
    std.crypto.hash.sha2.Sha256.hash(document, key[prefix.len + 48 ..][0..32], .{});
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&authority.catalog_digest);
    hash.update(&requirement);
    hash.final(key[key.len - 32 ..][0..32]);
    return key;
}

fn checksum(key: []const u8, body: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(key);
    hash.update(body);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn validateRecord(key: []const u8, raw: []const u8) !void {
    if (raw.len != record_len or !std.mem.eql(u8, raw[0..4], "APC2") or raw[37] > 1 or !std.mem.eql(u8, raw[135..167], &checksum(key, raw[0..135]))) return error.ArtifactCatalogCorrupt;
}

fn recordCurrent(txn: anytype, namespace: publication.Namespace, raw: []const u8) !bool {
    var primary_key = publication.inputRevisionKey(namespace, "");
    @memcpy(primary_key[primary_key.len - 32 ..], raw[71..103]);
    const primary = txn.get(&primary_key) catch |err| if (err == error.NotFound) return false else return err;
    if (!std.mem.eql(u8, primary, raw[4..37])) return false;
    var scope_key = publication.producerScopeRevisionKey(namespace, "", "");
    @memcpy(scope_key[scope_key.len - 32 ..], raw[103..135]);
    const scope = txn.get(&scope_key) catch |err| if (err == error.NotFound) null else return err;
    return if (raw[37] == 1) scope != null and std.mem.eql(u8, scope.?, raw[38..71]) else scope == null;
}

pub fn collectObsoletePage(a: std.mem.Allocator, store: anytype, root: u128) !bool {
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    return @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(a, store, prefix, 80, 80, if (root == 0) null else &identity);
}

/// A bounded cursor crosses live certificates as well as stale ones. Exact
/// record and revision rechecks keep renewal safe while the census is running.
pub fn collectCurrentPage(a: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    const cursor_key = prefix ++ "cursor";
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    var snapshot = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer snapshot.abort();
    const authority = (try publication.authority(&snapshot)) orelse return true;
    const selected = try selectedKey(owned, authority, root, @splat(0), "");
    const domain = selected[0 .. prefix.len + 48];
    const previous = snapshot.get(cursor_key) catch |err| if (err == error.NotFound) null else return err;
    const expected = if (previous) |bytes| try owned.dupe(u8, bytes) else null;
    const after = if (previous) |bytes| if (bytes.len == @sizeOf(Key) + 32 and std.mem.startsWith(u8, bytes, domain)) bytes[0..@sizeOf(Key)] else null else null;
    const sweep_stamp: [32]u8 = if (after != null) previous.?[@sizeOf(Key)..][0..32].* else try @import("artifact_authored_acceptance.zig").mutationStamp(&snapshot, authority);
    var cursor = try snapshot.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(after orelse domain);
    const Candidate = struct { key: []const u8, value: []const u8 };
    var candidates = std.ArrayList(Candidate).empty;
    var last: ?[]const u8 = null;
    var count: usize = 0;
    var bytes: usize = 0;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, domain)) break;
        if (after) |value| if (std.mem.eql(u8, value, item.key)) {
            entry = try cursor.next();
            continue;
        };
        if (count != 0 and (count >= 128 or bytes + item.key.len + item.value.len > 64 * 1024)) break;
        if (item.key.len != @sizeOf(Key)) return error.ArtifactCatalogCorrupt;
        try validateRecord(item.key, item.value);
        count += 1;
        bytes += item.key.len + item.value.len;
        last = try owned.dupe(u8, item.key);
        if (!try recordCurrent(&snapshot, authority.namespace, item.value)) try candidates.append(owned, .{ .key = last.?, .value = try owned.dupe(u8, item.value) });
        entry = try cursor.next();
    }
    const complete = entry == null or !std.mem.startsWith(u8, entry.?.key, domain);
    if (candidates.items.len == 0 and expected == null and complete) return true;
    var txn = try store.beginWriteTxn();
    errdefer txn.abort();
    if (!std.meta.eql(try publication.authority(&txn), @as(?publication.Authority, authority))) {
        txn.abort();
        return false;
    }
    const current_cursor = txn.get(cursor_key) catch |err| if (err == error.NotFound) null else return err;
    if ((expected == null) != (current_cursor == null) or (expected != null and !std.mem.eql(u8, expected.?, current_cursor.?))) {
        txn.abort();
        return false;
    }
    for (candidates.items) |candidate| {
        const current = txn.get(candidate.key) catch |err| if (err == error.NotFound) continue else return err;
        if (std.mem.eql(u8, current, candidate.value) and !try recordCurrent(&txn, authority.namespace, current)) try txn.delete(candidate.key);
    }
    if (complete) {
        try txn.delete(cursor_key);
    } else if (last) |key| {
        const next = try std.mem.concat(owned, u8, &.{ key, &sweep_stamp });
        try txn.put(cursor_key, next);
    }
    const stable = std.mem.eql(u8, &sweep_stamp, &try @import("artifact_authored_acceptance.zig").mutationStamp(&txn, authority));
    try txn.commit();
    return complete and stable;
}

pub const Closure = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    requirement: [32]u8,
    observation: Observation,
    document: []const u8,
    receipt_key: []const u8,
    receipt: []const u8,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn requireCurrent(self: @This(), txn: anytype, root: u128) !void {
        if (root != self.root) return error.EnrichmentSourceChanged;
        try self.observation.requireCurrent(txn, self.document);
        const current = txn.get(self.receipt_key) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (!std.mem.eql(u8, current, self.receipt)) return error.EnrichmentSourceChanged;
    }
};

pub fn read(a: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, requirement: [32]u8, stream: []const u8) !?Closure {
    const observation = try Observation.capture(txn, document);
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const key = try selectedKey(owned, observation.authority, root, requirement, document);
    const raw = txn.get(key) catch |err| if (err == error.NotFound) {
        arena.deinit();
        return null;
    } else return err;
    try validateRecord(key, raw);
    const encoded = if (try publication.inputRevision(txn, observation.authority.namespace, document)) |position| try position.encode() else return error.ArtifactPublicationPending;
    if (!std.mem.eql(u8, raw[4..37], &encoded)) {
        arena.deinit();
        return null;
    }
    const scope = try publication.producerScopeRevision(txn, observation.authority.namespace, document, stream);
    if (raw[37] > 1 or (raw[37] == 1) != (scope != null)) {
        arena.deinit();
        return null;
    }
    if (scope) |position| if (!std.mem.eql(u8, raw[38..71], &try position.encode())) {
        arena.deinit();
        return null;
    };
    return .{ .arena = arena, .root = root, .requirement = requirement, .observation = observation, .document = try owned.dupe(u8, document), .receipt_key = key, .receipt = try owned.dupe(u8, raw) };
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    root: u128,
    sequence: u64,
    records: []Record,
    budget: @import("commit_requirements.zig").OutputBudget = .{},
    other: ?commit_hooks.Participant,
    const Record = struct { requirement: [32]u8, command: publication.Command, primary: []const u8, source_bytes: []const u8, ttl: []const u8 };
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn init(a: std.mem.Allocator, store: anytype, plan: *const Manager.WritePlanSnapshot, root: u128, sequence: u64, producers: []const requests.PreparedProducer, writes: anytype, generated: anytype, removed: []const []const u8, other: ?commit_hooks.Participant) !Prepared {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var records = std.ArrayList(Record).empty;
        var txn = try store.beginReadTxn();
        defer txn.abort();
        if (try publication.authority(&txn)) |authority| {
            var final: std.StringHashMapUnmanaged([]const u8) = .empty;
            for (writes) |write| try final.put(owned, write.key, write.value);
            var by_scope: std.AutoHashMapUnmanaged([32]u8, std.ArrayList(publication.Mutation)) = .empty;
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            for (generated) |write| {
                const family = familyFor(write.key) orelse continue;
                if ((try seen.getOrPut(owned, write.key)).found_existing) continue;
                const document = try @import("artifact_publication_owner.zig").documentAlloc(owned, write.key);
                const stream = (try keys.artifactNameAlloc(owned, write.key)) orelse return error.InvalidBatchRequest;
                const entry = try by_scope.getOrPut(owned, scopeIdentity(document, stream));
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(owned, .{ .family = family, .key = write.key, .value = final.get(write.key) orelse return error.InvalidBatchRequest, .source_index = 0 });
            }
            for (removed) |key| {
                const family = familyFor(key) orelse continue;
                if ((try seen.getOrPut(owned, key)).found_existing) continue;
                const document = try @import("artifact_publication_owner.zig").documentAlloc(owned, key);
                const stream = (try keys.artifactNameAlloc(owned, key)) orelse return error.InvalidBatchRequest;
                const entry = try by_scope.getOrPut(owned, scopeIdentity(document, stream));
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(owned, .{ .family = family, .key = key, .value = null, .source_index = 0 });
            }
            var definitions: std.AutoHashMapUnmanaged([32]u8, requests.GeneratedEnrichmentRequest) = .empty;
            for (plan.generated_templates) |request| try definitions.put(owned, @import("artifact_producer_input.zig").definitionDigest(request), request);
            for (producers) |produced| {
                var request = definitions.get(produced.definition) orelse return error.ArtifactCatalogDrift;
                request.doc_key = produced.doc_key;
                const revision = produced.revision;
                if (@import("commit_requirements.zig").directEligibility(request) != .eligible) return error.InvalidBatchRequest;
                const binding = (try @import("artifact_producer_input.zig").authorizeTemplate(owned, &txn, request, plan)) orelse return error.ArtifactCatalogDrift;
                const doc_key = try keys.documentKeyAlloc(owned, request.doc_key);
                const row_key = try keys.relationalRowKeyAlloc(owned, request.doc_key);
                const primary = if (final.contains(row_key)) row_key else doc_key;
                const source = if (final.get(primary)) |bytes| bytes else txn.get(primary) catch return error.EnrichmentSourceChanged;
                const ttl_key = try keys.ttlKeyAlloc(owned, request.doc_key);
                const ttl_bytes = if (final.get(ttl_key)) |bytes| bytes else txn.get(ttl_key) catch return error.EnrichmentSourceChanged;
                const timestamp = @import("../ttl.zig").writeVersion(ttl_bytes) orelse return error.InvalidBatchRequest;
                var source_digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(source, &source_digest, .{});
                const source_record = try owned.create(publication.Source);
                source_record.* = .{ .document_key = request.doc_key, .content_digest = source_digest, .timestamp = timestamp, .input_position = null };
                const name = if (request.kind == .dense_embedding or request.kind == .sparse_embedding) requests.requestEmbeddingName(request) else requests.requestArtifactName(request);
                const mutations = by_scope.get(scopeIdentity(request.doc_key, name)) orelse std.ArrayList(publication.Mutation).empty;
                std.mem.sort(publication.Mutation, mutations.items, {}, struct {
                    fn less(_: void, x: publication.Mutation, y: publication.Mutation) bool {
                        return std.mem.order(u8, x.key, y.key) == .lt;
                    }
                }.less);
                const is_index = request.kind == .dense_embedding or request.kind == .sparse_embedding;
                try records.append(owned, .{ .requirement = binding.requirement.id, .primary = primary, .source_bytes = try owned.dupe(u8, source), .ttl = try owned.dupe(u8, ttl_bytes), .command = .{
                    .execution_identity = revision,
                    .namespace = authority.namespace,
                    .authority_epoch = authority.epoch,
                    .catalog_digest = authority.catalog_digest,
                    .producer_kind = if (is_index) .index else .enrichment,
                    .producer_name = if (is_index) request.index_name else name,
                    .producer_generation = if (is_index) plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift else authority.epoch,
                    .producer_artifact_name = name,
                    .sources = source_record[0..1],
                    .mutations = mutations.items,
                    .publication_digest = @splat(0),
                } });
            }
        }
        return .{ .arena = arena, .root = root, .sequence = sequence, .records = try records.toOwnedSlice(owned), .other = other };
    }

    pub fn participant(self: *@This()) commit_hooks.Participant {
        return .{ .ptr = self, .charge = charge, .reset = reset, .observe = observe, .stage = stage };
    }
    fn reset(ptr: *anyopaque) void {
        const self: *Prepared = @ptrCast(@alignCast(ptr));
        self.budget = .{};
        if (self.other) |other| other.reset(other.ptr);
    }
    fn charge(ptr: *anyopaque, key: []const u8, value: ?[]const u8) !void {
        const self: *Prepared = @ptrCast(@alignCast(ptr));
        try self.budget.reserve(key.len, 1);
        if (value) |bytes| try self.budget.reserve(bytes.len, 0);
        if (self.other) |other| if (other.charge) |account| try account(other.ptr, key, value);
    }
    fn observe(ptr: *anyopaque, key: []const u8, value: ?[]const u8) void {
        const self: *Prepared = @ptrCast(@alignCast(ptr));
        if (self.other) |other| other.observe(other.ptr, key, value);
    }
    fn stage(ptr: *anyopaque, view: commit_hooks.View, encoded: []const u8) !void {
        const self: *Prepared = @ptrCast(@alignCast(ptr));
        if (self.other) |other| try other.stage(other.ptr, view, encoded);
        const position = try publication.Position.decode(encoded);
        const a = self.arena.allocator();
        for (self.records) |record| {
            if (!std.mem.eql(u8, try view.get(record.primary), record.source_bytes)) return error.EnrichmentSourceChanged;
            const source = &@constCast(record.command.sources)[0];
            const ttl_key = try keys.ttlKeyAlloc(a, source.document_key);
            if (!std.mem.eql(u8, try view.get(ttl_key), record.ttl)) return error.EnrichmentSourceChanged;
            source.input_position = try publication.inputRevision(view, record.command.namespace, source.document_key);
            var command = record.command;
            command.publication_digest = command.digest();
            if (command.mutations.len != 0) {
                for (command.mutations) |mutation| {
                    const actual = view.get(mutation.key) catch |err| if (err == error.NotFound) null else return err;
                    if (mutation.value) |expected| {
                        if (actual == null or !std.mem.eql(u8, expected, actual.?)) return error.EnrichmentSourceChanged;
                    } else if (actual != null) return error.EnrichmentSourceChanged;
                }
                var proof = try provenance.fromCommand(a, command);
                defer proof.deinit();
                const bytes = try provenance.encodeAlloc(a, proof.proof);
                var refs = try provenance.prepareDocumentReferences(a, command);
                defer refs.deinit();
                try publication.stageReceipts(view, command, self.sequence);
                try provenance.stageIndexed(view, command, bytes, position, &refs);
                try publication.stageArtifactRevisions(a, view, command, position);
            }
            const observation = try Observation.capture(view, source.document_key);
            const key = try selectedKey(a, observation.authority, self.root, record.requirement, source.document_key);
            var raw: [record_len]u8 = @splat(0);
            @memcpy(raw[0..4], "APC2");
            @memcpy(raw[4..37], &try (source.input_position orelse return error.EnrichmentSourceChanged).encode());
            const scope = try publication.producerScopeRevision(view, command.namespace, source.document_key, command.producer_artifact_name);
            raw[37] = @intFromBool(scope != null);
            if (scope) |value| @memcpy(raw[38..71], &try value.encode());
            const primary_key = publication.inputRevisionKey(command.namespace, source.document_key);
            const scope_key = publication.producerScopeRevisionKey(command.namespace, source.document_key, command.producer_artifact_name);
            @memcpy(raw[71..103], primary_key[primary_key.len - 32 ..]);
            @memcpy(raw[103..135], scope_key[scope_key.len - 32 ..]);
            @memcpy(raw[135..167], &checksum(key, raw[0..135]));
            try view.put(key, &raw);
        }
    }
};

fn scopeIdentity(document: []const u8, stream: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, document.len, .little);
    hash.update(&length);
    hash.update(document);
    hash.update(stream);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn familyFor(key: []const u8) ?publication.Family {
    inline for (std.meta.tags(publication.Family)) |family| if (publication.validFamilyKey(family, key)) return family;
    return null;
}
