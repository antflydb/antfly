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

//! Receiver-local producer completion summaries. Primary/dependency mutations
//! enroll exact document revisions atomically. Only current native witnesses
//! settle requirements; queue admission and replay EOF never grant completion.
//! Status uses point reads. Migration and verification use bounded fair pages.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const completion = @import("artifact_completion_progress.zig");
const keys = @import("../internal_keys.zig");
const Manager = @import("catalog/index_manager.zig").IndexManager;
const registry_key = "\x00\x00__artifact_publication__:producer-readiness:registry";
const pending_prefix = "\x00\x00__artifact_publication__:producer-readiness:pending:";
const count_prefix = "\x00\x00__artifact_publication__:producer-readiness:count:";
const max_bytes = 1024 * 1024;

const State = struct {
    raw: []u8,
    authority: publication.Authority,
    root: u128,
    revision: u64,
    baseline: bool,
    requirements: []const u8,
    cursor: []const u8,
    work_cursor: []const u8,
    bound: []const u8,
    fn deinit(self: State, alloc: std.mem.Allocator) void {
        alloc.free(self.raw);
    }
};
fn load(alloc: std.mem.Allocator, txn: anytype) !?State {
    const borrowed = txn.get(registry_key) catch |err| if (err == error.NotFound) return null else return err;
    if (borrowed.len < 144 or borrowed.len > 4 * max_bytes or !std.mem.eql(u8, borrowed[0..4], "APR1") or borrowed[76] > 1) return error.ArtifactCatalogCorrupt;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(borrowed[0 .. borrowed.len - 32], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, borrowed[borrowed.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const n: usize = std.mem.readInt(u32, borrowed[80..84], .little);
    const c: usize = std.mem.readInt(u32, borrowed[84..88], .little);
    const w: usize = std.mem.readInt(u32, borrowed[88..92], .little);
    const b: usize = std.mem.readInt(u32, borrowed[92..96], .little);
    if (n > max_bytes / 32 or c > max_bytes or w > max_bytes or b > max_bytes or borrowed.len != 144 + n * 32 + c + w + b) return error.ArtifactCatalogCorrupt;
    const raw = try alloc.dupe(u8, borrowed);
    const end = 112 + n * 32;
    return .{ .raw = raw, .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .revision = std.mem.readInt(u64, raw[68..76], .little), .baseline = raw[76] == 1, .root = std.mem.readInt(u128, raw[96..112], .little), .requirements = raw[112..end], .cursor = raw[end .. end + c], .work_cursor = raw[end + c .. end + c + w], .bound = raw[end + c + w .. end + c + w + b] };
}
fn store(alloc: std.mem.Allocator, txn: anytype, state: State) !void {
    const raw = try alloc.alloc(u8, 144 + state.requirements.len + state.cursor.len + state.work_cursor.len + state.bound.len);
    defer alloc.free(raw);
    @memset(raw, 0);
    @memcpy(raw[0..4], "APR1");
    @memcpy(raw[4..28], &state.authority.namespace);
    std.mem.writeInt(u64, raw[28..36], state.authority.epoch, .little);
    @memcpy(raw[36..68], &state.authority.catalog_digest);
    std.mem.writeInt(u64, raw[68..76], state.revision, .little);
    raw[76] = @intFromBool(state.baseline);
    std.mem.writeInt(u32, raw[80..84], @intCast(state.requirements.len / 32), .little);
    std.mem.writeInt(u32, raw[84..88], @intCast(state.cursor.len), .little);
    std.mem.writeInt(u32, raw[88..92], @intCast(state.work_cursor.len), .little);
    std.mem.writeInt(u32, raw[92..96], @intCast(state.bound.len), .little);
    std.mem.writeInt(u128, raw[96..112], state.root, .little);
    var offset: usize = 112;
    for ([_][]const u8{ state.requirements, state.cursor, state.work_cursor, state.bound }) |part| {
        @memcpy(raw[offset..][0..part.len], part);
        offset += part.len;
    }
    std.crypto.hash.sha2.Sha256.hash(raw[0..offset], raw[offset..][0..32], .{});
    try txn.put(registry_key, raw);
}
fn scopedPrefix(comptime prefix: []const u8, authority: publication.Authority, root: u128) [prefix.len + 48]u8 {
    var out: [prefix.len + 48]u8 = undefined;
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, out[prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, out[prefix.len + 32 ..][0..16], root, .big);
    return out;
}
fn counterKey(authority: publication.Authority, root: u128, requirement: [32]u8) [count_prefix.len + 80]u8 {
    var out: [count_prefix.len + 80]u8 = undefined;
    @memcpy(out[0 .. count_prefix.len + 48], &scopedPrefix(count_prefix, authority, root));
    @memcpy(out[out.len - 32 ..], &requirement);
    return out;
}
fn count(txn: anytype, key: []const u8) !u64 {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
    if (raw.len != 8) return error.ArtifactCatalogCorrupt;
    return std.mem.readInt(u64, raw[0..8], .little);
}
fn putNumber(txn: anytype, key: []const u8, value: u64) !void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, value, .little);
    try txn.put(key, &raw);
}
fn optionalRevision(txn: anytype, key: []const u8) !?u64 {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 8) return error.ArtifactCatalogCorrupt;
    return std.mem.readInt(u64, raw[0..8], .little);
}

/// Called from the same mutation transaction as the existing work registry.
pub fn mark(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, document: []const u8) !void {
    var state = (try load(alloc, txn)) orelse return;
    defer state.deinit(alloc);
    if (!std.meta.eql(state.authority, authority)) return; // Next epoch needs its own baseline.
    state.revision = std.math.add(u64, state.revision, 1) catch return error.ResourceLimitExceeded;
    try store(alloc, txn, state);
    const prefix = scopedPrefix(pending_prefix, authority, state.root);
    var i: usize = 0;
    while (i < state.requirements.len) : (i += 32) {
        const id = state.requirements[i..][0..32].*;
        const selected = try std.mem.concat(alloc, u8, &.{ &prefix, &id, document });
        defer alloc.free(selected);
        if (try optionalRevision(txn, selected) == null) {
            const counter = counterKey(authority, state.root, id);
            try putNumber(txn, &counter, std.math.add(u64, try count(txn, &counter), 1) catch return error.ResourceLimitExceeded);
        }
        try putNumber(txn, selected, state.revision);
    }
}

fn register(alloc: std.mem.Allocator, store_handle: anytype, root: u128, plan: *const Manager.WritePlanSnapshot) !void {
    const requirements = if (plan.completion_plan) |*value| value else return;
    var txn = try store_handle.beginWriteTxn();
    errdefer txn.abort();
    const authority = (try publication.authority(&txn)) orelse {
        txn.abort();
        return;
    };
    if (!std.mem.eql(u8, &requirements.catalog, &authority.catalog_digest) or !plan.matchesArtifactInventory(try @import("artifact_inventory.zig").catalogs(&txn))) return error.ArtifactCatalogDrift;
    if (try load(alloc, &txn)) |current| {
        defer current.deinit(alloc);
        if (std.meta.eql(current.authority, authority) and current.root == root) {
            txn.abort();
            return;
        }
    }
    var ids: std.ArrayList(u8) = .empty;
    defer ids.deinit(alloc);
    for (requirements.nodes) |node| if (node.kind == .generated or node.kind == .unit_children) {
        try ids.appendSlice(alloc, &node.id);
        try putNumber(&txn, &counterKey(authority, root, node.id), 0);
    };
    // Fixed upper bound makes migration finite under continued inserts. All
    // writes after registration enroll debt even behind the census cursor.
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var last = try cursor.seekAtOrBefore(&.{keys.user_namespace + 1});
    if (last) |row| if (row.key.len != 0 and row.key[0] == keys.user_namespace + 1) {
        last = try cursor.prev();
    };
    const bound = if (last) |row| if (row.key.len != 0 and row.key[0] == keys.user_namespace) row.key else "" else "";
    try store(alloc, &txn, .{ .raw = &.{}, .authority = authority, .root = root, .revision = 0, .baseline = ids.items.len == 0 or bound.len == 0, .requirements = ids.items, .cursor = "", .work_cursor = "", .bound = bound });
    try txn.commit();
}

fn baselinePage(alloc: std.mem.Allocator, store_handle: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const before = (try load(a, &read)) orelse return;
    if (before.baseline) return;
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(if (before.cursor.len == 0) &.{keys.user_namespace} else before.cursor);
    if (entry) |row| if (std.mem.eql(u8, row.key, before.cursor)) {
        entry = try cursor.next();
    };
    var docs: std.ArrayList([]const u8) = .empty;
    var next = before.cursor;
    var visited: usize = 0;
    var bytes: usize = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |row| {
        if (std.mem.order(u8, row.key, before.bound) == .gt) {
            entry = null;
            break;
        }
        if (visited != 0 and (visited >= 64 or bytes >= 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        if (keys.isStoredDocumentRowKey(row.key)) try docs.append(a, (try keys.decodeStoredDocumentRowKeyAlloc(a, row.key)) orelse return error.ArtifactCatalogCorrupt);
        next = try a.dupe(u8, row.key);
        bytes +|= row.key.len;
        visited += 1;
        entry = try cursor.next();
    }
    var txn = try store_handle.beginWriteTxn();
    errdefer txn.abort();
    var current = (try load(a, &txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql((try publication.authority(&txn)), @as(?publication.Authority, current.authority)) or !std.meta.eql(current.authority, before.authority) or current.root != before.root or current.baseline or !std.mem.eql(u8, current.cursor, before.cursor)) {
        txn.abort();
        return;
    }
    for (docs.items) |doc| try mark(a, &txn, current.authority, doc);
    current = (try load(a, &txn)).?;
    current.cursor = if (entry == null) "" else next;
    current.baseline = entry == null;
    try store(a, &txn, current);
    try txn.commit();
}

pub fn advance(alloc: std.mem.Allocator, store_handle: anytype, root: u128, plan: *const Manager.WritePlanSnapshot) !bool {
    {
        var read = try store_handle.beginReadTxn();
        defer read.abort();
        if (try publication.authority(&read) == null) return false;
    }
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    _ = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, pending_prefix, 48, max_bytes + 48, &identity);
    _ = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, count_prefix, 48, 48, &identity);
    try register(alloc, store_handle, root, plan);
    try baselinePage(alloc, store_handle);
    const requirements = if (plan.completion_plan) |*value| value else return false;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const Item = struct { key: []const u8, revision: u64, witness: ?completion.StreamVerifier.Witness };
    var items: std.ArrayList(Item) = .empty;
    defer for (items.items) |*item| if (item.witness) |*witness| witness.deinit();
    var before: State = undefined;
    var next: []const u8 = "";
    {
        var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        before = (try load(a, &read)) orelse return false;
        const prefix = scopedPrefix(pending_prefix, before.authority, before.root);
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(if (before.work_cursor.len == 0) &prefix else before.work_cursor);
        if (entry) |row| if (std.mem.eql(u8, row.key, before.work_cursor)) {
            entry = try cursor.next();
        };
        var verifier: completion.StreamVerifier = .{ .plan = plan };
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        var bytes: usize = 0;
        while (entry) |row| {
            if (!std.mem.startsWith(u8, row.key, &prefix)) {
                entry = null;
                break;
            }
            if (items.items.len != 0 and (items.items.len >= 32 or bytes >= 64 * 1024 or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
            if (row.key.len <= prefix.len + 32) return error.ArtifactCatalogCorrupt;
            const id = row.key[prefix.len..][0..32];
            const node = for (requirements.nodes) |*node| {
                if (std.mem.eql(u8, &node.id, id)) break node;
            } else return error.ArtifactCatalogDrift;
            const selected = try a.dupe(u8, row.key);
            const revision = (try optionalRevision(&read, selected)).?;
            const witness = verifier.verify(a, &read, root, selected[prefix.len + 32 ..], node) catch |err| switch (err) {
                error.ArtifactPublicationPending, error.EnrichmentSourceChanged => null,
                else => return err,
            };
            try items.append(a, .{ .key = selected, .revision = revision, .witness = witness });
            next = selected;
            bytes +|= row.key.len;
            entry = try cursor.next();
        }
        if (entry == null) next = "";
    }
    if (items.items.len == 0 and before.work_cursor.len == 0) return !before.baseline;
    var txn = try store_handle.beginWriteTxn();
    errdefer txn.abort();
    var current = (try load(a, &txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current.authority, before.authority) or !std.meta.eql((try publication.authority(&txn)), @as(?publication.Authority, current.authority))) return error.ArtifactCatalogDrift;
    var settled = false;
    for (items.items) |item| if (item.witness) |witness| {
        if (try optionalRevision(&txn, item.key) != item.revision) continue;
        witness.requireCurrent(&txn, root) catch |err| switch (err) {
            error.ArtifactPublicationPending, error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => continue,
            else => return err,
        };
        settled = (try settle(&txn, current.authority, current.root, item.key, item.revision)) or settled;
    };
    current.work_cursor = next;
    try store(a, &txn, current);
    try txn.commit();
    return settled or next.len != 0 or !current.baseline;
}

pub fn pending(alloc: std.mem.Allocator, store_handle: anytype, root: u128) !bool {
    var txn = try store_handle.beginReadTxn();
    defer txn.abort();
    const authority = (try publication.authority(&txn)) orelse return false;
    const state = (try load(alloc, &txn)) orelse return true;
    defer state.deinit(alloc);
    if (!std.meta.eql(state.authority, authority) or state.root != root or !state.baseline) return true;
    var i: usize = 0;
    while (i < state.requirements.len) : (i += 32) {
        if (try count(&txn, &counterKey(authority, root, state.requirements[i..][0..32].*)) != 0) return true;
    }
    return false;
}

// Only advance() may call this after revalidating a native completion witness.
fn settle(txn: anytype, authority: publication.Authority, root: u128, selected: []const u8, revision: u64) !bool {
    if (try optionalRevision(txn, selected) != revision) return false;
    const prefix = scopedPrefix(pending_prefix, authority, root);
    if (!std.mem.startsWith(u8, selected, &prefix) or selected.len <= prefix.len + 32) return error.ArtifactCatalogCorrupt;
    const counter = counterKey(authority, root, selected[prefix.len..][0..32].*);
    try putNumber(txn, &counter, std.math.sub(u64, try count(txn, &counter), 1) catch return error.ArtifactCatalogCorrupt);
    try txn.delete(selected);
    return true;
}

/// Dependency closure comes from the pinned completion plan. A graph failure
/// cannot block text whose producer requirements have already settled.
pub fn sourceComplete(alloc: std.mem.Allocator, txn: anytype, plan: *const Manager.WritePlanSnapshot, root: u128, artifact: []const u8) !bool {
    const requirements = if (plan.completion_plan) |*value| value else return false;
    var relevant: std.StringHashMapUnmanaged(void) = .empty;
    defer relevant.deinit(alloc);
    try relevant.put(alloc, artifact, {});
    var changed = true;
    while (changed) {
        changed = false;
        for (requirements.nodes) |node| if ((relevant.contains(node.artifact) or relevant.contains(node.embedding)) and node.upstream.len != 0 and !relevant.contains(node.upstream)) {
            try relevant.put(alloc, node.upstream, {});
            changed = true;
        };
    }
    const state = try load(alloc, txn);
    defer if (state) |value| value.deinit(alloc);
    for (requirements.nodes) |node| {
        if ((node.kind != .generated and node.kind != .unit_children) or (!relevant.contains(node.artifact) and !relevant.contains(node.embedding))) continue;
        const current = state orelse return false;
        if (!current.baseline or current.root != root or !std.mem.eql(u8, &requirements.catalog, &current.authority.catalog_digest) or !std.meta.eql((try publication.authority(txn)), @as(?publication.Authority, current.authority))) return false;
        if (try count(txn, &counterKey(current.authority, current.root, node.id)) != 0) return false;
    }
    return true; // Authored sources have no asynchronous producer dependency.
}

// Local producers use the existing generation-fenced outcome writer, but
// each direct source needs its own tuple: an upstream or sibling outcome
// cannot discharge the final consumer's work.
pub fn localScopeAlloc(alloc: std.mem.Allocator, index: []const u8, artifact: []const u8, plan: *const Manager.WritePlanSnapshot) ![]u8 {
    var relevant: std.StringHashMapUnmanaged(void) = .empty;
    defer relevant.deinit(alloc);
    try relevant.put(alloc, artifact, {});
    const requirements = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    var changed = true;
    while (changed) {
        changed = false;
        for (requirements.nodes) |node| if ((relevant.contains(node.artifact) or relevant.contains(node.embedding)) and node.upstream.len != 0 and !relevant.contains(node.upstream)) {
            try relevant.put(alloc, node.upstream, {});
            changed = true;
        };
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    // Revision-fenced receipts use a new namespace so historical counter drift
    // is repaired once by the existing bounded source recovery owner.
    hash.update("antfly:local-source-completion:v2:");
    for (requirements.nodes) |node| {
        if ((node.kind == .generated or node.kind == .unit_children) and (relevant.contains(node.artifact) or relevant.contains(node.embedding))) hash.update(&node.id);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.mem.concat(alloc, u8, &.{ index, "\x00producer-source:", artifact, "\x00", &digest });
}
pub fn localScopeIndex(scope: []const u8) []const u8 {
    return scope[0 .. std.mem.indexOf(u8, scope, "\x00producer-source:") orelse scope.len];
}
/// Primary replay revision for local producers. Identity creation generations
/// fence delete/recreate, but deliberately do not advance on an overwrite.
pub fn localPrimaryRevisionKeyAlloc(alloc: std.mem.Allocator, document: []const u8) ![]u8 {
    return std.mem.concat(alloc, u8, &.{ "\x00\x00__source_primary_revision__:", document });
}
pub fn localPrimaryRevision(alloc: std.mem.Allocator, txn: anytype, document: []const u8) !?u64 {
    const key = try localPrimaryRevisionKeyAlloc(alloc, document);
    defer alloc.free(key);
    return optionalRevision(txn, key);
}

pub fn localReceiptRevisionKeyAlloc(alloc: std.mem.Allocator, marker: []const u8) ![]u8 {
    return std.mem.concat(alloc, u8, &.{ "\x00\x00__source_completion_revision__:", marker });
}
pub fn localSourceProduced(plan: *const Manager.WritePlanSnapshot, artifact: []const u8, source: []const u8) bool {
    if (std.mem.eql(u8, artifact, source)) return true;
    const requirements = if (plan.completion_plan) |*value| value else return false;
    for (requirements.nodes) |node| {
        if (node.kind != .unit_children or !std.mem.eql(u8, node.artifact, source)) continue;
        const ordinal = node.parent_template orelse continue;
        if (ordinal < plan.generated_templates.len and std.mem.eql(u8, plan.generated_templates[ordinal].artifact_name, artifact)) return true;
    }
    return false;
}

const TestStore = struct {
    entries: std.StringHashMapUnmanaged([]u8) = .empty,
    pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
        return self.entries.get(key) orelse error.NotFound;
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const a = std.testing.allocator;
        const owned = try a.dupe(u8, value);
        errdefer a.free(owned);
        const entry = try self.entries.getOrPut(a, key);
        if (entry.found_existing) a.free(entry.value_ptr.*) else {
            const owned_key = a.dupe(u8, key) catch |err| {
                _ = self.entries.remove(key);
                return err;
            };
            entry.key_ptr.* = owned_key;
        }
        entry.value_ptr.* = owned;
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        if (self.entries.fetchRemove(key)) |entry| {
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
    }
    fn deinit(self: *@This()) void {
        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
            std.testing.allocator.free(entry.value_ptr.*);
        }
        self.entries.deinit(std.testing.allocator);
    }
};

test "ordered artifact inventory producer readiness isolates streams and fences duplicate stale completion" {
    const a = std.testing.allocator;
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    const ids = [_][32]u8{ @splat(3), @splat(4) };
    var memory: TestStore = .{};
    defer memory.deinit();
    try publication.stageAuthority(&memory, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
    try store(a, &memory, .{ .raw = &.{}, .authority = authority, .root = 1, .revision = 0, .baseline = true, .requirements = std.mem.asBytes(&ids), .cursor = "", .work_cursor = "", .bound = "" });
    for (ids) |id| try putNumber(&memory, &counterKey(authority, 1, id), 0);
    const prefix = scopedPrefix(pending_prefix, authority, 1);
    const text = try std.mem.concat(a, u8, &.{ &prefix, &ids[0], "doc" });
    defer a.free(text);
    const graph = try std.mem.concat(a, u8, &.{ &prefix, &ids[1], "doc" });
    defer a.free(graph);
    try mark(a, &memory, authority, "doc");
    const first = (try optionalRevision(&memory, text)).?;
    try mark(a, &memory, authority, "doc");
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[0])));
    try std.testing.expect(!try settle(&memory, authority, 1, text, first));
    const current = (try optionalRevision(&memory, text)).?;
    try std.testing.expect(try settle(&memory, authority, 1, text, current));
    try std.testing.expect(!try settle(&memory, authority, 1, text, current));
    try std.testing.expectEqual(@as(u64, 0), try count(&memory, &counterKey(authority, 1, ids[0])));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[1])));

    const Node = @import("artifact_completion_plan.zig").Node;
    const nodes = [_]Node{
        .{ .id = ids[0], .kind = .generated, .scope = .document, .name = "text", .artifact = "text" },
        .{ .id = ids[1], .kind = .generated, .scope = .document, .name = "graph", .artifact = "relations", .upstream = "text" },
    };
    var plan: Manager.WritePlanSnapshot = .{ .alloc = a, .generation = 1, .dense_fields = &.{}, .sparse_fields = &.{}, .graph_fields = &.{}, .generated_templates = &.{}, .chunk_dependents = &.{}, .completion_plan = .{ .arena = std.heap.ArenaAllocator.init(a), .catalog = authority.catalog_digest, .digest = authority.catalog_digest, .nodes = &nodes, .providers = &.{}, .definitions = .empty } };
    defer plan.completion_plan.?.deinit();
    try std.testing.expect(try sourceComplete(a, &memory, &plan, 1, "text"));
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "relations"));
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 2, "text"));
    try std.testing.expect(try sourceComplete(a, &memory, &plan, 1, "authored"));
    try mark(a, &memory, authority, "doc");
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "text"));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[0])));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[1])));
    try std.testing.expect(!try settle(&memory, authority, 1, graph, current));
    // Reloaded durable state is the same evidence after an owner restart.
    const reloaded = (try load(a, &memory)).?;
    defer reloaded.deinit(a);
    try std.testing.expect(reloaded.baseline);
    try std.testing.expectEqual(@as(u64, 3), reloaded.revision);
}

test "producer readiness local source receipts exclude upstream and sibling graph outputs" {
    const alloc = std.testing.allocator;
    var templates = [_]@import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest{
        .{ .kind = .asset, .index_name = "", .artifact_name = "units", .doc_key = "", .source_field = "url" },
        .{ .kind = .asset, .index_name = "", .artifact_name = "relations", .doc_key = "", .source_field = "text", .upstream_artifact_name = "chunks" },
    };
    var plan: Manager.WritePlanSnapshot = .{ .alloc = alloc, .generation = 1, .dense_fields = &.{}, .sparse_fields = &.{}, .graph_fields = &.{}, .generated_templates = &templates, .chunk_dependents = &.{} };
    var nodes = [_]@import("artifact_completion_plan.zig").Node{
        .{ .id = @splat(1), .kind = .unit_children, .scope = .upstream_units, .name = "chunks", .artifact = "chunks", .upstream = "units", .parent_template = 0 },
    };
    var completion_plan: @import("artifact_completion_plan.zig").Plan = .{ .arena = std.heap.ArenaAllocator.init(alloc), .catalog = @splat(2), .digest = @splat(3), .nodes = &nodes, .providers = &.{}, .definitions = .empty };
    defer completion_plan.arena.deinit();
    plan.completion_plan = completion_plan;
    try std.testing.expect(localSourceProduced(&plan, "units", "chunks"));
    try std.testing.expect(localSourceProduced(&plan, "relations", "relations"));
    try std.testing.expect(!localSourceProduced(&plan, "units", "relations"));
    try std.testing.expect(!localSourceProduced(&plan, "relations", "chunks"));
    const text = try localScopeAlloc(alloc, "search", "chunks", &plan);
    defer alloc.free(text);
    const graph = try localScopeAlloc(alloc, "search", "relations", &plan);
    defer alloc.free(graph);
    try std.testing.expect(!std.mem.eql(u8, text, graph));
    try std.testing.expectEqualStrings("search", localScopeIndex(graph));
    var legacy_hash = std.crypto.hash.sha2.Sha256.init(.{});
    legacy_hash.update("antfly:local-source-completion:v1:");
    var legacy_digest: [32]u8 = undefined;
    legacy_hash.final(&legacy_digest);
    const legacy_graph = try std.mem.concat(alloc, u8, &.{ "search", "\x00producer-source:", "relations", "\x00", &legacy_digest });
    defer alloc.free(legacy_graph);
    try std.testing.expect(!std.mem.eql(u8, graph, legacy_graph));
    nodes[0].id = @splat(9);
    const changed_text = try localScopeAlloc(alloc, "search", "chunks", &plan);
    defer alloc.free(changed_text);
    const unchanged_graph = try localScopeAlloc(alloc, "search", "relations", &plan);
    defer alloc.free(unchanged_graph);
    try std.testing.expect(!std.mem.eql(u8, text, changed_text));
    try std.testing.expectEqualSlices(u8, graph, unchanged_graph);
}
