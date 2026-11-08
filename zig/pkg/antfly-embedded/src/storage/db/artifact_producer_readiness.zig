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
// v2 rebuilds summaries that older unfenced maintenance could undercount.
// These are derived caches; primary obligations and publication receipts stay authoritative.
const legacy_registry_key = "\x00\x00__artifact_publication__:producer-readiness:registry";
const legacy_pending_prefix = "\x00\x00__artifact_publication__:producer-readiness:pending:";
const legacy_count_prefix = "\x00\x00__artifact_publication__:producer-readiness:count:";
const registry_key = "\x00\x00__artifact_publication__:producer-readiness:v2:registry";
const enrollment_key = "\x00\x00__artifact_publication__:producer-readiness:v2:enrollment";
const revision_prefix = "\x00\x00__artifact_publication__:producer-readiness:v2:revision:";
const pending_prefix = "\x00\x00__artifact_publication__:producer-readiness:v2:pending:";
const count_prefix = "\x00\x00__artifact_publication__:producer-readiness:v2:count:";
const max_requirement_bytes = 1024 * 1024;
const max_cursor_bytes = publication.max_cursor_bytes;

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
    return loadAt(alloc, txn, registry_key);
}
fn loadAt(alloc: std.mem.Allocator, txn: anytype, key: []const u8) !?State {
    const borrowed = txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
    const compact = std.mem.eql(u8, key, enrollment_key);
    const maximum = 144 + max_requirement_bytes + (if (compact) @as(usize, 0) else 3 * max_cursor_bytes);
    if (borrowed.len < 144 or borrowed.len > maximum or !std.mem.eql(u8, borrowed[0..4], "APR1") or borrowed[76] > 1) return error.ArtifactCatalogCorrupt;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(borrowed[0 .. borrowed.len - 32], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, borrowed[borrowed.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const n: usize = std.mem.readInt(u32, borrowed[80..84], .little);
    const c: usize = std.mem.readInt(u32, borrowed[84..88], .little);
    const w: usize = std.mem.readInt(u32, borrowed[88..92], .little);
    const b: usize = std.mem.readInt(u32, borrowed[92..96], .little);
    if (compact and (c != 0 or w != 0 or b != 0 or std.mem.readInt(u64, borrowed[68..76], .little) != 0)) return error.ArtifactCatalogCorrupt;
    if (n > max_requirement_bytes / 32 or c > max_cursor_bytes or w > max_cursor_bytes or b > max_cursor_bytes or borrowed.len != 144 + n * 32 + c + w + b) return error.ArtifactCatalogCorrupt;
    const raw = try alloc.dupe(u8, borrowed);
    const end = 112 + n * 32;
    return .{ .raw = raw, .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .revision = std.mem.readInt(u64, raw[68..76], .little), .baseline = raw[76] == 1, .root = std.mem.readInt(u128, raw[96..112], .little), .requirements = raw[112..end], .cursor = raw[end .. end + c], .work_cursor = raw[end + c .. end + c + w], .bound = raw[end + c + w .. end + c + w + b] };
}
fn store(alloc: std.mem.Allocator, txn: anytype, state: State) !void {
    return storeAt(alloc, txn, registry_key, state);
}
fn storeEnrollment(alloc: std.mem.Allocator, txn: anytype, state: State) !void {
    var compact = state;
    compact.revision = 0;
    compact.cursor = "";
    compact.work_cursor = "";
    compact.bound = "";
    return storeAt(alloc, txn, enrollment_key, compact);
}
fn storeAt(alloc: std.mem.Allocator, txn: anytype, key: []const u8, state: State) !void {
    if (state.requirements.len > max_requirement_bytes or state.cursor.len > max_cursor_bytes or state.work_cursor.len > max_cursor_bytes or state.bound.len > max_cursor_bytes) return error.InvalidBatchRequest;
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
    try txn.put(key, raw);
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
    const state = (try loadAt(alloc, txn, enrollment_key)) orelse return;
    defer state.deinit(alloc);
    if (!std.meta.eql(state.authority, authority)) return; // Next epoch needs its own baseline.
    // Enrollment never reads or rewrites potentially huge scan checkpoints.
    const revision_key = scopedPrefix(revision_prefix, authority, state.root);
    const revision = std.math.add(u64, try count(txn, &revision_key), 1) catch return error.ResourceLimitExceeded;
    try putNumber(txn, &revision_key, revision);
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
        try putNumber(txn, selected, revision);
    }
}

fn matchesDurableCatalog(txn: anytype, plan: *const Manager.WritePlanSnapshot, authority: publication.Authority) !bool {
    // The plan serializes empty catalogs, while a durable empty catalog may
    // have no record. Bind authority to the exact stored bytes and independently
    // prove that the pinned plan represents those definitions.
    const catalogs = try @import("artifact_inventory.zig").catalogs(txn);
    return plan.matchesArtifactInventory(catalogs) and std.mem.eql(u8, &catalogs.digest(), &authority.catalog_digest);
}

fn register(alloc: std.mem.Allocator, store_handle: anytype, root: u128, plan: *const Manager.WritePlanSnapshot) !void {
    const requirements = if (plan.completion_plan) |*value| value else return;
    var txn = try store_handle.beginWriteTxn();
    errdefer txn.abort();
    const authority = (try publication.authority(&txn)) orelse {
        txn.abort();
        return;
    };
    if (!try matchesDurableCatalog(&txn, plan, authority)) return error.ArtifactCatalogDrift;
    if (try loadAt(alloc, &txn, enrollment_key)) |current| {
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
    const bound = blk: {
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        var last = try cursor.seekAtOrBefore(&.{keys.user_namespace + 1});
        if (last) |row| if (row.key.len != 0 and row.key[0] == keys.user_namespace + 1) {
            last = try cursor.prev();
        };
        const borrowed = if (last) |row| if (row.key.len != 0 and row.key[0] == keys.user_namespace) row.key else "" else "";
        // Own the boundary and close the transaction's cursor before mutation.
        break :blk try alloc.dupe(u8, borrowed);
    };
    defer alloc.free(bound);
    const initial: State = .{ .raw = &.{}, .authority = authority, .root = root, .revision = 0, .baseline = ids.items.len == 0 or bound.len == 0, .requirements = ids.items, .cursor = "", .work_cursor = "", .bound = bound };
    try store(alloc, &txn, initial);
    try storeEnrollment(alloc, &txn, initial);
    try putNumber(&txn, &scopedPrefix(revision_prefix, authority, root), 0);
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
        if (keys.isStoredSourceRowKey(row.key)) try docs.append(a, (try keys.decodeStoredSourceRowKeyAlloc(a, row.key)) orelse return error.ArtifactCatalogCorrupt);
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
    if (current.baseline) try storeEnrollment(a, &txn, current);
    try txn.commit();
}

/// Retire obsolete derived summaries in bounded pages, including one oversized
/// identity. The caller supplies the same write admission as primary enrollment.
pub fn collectLegacyPage(alloc: std.mem.Allocator, store_handle: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var retired: std.ArrayList([]const u8) = .empty;
    {
        var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const registry_exists = if (read.get(legacy_registry_key)) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
        if (registry_exists) try retired.append(a, legacy_registry_key);
        var bytes: usize = 0;
        var visited: usize = 0;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        for ([_][]const u8{ legacy_pending_prefix, legacy_count_prefix }) |prefix| {
            var cursor = try read.openPhysicalCursorAdapter();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (visited != 0 and (visited >= 128 or bytes >= 64 * 1024 or row.key.len > 64 * 1024 - bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
                try retired.append(a, try a.dupe(u8, row.key));
                bytes +|= row.key.len;
                visited += 1;
                entry = try cursor.next();
            }
        }
    }
    if (retired.items.len == 0) return;
    var write = try store_handle.beginWriteTxn();
    errdefer write.abort();
    for (retired.items) |key| write.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try write.commit();
}

pub fn advance(alloc: std.mem.Allocator, store_handle: anytype, root: u128, plan: *const Manager.WritePlanSnapshot) !bool {
    {
        var read = try store_handle.beginReadTxn();
        defer read.abort();
        if (try publication.authority(&read) == null) return false;
    }
    try collectLegacyPage(alloc, store_handle);
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    _ = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, pending_prefix, 48, max_cursor_bytes + 48, &identity);
    _ = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, count_prefix, 48, 48, &identity);
    _ = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store_handle, revision_prefix, 16, 16, &identity);
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
            if (row.key.len < prefix.len + 32) return error.ArtifactCatalogCorrupt;
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
            error.ArtifactPublicationPending, error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => {
                std.log.debug("producer source readiness witness changed before commit: {s}", .{@errorName(err)});
                continue;
            },
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
    const state = (try loadAt(alloc, &txn, enrollment_key)) orelse return true;
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
    if (!std.mem.startsWith(u8, selected, &prefix) or selected.len < prefix.len + 32) return error.ArtifactCatalogCorrupt;
    const counter = counterKey(authority, root, selected[prefix.len..][0..32].*);
    try putNumber(txn, &counter, std.math.sub(u64, try count(txn, &counter), 1) catch return error.ArtifactCatalogCorrupt);
    try txn.delete(selected);
    return true;
}

/// Dependency closure comes from the pinned completion plan. A graph failure
/// cannot block text whose producer requirements have already settled.
pub fn sourceComplete(alloc: std.mem.Allocator, txn: anytype, plan: *const Manager.WritePlanSnapshot, root: u128, artifact: []const u8, inputs_empty: bool) !bool {
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
    const state = try loadAt(alloc, txn, enrollment_key);
    defer if (state) |value| value.deinit(alloc);
    // A new empty owner has no registry until its first primary mutation.
    // The caller proves absence through this same pinned transaction; a
    // missing registry on any populated/unknown owner remains incomplete.
    if (state == null and inputs_empty) return true;
    for (requirements.nodes) |node| {
        if ((node.kind != .generated and node.kind != .unit_children) or (!relevant.contains(node.artifact) and !relevant.contains(node.embedding))) continue;
        const current = state orelse {
            std.log.debug("producer source readiness pending: registry not published", .{});
            return false;
        };
        if (!current.baseline or current.root != root) {
            std.log.debug("producer source readiness pending: baseline={} root_current={}", .{ current.baseline, current.root == root });
            return false;
        }
        if (!try matchesDurableCatalog(txn, plan, current.authority)) {
            std.log.debug("producer source readiness pending: catalog differs", .{});
            return false;
        }
        if (!std.meta.eql((try publication.authority(txn)), @as(?publication.Authority, current.authority))) {
            std.log.debug("producer source readiness pending: authority differs", .{});
            return false;
        }
        const remaining = try count(txn, &counterKey(current.authority, current.root, node.id));
        if (remaining != 0) {
            std.log.debug("producer source readiness pending: {d} generated inputs", .{remaining});
            return false;
        }
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

test "privacy ordered artifact inventory producer readiness isolates streams and fences duplicate stale completion" {
    const a = std.testing.allocator;
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = (@import("artifact_inventory.zig").Catalogs{}).digest() };
    const ids = [_][32]u8{ @splat(3), @splat(4) };
    var memory: TestStore = .{};
    defer memory.deinit();
    try publication.stageAuthority(&memory, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
    try store(a, &memory, .{ .raw = &.{}, .authority = authority, .root = 1, .revision = 0, .baseline = true, .requirements = std.mem.asBytes(&ids), .cursor = "", .work_cursor = "", .bound = "" });
    try storeEnrollment(a, &memory, .{ .raw = &.{}, .authority = authority, .root = 1, .revision = 0, .baseline = true, .requirements = std.mem.asBytes(&ids), .cursor = "", .work_cursor = "", .bound = "" });
    try putNumber(&memory, &scopedPrefix(revision_prefix, authority, 1), 0);
    for (ids) |id| try putNumber(&memory, &counterKey(authority, 1, id), 0);
    const prefix = scopedPrefix(pending_prefix, authority, 1);
    const text = try std.mem.concat(a, u8, &.{ &prefix, &ids[0], "" });
    defer a.free(text);
    const graph = try std.mem.concat(a, u8, &.{ &prefix, &ids[1], "" });
    defer a.free(graph);
    try mark(a, &memory, authority, "");
    const first = (try optionalRevision(&memory, text)).?;
    try mark(a, &memory, authority, "");
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
    var plan: Manager.WritePlanSnapshot = .{ .alloc = a, .generation = 1, .dense_fields = &.{}, .sparse_fields = &.{}, .graph_fields = &.{}, .generated_templates = &.{}, .chunk_dependents = &.{}, .artifact_catalogs = .{}, .catalog_empty = @splat(true), .completion_plan = .{ .arena = std.heap.ArenaAllocator.init(a), .catalog = authority.catalog_digest, .digest = authority.catalog_digest, .nodes = &nodes, .providers = &.{}, .definitions = .empty } };
    defer plan.completion_plan.?.deinit();
    try std.testing.expect(try sourceComplete(a, &memory, &plan, 1, "text", false));
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "relations", false));
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 2, "text", false));
    try std.testing.expect(try sourceComplete(a, &memory, &plan, 1, "authored", false));
    try mark(a, &memory, authority, "");
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "text", false));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[0])));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[1])));
    try std.testing.expect(!try settle(&memory, authority, 1, graph, current));
    // Reloaded durable state is the same evidence after an owner restart.
    const reloaded = (try load(a, &memory)).?;
    defer reloaded.deinit(a);
    try std.testing.expect(reloaded.baseline);
    try std.testing.expectEqual(@as(u64, 0), reloaded.revision);
    try std.testing.expectEqual(@as(u64, 3), try count(&memory, &scopedPrefix(revision_prefix, authority, 1)));
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "text", true));
    try memory.delete(registry_key);
    try memory.delete(enrollment_key);
    try std.testing.expect(!try sourceComplete(a, &memory, &plan, 1, "text", false));
    try std.testing.expect(try sourceComplete(a, &memory, &plan, 1, "text", true));
}

test "privacy producer readiness local source receipts exclude upstream and sibling graph outputs" {
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

test "privacy producer readiness round trips large persisted cursors in APR1" {
    const a = std.testing.allocator;
    const cursor = try a.alloc(u8, 1_100_000);
    defer a.free(cursor);
    @memset(cursor, 'x');
    var memory: TestStore = .{};
    defer memory.deinit();
    const requirements: [32]u8 = @splat(9);
    try store(a, &memory, .{ .raw = &.{}, .authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, .root = 1, .revision = 3, .baseline = false, .requirements = &requirements, .cursor = cursor, .work_cursor = cursor, .bound = cursor });
    const restored = (try load(a, &memory)).?;
    defer restored.deinit(a);
    try std.testing.expectEqualSlices(u8, "APR1", restored.raw[0..4]);
    try std.testing.expectEqualSlices(u8, cursor, restored.cursor);
    try std.testing.expectEqualSlices(u8, cursor, restored.work_cursor);
    try std.testing.expectEqualSlices(u8, cursor, restored.bound);
    try std.testing.expectEqualSlices(u8, &requirements, restored.requirements);
    try std.testing.expectEqual(@as(u64, 3), restored.revision);
}

test "privacy readiness enrollment leaves large checkpoints unchanged and status descriptors small" {
    const a = std.testing.allocator;
    var memory: TestStore = .{};
    defer memory.deinit();
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    const ids = [_][32]u8{@splat(3)};
    const cursor = try a.alloc(u8, 1_100_000);
    defer a.free(cursor);
    @memset(cursor, 'x');
    const initial: State = .{ .raw = &.{}, .authority = authority, .root = 1, .revision = 0, .baseline = true, .requirements = std.mem.asBytes(&ids), .cursor = cursor, .work_cursor = cursor, .bound = cursor };
    try store(a, &memory, initial);
    try storeEnrollment(a, &memory, initial);
    try putNumber(&memory, &scopedPrefix(revision_prefix, authority, 1), 0);
    try putNumber(&memory, &counterKey(authority, 1, ids[0]), 0);
    const before = try a.dupe(u8, try memory.get(registry_key));
    defer a.free(before);
    for (0..100) |_| try mark(a, &memory, authority, "doc");
    try std.testing.expectEqualSlices(u8, before, try memory.get(registry_key));
    try std.testing.expect((try memory.get(enrollment_key)).len < 1024);
    try std.testing.expectEqual(@as(u64, 100), try count(&memory, &scopedPrefix(revision_prefix, authority, 1)));
    try std.testing.expectEqual(@as(u64, 1), try count(&memory, &counterKey(authority, 1, ids[0])));
    try memory.put(enrollment_key, before);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, mark(a, &memory, authority, "doc"));
    try std.testing.expectEqual(@as(u64, 100), try count(&memory, &scopedPrefix(revision_prefix, authority, 1)));
}
