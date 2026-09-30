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

//! A bounded physical-key migration establishes obligations for rows predating
//! authority activation. New writes are covered by the transaction capture
//! hook, including keys inserted behind this durable cursor.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const obligations = @import("artifact_producer_obligations.zig");
const keys = @import("../internal_keys.zig");
const activation_boundary = @import("artifact_activation_boundary.zig");
const bound_key = "\x00\x00__artifact_publication__:baseline_bound";

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    command: publication.Command,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
    }
};

fn loadBound(txn: anytype, authority: publication.Authority) !?[]const u8 {
    const raw = txn.get(bound_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len < 64 or raw.len > 64 + obligations.max_cursor_bytes) return error.ArtifactCatalogCorrupt;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, raw[0..24], &authority.namespace) or std.mem.readInt(u64, raw[24..32], .little) != authority.epoch) return null;
    return raw[32 .. raw.len - 32];
}

fn stageBound(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, bound: []const u8) !void {
    const raw = try alloc.alloc(u8, 64 + bound.len);
    defer alloc.free(raw);
    @memcpy(raw[0..24], &authority.namespace);
    std.mem.writeInt(u64, raw[24..32], authority.epoch, .little);
    @memcpy(raw[32 .. raw.len - 32], bound);
    std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    try txn.put(bound_key, raw);
}

fn loadState(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority) !?obligations.State {
    if (try obligations.load(txn)) |state| return state;
    // Direct-artifact epochs do not enroll asynchronous producer work. Only
    // an exact catalog capability can explain an absent obligation record;
    // absence in an expanded epoch is corruption, never completion.
    var ordered = (try @import("artifact_inventory.zig").load(alloc, txn)) orelse return error.ArtifactCatalogCorrupt;
    defer ordered.deinit();
    const command = ordered.value.command;
    if (command.binding.epoch != authority.epoch or !std.mem.eql(u8, &command.namespace, &authority.namespace) or
        !std.mem.eql(u8, &command.binding.digest, &authority.catalog_digest)) return error.ArtifactCatalogDrift;
    if (command.binding.effect_protocol == 14) return null;
    return error.ArtifactCatalogCorrupt;
}

/// Only discovers work. The caller must release this snapshot before queue
/// admission, and must never credit successful enqueue as baseline progress.
/// A fixed upper key makes the migration finite even under continued inserts;
/// all post-activation writes are already captured as obligations atomically.
pub fn prepareRaft(alloc: std.mem.Allocator, store: anytype) !?Prepared {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
    defer read.abort();
    const authority = (try publication.authority(&read)) orelse {
        arena.deinit();
        return null;
    };
    _ = try @import("../source_authority.zig").require(&read, .raft, authority.namespace);
    const state = (try loadState(owned, &read, authority)) orelse {
        arena.deinit();
        return null;
    };
    try state.requireAuthority(authority);
    _ = try activation_boundary.requireAuthority(&read, authority);
    if (state.baseline_complete) {
        arena.deinit();
        return null;
    }
    const marker = try read.get(&keys.raft_document_applied_entry_key);
    if (marker.len != 16) return error.ArtifactCatalogCorrupt;
    const term = std.mem.readInt(u64, marker[0..8], .little);
    const index = std.mem.readInt(u64, marker[8..16], .little);
    const expected = try owned.dupe(u8, state.cursor);
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    const bound = if (try loadBound(&read, authority)) |value| try owned.dupe(u8, value) else blk: {
        if (expected.len != 0) return error.ArtifactCatalogCorrupt;
        var last = try cursor.seekAtOrBefore(&.{keys.user_namespace + 1});
        if (last) |item| if (item.key.len != 0 and item.key[0] == keys.user_namespace + 1) {
            last = try cursor.prev();
        };
        break :blk if (last) |item| if (item.key.len != 0 and item.key[0] == keys.user_namespace)
            try owned.dupe(u8, item.key)
        else
            "" else "";
    };
    var entry = if (bound.len == 0) null else try cursor.seekAtOrAfter(if (expected.len == 0) &.{keys.user_namespace} else expected);
    if (entry) |item| if (std.mem.eql(u8, item.key, expected)) {
        entry = try cursor.next();
    };
    var rows: std.ArrayList([]const u8) = .empty;
    var next: []const u8 = expected;
    var visited: usize = 0;
    var bytes: usize = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |item| {
        if (std.mem.order(u8, item.key, bound) == .gt) {
            entry = null;
            break;
        }
        if (visited != 0 and (visited >= 128 or bytes >= 64 * 1024 or item.key.len > 64 * 1024 - bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        if (item.key.len > obligations.max_cursor_bytes) return error.TransactionTooLarge;
        next = try owned.dupe(u8, item.key);
        if (keys.isStoredDocumentRowKey(item.key)) try rows.append(owned, next);
        visited += 1;
        bytes += item.key.len;
        entry = try cursor.next();
    }
    var command: publication.Command = .{ .mode = .baseline, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .baseline = .{ .observed_term = term, .observed_index = index, .expected_cursor = expected, .next_cursor = next, .upper_bound = bound, .row_keys = rows.items, .at_end = entry == null } };
    command.publication_digest = command.digest();
    try command.validate(owned);
    return .{ .arena = arena, .command = command };
}

/// Applies a trusted leader's bounded page in the same transaction as its
/// Raft watermark/outbox. Followers never independently choose scan pages.
/// Stale cursor retries are inert, while foreground dirty counts are retained.
pub fn stageRaft(alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !bool {
    const page_control = command.baseline orelse return error.InvalidBatchRequest;
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (authority.epoch != command.authority_epoch or !std.mem.eql(u8, &authority.namespace, &command.namespace) or
        !std.mem.eql(u8, &authority.catalog_digest, &command.catalog_digest)) return error.ArtifactCatalogDrift;
    const before = (try obligations.load(txn)) orelse return error.ArtifactCatalogDrift;
    try before.requireAuthority(authority);
    _ = try activation_boundary.requireAuthority(txn, authority);
    if (before.baseline_complete) return true;
    if (!std.mem.eql(u8, before.cursor, page_control.expected_cursor)) return false;
    const marker = try txn.get(&keys.raft_document_applied_entry_key);
    if (marker.len != 16) return error.ArtifactCatalogCorrupt;
    const term = std.mem.readInt(u64, marker[0..8], .little);
    const index = std.mem.readInt(u64, marker[8..16], .little);
    if (index < page_control.observed_index or term < page_control.observed_term or
        (index == page_control.observed_index and term != page_control.observed_term)) return error.InvalidBatchRequest;
    if (try loadBound(txn, authority)) |bound| {
        if (!std.mem.eql(u8, bound, page_control.upper_bound)) return false;
    } else {
        if (page_control.expected_cursor.len != 0) return error.ArtifactCatalogCorrupt;
        try stageBound(alloc, txn, authority, page_control.upper_bound);
    }
    for (page_control.row_keys) |row| {
        const document = (try keys.decodeStoredDocumentRowKeyAlloc(alloc, row)) orelse return error.InvalidBatchRequest;
        defer alloc.free(document);
        _ = try obligations.mark(alloc, txn, authority, document, try publication.inputRevision(txn, authority.namespace, document));
    }
    try obligations.advanceBaseline(alloc, txn, authority, page_control.expected_cursor, page_control.next_cursor, page_control.at_end);
    if (page_control.at_end) try txn.delete(bound_key);
    return page_control.at_end;
}

pub fn page(alloc: std.mem.Allocator, store: anytype) !bool {
    var read = try store.beginReadTxn();
    defer read.abort();
    const authority = (try publication.authority(&read)) orelse return true;
    // Raft owners must go through prepareRaft -> ordered stageRaft. A local
    // maintenance task can never certify their distributed baseline.
    _ = try @import("../source_authority.zig").require(&read, .native, authority.namespace);
    const before = (try loadState(alloc, &read, authority)) orelse return true;
    try before.requireAuthority(authority);
    const boundary = try activation_boundary.requireAuthority(&read, authority);
    if (before.baseline_complete) return true;
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(before.cursor);
    if (entry) |item| if (before.cursor.len != 0 and std.mem.eql(u8, before.cursor, item.key)) {
        entry = try cursor.next();
    };
    var documents: std.ArrayList([]u8) = .empty;
    defer {
        for (documents.items) |document| alloc.free(document);
        documents.deinit(alloc);
    }
    var last: std.ArrayList(u8) = .empty;
    defer last.deinit(alloc);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    var count: usize = 0;
    var bytes: usize = 0;
    while (entry) |item| {
        if (count != 0 and (count >= 128 or bytes >= 64 * 1024 or item.key.len > 64 * 1024 - bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        if (item.key.len > obligations.max_cursor_bytes) return error.TransactionTooLarge;
        if (keys.isStoredDocumentRowKey(item.key)) {
            const document = (try keys.decodeStoredDocumentRowKeyAlloc(alloc, item.key)) orelse return error.ArtifactCatalogCorrupt;
            errdefer alloc.free(document);
            try documents.append(alloc, document);
        }
        try last.resize(alloc, item.key.len);
        @memcpy(last.items, item.key);
        count += 1;
        bytes += item.key.len;
        entry = try cursor.next();
    }
    var write = try store.beginWriteTxn();
    errdefer write.abort();
    const current_authority = (try publication.authority(&write)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(authority, current_authority)) return error.ArtifactCatalogDrift;
    try boundary.requireCurrent(&write);
    const current = (try obligations.load(&write)) orelse return error.ArtifactCatalogDrift;
    try current.requireAuthority(authority);
    if (current.baseline_complete or !std.mem.eql(u8, current.cursor, before.cursor)) {
        write.abort();
        return current.baseline_complete;
    }
    for (documents.items) |document| {
        // Stamp the CURRENT input, never the potentially older discovery
        // snapshot. A concurrent deletion has its own captured dirty input;
        // marking it again coalesces without resurrecting the document.
        _ = try obligations.mark(alloc, &write, authority, document, try publication.inputRevision(&write, authority.namespace, document));
    }
    try obligations.advanceBaseline(alloc, &write, authority, before.cursor, if (count == 0) before.cursor else last.items, entry == null);
    try write.commit();
    return entry == null;
}
