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

//! Root-local scoped producer outbox. Job insertion and discovery advancement
//! share one transaction; neither operation grants accepted-result evidence.
//! Callers must include the worker wakeup in that same transaction. Per-scope
//! headroom bounds outstanding jobs across generations, not merely one page.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const dispatch = @import("artifact_unit_dispatch.zig");
const Digest = publication.Digest;
const prefix = "\x00\x00__artifact_publication__:unit-jobs:";
const scope_bytes = prefix.len + 80;
pub const Scope = [scope_bytes]u8;
const ControlKey = [scope_bytes + 1]u8;
pub const JobKey = [scope_bytes + 65]u8;
const max_identity_bytes = 1024 * 1024;
const directory_prefix = "\x00\x00__artifact_publication__:unit-worker:";
const DocumentScope = [directory_prefix.len + 80]u8;
const DirectoryKey = [@sizeOf(DocumentScope) + 33]u8;

fn documentScope(root: u128, authority: publication.Authority, document: []const u8) !DocumentScope {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    if (document.len == 0 or document.len > max_identity_bytes) return error.InvalidBatchRequest;
    var result: DocumentScope = undefined;
    @memcpy(result[0..directory_prefix.len], directory_prefix);
    @memcpy(result[directory_prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, result[directory_prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, result[directory_prefix.len + 32 ..][0..16], root, .big);
    @memcpy(result[result.len - 32 ..], &digest(document));
    return result;
}

fn directoryKey(document: DocumentScope, selected: Scope) DirectoryKey {
    var key: DirectoryKey = undefined;
    @memcpy(key[0..document.len], &document);
    key[document.len] = 1;
    @memcpy(key[key.len - 32 ..], selected[selected.len - 32 ..]);
    return key;
}

fn directoryValue(key: DirectoryKey) Digest {
    return checksum(&key, "AUQ1");
}

fn requireDirectory(txn: anytype, key: DirectoryKey, exists: bool) !void {
    const value = txn.get(&key) catch |err| if (err == error.NotFound) null else return err;
    if (exists != (value != null)) return error.ArtifactCatalogCorrupt;
    if (value) |raw| if (!std.mem.eql(u8, raw, &directoryValue(key))) return error.ArtifactCatalogCorrupt;
}

const DocumentPosition = struct {
    revision: u64 = 0,
    scopes: u64 = 0,
    generation: Digest = @splat(0),
    after: ?Digest = null,

    fn encode(self: DocumentPosition, selected: DocumentScope) [117]u8 {
        var raw: [117]u8 = @splat(0);
        @memcpy(raw[0..4], "AUDW");
        std.mem.writeInt(u64, raw[4..12], self.revision, .little);
        std.mem.writeInt(u64, raw[12..20], self.scopes, .little);
        @memcpy(raw[20..52], &self.generation);
        raw[52] = @intFromBool(self.after != null);
        if (self.after) |value| @memcpy(raw[53..85], &value);
        @memcpy(raw[85..], &checksum(&selected, raw[0..85]));
        return raw;
    }

    fn loadOptional(txn: anytype, selected: DocumentScope) !?DocumentPosition {
        const raw = txn.get(&selected) catch |err| if (err == error.NotFound) return null else return err;
        if (raw.len != 117 or !std.mem.eql(u8, raw[0..4], "AUDW") or raw[52] > 1 or
            !std.mem.eql(u8, raw[85..], &checksum(&selected, raw[0..85]))) return error.ArtifactCatalogCorrupt;
        const revision = std.mem.readInt(u64, raw[4..12], .little);
        const count = std.mem.readInt(u64, raw[12..20], .little);
        if (count == 0 or std.mem.allEqual(u8, raw[20..52], 0) or (revision == 0 and raw[52] != 0) or
            (raw[52] == 0 and !std.mem.allEqual(u8, raw[53..85], 0))) return error.ArtifactCatalogCorrupt;
        return .{ .revision = revision, .scopes = count, .generation = raw[20..52].*, .after = if (raw[52] == 0) null else raw[53..85].* };
    }

    fn load(txn: anytype, selected: DocumentScope) !DocumentPosition {
        return (try loadOptional(txn, selected)) orelse .{};
    }
};

/// One nonempty child scope, selected without visiting the catalog or reading
/// unit payloads. The durable document cursor gives siblings independent turns
/// even when one child continually fails. It conveys no completion evidence.
pub const DocumentTurn = struct {
    root: u128,
    authority: publication.Authority,
    document: DocumentScope,
    selected: ?Scope,
    expected_revision: u64,
    generation: Digest,

    pub fn stage(self: DocumentTurn, txn: anytype, root: u128) !void {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        if (!std.meta.eql(self.authority, (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift)) return error.ArtifactCatalogDrift;
        const previous = try DocumentPosition.load(txn, self.document);
        if (previous.scopes == 0 or !std.mem.eql(u8, &previous.generation, &self.generation)) return error.EnrichmentSourceChanged;
        var next = previous;
        next.revision = std.math.add(u64, self.expected_revision, 1) catch return error.ResourceLimitExceeded;
        next.after = if (self.selected) |value| value[value.len - 32 ..].* else null;
        if (previous.revision != self.expected_revision) {
            if (std.meta.eql(previous, next)) return;
            return error.EnrichmentSourceChanged;
        }
        try txn.put(&self.document, &next.encode(self.document));
    }
};

pub fn prepareDocumentTurn(txn: anytype, root: u128, document: []const u8) !?DocumentTurn {
    const authority = (try publication.authority(txn)) orelse return null;
    const selected_document = try documentScope(root, authority, document);
    // Most required-work documents have no scoped child jobs. Keep that path
    // to one point read; only an admitted nonempty directory needs a cursor.
    const previous = (try DocumentPosition.loadOptional(txn, selected_document)) orelse return null;
    var seek: DirectoryKey = undefined;
    @memcpy(seek[0..selected_document.len], &selected_document);
    seek[selected_document.len] = 1;
    @memcpy(seek[seek.len - 32 ..], &(previous.after orelse @as(Digest, @splat(0))));
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var found = try cursor.seekAtOrAfter(if (previous.after != null) &seek else seek[0 .. selected_document.len + 1]);
    if (previous.after != null) if (found) |entry| if (std.mem.eql(u8, entry.key, &seek)) {
        found = try cursor.next();
    };
    var selected: ?Scope = null;
    if (found) |entry| if (std.mem.startsWith(u8, entry.key, seek[0 .. selected_document.len + 1])) {
        if (entry.key.len != seek.len or !std.mem.eql(u8, entry.value, &directoryValue(entry.key[0..@sizeOf(DirectoryKey)].*))) return error.ArtifactCatalogCorrupt;
        selected = try scope(root, authority, entry.key[entry.key.len - 32 ..][0..32].*);
        const state = (try metadata(txn, selected.?)) orelse return error.ArtifactCatalogCorrupt;
        if (state.jobs == 0 or previous.scopes == 0) return error.ArtifactCatalogCorrupt;
    };
    // No cursor/no work needs neither allocation nor a new metadata record.
    if (selected == null and previous.after == null) {
        if (previous.scopes != 0) return error.ArtifactCatalogCorrupt;
        return null;
    }
    return .{ .root = root, .authority = authority, .document = selected_document, .selected = selected, .expected_revision = previous.revision, .generation = previous.generation };
}

pub const Limits = struct {
    jobs: u64 = 1024,
    bytes: u64 = 16 * 1024 * 1024,
};

pub fn scope(root: u128, authority: publication.Authority, identity: Digest) !Scope {
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    var selected: Scope = undefined;
    @memcpy(selected[0..prefix.len], prefix);
    @memcpy(selected[prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, selected[prefix.len + 24 ..][0..8], authority.epoch, .big);
    std.mem.writeInt(u128, selected[prefix.len + 32 ..][0..16], root, .big);
    @memcpy(selected[selected.len - 32 ..], &identity);
    return selected;
}

fn controlKey(selected: Scope, kind: u8) ControlKey {
    var result: ControlKey = undefined;
    @memcpy(result[0..selected.len], &selected);
    result[selected.len] = kind;
    return result;
}

fn jobKey(selected: Scope, generation: Digest, unit: []const u8) JobKey {
    var result: JobKey = undefined;
    @memcpy(result[0..selected.len], &selected);
    result[selected.len] = 2;
    @memcpy(result[selected.len + 1 ..][0..32], &generation);
    std.crypto.hash.sha2.Sha256.hash(unit, result[result.len - 32 ..][0..32], .{});
    return result;
}

fn checksum(selected: []const u8, raw: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-outbox:v1:");
    hash.update(selected);
    hash.update(raw);
    return hash.finalResult();
}

fn digest(raw: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &result, .{});
    return result;
}

pub const Metadata = struct {
    revision: u64,
    jobs: u64,
    bytes: u64,
    generation: Digest,
    cursor_digest: Digest,
    admission_digest: Digest,
    exhausted: bool,

    fn encode(self: Metadata, selected: Scope) [157]u8 {
        var raw: [157]u8 = undefined;
        @memcpy(raw[0..4], "AUM1");
        std.mem.writeInt(u64, raw[4..12], self.revision, .little);
        std.mem.writeInt(u64, raw[12..20], self.jobs, .little);
        std.mem.writeInt(u64, raw[20..28], self.bytes, .little);
        @memcpy(raw[28..60], &self.generation);
        @memcpy(raw[60..92], &self.cursor_digest);
        @memcpy(raw[92..124], &self.admission_digest);
        raw[124] = @intFromBool(self.exhausted);
        @memcpy(raw[125..157], &checksum(&selected, raw[0..125]));
        return raw;
    }

    fn decode(selected: Scope, raw: []const u8) !Metadata {
        if (raw.len != 157 or !std.mem.eql(u8, raw[0..4], "AUM1") or raw[124] > 1 or
            !std.mem.eql(u8, raw[125..157], &checksum(&selected, raw[0..125]))) return error.ArtifactCatalogCorrupt;
        const result: Metadata = .{ .revision = std.mem.readInt(u64, raw[4..12], .little), .jobs = std.mem.readInt(u64, raw[12..20], .little), .bytes = std.mem.readInt(u64, raw[20..28], .little), .generation = raw[28..60].*, .cursor_digest = raw[60..92].*, .admission_digest = raw[92..124].*, .exhausted = raw[124] != 0 };
        if (result.revision == 0 or (result.jobs == 0) != (result.bytes == 0) or
            std.mem.allEqual(u8, &result.generation, 0)) return error.ArtifactCatalogCorrupt;
        return result;
    }
};

fn metadata(txn: anytype, selected: Scope) !?Metadata {
    const raw = txn.get(&controlKey(selected, 0)) catch |err| if (err == error.NotFound) return null else return err;
    return try Metadata.decode(selected, raw);
}

pub const Snapshot = struct { metadata: Metadata, cursor: dispatch.Cursor };

/// The cursor borrows the caller's snapshot. Missing metadata with a surviving
/// cursor is corruption, not permission to restart and overwrite durable work.
pub fn load(txn: anytype, selected: Scope) !?Snapshot {
    const state = try metadata(txn, selected);
    const raw = txn.get(&controlKey(selected, 1)) catch |err| if (err == error.NotFound) null else return err;
    if (state == null and raw == null) return null;
    if (state == null or raw == null) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &state.?.cursor_digest, &digest(raw.?))) return error.ArtifactCatalogCorrupt;
    const cursor = try dispatch.Cursor.decode(raw.?);
    if (!std.mem.eql(u8, &cursor.generation, &state.?.generation) or
        !std.mem.eql(u8, &cursor.identity, selected[selected.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    return .{ .metadata = state.?, .cursor = cursor };
}

pub const Job = struct {
    generation: Digest,
    document: []const u8,
    child: []const u8,
    unit: []const u8,

    fn encodeAlloc(self: Job, alloc: std.mem.Allocator, selected: JobKey) ![]u8 {
        var size: usize = 80;
        for ([_][]const u8{ self.document, self.child, self.unit }) |name| {
            if (name.len == 0 or name.len > max_identity_bytes) return error.ResourceBudgetExceeded;
            size += name.len;
        }
        const raw = try alloc.alloc(u8, size);
        @memcpy(raw[0..4], "AUJ1");
        @memcpy(raw[4..36], &self.generation);
        var offset: usize = 48;
        for ([_][]const u8{ self.document, self.child, self.unit }, 0..) |name, i| {
            std.mem.writeInt(u32, raw[36 + 4 * i ..][0..4], @intCast(name.len), .little);
            @memcpy(raw[offset..][0..name.len], name);
            offset += name.len;
        }
        @memcpy(raw[offset..], &checksum(&selected, raw[0..offset]));
        return raw;
    }

    /// Borrowed identities, bound to the root/scope, generation and unit key.
    pub fn decode(selected: JobKey, raw: []const u8) !Job {
        if (raw.len < 83 or raw.len > 80 + 3 * max_identity_bytes or !std.mem.eql(u8, raw[0..4], "AUJ1") or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(&selected, raw[0 .. raw.len - 32]))) return error.ArtifactCatalogCorrupt;
        var names: [3][]const u8 = undefined;
        var offset: usize = 48;
        for (&names, 0..) |*name, i| {
            const count: usize = std.mem.readInt(u32, raw[36 + i * 4 ..][0..4], .little);
            if (count == 0 or count > max_identity_bytes or count > raw.len - 32 - offset) return error.ArtifactCatalogCorrupt;
            name.* = raw[offset..][0..count];
            offset += count;
        }
        const result: Job = .{ .generation = raw[4..36].*, .document = names[0], .child = names[1], .unit = names[2] };
        if (offset != raw.len - 32 or !std.mem.eql(u8, &selected, &jobKey(selected[0..scope_bytes].*, result.generation, result.unit))) return error.ArtifactCatalogCorrupt;
        return result;
    }
};

pub const WorkPage = struct {
    arena: std.heap.ArenaAllocator,
    items: []const Item,
    after: ?JobKey,
    at_end: bool,

    pub const Item = struct { key: JobKey, raw: []const u8, job: Job };

    pub fn deinit(self: *WorkPage) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Owned, bounded work across ALL generations of this scope. Filtering to
/// the active generation would strand stale jobs and permanently consume its
/// headroom. Callers wrap at scan-end to find insertions behind their cursor.
pub fn scan(alloc: std.mem.Allocator, txn: anytype, selected: Scope, after: ?JobKey, limits: @import("artifact_stream_census.zig").Limits) !WorkPage {
    try limits.validate();
    const selected_prefix = controlKey(selected, 2);
    if (after) |last| if (!std.mem.startsWith(u8, &last, &selected_prefix)) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var items: std.ArrayList(@typeInfo(@FieldType(WorkPage, "items")).pointer.child) = .empty;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const resume_key = after orelse undefined;
    var entry = try cursor.seekAtOrAfter(if (after != null) &resume_key else &selected_prefix);
    if (after != null) if (entry) |found| if (std.mem.eql(u8, found.key, &resume_key)) {
        entry = try cursor.next();
    };
    var next = after;
    var bytes: usize = 0;
    var at_end = false;
    while (entry) |found| {
        if (!std.mem.startsWith(u8, found.key, &selected_prefix)) {
            at_end = true;
            break;
        }
        if (found.key.len != @sizeOf(JobKey) or found.value.len > 80 + 3 * max_identity_bytes) return error.ArtifactCatalogCorrupt;
        const selected_job: JobKey = found.key[0..@sizeOf(JobKey)].*;
        // Validate borrowed framing before allocating. A single bounded large
        // identity may exceed the soft byte budget, so it cannot starve.
        const cost = found.key.len + found.value.len;
        if (items.items.len != 0 and bytes +| cost > limits.bytes) break;
        const decoded = try Job.decode(selected_job, found.value);
        const raw = try owned.dupe(u8, found.value);
        // Rebase the verified slices into one owned allocation; do not hash
        // and parse the same potentially large identity a second time.
        const job: Job = .{ .generation = decoded.generation, .document = raw[48..][0..decoded.document.len], .child = raw[48 + decoded.document.len ..][0..decoded.child.len], .unit = raw[48 + decoded.document.len + decoded.child.len ..][0..decoded.unit.len] };
        try items.append(owned, .{ .key = selected_job, .raw = raw, .job = job });
        next = selected_job;
        bytes +|= cost;
        if (items.items.len == limits.visits or bytes >= limits.bytes) break;
        entry = try cursor.next();
    }
    if (entry == null) at_end = true;
    return .{ .arena = arena, .items = items.items, .after = next, .at_end = at_end };
}

fn catalogStamp(txn: anytype) !?[40]u8 {
    const raw = txn.get(@import("artifact_inventory.zig").local_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 40) return error.ArtifactCatalogCorrupt;
    return raw[0..40].*;
}

/// Independent of discovery/admission. A failed provider still consumes its
/// scheduling turn, but not its job: wrapping retries it without starving the
/// rest of the scope. Neither this cursor nor scan-end is completion evidence.
const WorkerPosition = struct {
    revision: u64 = 0,
    after: ?JobKey = null,
    const encoded_bytes = 45 + @sizeOf(JobKey);

    fn encode(self: WorkerPosition, selected: Scope) [encoded_bytes]u8 {
        var raw: [encoded_bytes]u8 = @splat(0);
        @memcpy(raw[0..4], "AUW1");
        std.mem.writeInt(u64, raw[4..12], self.revision, .little);
        raw[12] = @intFromBool(self.after != null);
        if (self.after) |key| @memcpy(raw[13 .. raw.len - 32], &key);
        @memcpy(raw[raw.len - 32 ..], &checksum(&controlKey(selected, 3), raw[0 .. raw.len - 32]));
        return raw;
    }

    fn decode(selected: Scope, raw: []const u8) !WorkerPosition {
        if (raw.len != encoded_bytes or !std.mem.eql(u8, raw[0..4], "AUW1") or raw[12] > 1 or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(&controlKey(selected, 3), raw[0 .. raw.len - 32]))) return error.ArtifactCatalogCorrupt;
        const revision = std.mem.readInt(u64, raw[4..12], .little);
        if (revision == 0) return error.ArtifactCatalogCorrupt;
        const key: JobKey = raw[13..][0..@sizeOf(JobKey)].*;
        if (raw[12] == 0) {
            if (!std.mem.allEqual(u8, &key, 0)) return error.ArtifactCatalogCorrupt;
        } else if (!std.mem.startsWith(u8, &key, &controlKey(selected, 2))) return error.ArtifactCatalogCorrupt;
        return .{ .revision = revision, .after = if (raw[12] == 0) null else key };
    }

    fn load(txn: anytype, selected: Scope) !WorkerPosition {
        const raw = txn.get(&controlKey(selected, 3)) catch |err| if (err == error.NotFound) return .{} else return err;
        return decode(selected, raw);
    }
};

pub const WorkTurn = struct {
    page: WorkPage,
    root: u128,
    selected: Scope,
    catalog_stamp: ?[40]u8,
    expected_revision: u64,

    pub fn deinit(self: *WorkTurn) void {
        self.page.deinit();
        self.* = undefined;
    }

    /// Commit only after attempting this bounded page. A crash before commit
    /// repeats attempts; a crash after commit resumes the next page. Jobs are
    /// never removed here, even when every callback has returned successfully.
    pub fn stage(self: *const WorkTurn, txn: anytype, root: u128) !enum { advanced, duplicate } {
        if (root != self.root) return error.DurableRootIncarnationUnavailable;
        try requireScopeCurrent(txn, root, self.selected);
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        const previous = try WorkerPosition.load(txn, self.selected);
        const next: WorkerPosition = .{
            .revision = std.math.add(u64, self.expected_revision, 1) catch return error.ResourceLimitExceeded,
            .after = if (self.page.at_end) null else self.page.after,
        };
        if (previous.revision != self.expected_revision) {
            if (std.meta.eql(previous, next)) return .duplicate;
            return error.EnrichmentSourceChanged;
        }
        try txn.put(&controlKey(self.selected, 3), &next.encode(self.selected));
        return .advanced;
    }
};

fn requireScopeCurrent(txn: anytype, root: u128, selected: Scope) !void {
    if (!std.mem.startsWith(u8, &selected, prefix)) return error.InvalidBatchRequest;
    if (root == 0 or std.mem.readInt(u128, selected[prefix.len + 32 ..][0..16], .big) != root) return error.DurableRootIncarnationUnavailable;
    const authority = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.mem.eql(u8, &authority.namespace, selected[prefix.len..][0..24]) or
        authority.epoch != std.mem.readInt(u64, selected[prefix.len + 24 ..][0..8], .big)) return error.ArtifactCatalogDrift;
    if (try metadata(txn, selected) == null) return error.ArtifactCatalogCorrupt;
}

/// Snapshot-owned scan state is copied out before callbacks. Admission and
/// receipt retirement may proceed while this turn runs: neither rewinds this
/// independent cursor, and insertions behind it are found on the next wrap.
pub fn prepareTurn(alloc: std.mem.Allocator, txn: anytype, root: u128, selected: Scope, limits: @import("artifact_stream_census.zig").Limits) !WorkTurn {
    try requireScopeCurrent(txn, root, selected);
    const stamp = try catalogStamp(txn);
    const previous = try WorkerPosition.load(txn, selected);
    return .{ .page = try scan(alloc, txn, selected, previous.after, limits), .root = root, .selected = selected, .catalog_stamp = stamp, .expected_revision = previous.revision };
}

/// Resume discovery independently of worker attempts. Parent replacement
/// restarts enumeration in the new immutable directory; existing old jobs
/// remain charged until receipt-checked obsolete retirement releases them.
pub fn discover(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, producer: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, limits: @import("artifact_stream_census.zig").Limits) !dispatch.Page {
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const child = try completion.unitChild(producer);
    const ordinal = child.parent_template orelse return error.ArtifactPublicationPending;
    if (ordinal >= plan.generated_templates.len) return error.ArtifactCatalogDrift;
    var parent = plan.generated_templates[ordinal];
    parent.doc_key = document;
    const session = (try @import("artifact_chunk_publication.zig").unitVerificationSession(alloc, txn, parent, producer, plan)) orelse return error.ArtifactCatalogDrift;
    const selected = try scope(root, session.authority, dispatch.scopeIdentity(session));
    const previous = try load(txn, selected);
    const after = if (previous) |value| (if (value.metadata.exhausted) null else value.cursor) else null;
    return dispatch.prepare(alloc, session, after, limits) catch |err| {
        if (err == error.EnrichmentSourceChanged and after != null) return dispatch.prepare(alloc, session, null, limits);
        return err;
    };
}

pub const Retirement = struct {
    root: u128,
    selected: Scope,
    document_scope: DocumentScope,
    key: JobKey,
    bytes: usize,
    checksum: Digest,
    catalog_stamp: ?[40]u8,
    resolution: @import("artifact_chunk_publication.zig").JobResolution,

    pub fn deinit(self: *Retirement) void {
        self.resolution.deinit();
        self.* = undefined;
    }

    /// Point-only final fence. Removing scheduling state never completes a
    /// stream or changes its discovery revision. Concurrent admissions and
    /// retirements therefore share current counters without rewinding cursors.
    pub fn stage(self: *const Retirement, txn: anytype, root: u128) !bool {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        const raw = txn.get(&self.key) catch |err| if (err == error.NotFound) return false else return err;
        // A lost reply may be retried after the parent head or catalog has
        // advanced. An absent, root-bound job is already retired; it needs no
        // old acceptance proof and must not resurrect scheduling metadata.
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        try self.resolution.requireCurrent(txn);
        if (raw.len != self.bytes or !std.mem.eql(u8, raw[raw.len - 32 ..], &self.checksum)) return error.EnrichmentSourceChanged;
        var state = (try metadata(txn, self.selected)) orelse return error.ArtifactCatalogCorrupt;
        const document = self.document_scope;
        const directory = directoryKey(document, self.selected);
        try requireDirectory(txn, directory, true);
        const charged = self.key.len + self.bytes;
        if (state.jobs == 0 or state.bytes < charged) return error.ArtifactCatalogCorrupt;
        state.jobs -= 1;
        state.bytes -= charged;
        if ((state.jobs == 0) != (state.bytes == 0)) return error.ArtifactCatalogCorrupt;
        try txn.delete(&self.key);
        if (state.jobs == 0) {
            var position = try DocumentPosition.load(txn, document);
            if (position.scopes == 0) return error.ArtifactCatalogCorrupt;
            position.scopes -= 1;
            try txn.delete(&directory);
            if (position.scopes == 0) try txn.delete(&document) else try txn.put(&document, &position.encode(document));
        }
        try txn.put(&controlKey(self.selected, 0), &state.encode(self.selected));
        return true;
    }
};

/// A callback returning successfully (or an admitted upload) is not sufficient.
/// Reconstruct current accepted-head/child evidence before releasing headroom.
/// Catalog/root changes use obsolete-scope GC instead of adopting old jobs.
fn requireJobRoot(selected_job: JobKey, root: u128) !void {
    if (!std.mem.startsWith(u8, &selected_job, prefix) or selected_job[scope_bytes] != 2) return error.InvalidBatchRequest;
    if (root == 0 or std.mem.readInt(u128, selected_job[prefix.len + 32 ..][0..16], .big) != root) return error.DurableRootIncarnationUnavailable;
}

pub fn RetirementSession(comptime Txn: type) type {
    return struct {
        txn: Txn,
        root: u128,
        selected: Scope,
        document_scope: DocumentScope,
        catalog_stamp: ?[40]u8,
        resolver: @import("artifact_chunk_publication.zig").UnitVerificationSession(Txn).JobResolver,

        pub fn deinit(self: *@This()) void {
            self.resolver.deinit();
            self.* = undefined;
        }

        fn prepareDecoded(self: *@This(), alloc: std.mem.Allocator, selected_job: JobKey, raw: []const u8, job: Job) !?Retirement {
            try requireJobRoot(selected_job, self.root);
            if (!std.mem.eql(u8, &self.selected, selected_job[0..scope_bytes])) return error.ArtifactCatalogDrift;
            if (!std.mem.eql(u8, job.document, self.resolver.session.request.doc_key) or
                !std.mem.eql(u8, job.child, self.resolver.session.request.artifact_name)) return error.ArtifactCatalogCorrupt;
            const resolution = (try self.resolver.resolveJob(alloc, job.generation, job.unit)) orelse return null;
            return .{ .root = self.root, .selected = self.selected, .document_scope = self.document_scope, .key = selected_job, .bytes = raw.len, .checksum = raw[raw.len - 32 ..][0..32].*, .catalog_stamp = self.catalog_stamp, .resolution = resolution };
        }

        pub fn prepare(self: *@This(), alloc: std.mem.Allocator, selected_job: JobKey) !?Retirement {
            try requireJobRoot(selected_job, self.root);
            if (!std.mem.eql(u8, &self.selected, selected_job[0..scope_bytes])) return error.ArtifactCatalogDrift;
            const raw = self.txn.get(&selected_job) catch |err| if (err == error.NotFound) return null else return err;
            return self.prepareDecoded(alloc, selected_job, raw, try Job.decode(selected_job, raw));
        }

        /// The bounded worker page already owns these bytes in this pinned
        /// snapshot. Revalidate its framing without a second LSM point read;
        /// the writer still compares the exact record before deletion.
        pub fn preparePageItem(self: *@This(), alloc: std.mem.Allocator, item: WorkPage.Item) !?Retirement {
            const decoded = try Job.decode(item.key, item.raw);
            if (!std.meta.eql(decoded.generation, item.job.generation) or
                !std.mem.eql(u8, decoded.document, item.job.document) or
                !std.mem.eql(u8, decoded.child, item.job.child) or
                !std.mem.eql(u8, decoded.unit, item.job.unit)) return error.ArtifactCatalogCorrupt;
            return self.prepareDecoded(alloc, item.key, item.raw, decoded);
        }
    };
}

/// Share catalog binding and accepted-parent verification across one bounded
/// worker page. The session borrows its read transaction; prepared retirements
/// own their fences and can outlive it. Never retain this across provider I/O.
pub fn retirementSession(alloc: std.mem.Allocator, txn: anytype, root: u128, document: []const u8, producer: []const u8, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !RetirementSession(@TypeOf(txn)) {
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const child = try completion.unitChild(producer);
    const ordinal = child.parent_template orelse return error.ArtifactPublicationPending;
    if (ordinal >= plan.generated_templates.len) return error.ArtifactCatalogDrift;
    var parent = plan.generated_templates[ordinal];
    parent.doc_key = document;
    const session = (try @import("artifact_chunk_publication.zig").unitVerificationSession(alloc, txn, parent, producer, plan)) orelse return error.ArtifactCatalogDrift;
    const selected = try scope(root, session.authority, dispatch.scopeIdentity(session));
    if (try metadata(txn, selected) == null) return error.ArtifactCatalogCorrupt;
    const stamp = try catalogStamp(txn);
    return .{ .txn = txn, .root = root, .selected = selected, .document_scope = try documentScope(root, session.authority, document), .catalog_stamp = stamp, .resolver = try session.jobResolver(alloc) };
}

pub fn prepareRetirement(alloc: std.mem.Allocator, txn: anytype, root: u128, selected_job: JobKey, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?Retirement {
    try requireJobRoot(selected_job, root);
    const raw = txn.get(&selected_job) catch |err| if (err == error.NotFound) return null else return err;
    const job = try Job.decode(selected_job, raw);
    var session = try retirementSession(alloc, txn, root, job.document, job.child, plan);
    defer session.deinit();
    return session.prepareDecoded(alloc, selected_job, raw, job);
}

pub const Admission = struct {
    arena: std.heap.ArenaAllocator,
    /// Borrowed owned discovery page; no read transaction is retained.
    page: *const dispatch.Page,
    root: u128,
    selected: Scope,
    document_scope: DocumentScope,
    expected_revision: u64,
    catalog_stamp: ?[40]u8,
    encoded_cursor: []const u8,
    cursor_digest: Digest,
    admission_digest: Digest,
    jobs: []const struct { key: JobKey, value: []const u8 },
    limits: Limits,

    pub fn deinit(self: *Admission) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Must share the caller's store transaction with its durable worker wakeup.
    /// Capacity checks and all point fences precede writes. As with other store
    /// batches, the caller must abort the transaction after ANY stage error.
    pub fn stage(self: *const Admission, txn: anytype, root: u128) !enum { admitted, duplicate } {
        if (root == 0 or root != self.root) return error.DurableRootIncarnationUnavailable;
        if (!std.meta.eql(self.catalog_stamp, try catalogStamp(txn))) return error.ArtifactCatalogDrift;
        try self.page.requireCurrent(txn);
        const obligations = (try @import("artifact_producer_obligations.zig").load(txn)) orelse return error.ArtifactCatalogDrift;
        try obligations.requireAuthority(self.page.observation.authority);
        if (obligations.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
        const old = try metadata(txn, self.selected);
        const revision = if (old) |value| value.revision else 0;
        const next_revision = std.math.add(u64, self.expected_revision, 1) catch return error.ResourceLimitExceeded;
        if (revision != self.expected_revision) {
            if (old) |value| if (revision == next_revision and value.exhausted == self.page.at_end and
                std.mem.eql(u8, &value.admission_digest, &self.admission_digest)) return .duplicate;
            return error.EnrichmentSourceChanged;
        }
        var count: u64 = if (old) |value| value.jobs else 0;
        var bytes: u64 = if (old) |value| value.bytes else 0;
        var insert: [128]bool = @splat(false);
        for (self.jobs, 0..) |job, i| {
            const existing = txn.get(&job.key) catch |err| if (err == error.NotFound) null else return err;
            if (existing) |value| {
                if (!std.mem.eql(u8, value, job.value)) return error.ArtifactCatalogCorrupt;
            } else {
                count = std.math.add(u64, count, 1) catch return error.ResourceLimitExceeded;
                bytes = std.math.add(u64, bytes, job.key.len + job.value.len) catch return error.ResourceLimitExceeded;
                insert[i] = true;
            }
        }
        if (count > self.limits.jobs or bytes > self.limits.bytes) return error.ResourceBudgetExceeded;
        const document = self.document_scope;
        const directory = directoryKey(document, self.selected);
        const had_jobs = if (old) |value| value.jobs != 0 else false;
        try requireDirectory(txn, directory, had_jobs);
        if (count != 0 and !had_jobs) {
            var position = try DocumentPosition.load(txn, document);
            if (position.scopes == 0) {
                // A scope's admission revision never rewinds within a root.
                // Bind a fresh document queue generation to its first scope
                // and revision, preventing a retired/refilled queue ABA.
                var revision_bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &revision_bytes, next_revision, .little);
                position.generation = checksum(&self.selected, &revision_bytes);
            }
            position.scopes = std.math.add(u64, position.scopes, 1) catch return error.ResourceLimitExceeded;
            try txn.put(&document, &position.encode(document));
            try txn.put(&directory, &directoryValue(directory));
        }
        for (self.jobs, 0..) |job, i| if (insert[i]) try txn.put(&job.key, job.value);
        try txn.put(&controlKey(self.selected, 1), self.encoded_cursor);
        const next: Metadata = .{ .revision = next_revision, .jobs = count, .bytes = bytes, .generation = self.page.after.generation, .cursor_digest = self.cursor_digest, .admission_digest = self.admission_digest, .exhausted = self.page.at_end };
        try txn.put(&controlKey(self.selected, 0), &next.encode(self.selected));
        return .admitted;
    }
};

pub fn prepare(alloc: std.mem.Allocator, txn: anytype, root: u128, page: *const dispatch.Page, limits: Limits) !Admission {
    if (limits.jobs == 0 or limits.bytes == 0 or page.missing.len > 128) return error.InvalidBatchRequest;
    const selected = try scope(root, page.observation.authority, page.after.identity);
    const old = try load(txn, selected);
    if (page.before) |before| {
        if (old == null or !std.meta.eql(before.identity, old.?.cursor.identity) or !std.meta.eql(before.generation, old.?.cursor.generation) or
            before.phase != old.?.cursor.phase or before.ordinal != old.?.cursor.ordinal or
            !std.mem.eql(u8, before.retirement_key, old.?.cursor.retirement_key)) return error.EnrichmentSourceChanged;
    } else if (old) |value| {
        if (!value.metadata.exhausted and std.mem.eql(u8, &value.metadata.generation, &page.after.generation)) return error.EnrichmentSourceChanged;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const encoded = try page.after.encodeAlloc(owned);
    const jobs = try owned.alloc(@typeInfo(@FieldType(Admission, "jobs")).pointer.child, page.missing.len);
    var seen: std.AutoHashMapUnmanaged(Digest, void) = .empty;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:unit-job-admission:v1:");
    hash.update(&selected);
    hash.update(&digest(encoded));
    hash.update(&.{@intFromBool(page.at_end)});
    for (page.missing, jobs) |unit, *job| {
        job.key = jobKey(selected, page.after.generation, unit);
        if ((try seen.getOrPut(owned, job.key[job.key.len - 32 ..][0..32].*)).found_existing) return error.InvalidBatchRequest;
        job.value = try (Job{ .generation = page.after.generation, .document = page.document, .child = page.child, .unit = unit }).encodeAlloc(owned, job.key);
        hash.update(&job.key);
        hash.update(&digest(job.value));
    }
    return .{ .arena = arena, .page = page, .root = root, .selected = selected, .document_scope = try documentScope(root, page.observation.authority, page.document), .expected_revision = if (old) |value| value.metadata.revision else 0, .catalog_stamp = try catalogStamp(txn), .encoded_cursor = encoded, .cursor_digest = digest(encoded), .admission_digest = hash.finalResult(), .jobs = jobs, .limits = limits };
}

/// Jobs are receiver-local scheduling state. Old-root/epoch rows cannot be
/// adopted as acceptance and are retired by the shared bounded metadata GC.
/// Current-generation job retirement must separately update its live counters.
pub fn collectObsoletePage(alloc: std.mem.Allocator, store: anytype, root: u128) !bool {
    if (root == 0) return true;
    var identity: [16]u8 = undefined;
    std.mem.writeInt(u128, &identity, root, .big);
    const jobs_done = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, prefix, 49, 113, &identity);
    const directory_done = try @import("artifact_producer_obligations.zig").collectObsoleteEpochPageForIdentity(alloc, store, directory_prefix, 48, 81, &identity);
    return jobs_done and directory_done;
}

test "ordered artifact inventory unit jobs bind scope generation and exact framing" {
    const alloc = std.testing.allocator;
    const selected = try scope(7, .{ .namespace = @splat(1), .epoch = 3, .catalog_digest = @splat(4) }, @splat(5));
    const job: Job = .{ .generation = @splat(6), .document = "doc\x00", .child = "child", .unit = "unit\x00\xff" };
    const key = jobKey(selected, job.generation, job.unit);
    const raw = try job.encodeAlloc(alloc, key);
    defer alloc.free(raw);
    try std.testing.expectEqualDeep(job, try Job.decode(key, raw));
    for (0..raw.len) |length| try std.testing.expectError(error.ArtifactCatalogCorrupt, Job.decode(key, raw[0..length]));
    var foreign = key;
    foreign[prefix.len + 32] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Job.decode(foreign, raw));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Job.decode(jobKey(selected, @splat(8), job.unit), raw));
    // Recompute the checksum to ensure structural lengths are checked too.
    std.mem.writeInt(u32, raw[36..40], std.math.maxInt(u32), .little);
    @memcpy(raw[raw.len - 32 ..], &checksum(&key, raw[0 .. raw.len - 32]));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Job.decode(key, raw));
    const state: Metadata = .{ .revision = 1, .jobs = 1, .bytes = key.len + raw.len, .generation = job.generation, .cursor_digest = @splat(9), .admission_digest = @splat(10), .exhausted = false };
    const encoded = state.encode(selected);
    try std.testing.expectEqualDeep(state, try Metadata.decode(selected, &encoded));
    var invalid = state;
    invalid.jobs = 0;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, Metadata.decode(selected, &invalid.encode(selected)));
}

test "ordered artifact inventory worker continuation is canonical and scope bound" {
    const selected = try scope(7, .{ .namespace = @splat(1), .epoch = 3, .catalog_digest = @splat(4) }, @splat(5));
    const position: WorkerPosition = .{ .revision = 2, .after = jobKey(selected, @splat(6), "unit") };
    const raw = position.encode(selected);
    try std.testing.expectEqualDeep(position, try WorkerPosition.decode(selected, &raw));
    for (0..raw.len) |length| try std.testing.expectError(error.ArtifactCatalogCorrupt, WorkerPosition.decode(selected, raw[0..length]));
    var foreign = selected;
    foreign[foreign.len - 1] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, WorkerPosition.decode(foreign, &raw));
    const wrapped: WorkerPosition = .{ .revision = 3 };
    try std.testing.expectEqualDeep(wrapped, try WorkerPosition.decode(selected, &wrapped.encode(selected)));
    var invalid = position;
    invalid.revision = 0;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, WorkerPosition.decode(selected, &invalid.encode(selected)));
    invalid = position;
    invalid.after.?[0] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, WorkerPosition.decode(selected, &invalid.encode(selected)));
    var noncanonical = raw;
    noncanonical[12] = 0;
    @memcpy(noncanonical[noncanonical.len - 32 ..], &checksum(&controlKey(selected, 3), noncanonical[0 .. noncanonical.len - 32]));
    try std.testing.expectError(error.ArtifactCatalogCorrupt, WorkerPosition.decode(selected, &noncanonical));
}

test "ordered artifact inventory document worker fairly resumes nonempty children and wraps behind inserts" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/unit-worker-directory", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try @import("antfly_source_root").antfly_sources.physical_db.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc, "{}");
    try db.addEnrichment(.{ .name = "chunks", .kind = .chunk, .field = "text", .chunk_size = 4 });
    var ordered = try db.artifactInventoryCommand(alloc);
    defer ordered.catalogs.deinit(alloc);
    ordered.binding.effect_protocol = 15;
    try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .artifact_catalog = ordered }, .{ .term = 1, .index = 1 });
    var writer = try db.core.store.beginWriteTxn();
    var writer_open = true;
    defer if (writer_open) writer.abort();
    // Install only the local authority fixture needed by this scheduling
    // test. Catalog replication alone intentionally does not activate owners.
    try publication.stageAuthority(&writer, .{ .mode = .activate, .namespace = ordered.namespace, .authority_epoch = ordered.binding.epoch, .catalog_digest = ordered.catalogs.digest(), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
    const authority = (try publication.authority(&writer)) orelse return error.TestExpectedAuthority;
    const document = try documentScope(db.root_incarnation, authority, "doc\x00\xff");
    const first = try scope(db.root_incarnation, authority, @splat(1));
    const second = try scope(db.root_incarnation, authority, @splat(2));
    const behind = try scope(db.root_incarnation, authority, @splat(0));
    // Scheduling-only fixtures: they cannot supply acceptance or payloads.
    const state: Metadata = .{ .revision = 1, .jobs = 1, .bytes = 100, .generation = @splat(3), .cursor_digest = @splat(4), .admission_digest = @splat(5), .exhausted = true };
    for ([_]Scope{ first, second }) |selected| {
        try writer.put(&controlKey(selected, 0), &state.encode(selected));
        const key = directoryKey(document, selected);
        try writer.put(&key, &directoryValue(key));
    }
    try writer.put(&document, &(DocumentPosition{ .scopes = 2, .generation = @splat(7) }).encode(document));
    const first_turn = (try prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff")).?;
    try std.testing.expectEqualDeep(first, first_turn.selected.?);
    try first_turn.stage(&writer, db.root_incarnation);
    try first_turn.stage(&writer, db.root_incarnation);
    try writer.put(&controlKey(behind, 0), &state.encode(behind));
    const behind_key = directoryKey(document, behind);
    try writer.put(&behind_key, &directoryValue(behind_key));
    var changed_count = try DocumentPosition.load(&writer, document);
    changed_count.scopes += 1;
    try writer.put(&document, &changed_count.encode(document));
    // A duplicate scheduling commit must preserve concurrently admitted scope
    // counts, not overwrite them with a value captured by the old worker.
    try first_turn.stage(&writer, db.root_incarnation);
    try std.testing.expectEqual(@as(u64, 3), (try DocumentPosition.load(&writer, document)).scopes);
    // A failed first child remains pending but cannot monopolize every turn.
    const second_turn = (try prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff")).?;
    try std.testing.expectEqualDeep(second, second_turn.selected.?);
    try second_turn.stage(&writer, db.root_incarnation);
    try std.testing.expectError(error.EnrichmentSourceChanged, first_turn.stage(&writer, db.root_incarnation));
    // Admission behind the saved key is found on wrap; retirement of the
    // current key does not require that cursor target to remain present.
    try writer.delete(&directoryKey(document, second));
    try writer.delete(&controlKey(second, 0));
    changed_count = try DocumentPosition.load(&writer, document);
    changed_count.scopes -= 1;
    try writer.put(&document, &changed_count.encode(document));
    const wrap = (try prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff")).?;
    try std.testing.expectEqual(null, wrap.selected);
    try wrap.stage(&writer, db.root_incarnation);
    const resumed = (try prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff")).?;
    try std.testing.expectEqualDeep(behind, resumed.selected.?);
    try resumed.stage(&writer, db.root_incarnation);
    const retry = (try prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff")).?;
    try std.testing.expectEqualDeep(first, retry.selected.?);
    try std.testing.expectEqual(null, try prepareDocumentTurn(&writer, db.root_incarnation, "doc"));
    try writer.put(&directoryKey(document, first), "corrupt");
    try std.testing.expectError(error.ArtifactCatalogCorrupt, prepareDocumentTurn(&writer, db.root_incarnation, "doc\x00\xff"));
    const first_key = directoryKey(document, first);
    try writer.put(&first_key, &directoryValue(first_key));
    // A drained/refilled queue can have the same cursor revision, but cannot
    // accept a cursor from its old generation (including revision-zero ABA).
    try writer.put(&document, &(DocumentPosition{ .scopes = 2, .generation = @splat(8) }).encode(document));
    try std.testing.expectError(error.EnrichmentSourceChanged, first_turn.stage(&writer, db.root_incarnation));
    try writer.commit();
    writer_open = false;
    // Root-local scheduling state is not adopted by a replacement owner.
    for (0..16) |_| {
        if (try collectObsoletePage(alloc, db.core.store, db.root_incarnation + 1)) break;
    } else return error.TestUnexpectedResult;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(&first_key));
    try std.testing.expectError(error.NotFound, read.get(&document));
    try std.testing.expectError(error.NotFound, read.get(&controlKey(first, 0)));
}
