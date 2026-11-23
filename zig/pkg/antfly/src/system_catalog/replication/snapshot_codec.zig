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

//! Validate retained subscription snapshot rows and page coverage against the
//! provider roster and the durable initialization receipt.
const std = @import("std");
const local = @import("antfly_local_sources");
const origin = local.system_catalog_logical_replication_origin;
const purpose = local.api_replication_source_purpose;
const artifact = local.storage_artifact_identity;
const payload = @import("payload.zig");
const ArtifactStore = @import("../../serverless/artifacts/store.zig").ArtifactStore;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

fn sourceDigest(source: origin.Identity, group: u64, incarnation: [32]u8) ![32]u8 {
    return @import("../../metadata/replication_digest.zig").digest("antfly/source-origin/v3", .{ group, incarnation, source }, origin.max_encoded_bytes);
}

pub fn sourceScope(source: origin.Identity, held: purpose.Purpose) !artifact.UploadScope {
    try source.validate();
    if (!source.owner.eql(held.owner) or source.origin_generation != held.birth or
        !std.mem.eql(u8, &source.recipe_digest, &held.recipe_digest) or
        !std.mem.eql(u8, &source.roster_digest, &held.roster_digest) or
        !std.mem.eql(u8, &try sourceDigest(source, held.metadata_group, held.incarnation), &held.source_digest))
        return error.InvalidReplicationSnapshotCursor;
    return payload.uploadScope(source, held.metadata_group, held.incarnation, held.upload.token, held.upload.nonce);
}

pub const max_capture_pages = 65536;
pub const Row = struct {
    value: std.json.Value,
    /// Provider-proven JSON nulls; every other explicit null is SQL NULL.
    json_null_fields: []const []const u8,
};
pub const CapturePage = struct {
    version: u8 = 2,
    relation_id: u32,
    offset: u64,
    rows: []const Row,
    pub fn validate(self: @This(), relation: origin.Relation, expected_offset: u64, expected_count: u64) !void {
        if (self.version != 2 or self.relation_id != relation.id or self.offset != expected_offset or self.rows.len != expected_count or self.rows.len == 0 or self.rows.len > 4096) return error.InvalidReplicationSnapshotCursor;
        for (self.rows) |row| {
            if (row.value != .object or row.value.object.count() != relation.columns.len) return error.InvalidReplicationSourceRow;
            for (relation.columns) |column| if (!row.value.object.contains(column.name)) return error.InvalidReplicationSourceRow;
            for (row.json_null_fields, 0..) |name, i| {
                const value = row.value.object.get(name) orelse return error.InvalidReplicationSourceRow;
                const column = for (relation.columns) |item| {
                    if (std.mem.eql(u8, item.name, name)) break item;
                } else return error.InvalidReplicationSourceRow;
                if (value != .null or (column.type_oid != 114 and column.type_oid != 3802)) return error.InvalidReplicationSourceRow;
                for (row.json_null_fields[0..i]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidReplicationSourceRow;
            }
        }
    }
};
pub const CaptureRelation = struct { relation_id: u32, first_offset: u64, count: u64 };
pub const CaptureEntry = struct {
    relation_id: u32,
    offset: u64,
    count: u32,
    retained: artifact.Retained,

    pub fn write(a: A, store: *ArtifactStore, scope: artifact.UploadScope, relation: origin.Relation, page: CapturePage, cancellation: Cancellation) !CaptureEntry {
        try cancellation.check();
        try page.validate(relation, page.offset, page.rows.len);
        const bytes = try std.json.Stringify.valueAlloc(a, page, .{});
        defer a.free(bytes);
        if (bytes.len > 4 * 1024 * 1024) return error.InvalidReplicationPayloadAdmission;
        return .{
            .relation_id = relation.id,
            .offset = page.offset,
            .count = @intCast(page.rows.len),
            .retained = try payload.retain(a, store, scope, bytes, cancellation),
        };
    }

    pub fn read(self: @This(), a: A, store: *ArtifactStore, scope: artifact.UploadScope, relation: origin.Relation, cancellation: Cancellation) !std.json.Parsed(CapturePage) {
        if (self.relation_id != relation.id or self.count == 0 or self.count > 4096) return error.InvalidReplicationSnapshotCursor;
        const parsed = try payload.readJson(CapturePage, a, store, self.retained, scope, cancellation, 4 * 1024 * 1024);
        errdefer parsed.deinit();
        try parsed.value.validate(relation, self.offset, self.count);
        return parsed;
    }
};
const chain = @import("snapshot_chain.zig");

pub fn pageCommitment(entry: CaptureEntry, next: chain.Digest) chain.Page {
    return .{ .relation = entry.relation_id, .offset = entry.offset, .count = entry.count, .content_digest = entry.retained.content_digest, .next = next };
}

pub fn pageChain(pages: []const CaptureEntry, links: ?[]chain.Digest) !chain.Digest {
    if (links) |out| if (out.len != pages.len) return error.InvalidReplicationSnapshotCursor;
    var next = chain.end;
    var index = pages.len;
    while (index > 0) {
        index -= 1;
        if (links) |out| out[index] = next;
        next = try pageCommitment(pages[index], next).commitment();
    }
    return next;
}

pub const CaptureManifest = struct {
    version: u8 = 2,
    owner: origin.Owner,
    birth: u64,
    provider_identity: [32]u8,
    roster_digest: [32]u8,
    source_point: u64,
    snapshot_token_digest: [32]u8,
    total_rows: u64,
    relations: []const CaptureRelation,
    pages: []const CaptureEntry,

    pub fn read(a: A, store: *ArtifactStore, source: origin.Identity, held: purpose.Purpose, ready: purpose.Ready, observed: origin.Observation, cancellation: Cancellation) !std.json.Parsed(CaptureManifest) {
        try held.validate();
        try ready.validate();
        if (!held.upload.sameIdentity(ready.upload)) return error.InvalidReplicationSnapshotCursor;
        if (!std.mem.eql(u8, &source.provider_identity, &observed.provider_identity)) return error.InvalidReplicationSnapshotCursor;
        const scope = try sourceScope(source, held);
        const parsed = try payload.readJson(CaptureManifest, a, store, ready.capture, scope, cancellation, 64 * 1024 * 1024);
        errdefer parsed.deinit();
        try parsed.value.validate(a, held, ready, observed);
        return parsed;
    }

    pub fn validate(self: @This(), a: A, held: purpose.Purpose, ready: purpose.Ready, observed: origin.Observation) !void {
        try held.validate();
        try ready.validate();
        if (!held.upload.sameIdentity(ready.upload)) return error.InvalidReplicationSnapshotCursor;
        try observed.validate(a);
        if (self.version != 2 or !self.owner.eql(held.owner) or self.birth != held.birth or self.source_point != ready.source_point or self.total_rows != ready.total_rows or
            !std.mem.eql(u8, &self.provider_identity, &observed.provider_identity) or !std.mem.eql(u8, &self.roster_digest, &held.roster_digest) or
            !std.mem.eql(u8, &self.roster_digest, &try observed.digest(a)) or !std.mem.eql(u8, &self.snapshot_token_digest, &ready.snapshot_token_digest) or
            self.relations.len != observed.relations.len or self.pages.len > max_capture_pages) return error.InvalidReplicationSnapshotCursor;
        var offset: u64 = 0;
        var page_index: usize = 0;
        for (self.relations, observed.relations) |selected, relation| {
            if (selected.relation_id != relation.id or selected.first_offset != offset) return error.InvalidReplicationSnapshotCursor;
            const end = std.math.add(u64, offset, selected.count) catch return error.InvalidReplicationSnapshotCursor;
            while (offset < end) {
                if (page_index == self.pages.len) return error.InvalidReplicationSnapshotCursor;
                const page = self.pages[page_index];
                try page.retained.validate();
                if (page.relation_id != relation.id or page.offset != offset or page.count == 0 or page.count > 4096 or
                    !std.meta.eql(page.retained.scope, ready.capture.scope)) return error.InvalidReplicationSnapshotCursor;
                offset = std.math.add(u64, offset, page.count) catch return error.InvalidReplicationSnapshotCursor;
                if (offset > end) return error.InvalidReplicationSnapshotCursor;
                page_index += 1;
            }
        }
        if (offset != self.total_rows or page_index != self.pages.len) return error.InvalidReplicationSnapshotCursor;
    }
};

/// A verified manifest with a forward-only page cursor. The caller owns
/// each decoded page and can release it before reading the next one.
pub const Snapshot = struct {
    manifest: std.json.Parsed(CaptureManifest),
    // Borrowed for the lifetime of the iterator.
    observed: origin.Observation,
    store: *ArtifactStore,
    scope: artifact.UploadScope,
    position: usize = 0,
    page_chain: chain.Digest,
    links: []const chain.Digest,

    pub fn open(a: A, store: *ArtifactStore, source: origin.Identity, held: purpose.Purpose, ready: purpose.Ready, observed: origin.Observation, cancellation: Cancellation) !Snapshot {
        const manifest = try CaptureManifest.read(a, store, source, held, ready, observed, cancellation);
        errdefer manifest.deinit();
        const links = try manifest.arena.allocator().alloc(chain.Digest, manifest.value.pages.len);
        return .{
            .manifest = manifest,
            .page_chain = try pageChain(manifest.value.pages, links),
            .links = links,
            .observed = observed,
            .store = store,
            .scope = ready.capture.scope,
        };
    }

    pub fn deinit(self: *Snapshot) void {
        self.manifest.deinit();
        self.* = undefined;
    }

    /// Resume at an exact committed page boundary or the end of the snapshot.
    pub fn currentCommitment(self: *const Snapshot) !chain.Digest {
        if (self.position == self.manifest.value.pages.len) return chain.end;
        return pageCommitment(self.manifest.value.pages[self.position], self.links[self.position]).commitment();
    }

    pub fn seek(self: *Snapshot, offset: u64) !void {
        const pages = self.manifest.value.pages;
        if (offset == self.manifest.value.total_rows) {
            self.position = pages.len;
            return;
        }
        var low: usize = 0;
        var high = pages.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const candidate = pages[mid].offset;
            if (candidate == offset) {
                self.position = mid;
                return;
            }
            if (candidate < offset) low = mid + 1 else high = mid;
        }
        return error.InvalidReplicationSnapshotCursor;
    }

    pub fn next(self: *Snapshot, a: A, cancellation: Cancellation) !?std.json.Parsed(CapturePage) {
        try cancellation.check();
        if (self.position == self.manifest.value.pages.len) return null;
        const entry = self.manifest.value.pages[self.position];
        const relation = self.observed.find(entry.relation_id) orelse return error.InvalidReplicationSnapshotCursor;
        const decoded = try entry.read(a, self.store, self.scope, relation, cancellation);
        self.position += 1;
        return decoded;
    }

    pub fn nextWithProof(self: *Snapshot, a: A, cancellation: Cancellation) !?struct { page: std.json.Parsed(CapturePage), proof: chain.Page } {
        const index = self.position;
        const page = (try self.next(a, cancellation)) orelse return null;
        return .{ .page = page, .proof = pageCommitment(self.manifest.value.pages[index], self.links[index]) };
    }
};

test "replication snapshot capture page distinguishes SQL null and JSON null" {
    const a = std.testing.allocator;
    const relation: origin.Relation = .{
        .id = 7,
        .namespace = "public",
        .name = "records",
        .replica_identity = .default,
        .columns = &.{
            .{ .name = "id", .type_oid = 20, .type_modifier = -1, .key = true },
            .{ .name = "payload", .type_oid = 3802, .type_modifier = -1, .key = false },
        },
    };
    var json = try std.json.parseFromSlice(std.json.Value, a, "{\"id\":null,\"payload\":null}", .{});
    defer json.deinit();
    var row: Row = .{ .value = json.value, .json_null_fields = &.{"payload"} };
    const page: CapturePage = .{ .relation_id = 7, .offset = 64, .rows = (&row)[0..1] };
    try page.validate(relation, 64, 1);
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, page.validate(relation, 63, 1));
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, page.validate(relation, 64, 2));
    row.json_null_fields = &.{"id"};
    try std.testing.expectError(error.InvalidReplicationSourceRow, page.validate(relation, 64, 1));
    row.json_null_fields = &.{ "payload", "payload" };
    try std.testing.expectError(error.InvalidReplicationSourceRow, page.validate(relation, 64, 1));
}

test "replication snapshot capture manifest rejects gaps and other upload attempts" {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const observed: origin.Observation = .{ .provider_identity = @splat(1), .relations = &.{.{
        .id = 7,
        .namespace = "public",
        .name = "records",
        .replica_identity = .default,
        .columns = &.{.{ .name = "id", .type_oid = 20, .type_modifier = -1, .key = true }},
    }} };
    const owner: origin.Owner = .{ .logical_subscription = .{ .database_id = 1, .subscription_id = 2 } };
    const nonce: [16]u8 = @splat(7);
    const incarnation = "0123456789abcdef0123456789abcdef".*;
    const source: origin.Identity = .{
        .owner = owner,
        .origin_generation = 2,
        .provider_identity = observed.provider_identity,
        .recipe_digest = @splat(10),
        .slot_name = "subscriber",
        .roster_digest = try observed.digest(a),
    };
    const scope = try payload.uploadScope(source, 1, incarnation, 4, nonce);
    const digest: [32]u8 = @splat(5);
    const retained: artifact.Retained = .{
        .scope = scope,
        .artifact_id = try scope.artifactId(&std.fmt.bytesToHex(&digest, .lower)),
        .manifest_digest = digest,
        .manifest_bytes = 1,
        .byte_length = 1,
        .content_digest = @splat(6),
        .chunk_count = 1,
    };
    const upload: purpose.Upload = .{ .token = scope.fencingToken(), .nonce = nonce, .expires_at_unix_ns = 8 };
    const held: purpose.Purpose = .{
        .metadata_group = 1,
        .incarnation = incarnation,
        .owner = owner,
        .birth = 2,
        .source_digest = try sourceDigest(source, 1, incarnation),
        .recipe_digest = @splat(10),
        .roster_digest = try observed.digest(a),
        .upload = upload,
    };
    var ready: purpose.Ready = .{ .upload = upload, .capture = retained, .execution_context = retained, .source_point = 7, .snapshot_token_digest = @splat(11), .total_rows = 2 };
    var pages = [_]CaptureEntry{
        .{ .relation_id = 7, .offset = 0, .count = 1, .retained = retained },
        .{ .relation_id = 7, .offset = 1, .count = 1, .retained = retained },
    };
    const manifest: CaptureManifest = .{
        .owner = owner,
        .birth = held.birth,
        .provider_identity = observed.provider_identity,
        .roster_digest = held.roster_digest,
        .source_point = ready.source_point,
        .snapshot_token_digest = ready.snapshot_token_digest,
        .total_rows = 2,
        .relations = &.{.{ .relation_id = 7, .first_offset = 0, .count = 2 }},
        .pages = &pages,
    };
    try manifest.validate(a, held, ready, observed);
    var directory = try local.common_test_directory.TestDirectory.init("replication-snapshot-manifest");
    defer directory.cleanup();
    var fs = try @import("../../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    for (&pages) |*entry| {
        var row: std.json.ObjectMap = .empty;
        try row.put(a, "id", .{ .integer = @intCast(entry.offset + 10) });
        const page: CapturePage = .{
            .relation_id = entry.relation_id,
            .offset = entry.offset,
            .rows = &.{.{ .value = .{ .object = row }, .json_null_fields = &.{} }},
        };
        entry.* = try CaptureEntry.write(a, &store, scope, observed.relations[0], page, .none);
    }
    const bytes = try std.json.Stringify.valueAlloc(a, manifest, .{});
    ready.capture = try payload.retainTestPayload(a, &store, scope, bytes, false);
    const decoded = try CaptureManifest.read(a, &store, source, held, ready, observed, .none);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(manifest, decoded.value);
    var stream = try Snapshot.open(a, &store, source, held, ready, observed, .none);
    defer stream.deinit();
    try stream.seek(1);
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, stream.seek(3));
    const second = (try stream.next(a, .none)).?;
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 1), second.value.offset);
    try std.testing.expectEqualStrings("11", second.value.rows[0].value.object.get("id").?.number_string);
    try std.testing.expect((try stream.next(a, .none)) == null);
    try stream.seek(0);
    const first = (try stream.next(a, .none)).?;
    defer first.deinit();
    try std.testing.expectEqual(@as(u64, 0), first.value.offset);
    try stream.seek(2);
    try std.testing.expect((try stream.next(a, .none)) == null);
    var foreign_source = source;
    foreign_source.slot_name = "other_subscriber";
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, CaptureManifest.read(a, &store, foreign_source, held, ready, observed, .none));
    var foreign_held = held;
    foreign_held.metadata_group += 1;
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, CaptureManifest.read(a, &store, source, foreign_held, ready, observed, .none));
    pages[1].offset = 2;
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, manifest.validate(a, held, ready, observed));
    pages[1].offset = 1;
    ready.upload.nonce[0] ^= 1;
    try std.testing.expectError(error.InvalidReplicationSnapshotCursor, manifest.validate(a, held, ready, observed));
}
pub const ReplayOrigin = struct {
    pub const max_bytes = origin.max_encoded_bytes + 4096;

    source: origin.Identity,
    observed: origin.Observation,
};

pub fn readReplayOrigin(a: A, store: *ArtifactStore, held: purpose.Purpose, ready: purpose.Ready, cancellation: Cancellation) !std.json.Parsed(ReplayOrigin) {
    try held.validate();
    try ready.validate();
    if (!held.upload.sameIdentity(ready.upload)) return error.InvalidReplicationSnapshotCursor;
    const context = try payload.readJson(ReplayOrigin, a, store, ready.execution_context, ready.capture.scope, cancellation, ReplayOrigin.max_bytes);
    errdefer context.deinit();
    const scope = try sourceScope(context.value.source, held);
    if (!std.meta.eql(scope, ready.capture.scope) or !std.mem.eql(u8, &try context.value.observed.digest(a), &held.roster_digest))
        return error.InvalidReplicationSnapshotCursor;
    return context;
}

pub const RetainedSnapshot = struct {
    context: std.json.Parsed(ReplayOrigin),
    snapshot: Snapshot,

    pub fn open(a: A, store: *ArtifactStore, held: purpose.Purpose, ready: purpose.Ready, expected_chain: [32]u8, offset: u64, cancellation: Cancellation) !RetainedSnapshot {
        const context = try readReplayOrigin(a, store, held, ready, cancellation);
        errdefer context.deinit();
        var snapshot = try Snapshot.open(a, store, context.value.source, held, ready, context.value.observed, cancellation);
        errdefer snapshot.deinit();
        if (!std.mem.eql(u8, &snapshot.page_chain, &expected_chain)) return error.InvalidReplicationSnapshotCursor;
        try snapshot.seek(offset);
        return .{ .context = context, .snapshot = snapshot };
    }

    pub fn deinit(self: *RetainedSnapshot) void {
        self.snapshot.deinit();
        self.context.deinit();
        self.* = undefined;
    }
};
