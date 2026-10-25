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

//! Bounded native graph tracing for a durable hosted collector. Scheduling,
//! marks and its frontier live in the coordinator's transactional store; every
//! object is decoded/authenticated by the same native serving formats.
const std = @import("std");
const tree = @import("graph_segment/page_tree.zig");
const PageStore = @import("graph_segment/page_store.zig").PageStore;
const facts = @import("build/document_facts.zig");
const artifacts_mod = @import("artifacts/mod.zig");
const artifact_ids = @import("artifacts/store.zig");
const manifests_mod = @import("manifest/mod.zig");
const progress_mod = @import("catalog/mod.zig");
const Wal = @import("wal/indexed_object_store.zig").IndexedObjectStore;
const A = std.mem.Allocator;

pub const Task = struct {
    kind: enum { manifest, facts_root, facts_page, wal_page, text_root, text_page, projection_root, projection_page, vector_docs_page, vector_centroid_page, reindex_state, blob },
    version: ?[]const u8 = null,
    artifact: ?struct { artifact_id: []const u8, byte_len: u64, checksum: []const u8, metadata_version: u16 = 0, kind: ?manifests_mod.ArtifactKind = null } = null,
    page: ?tree.Ref = null,
    offset: usize = 0,
    vector_dims: u32 = 0,
};
pub const Context = struct {
    alloc: A,
    namespace: []const u8,
    prefix: []const u8,
    artifacts: *artifacts_mod.ArtifactStore,
    manifests: *manifests_mod.ManifestStore,
    progress: *progress_mod.ProgressStore,
    wal: *Wal,
    io: std.Io,
    reindex_registry: ?@import("hosted_reindex.zig").Registry = null,

    pub fn prepare(self: Context) ![]u8 {
        const lease = @import("build/work_lease.zig");
        var nonce: [16]u8 = undefined;
        self.io.random(&nonce);
        const owner = std.fmt.bytesToHex(&nonce, .lower);
        var held = (try lease.acquireHeld(try self.progress.workLeaseProvider(), self.io, self.namespace, &owner, 30 * std.time.ns_per_s)) orelse return error.WorkLeaseLost;
        defer if (!held.released) {
            _ = held.release() catch false;
        };
        const retired = held.acquisition.fencing_token;
        var registry_arena = std.heap.ArenaAllocator.init(self.alloc);
        defer registry_arena.deinit();
        const active_job = if (self.reindex_registry) |registry| try registry.load(registry_arena.allocator(), lease.nowUnixNs(self.io)) else @import("hosted_reindex.zig").Saved{};
        if (!try held.release()) return error.WorkLeaseLost;
        const head: ?u64 = self.progress.getHead(self.namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var manifests = self.manifests.*;
        manifests.allocator = a;
        var roots: std.ArrayListUnmanaged(Task) = .empty;
        var version: ?u64 = head;
        var floor: u64 = head orelse 0;
        const existing = try self.progress.getManifestGcFloor(self.namespace);
        if (head == null and existing != null and existing.? != 0) return error.PublishedHeadManifestMissing;
        // Keep the current and parent publication, plus all durable reader pins.
        for (0..2) |_| {
            const v = version orelse break;
            if (existing) |minimum| if (v < minimum) break;
            var manifest = try manifests.getAlloc(self.namespace, v);
            defer manifest.deinit(a);
            try roots.append(a, .{ .kind = .manifest, .version = try std.fmt.allocPrint(a, "{d}", .{v}) });
            floor = v;
            version = if (manifest.publication_lineage_tracked) manifest.publication_parent_version else null;
        }
        if (active_job.state) |state| {
            if (existing != null and state.source_version < existing.?) return error.ReindexSourceRetired;
            floor = @min(floor, state.source_version);
            try roots.append(a, .{ .kind = .manifest, .version = try std.fmt.allocPrint(a, "{d}", .{state.source_version}) });
            const ref = active_job.pointer.?.artifact;
            try roots.append(a, .{ .kind = .reindex_state, .artifact = .{ .artifact_id = ref.artifact_id, .byte_len = ref.byte_len, .checksum = ref.checksum } });
        }
        if (head != null and (existing == null or floor > existing.?)) {
            if (!try self.progress.compareAndSwapManifestGcFloor(self.namespace, existing, floor)) return error.CollectionConflict;
        }
        const domain = PageStore.namespaceDomain(self.namespace);
        return std.json.Stringify.valueAlloc(self.alloc, .{
            .roots = roots.items,
            .floor = try std.fmt.allocPrint(a, "{d}", .{floor}),
            .retired_before = try std.fmt.allocPrint(a, "{d}", .{retired}),
            .artifact_prefix = try std.fmt.allocPrint(a, "{s}/artifacts/graph/{s}/", .{ self.prefix, std.fmt.bytesToHex(domain, .lower) }),
            .manifest_prefix = try std.fmt.allocPrint(a, "{s}/manifests/{s}/manifests/", .{ self.prefix, self.namespace }),
            .pin_prefix = try std.fmt.allocPrint(a, "{s}/progress/{s}/READ_PINS/", .{ self.prefix, self.namespace }),
        }, .{});
    }

    pub fn prepareWal(self: Context, keep_from: u64, id: [16]u8) ![]u8 {
        _ = try self.wal.truncatePrefix(self.namespace, keep_from);
        if (try self.wal.compactPrefixStep(self.namespace, 256)) return std.json.Stringify.valueAlloc(self.alloc, .{ .has_more = true }, .{});
        const snapshot = try self.wal.beginCollection(self.namespace, id);
        var roots: [2]Task = undefined;
        var count: usize = 0;
        for ([_]?tree.Ref{ snapshot.records, snapshot.operations }) |ref| if (ref) |page| {
            roots[count] = .{ .kind = .wal_page, .page = page };
            count += 1;
        };
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(self.namespace, &digest, .{});
        var value: [20]u8 = undefined;
        const prefix = try std.fmt.allocPrint(self.alloc, "{s}/wal-v1/{s}/pages/", .{ self.prefix, std.fmt.bytesToHex(digest, .lower) });
        defer self.alloc.free(prefix);
        return std.json.Stringify.valueAlloc(self.alloc, .{ .has_more = false, .roots = roots[0..count], .retired_before = try std.fmt.bufPrint(&value, "{d}", .{snapshot.retired_before}), .wal_prefix = prefix }, .{});
    }

    pub fn trace(self: Context, task: Task) ![]u8 {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var children: std.ArrayListUnmanaged(Task) = .empty;
        var key: []const u8 = undefined;
        var wal_keep: ?[]const u8 = null;
        var artifacts = self.artifacts.*;
        artifacts.allocator = a;
        var remaining: u64 = 4 * 1024 * 1024;
        var writes: u64 = 0;
        var pages = PageStore{ .domain = PageStore.namespaceDomain(self.namespace), .artifacts = &artifacts, .remaining_read_bytes = &remaining, .remaining_write_bytes = &writes };
        switch (task.kind) {
            .manifest => {
                const v = try std.fmt.parseInt(u64, task.version orelse return error.InvalidCollectionTask, 10);
                var manifests = self.manifests.*;
                manifests.allocator = a;
                const manifest = try manifests.getAlloc(self.namespace, v);
                key = try std.fmt.allocPrint(a, "{s}/manifests/{s}/manifests/{d}.bin", .{ self.prefix, self.namespace, v });
                wal_keep = try std.fmt.allocPrint(a, "{d}", .{manifest.wal_start_lsn});
                // Fail closed on formats whose transitive references this
                // deployment profile does not yet know how to enumerate.
                const end = @min(manifest.artifacts.len, task.offset +| 128);
                if (task.offset > end) return error.InvalidCollectionTask;
                try appendArtifacts(a, &children, manifest.artifacts[task.offset..end]);
                if (end < manifest.artifacts.len) {
                    var continuation = task;
                    continuation.offset = end;
                    try children.append(a, continuation);
                }
            },
            .reindex_state => {
                const artifact = task.artifact orelse return error.InvalidCollectionTask;
                const registry = self.reindex_registry orelse return error.InvalidCollectionTask;
                const state = try registry.readState(a, .{ .kind = .stored_fields, .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len });
                key = try self.artifactKey(a, artifact.artifact_id);
                wal_keep = try std.fmt.allocPrint(a, "{d}", .{state.staged.wal_end_lsn});
                try appendArtifacts(a, &children, state.staged.artifacts);
            },
            .facts_root => {
                const artifact = task.artifact orelse return error.InvalidCollectionTask;
                const root = try facts.loadRoot(a, &pages, .{ .kind = .document_facts, .artifact_id = artifact.artifact_id, .checksum = artifact.checksum, .byte_len = artifact.byte_len, .metadata_version = artifact.metadata_version });
                if (!std.mem.eql(u8, &root.domain, &pages.domain)) return error.GraphPageDomainMismatch;
                key = try self.artifactKey(a, artifact.artifact_id);
                for ([_]?tree.Ref{root.page} ++ root.pending_pages) |ref| if (ref) |page| try children.append(a, .{ .kind = .facts_page, .page = page });
            },
            .projection_root => {
                const artifact = task.artifact orelse return error.InvalidCollectionTask;
                const parsed = try @import("paged_projection.zig").load(a, &pages, .{ .kind = artifact.kind orelse return error.InvalidCollectionTask, .metadata_version = artifact.metadata_version, .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum });
                defer parsed.deinit();
                key = try self.artifactKey(a, artifact.artifact_id);
                if (parsed.value.kind == .vector_segment) {
                    if (parsed.value.docs) |page| try children.append(a, .{ .kind = .vector_docs_page, .page = page, .vector_dims = parsed.value.dims });
                } else if (parsed.value.docs) |page| try children.append(a, .{ .kind = .projection_page, .page = page });
                for ([_]?tree.Ref{ parsed.value.postings, parsed.value.directory, parsed.value.global_centers }) |ref| if (ref) |page| try children.append(a, .{ .kind = .projection_page, .page = page });
                for (parsed.value.vector_runs) |page| try children.append(a, .{ .kind = .vector_docs_page, .page = page, .vector_dims = parsed.value.dims });
                if (parsed.value.vector_merge) |merge| if (merge.output) |page| try children.append(a, .{ .kind = .vector_docs_page, .page = page, .vector_dims = parsed.value.dims });
                if (parsed.value.centroids) |page| try children.append(a, .{ .kind = .vector_centroid_page, .page = page, .vector_dims = parsed.value.dims });
            },
            .text_root => {
                const artifact = task.artifact orelse return error.InvalidCollectionTask;
                const parsed = try @import("text_segment/paged.zig").loadRoot(a, &pages, .{ .kind = .text_segment, .metadata_version = artifact.metadata_version, .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum });
                defer parsed.deinit();
                key = try self.artifactKey(a, artifact.artifact_id);
                for ([_]?tree.Ref{ parsed.value.docs, parsed.value.postings }) |ref| if (ref) |page| try children.append(a, .{ .kind = .text_page, .page = page });
            },
            .facts_page, .wal_page, .text_page, .projection_page, .vector_docs_page, .vector_centroid_page => {
                const ref = task.page orelse return error.InvalidCollectionTask;
                var page = if (task.kind == .wal_page) try self.wal.tracePage(a, self.namespace, ref) else try tree.readPage(a, pages.store(), ref);
                defer page.deinit(a);
                key = if (task.kind == .wal_page) try self.wal.pageKeyAlloc(a, self.namespace, ref) else try self.artifactKey(a, &try PageStore.identity(pages.domain, ref));
                const end = @min(page.entries.len, task.offset +| 128);
                if (task.offset > page.entries.len) return error.InvalidCollectionTask;
                for (page.entries[task.offset..end]) |entry| {
                    if (entry.child) |child| {
                        try children.append(a, .{ .kind = task.kind, .page = child.*, .vector_dims = task.vector_dims });
                    } else if (task.kind == .vector_docs_page and entry.value.len != 0 and @import("vector_segment/blocks.zig").isCompact(entry.value)) {
                        const c = try @import("vector_segment/blocks.zig").control(entry.value, task.vector_dims);
                        const id = try c.residual.identity(pages.domain);
                        try children.append(a, .{ .kind = .blob, .artifact = .{ .artifact_id = try a.dupe(u8, &id), .byte_len = c.residual.bytes, .checksum = try a.dupe(u8, &std.fmt.bytesToHex(c.residual.digest, .lower)) } });
                    } else if (task.kind == .vector_centroid_page) {
                        if (task.vector_dims == 0 or task.vector_dims > 16384 or entry.value.len != 72 + @as(usize, task.vector_dims) * 4) return error.InvalidCollectionTask;
                        const blob = try @import("paged_projection.zig").Blob.decode(entry.value);
                        const id = try blob.identity(pages.domain);
                        try children.append(a, .{ .kind = .blob, .artifact = .{ .artifact_id = try a.dupe(u8, &id), .byte_len = blob.bytes, .checksum = try a.dupe(u8, &std.fmt.bytesToHex(blob.digest, .lower)) } });
                    } else if (task.kind == .facts_page) {
                        const fact = try facts.Fact.decode(entry.value);
                        const id = try fact.body.identity(pages.domain);
                        try children.append(a, .{ .kind = .blob, .artifact = .{ .artifact_id = try a.dupe(u8, &id), .byte_len = if (fact.body.pack) |pack| pack.bytes else fact.body.bytes, .checksum = try a.dupe(u8, &std.fmt.bytesToHex(if (fact.body.pack) |pack| pack.digest else fact.body.digest, .lower)) } });
                    }
                }
                if (end < page.entries.len) {
                    var continuation = task;
                    continuation.offset = end;
                    try children.append(a, continuation);
                }
            },
            .blob => key = try self.artifactKey(a, (task.artifact orelse return error.InvalidCollectionTask).artifact_id),
        }
        return std.json.Stringify.valueAlloc(self.alloc, .{ .key = key, .children = children.items, .wal_keep_from = wal_keep }, .{});
    }
    fn artifactKey(self: Context, a: A, id: []const u8) ![]u8 {
        const suffix = try artifact_ids.storageSuffixAlloc(a, id);
        return std.fmt.allocPrint(a, "{s}/artifacts/{s}", .{ self.prefix, suffix });
    }
};

fn appendArtifacts(a: A, children: *std.ArrayListUnmanaged(Task), refs: []const manifests_mod.ArtifactRef) !void {
    for (refs) |artifact| switch (artifact.kind) {
        .document_facts => {
            if (artifact.metadata_version != facts.Root.metadata_version and artifact.metadata_version != 4) return error.CollectionArtifactUnsupported;
            try children.append(a, .{ .kind = .facts_root, .artifact = .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum, .metadata_version = artifact.metadata_version } });
        },
        .text_segment => try children.append(a, .{ .kind = if (artifact.metadata_version == @import("text_segment/paged.zig").metadata_version) .text_root else .blob, .artifact = .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum, .metadata_version = artifact.metadata_version } }),
        .vector_segment, .sparse_segment => {
            if (artifact.metadata_version > 2) return error.CollectionArtifactUnsupported;
            try children.append(a, .{ .kind = if (artifact.metadata_version == 2) .projection_root else .blob, .artifact = .{ .kind = artifact.kind, .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum, .metadata_version = artifact.metadata_version } });
        },
        .document_segment, .mutation_segment, .doc_values, .stored_fields => try children.append(a, .{ .kind = .blob, .artifact = .{ .artifact_id = artifact.artifact_id, .byte_len = artifact.byte_len, .checksum = artifact.checksum, .metadata_version = artifact.metadata_version } }),
        else => return error.CollectionArtifactUnsupported,
    };
}
