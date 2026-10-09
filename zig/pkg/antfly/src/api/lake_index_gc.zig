// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Reader-safe native artifact collection. Publication/retirement authority is
//! native metadata; object listing is only a census of fenced upload attempts.
const std = @import("std");
const local = @import("antfly_local_sources");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const lease_api = @import("lake_index_reader_lease.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
pub const Options = struct { dry_run: bool = true, max_marked: usize = 262144, max_read_bytes: u64 = 512 * 1024 * 1024, max_deleted: usize = 4096 };
pub const Result = struct { marked: usize = 0, eligible: usize = 0, deleted: usize = 0, complete: bool = false };
pub const Collector = struct {
    a: A,
    table: u64,
    authority: lease_api.Authority,
    store: stores.ArtifactStore,
    identity: [32]u8,
    context: Context,
    options: Options = .{},
    token: [16]u8,
    expires_ms: u64 = 0,
    authority_deadline: u64 = 0,
    remaining_reads: u64 = 0,
    marked: std.StringHashMapUnmanaged(artifacts.ChunkRef) = .empty,
    result: Result = .{},
    upload_cutoff: u64 = 0,
    upload_floor: u64 = 0,
    namespace: ?[32]u8 = null,
    progress: ?*@import("lake_index_gc_progress.zig").Progress = null,

    pub fn run(self: *Collector) !Result {
        self.result = .{};
        if (self.options.max_marked == 0 or self.options.max_deleted == 0 or self.options.max_read_bytes == 0) return error.InvalidLakeIndexGcLimits;
        try self.context.ensureActive();
        const authority_start = @import("antfly_platform").time.authorityNs();
        const started = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        if (authority_start == 0 or started == 0) return error.LakeIndexCollectionFenceChanged;
        var control = std.heap.ArenaAllocator.init(self.a);
        defer control.deinit();
        const a = control.allocator();
        var state = try self.authority.readState(a, self.table);
        if (self.options.dry_run) {
            // Planning never changes reader admission or creates a durable cut.
            state = try state.beginCollection(a, self.token, self.identity, started);
        } else if (state.collection == null or !std.mem.eql(u8, &state.collection.?.token, &self.token) or started >= state.collection.?.expires_ms) {
            try self.authority.apply(self.table, .{ .begin_collection = .{ .token = self.token, .store = self.identity, .now_ms = started } });
            state = try self.authority.readState(a, self.table);
        }
        const collection = state.collection orelse return error.LakeIndexCollectionFenceChanged;
        if (!std.mem.eql(u8, &collection.token, &self.token) or !std.mem.eql(u8, &collection.store, &self.identity)) return error.LakeIndexCollectionFenceChanged;
        self.expires_ms = collection.expires_ms;
        if (started >= collection.expires_ms) return error.LakeIndexCollectionFenceChanged;
        self.authority_deadline = authority_start +| (collection.expires_ms - started) * std.time.ns_per_ms;
        self.upload_cutoff = collection.upload_cutoff;
        self.upload_floor = collection.upload_floor;
        self.namespace = collection.namespace;
        self.remaining_reads = self.options.max_read_bytes;
        if (!self.options.dry_run) return @import("lake_index_gc_progress.zig").run(self, state);
        defer {
            var keys = self.marked.keyIterator();
            while (keys.next()) |key| self.a.free(key.*);
            self.marked.deinit(self.a);
            self.marked = .empty;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        // Every non-retired generation is a root, including long-lived reader
        // and builder sessions. Reused roots retain their original upload IDs.
        for (state.publications) |publication| {
            if (!std.meta.eql(publication.namespace, self.namespace) or !std.mem.eql(u8, &publication.signature.store, &self.identity)) continue;
            const retired = for (collection.retired) |generation| {
                if (generation == publication.generation) break true;
            } else false;
            if (retired) continue;
            try self.check();
            _ = scratch.reset(.retain_capacity);
            const sa = scratch.allocator();
            var hydrated = publication;
            if (hydrated.directory) |directory| {
                _ = try self.mark(.{ .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len });
                try stores.chargeReadBudget(&self.remaining_reads, directory.byte_len);
                const directories = @import("lake_index_directory.zig");
                const document = try directories.loadDocument(sa, self.store, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, self.cancellation(), null);
                hydrated.declarations = document.declarations;
                hydrated.file_contributions = document.file_contributions;
                if (document.contribution_index) |root| try self.markContributionIndex(sa, root);
                for (document.contribution_pages) |page| {
                    _ = try self.mark(.{ .artifact_id = page.artifact_id, .checksum = page.checksum, .byte_len = page.byte_len });
                    try stores.chargeReadBudget(&self.remaining_reads, page.byte_len);
                    for (try directories.loadContributionPage(sa, self.store, page, self.cancellation(), null)) |contribution| try self.markArtifact(sa, contribution.artifact);
                }
            }
            _ = try self.mark(.{ .artifact_id = publication.inventory.artifact_id, .checksum = publication.inventory.checksum, .byte_len = publication.inventory.byte_len });
            for (hydrated.declarations) |declaration| try self.markArtifact(sa, declaration.artifact);
            for (hydrated.file_contributions) |contribution| try self.markArtifact(sa, contribution.artifact);
        }
        self.result.marked = self.marked.count();
        // No destructive work until the complete bounded mark phase succeeds.
        try self.check();
        try self.store.visitScopedUploads(@import("lake_index_publication.zig").uploadDomainWithNamespace(self.table, self.identity, self.namespace), .{ .ptr = self, .visit = visit }, self.cancellation());
        // A dry-run census has completed even when a destructive pass would
        // exhaust its deletion budget. It has no durable sweep to resume.
        if (!self.options.dry_run) {
            try self.check();
            try self.store.cleanupRetiredScopedTemporaryRange(@import("lake_index_publication.zig").uploadDomainWithNamespace(self.table, self.identity, self.namespace), self.upload_floor, self.upload_cutoff, self.cancellation());
            try self.authority.apply(self.table, .{ .finish_collection = .{ .token = self.token, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms } });
        }
        self.result.complete = true;
        return self.result;
    }
    pub fn cancellation(self: *Collector) @import("antfly_cancellation").CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *Collector = @ptrCast(@alignCast(@constCast(raw)));
        self.check() catch return true;
        return false;
    }
    pub fn check(self: *Collector) !void {
        try self.context.ensureActive();
        const unix = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        const authority = @import("antfly_platform").time.authorityNs();
        if (unix == 0 or authority == 0 or unix >= self.expires_ms or authority >= self.authority_deadline) return error.LakeIndexCollectionFenceChanged;
    }
    pub fn mark(self: *Collector, ref: artifacts.ChunkRef) !bool {
        if (self.progress) |progress| {
            try progress.enqueue(.{ .chunk = ref });
            return true;
        }
        try self.check();
        try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeGcReference;
        const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(self.table, self.identity, self.namespace);
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeGcReference;
        if (self.marked.get(ref.artifact_id)) |previous| {
            if ((previous.byte_len != 0 and ref.byte_len != 0 and previous.byte_len != ref.byte_len) or !std.mem.eql(u8, previous.checksum, ref.checksum)) return error.InvalidNativeLakeGcReference;
            return false;
        }
        if (self.marked.count() == self.options.max_marked) return error.LakeIndexGcBudgetExceeded;
        const owned = try self.a.dupe(u8, ref.artifact_id);
        errdefer self.a.free(owned);
        // The checksum is part of this canonical immutable identity. Borrowing
        // it from the owned ID avoids a second per-artifact string allocation.
        try self.marked.put(self.a, owned, .{ .artifact_id = owned, .checksum = owned[7..71], .byte_len = ref.byte_len });
        return true;
    }
    /// Collection must understand the preceding immutable root format after an
    /// upgrade, while serving and rebuild selection continue to require current metadata.
    fn retainedNativeRoot(self: *Collector, a: A, comptime native: type, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !native.Root {
        if (ref.metadata_version == native.metadata_version) {
            if (@hasDecl(native, "loadRootDirectory")) return native.loadRootDirectory(a, self.store, ref, self.cancellation(), null);
            return native.loadRoot(a, self.store, ref, self.cancellation(), null);
        }
        const supported_old = if (@hasField(native.Root, "seekable")) ref.metadata_version >= 1 and ref.metadata_version < native.metadata_version else if (@hasField(native.Root, "tuple_encoding")) ref.metadata_version >= 2 and ref.metadata_version < native.metadata_version else ref.metadata_version +| 1 == native.metadata_version;
        if (!supported_old) return error.InvalidNativeLakeGcReference;
        const limit = if (@hasDecl(native, "max_root_bytes")) native.max_root_bytes else @import("lake_index_native_files.zig").max_root_bytes;
        if (ref.byte_len > limit) return error.InvalidNativeLakeGcReference;
        const bytes = try artifacts.readArtifact(a, self.store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, self.cancellation(), null);
        defer a.free(bytes);
        var root = try std.json.parseFromSliceLeaky(native.Root, a, bytes, .{ .allocate = .alloc_always });
        if (root.version != ref.metadata_version) return error.InvalidNativeLakeGcReference;
        root.version = native.metadata_version;
        if (@hasField(native.Root, "tuple_encoding")) {
            if (root.tuple_encoding == 0 or root.tuple_encoding > local.storage_db_relational_index_keys.encoding_version) return error.InvalidNativeLakeGcReference;
            root.tuple_encoding = local.storage_db_relational_index_keys.encoding_version;
        }
        if (@hasDecl(native.Root, "validate")) try root.validate() else {
            try root.binding.validate();
            try root.generation.validate();
            try @import("lake_index_native_state.zig").validate(root.file_states, root.generation.domain);
        }
        const domain = if (@hasField(native.Root, "domain")) root.domain else root.generation.domain;
        const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeGcReference;
        if (!std.mem.eql(u8, &domain, &scope.domain)) return error.InvalidNativeLakeGcReference;
        return root;
    }
    fn markContributionIndex(self: *Collector, a: A, root: @import("../serverless/graph_segment/page_tree.zig").Ref) anyerror!void {
        const page_store = @import("../serverless/graph_segment/page_store.zig");
        const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(self.table, self.identity, self.namespace);
        const id = try page_store.PageStore.identity(domain, root);
        if (!try self.mark(.{ .artifact_id = &id, .checksum = &std.fmt.bytesToHex(&root.digest, .lower), .byte_len = root.bytes })) return;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = domain, .artifacts = &self.store, .cancellation = self.cancellation(), .remaining_read_bytes = &self.remaining_reads, .remaining_write_bytes = &writes };
        const Visitor = struct {
            collector: *Collector,
            a: A,
            pub fn child(v: *@This(), ref: @import("../serverless/graph_segment/page_tree.zig").Ref) !void {
                try v.collector.markContributionIndex(v.a, ref);
            }
            pub fn record(v: *@This(), key: []const u8, bytes: []const u8) !void {
                const value = try std.json.parseFromSliceLeaky(local.metadata_lake_index_catalog.FileContribution, v.a, bytes, .{ .allocate = .alloc_always });
                try @import("lake_index_contributions.zig").validate(value);
                if (!std.mem.eql(u8, key, &@import("lake_index_contributions.zig").identity(value))) return error.InvalidLakeIndexCatalog;
                try v.collector.markArtifact(v.a, value.artifact);
            }
        };
        var visitor: Visitor = .{ .collector = self, .a = a };
        try @import("../serverless/graph_segment/page_tree.zig").walkPage(a, pages.store(), root, &visitor);
    }
    pub fn markTextDirectory(self: *Collector, a: A, ref: artifacts.ChunkRef) !void {
        const fresh = if (self.progress) |progress| try progress.expand(ref, "native-text-directory-v1") else try self.mark(ref);
        if (!fresh) return;
        try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
        const directory = try @import("lake_index_seekable_text.zig").loadDirectory(a, .{ .store = self.store, .cache = null, .context = .{}, .cancellation = self.cancellation() }, ref);
        for (directory.metadata) |piece| _ = try self.mark(piece.retainedArtifact());
        for (directory.blocks) |piece| _ = try self.mark(piece.retainedArtifact());
    }
    pub fn markTextManifest(self: *Collector, a: A, ref: artifacts.ChunkRef, seekable: bool) !void {
        const fresh = if (self.progress) |progress| try progress.expand(ref, "text-manifest") else try self.mark(ref);
        if (!fresh) return;
        // Charge before I/O, independently from the serving metadata budget.
        try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
        const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(self.table, self.identity, self.namespace);
        const group = try @import("lake_index_native_text.zig").loadFileManifest(a, self.store, ref, domain, self.cancellation(), null);
        for (group.rows) |block| {
            if (block.bitmap) |bitmap| _ = try self.mark(bitmap);
        }
        for (group.segments) |segment| {
            if (seekable) {
                if (self.progress) |progress| try progress.enqueue(.{ .text_directory = segment }) else try self.markTextDirectory(a, segment);
            } else _ = try self.mark(segment);
        }
    }
    pub fn markArtifact(self: *Collector, a: A, ref: local.serverless_manifest_artifact_ref.ArtifactRef) !void {
        const root_chunk: artifacts.ChunkRef = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
        const fresh = if (self.progress) |progress| try progress.expand(root_chunk, @tagName(ref.kind)) else try self.mark(root_chunk);
        if (!fresh) return;
        switch (ref.kind) {
            .algebraic_segment => {
                if (!artifacts.supportsMetadataVersion(ref.metadata_version)) return error.InvalidNativeLakeGcReference;
                if (ref.metadata_version == 3) {
                    try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len *| 3);
                    for (try artifacts.partitionChildren(a, self.store, ref, self.cancellation())) |child| {
                        if (self.progress) |progress| try progress.enqueue(.{ .artifact = child }) else try self.markArtifact(a, child);
                    }
                    return;
                }
                try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len *| 2);
                for (try artifacts.descendantsAlloc(a, self.store, ref, self.cancellation())) |child| _ = try self.mark(child);
            },
            .ordered_row_index => {
                try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
                const root = try self.retainedNativeRoot(a, @import("lake_index_ordered_rows.zig"), ref);
                try self.markPages(a, root.domain, root.page, true);
                try self.markPages(a, root.domain, root.reverse, false);
                try self.markPages(a, root.domain, root.ties, false);
                try self.markPages(a, root.domain, root.predicates, false);
            },
            // These lake producers publish self-contained segments. New paged
            // formats must register a child-reference walker before GC accepts
            // them; arbitrary serverless manifest graphs aren't lake roots.
            .graph_segment => {
                if (ref.metadata_version == @import("../serverless/graph_segment/page_graph.zig").Root.metadata_version) {
                    var write_bytes: u64 = 0;
                    const page_store = @import("../serverless/graph_segment/page_store.zig");
                    var pages: page_store.PageStore = .{ .artifacts = &self.store, .cancellation = self.cancellation(), .remaining_read_bytes = &self.remaining_reads, .remaining_write_bytes = &write_bytes };
                    const root = try pages.loadRoot(a, ref);
                    try self.markPages(a, root.domain, root.page, false);
                } else if (ref.metadata_version != 0) return error.InvalidNativeLakeGcReference;
            },
            .text_segment => {
                if (ref.metadata_version >= 1 and ref.metadata_version <= @import("lake_index_native_text.zig").metadata_version) {
                    try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
                    const root = try self.retainedNativeRoot(a, @import("lake_index_native_text.zig"), ref);
                    for (root.manifests) |manifest| {
                        if (self.progress) |progress| try progress.enqueue(.{ .text_manifest = .{ .ref = manifest, .seekable = root.seekable } }) else try self.markTextManifest(a, manifest, root.seekable);
                    }
                    for (root.file_groups) |group| for (group.rows) |block| {
                        if (block.bitmap) |bitmap| _ = try self.mark(bitmap);
                    };
                    for (root.segments) |segment| {
                        if (root.seekable) {
                            if (self.progress) |progress| try progress.enqueue(.{ .text_directory = segment }) else try self.markTextDirectory(a, segment);
                        } else _ = try self.mark(segment);
                    }
                } else if (ref.metadata_version != 0) return error.InvalidNativeLakeGcReference;
            },
            .sparse_segment => {
                if ((ref.metadata_version == @import("lake_index_native_sparse.zig").metadata_version or ref.metadata_version +| 1 == @import("lake_index_native_sparse.zig").metadata_version)) {
                    try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
                    const root = try self.retainedNativeRoot(a, @import("lake_index_native_sparse.zig"), ref);
                    for (root.file_states) |file| for (file.docs) |document_ref| {
                        _ = try self.mark(document_ref);
                    };
                    for (root.generation.files) |file| for (file.chunks) |chunk| {
                        _ = try self.mark(chunk);
                    };
                } else if (ref.metadata_version != 0) return error.InvalidNativeLakeGcReference;
            },
            .vector_segment => {
                if ((ref.metadata_version == @import("lake_index_native_dense.zig").metadata_version or ref.metadata_version +| 1 == @import("lake_index_native_dense.zig").metadata_version)) {
                    try stores.chargeReadBudget(&self.remaining_reads, ref.byte_len);
                    const root = try self.retainedNativeRoot(a, @import("lake_index_native_dense.zig"), ref);
                    for (root.file_states) |file| for (file.docs) |document_ref| {
                        _ = try self.mark(document_ref);
                    };
                    for (root.generation.files) |file| for (file.chunks) |chunk| {
                        _ = try self.mark(chunk);
                    };
                } else if (ref.metadata_version != 0) return error.InvalidNativeLakeGcReference;
            },
            .graph_metric_segment => if (ref.metadata_version != local.serverless_manifest_artifact_ref.graph_metric_segment_wire_version) return error.InvalidNativeLakeGcReference,
            else => return error.InvalidNativeLakeGcReference,
        }
    }
    fn markPages(self: *Collector, a: A, domain: [32]u8, root: ?@import("../serverless/graph_segment/page_tree.zig").Ref, ordered_rows: bool) !void {
        if (self.progress) |progress| {
            if (root) |page| try progress.enqueue(.{ .page = .{ .ref = page, .ordered_rows = ordered_rows } });
            return;
        }
        const page_store = @import("../serverless/graph_segment/page_store.zig");
        const tree = @import("../serverless/graph_segment/page_tree.zig");
        var write_bytes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = domain, .artifacts = &self.store, .cancellation = self.cancellation(), .remaining_read_bytes = &self.remaining_reads, .remaining_write_bytes = &write_bytes };
        const Walker = struct {
            collector: *Collector,
            domain: [32]u8,
            ordered_rows: bool,
            a: A,
            pub fn visitRecord(w: *@This(), _: tree.Ref, _: []const u8, value: []const u8) !void {
                if (!w.ordered_rows) return;
                if (try @import("lake_index_ordered_rows.zig").coverReference(w.a, w.domain, value)) |cover| {
                    defer w.a.free(cover.block.artifact_id);
                    _ = try w.collector.mark(cover.block);
                }
            }
            pub fn skip(w: *@This(), child: tree.Ref) !bool {
                return !try w.collector.mark(.{ .artifact_id = &try page_store.PageStore.identity(w.domain, child), .checksum = &std.fmt.bytesToHex(&child.digest, .lower), .byte_len = child.bytes });
            }
            pub fn visit(_: *@This(), _: tree.Ref) !void {}
        };
        var walker: Walker = .{ .collector = self, .domain = domain, .ordered_rows = ordered_rows, .a = a };
        if (root) |page| try tree.walkPostOrder(a, pages.store(), page, &walker, false);
    }
    fn visit(raw: *anyopaque, scope: stores.UploadScope, id: []const u8) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        try self.check();
        if (scope.fencingToken() < self.upload_floor or scope.fencingToken() >= self.upload_cutoff or self.marked.contains(id)) return;
        self.result.eligible += 1;
        if (self.options.dry_run or self.result.deleted == self.options.max_deleted) return;
        try self.store.delete(id);
        self.result.deleted += 1;
    }
};

test "external lake native GC retains shared aggregate blocks and durable reader roots" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-lake-gc");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(4);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, namespace);
    store.upload_scope = try stores.UploadScope.forPublication(@import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, @splat(9)), 1, std.testing.io);
    var foreign = try store.put("another cluster's abandoned artifact");
    defer foreign.deinit(a);
    const scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    store.upload_scope = scope;
    var inventory = try store.put("native inventory");
    defer inventory.deinit(a);
    const spec: local.sql_operators.AggregateSpec = .{ .kind = .sum, .input_type = .integer };
    const recipe: local.sql_aggregate_materialization.Recipe = .{ .keys = &.{}, .inputs = &.{.{ .spec = spec, .column = .{ .path = "amount", .type = .integer, .nullable = false } }} };
    const group = try local.sql_operators.Grouped.create(a, &.{spec}, .{ .groups = 1, .bytes = 1024 * 1024 });
    defer group.deinit();
    try group.add(&.{}, &.{local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 })});
    const flat_root = try artifacts.publish(a, ca, &store, "sum", group, recipe, .none);
    var partition: std.ArrayList(artifacts.Partition) = .empty;
    try partition.append(ca, .{ .bucket = 0, .artifact = flat_root, .groups = 1 });
    const partitioned = try artifacts.publishPartitionRoots(a, ca, &store, &.{"sum"}, recipe, &.{partition}, .none);
    const root = partitioned[0];
    // A distinct root over the same cohort demonstrates both sharing and an
    // old reader's sole reference to an otherwise obsolete logical root.
    const old_group = try local.sql_operators.Grouped.create(a, &.{spec}, .{ .groups = 1, .bytes = 1024 * 1024 });
    defer old_group.deinit();
    try old_group.add(&.{}, &.{local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 })});
    const old_root = try artifacts.publish(a, ca, &store, "old_sum", old_group, recipe, .none);
    // Cover payloads are leaf children, not declaration roots. Keep one in
    // both generations to prove collection traverses the complete row DAG.
    const Checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    };
    var checkpoint: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &checkpoint, .checkpoint = Checkpoint.check };
    defer manager.deinit();
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
    defer sort.deinit();
    const ordered = @import("lake_index_ordered_rows.zig");
    const key = ordered.coordinate(0, .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = "part", .row_group_ordinal = 0, .row_ordinal = 0 });
    try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 })}, .ordinal = 0 });
    const row_root = try ordered.publish(a, ca, &store, &sort, "sql-rows:amount", @splat(3), .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file://source"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = @constCast(&[_]local.serverless_external_source_types.FileEntry{.{ .file_id = @constCast("part"), .object_uri = @constCast("file://part"), .byte_len = 1, .row_count = 1, .row_groups = &.{} }}) }, &.{"amount"}, .none);
    const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .algebraic, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"amount"} };
    var row_binding = binding;
    row_binding.sidecar_kind = .ordered_rows;
    var text_binding = binding;
    text_binding.sidecar_kind = .text;
    var text_segment = try store.put("authenticated native text child");
    defer text_segment.deinit(a);
    var holes = local.encoding_roaring.RoaringBitmap.init(a);
    defer holes.deinit();
    try holes.add(0);
    try holes.add(2);
    const hole_bytes = try holes.toBytes(ca);
    var hole_upload = try store.put(hole_bytes);
    defer hole_upload.deinit(a);
    const text = @import("lake_index_native_text.zig");
    const manifest_bytes = try std.json.Stringify.valueAlloc(ca, text.FileGroup{
        .file = .{ .id = "part", .digest = @splat(7) },
        .segments = &.{.{ .artifact_id = text_segment.artifact_id, .checksum = text_segment.checksum, .byte_len = text_segment.byte_len }},
        .rows = &.{.{ .group = 0, .high = 0, .base = 0, .count = 2, .lower = 0, .bitmap = .{ .artifact_id = hole_upload.artifact_id, .checksum = hole_upload.checksum, .byte_len = hole_upload.byte_len } }},
    }, .{});
    var text_manifest = try store.put(manifest_bytes);
    defer text_manifest.deinit(a);
    const text_root_bytes = try std.json.Stringify.valueAlloc(ca, text.Root{ .domain = domain, .binding = text_binding, .config_json = "{}", .manifests = &.{.{ .artifact_id = text_manifest.artifact_id, .checksum = text_manifest.checksum, .byte_len = text_manifest.byte_len }} }, .{});
    var text_upload = try store.put(text_root_bytes);
    defer text_upload.deinit(a);
    const text_ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .name = "text", .kind = .text_segment, .metadata_version = @import("lake_index_native_text.zig").metadata_version, .artifact_id = text_upload.artifact_id, .checksum = text_upload.checksum, .byte_len = text_upload.byte_len };
    var sparse_binding = binding;
    sparse_binding.sidecar_kind = .sparse;
    var sparse_child = try store.put("authenticated native sparse file block");
    defer sparse_child.deinit(a);
    const sparse_bytes = try std.json.Stringify.valueAlloc(ca, @import("lake_index_native_sparse.zig").Root{
        .binding = sparse_binding,
        .config_json = "{}",
        .generation = .{ .domain = domain, .files = &.{.{ .path = "manifest.bin", .bytes = sparse_child.byte_len, .chunks = &.{.{ .artifact_id = sparse_child.artifact_id, .checksum = sparse_child.checksum, .byte_len = sparse_child.byte_len }} }} },
    }, .{});
    var sparse_upload = try store.put(sparse_bytes);
    defer sparse_upload.deinit(a);
    const sparse_ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .name = "sparse", .kind = .sparse_segment, .metadata_version = @import("lake_index_native_sparse.zig").metadata_version, .artifact_id = sparse_upload.artifact_id, .checksum = sparse_upload.checksum, .byte_len = sparse_upload.byte_len };
    var contribution_index: @import("lake_index_contributions.zig").Index = undefined;
    try contribution_index.init(a, store, null, .none);
    defer contribution_index.deinit();
    const directory_one = try @import("lake_index_directory.zig").publishIndexed(ca, &store, &.{ .{ .name = "sum", .binding = binding, .artifact = root }, .{ .name = "old_sum", .binding = binding, .artifact = old_root }, .{ .name = row_root.name, .binding = row_binding, .artifact = row_root }, .{ .name = "text", .binding = text_binding, .artifact = text_ref }, .{ .name = "sparse", .binding = sparse_binding, .artifact = sparse_ref } }, &.{.{ .file = @splat(1), .recipe = recipe.fingerprint(), .name = "sum", .artifact = root }}, &contribution_index, .none);
    var orphan = try store.put("abandoned build artifact");
    defer orphan.deinit(a);
    var second_orphan = try store.put("another abandoned build artifact");
    defer second_orphan.deinit(a);
    const second_scope = try stores.UploadScope.forPublication(domain, 2, std.testing.io);
    store.upload_scope = second_scope;
    const directory_two = try @import("lake_index_directory.zig").publishWithContributions(ca, &store, &.{ .{ .name = "sum", .binding = binding, .artifact = root }, .{ .name = row_root.name, .binding = row_binding, .artifact = row_root }, .{ .name = "text", .binding = text_binding, .artifact = text_ref }, .{ .name = "sparse", .binding = sparse_binding, .artifact = sparse_ref } }, &.{.{ .file = @splat(1), .recipe = recipe.fingerprint(), .name = "sum", .artifact = root }}, .none);
    var publication: local.metadata_lake_index_catalog.Publication = .{
        .reader_protocol = 24,
        .namespace = namespace,
        .generation = 1,
        .token = scope.attempt,
        .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = identity },
        .published_at_ms = 1,
        .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } },
        .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len },
        .directory = directory_one,
    };
    const Harness = struct {
        a: A,
        state: lifecycle.State,
        fn read(raw: *anyopaque, alloc: A, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return lifecycle.encode(alloc, self.state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, mutation: lifecycle.Mutation, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (revision != self.state.revision) return error.CatalogGenerationChanged;
            self.state = try mutation.apply(self.a, self.state);
        }
    };
    var harness: Harness = .{ .a = ca, .state = try (lifecycle.State{}).synchronize(ca, .{ .namespace = namespace, .generation = 1, .published = publication }) };
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    harness.state = try harness.state.acquire(ca, @splat(9), 1, now);
    publication.generation = 2;
    publication.token = second_scope.attempt;
    publication.directory = directory_two;
    harness.state = try harness.state.synchronize(ca, .{ .namespace = namespace, .generation = 2, .published = publication });
    const authority: lease_api.Authority = .{ .ptr = &harness, .context = .{}, .read = Harness.read, .mutate = Harness.mutate };
    var planner: Collector = .{ .a = a, .table = 4, .authority = authority, .store = store, .identity = identity, .context = .{}, .options = .{ .dry_run = true, .max_deleted = 1 }, .token = @splat(7) };
    const planned = try planner.run();
    try std.testing.expectEqual(@as(usize, 2), planned.eligible);
    try std.testing.expect(planned.complete);
    try std.testing.expectEqual(@as(usize, 0), planned.deleted);
    try std.testing.expectEqual(@as(u64, 3), harness.state.revision);
    // Each collector is destroyed between passes. A one-job mark budget is
    // intentionally smaller than this live graph, proving durable continuation.
    var deleted: usize = 0;
    var completed = false;
    var collector_token: [16]u8 = @splat(7);
    for (0..256) |pass| {
        if (pass == 1) {
            const old = harness.state.collection.?.progress.?;
            harness.state.collection.?.expires_ms = now - 1;
            collector_token = @splat(6);
            harness.state = try harness.state.beginCollection(ca, collector_token, identity, now);
            try std.testing.expectEqualStrings(old.artifact_id, harness.state.collection.?.progress.?.artifact_id);
            try std.testing.expectError(error.LakeIndexCollectionFenceChanged, harness.state.checkpointCollection(ca, @splat(7), old.artifact_id, old, now));
            try std.testing.expectError(error.LakeIndexCollectionFenceChanged, harness.state.checkpointCollection(ca, collector_token, "stale-checkpoint", old, now));
        }
        var collector: Collector = .{ .a = a, .table = 4, .authority = authority, .store = store, .identity = identity, .context = .{}, .options = .{ .dry_run = false, .max_deleted = 1, .max_marked = 1 }, .token = collector_token };
        const result = try collector.run();
        deleted += result.deleted;
        if (pass == 0) {
            try std.testing.expect(!result.complete);
            try std.testing.expectEqual(@as(usize, 0), result.deleted);
            try std.testing.expect(harness.state.collection.?.progress != null);
        }
        if (result.complete) {
            completed = true;
            break;
        }
    }
    try std.testing.expect(completed);
    try std.testing.expectEqual(@as(usize, 2), deleted);
    const retained_old = try store.getAlloc(old_root.artifact_id);
    defer a.free(retained_old);
    harness.state = try harness.state.release(ca, @splat(9));
    var second: Collector = .{ .a = a, .table = 4, .authority = authority, .store = store, .identity = identity, .context = .{}, .options = .{ .dry_run = false }, .token = @splat(8) };
    try std.testing.expect((try second.run()).deleted >= 2);
    try std.testing.expectEqual(@as(usize, 1), harness.state.publications.len);
    const retained_holes = try store.getAlloc(hole_upload.artifact_id);
    defer a.free(retained_holes);
    try std.testing.expectEqualSlices(u8, hole_bytes, retained_holes);
    const retained_manifest = try store.getAlloc(text_manifest.artifact_id);
    defer a.free(retained_manifest);
    try std.testing.expectEqualSlices(u8, manifest_bytes, retained_manifest);
    const retained_sparse = try store.getAlloc(sparse_child.artifact_id);
    defer a.free(retained_sparse);
    try std.testing.expectEqualStrings("authenticated native sparse file block", retained_sparse);
    const cursor = (try artifacts.Reader.open(a, store, root, recipe, .none)).cursor();
    defer cursor.close(cursor.ptr);
    const result = (try cursor.next(cursor.ptr, ca, 1)).?;
    var partial = try local.sql_aggregate_partial.decode(a, result[0].aggregates[0], spec);
    defer partial.deinit();
    try std.testing.expectEqual(@as(i128, 9007199254740993), partial.integer_sum);
    var row_reader: ordered.Reader = undefined;
    try row_reader.init(a, &store, try ordered.loadRoot(ca, store, row_root, .none, null), @splat(3), "", null, .none);
    defer row_reader.deinit();
    const entries = try row_reader.nextEntries(ca, 1);
    const cover = entries[0].cover.?;
    const cover_bytes = try store.getVerifiedAllocWithCancellation(cover.block.artifact_id, cover.block.byte_len, cover.block.checksum, .none);
    defer a.free(cover_bytes);
    const block = try local.sql_spill.decodeColumnarBlockInArena(ca, cover_bytes, artifacts.max_block_bytes);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try block.cell(cover.row, 0)).value.integer);
    const foreign_bytes = try store.getAlloc(foreign.artifact_id);
    defer a.free(foreign_bytes);
    try std.testing.expectEqualStrings("another cluster's abandoned artifact", foreign_bytes);
    const text_bytes = try store.getVerifiedAllocWithCancellation(text_segment.artifact_id, text_segment.byte_len, text_segment.checksum, .none);
    defer a.free(text_bytes);
    try std.testing.expectEqualStrings("authenticated native text child", text_bytes);
}

test "external lake native GC marks physical text packs instead of range cache identities" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("packed-native-text-gc");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(4);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, namespace);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    const bytes = try ca.alloc(u8, 1024 * 1024 + 11);
    @memset(bytes, 7);
    const seekable = @import("lake_index_seekable_text.zig");
    const root = try seekable.publish(ca, ca, &store, bytes, .none);
    var collector: Collector = .{ .a = a, .table = 4, .authority = undefined, .store = store, .identity = identity, .context = .{}, .token = @splat(1), .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = 16 * 1024 * 1024, .namespace = namespace };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        collector.marked.deinit(a);
    }
    try collector.markTextDirectory(ca, root);
    const decoded = try seekable.loadDirectory(ca, .{ .store = store, .cache = null, .context = .{}, .cancellation = .none }, root);
    try std.testing.expectEqual(@as(u32, 3), collector.marked.count());
    for (decoded.blocks) |piece| {
        try std.testing.expect(collector.marked.contains(piece.pack.?.artifact_id));
        if (!std.mem.eql(u8, piece.ref.artifact_id, piece.pack.?.artifact_id)) try std.testing.expect(!collector.marked.contains(piece.ref.artifact_id));
    }
}

test "external lake native GC admits text manifest reads before storage access" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("text-manifest-gc-budget");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(4);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(4, identity, namespace);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    // Deliberately absent: child I/O would produce FileNotFound instead of
    // the expected budget rejection. Directory loading must stay shallow.
    const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const id = try store.upload_scope.?.artifactId(checksum);
    const manifest: artifacts.ChunkRef = .{ .artifact_id = &id, .checksum = checksum, .byte_len = 4096 };
    const text = @import("lake_index_native_text.zig");
    const binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} };
    const encoded = try std.json.Stringify.valueAlloc(ca, text.Root{ .domain = domain, .binding = binding, .config_json = "{}", .manifests = &.{manifest} }, .{});
    var upload = try store.put(encoded);
    defer upload.deinit(a);
    const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .name = "text", .kind = .text_segment, .metadata_version = text.metadata_version, .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
    const shallow = try text.loadRootDirectory(ca, store, ref, .none, null);
    try std.testing.expectEqual(@as(usize, 1), shallow.manifests.len);
    try std.testing.expectEqual(@as(usize, 0), shallow.file_groups.len);
    var collector: Collector = .{ .a = a, .table = 4, .authority = undefined, .store = store, .identity = identity, .context = .{}, .token = @splat(1), .expires_ms = std.math.maxInt(u64), .authority_deadline = std.math.maxInt(u64), .remaining_reads = upload.byte_len, .namespace = namespace };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        collector.marked.deinit(a);
    }
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, collector.markArtifact(ca, ref));
    try std.testing.expectEqual(@as(u64, 0), collector.remaining_reads);
}
