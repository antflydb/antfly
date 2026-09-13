// Copyright 2026 Antfly, Inc.
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

//! Operation-local bridge between immutable graph pages and artifact storage.
//! The capability is borrowed; its owner and allocator are never modified.
const std = @import("std");
const tree = @import("page_tree.zig");
const artifacts = @import("../artifacts/store.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const graph = @import("page_graph.zig");
const refs = @import("../manifest/artifact_ref.zig");

pub const ReadCache = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, std.mem.Allocator, *artifacts.ArtifactStore, refs.ArtifactRef, u64, usize, [32]u8, CancellationToken, *u64) anyerror![]u8,
};

pub const PageStore = struct {
    domain: [32]u8 = @splat(0),
    attempt: [16]u8 = @splat(0),
    artifacts: *artifacts.ArtifactStore,
    cancellation: CancellationToken = .none,
    remaining_read_bytes: *u64,
    remaining_write_bytes: *u64,
    read_cache: ?ReadCache = null,

    pub fn store(self: *PageStore) tree.Store {
        return .{ .domain = self.domain, .attempt = self.attempt, .ptr = self, .get = get, .put = put, .check = check };
    }

    pub fn namespaceDomain(namespace: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:graph-reclamation-domain:v1:");
        hash.update(namespace);
        return hash.finalResult();
    }

    pub fn loadRoot(self: *PageStore, alloc: std.mem.Allocator, source: refs.ArtifactRef) !graph.Root {
        if (source.kind != .graph_segment or source.metadata_version != graph.Root.metadata_version or
            source.byte_len != graph.Root.encoded_bytes) return error.InvalidGraphRoot;
        try self.cancellation.check();
        artifacts.validateSha256ArtifactIdentity(source.artifact_id, source.checksum) catch return error.ArtifactIntegrityMismatch;
        const scope = (try artifacts.uploadScopeFromArtifactId(source.artifact_id)) orelse return error.InvalidGraphRoot;
        const bytes = if (self.read_cache) |cache|
            try cache.read(cache.ptr, alloc, self.artifacts, source, 0, graph.Root.encoded_bytes, try artifacts.sha256DigestFromChecksum(source.checksum), self.cancellation, self.remaining_read_bytes)
        else bytes: {
            try artifacts.chargeReadBudget(self.remaining_read_bytes, source.byte_len);
            break :bytes try self.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, source.artifact_id, 0, graph.Root.encoded_bytes, self.cancellation);
        };
        defer alloc.free(bytes);
        if (bytes.len != graph.Root.encoded_bytes) return error.ArtifactIntegrityMismatch;
        try artifacts.validatePayloadSha256WithCancellation(bytes, source.checksum, self.cancellation);
        const root = try graph.Root.decode(bytes);
        if (std.mem.allEqual(u8, &root.domain, 0)) return error.GraphPageDomainMissing;
        if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.GraphPageDomainMismatch;
        if (!std.mem.allEqual(u8, &self.domain, 0) and !std.mem.eql(u8, &self.domain, &root.domain)) return error.GraphPageDomainMismatch;
        self.domain = root.domain;
        return root;
    }

    /// Uploads only an immutable root. The caller must bind the returned ref and
    /// metric provenance in its existing fenced manifest/HEAD publication.
    pub fn publishRoot(self: *PageStore, alloc: std.mem.Allocator, root: graph.Root, name: []const u8) !refs.ArtifactRef {
        if (std.mem.allEqual(u8, &self.domain, 0)) return error.GraphPageDomainMissing;
        if (!std.mem.eql(u8, &root.domain, &self.domain)) return error.GraphPageDomainMismatch;
        const bytes = root.encode();
        _ = try graph.Root.decode(&bytes);
        if (bytes.len > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes.len;
        var metadata = try self.artifacts.putScoped(.{ .domain = self.domain, .attempt = self.attempt }, &bytes, self.cancellation);
        defer metadata.deinit(self.artifacts.allocator);
        try artifacts.validateSha256ArtifactIdentity(metadata.artifact_id, metadata.checksum);
        if (metadata.byte_len != bytes.len) return error.ArtifactIntegrityMismatch;
        try artifacts.validatePayloadSha256WithCancellation(&bytes, metadata.checksum, self.cancellation);
        const id = try alloc.dupe(u8, metadata.artifact_id);
        errdefer alloc.free(id);
        const checksum = try alloc.dupe(u8, metadata.checksum);
        errdefer alloc.free(checksum);
        return .{
            .kind = .graph_segment,
            .name = if (name.len == 0) "" else try alloc.dupe(u8, name),
            .artifact_id = id,
            .checksum = checksum,
            .byte_len = bytes.len,
            .metadata_version = graph.Root.metadata_version,
        };
    }

    fn check(ptr: *anyopaque) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        try self.cancellation.check();
    }

    pub fn identity(domain: [32]u8, ref: tree.Ref) ![175]u8 {
        return (artifacts.UploadScope{ .domain = domain, .attempt = ref.attempt }).artifactId(&std.fmt.bytesToHex(&ref.digest, .lower));
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, ref: tree.Ref) ![]u8 {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        try ref.validate();
        const id = try identity(self.domain, ref);
        if (self.read_cache) |cache| return cache.read(cache.ptr, alloc, self.artifacts, .{
            .kind = .graph_segment,
            .artifact_id = &id,
            .checksum = id[7..71],
            .byte_len = ref.bytes,
        }, 0, ref.bytes, ref.digest, self.cancellation, self.remaining_read_bytes);
        try artifacts.chargeReadBudget(self.remaining_read_bytes, ref.bytes);
        // A bounded range read avoids an extra stat request per small page.
        // page_tree verifies both exact length and digest before decoding.
        return self.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, &id, 0, ref.bytes, self.cancellation);
    }

    fn put(ptr: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self: *PageStore = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, &ref.attempt, &self.attempt)) return error.GraphPageAttemptMismatch;
        if (bytes.len > self.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes.len;
        var metadata = try self.artifacts.putScoped(.{ .domain = self.domain, .attempt = self.attempt }, bytes, self.cancellation);
        defer metadata.deinit(self.artifacts.allocator);
        const id = try identity(self.domain, ref);
        if (metadata.byte_len != ref.bytes or !std.mem.eql(u8, metadata.artifact_id, &id) or
            !std.mem.eql(u8, metadata.checksum, id[7..71])) return error.ArtifactIntegrityMismatch;
    }
};

/// Mark root reachability before sweeping any content. The caller includes
/// published lineage, pinned versions and above-HEAD candidates in this set.
pub fn retainRoot(alloc: std.mem.Allocator, pages: *PageStore, source: refs.ArtifactRef, retained: *std.StringHashMapUnmanaged(void)) !void {
    if (retained.contains(source.artifact_id)) return;
    const root = try pages.loadRoot(alloc, source);
    const Visitor = struct {
        alloc: std.mem.Allocator,
        retained: *std.StringHashMapUnmanaged(void),
        domain: [32]u8,

        pub fn skip(self: *@This(), ref: tree.Ref) !bool {
            return self.retained.contains(&try PageStore.identity(self.domain, ref));
        }

        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            const owned = try self.alloc.dupe(u8, &try PageStore.identity(self.domain, ref));
            errdefer self.alloc.free(owned);
            try self.retained.put(self.alloc, owned, {});
        }
    };
    var visitor: Visitor = .{ .alloc = alloc, .retained = retained, .domain = root.domain };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, false);
    const owned = try alloc.dupe(u8, source.artifact_id);
    errdefer alloc.free(owned);
    try retained.put(alloc, owned, {});
}

/// Reclamation replay is child-before-parent all the way through the small
/// graph root. Never delete the root first: it is the recovery inventory after
/// cancellation or a process crash, including partially deleted subtrees.
pub fn reclaimRoot(alloc: std.mem.Allocator, pages: *PageStore, source: refs.ArtifactRef, retained: *const std.StringHashMapUnmanaged(void)) !usize {
    if (retained.contains(source.artifact_id)) return 0;
    const root = pages.loadRoot(alloc, source) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    const Visitor = struct {
        pages: *PageStore,
        retained: *const std.StringHashMapUnmanaged(void),
        deleted: usize = 0,

        pub fn skip(self: *@This(), ref: tree.Ref) !bool {
            return self.retained.contains(&try PageStore.identity(self.pages.domain, ref));
        }

        pub fn visit(self: *@This(), ref: tree.Ref) !void {
            try self.pages.cancellation.check();
            self.pages.artifacts.delete(&try PageStore.identity(self.pages.domain, ref)) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            self.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .pages = pages, .retained = retained };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, true);
    try pages.cancellation.check();
    pages.artifacts.delete(source.artifact_id) catch |err| switch (err) {
        error.FileNotFound => return visitor.deleted,
        else => return err,
    };
    return visitor.deleted + 1;
}

const TestArtifacts = struct {
    memory: tree.testing.MemoryStore,
    deleted: usize = 0,
    fail_delete_after: ?usize = null,

    fn capability(self: *@This()) artifacts.ArtifactStore {
        return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &.{
            .deinit = deinit,
            .put = put,
            .put_scoped = putScoped,
            .get_alloc = get,
            .get_range_alloc = range,
            .stat = stat,
            .delete = delete,
        } };
    }

    fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}

    fn metadata(alloc: std.mem.Allocator, id: []const u8, len: usize) !artifacts.ArtifactMetadata {
        const owned = try alloc.dupe(u8, id);
        errdefer alloc.free(owned);
        return .{ .artifact_id = owned, .checksum = try alloc.dupe(u8, try artifacts.sha256ChecksumFromArtifactId(id)), .byte_len = len };
    }

    fn address(id: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
        return digest;
    }

    fn put(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8) !artifacts.ArtifactMetadata {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const id = "sha256:".* ++ std.fmt.bytesToHex(&digest, .lower);
        return save(ptr, alloc, &id, bytes);
    }

    fn putScoped(ptr: *anyopaque, alloc: std.mem.Allocator, scope: artifacts.UploadScope, bytes: []const u8, cancellation: CancellationToken) !artifacts.ArtifactMetadata {
        try cancellation.check();
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const id = try scope.artifactId(&std.fmt.bytesToHex(&digest, .lower));
        return save(ptr, alloc, &id, bytes);
    }

    fn save(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, bytes: []const u8) !artifacts.ArtifactMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const store = self.memory.store();
        try store.put(store.ptr, .{ .digest = address(id), .bytes = @intCast(bytes.len), .height = 0, .records = 1 }, bytes);
        return metadata(alloc, id, bytes.len);
    }

    fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
        return error.UnexpectedFullRead;
    }

    fn range(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const bytes = self.memory.pages.get(address(id)) orelse return error.FileNotFound;
        if (offset > bytes.len or len > bytes.len - offset) return error.InvalidRange;
        self.memory.reads += 1;
        return alloc.dupe(u8, bytes[@intCast(offset)..][0..len]);
    }

    fn stat(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8) !artifacts.ArtifactMetadata {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const bytes = self.memory.pages.get(address(id)) orelse return error.FileNotFound;
        return metadata(alloc, id, bytes.len);
    }

    fn delete(ptr: *anyopaque, id: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail_delete_after) |limit| if (self.deleted == limit) return error.InjectedGcInterruption;
        const removed = self.memory.pages.fetchRemove(address(id)) orelse return error.FileNotFound;
        self.memory.alloc.free(removed.value);
        self.deleted += 1;
    }
};

fn freeRef(alloc: std.mem.Allocator, ref: refs.ArtifactRef) void {
    if (ref.name.len != 0) alloc.free(ref.name);
    alloc.free(ref.artifact_id);
    alloc.free(ref.checksum);
}

test "serverless graph page reclamation domains isolate identical graphs across namespaces" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 10 * 1024 * 1024;
    var writes: u64 = 10 * 1024 * 1024;
    var left: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("left"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var right: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("right"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const replacements = &[_]graph.Replacement{.{ .id = "a", .edges = &.{.{ .source = "a", .target = "b", .kind = "links" }} }};
    var left_plan = try graph.plan(alloc, left.store(), .{}, replacements);
    defer left_plan.deinit();
    var right_plan = try graph.plan(alloc, right.store(), .{}, replacements);
    defer right_plan.deinit();
    try std.testing.expectError(error.GraphPageDomainMismatch, left_plan.publish(right.store(), .{}));
    const left_root = try left_plan.publish(left.store(), .{});
    const right_root = try right_plan.publish(right.store(), .{});
    try std.testing.expect(!left_root.page.?.eql(right_root.page.?));
    try std.testing.expectError(error.GraphPageDomainMismatch, graph.plan(alloc, right.store(), left_root, replacements));
    const left_ref = try left.publishRoot(alloc, left_root, "graph");
    defer freeRef(alloc, left_ref);
    const right_ref = try right.publishRoot(alloc, right_root, "graph");
    defer freeRef(alloc, right_ref);
    try std.testing.expect(!std.mem.eql(u8, left_ref.artifact_id, right_ref.artifact_id));
    try std.testing.expectError(error.GraphPageDomainMismatch, left.loadRoot(alloc, right_ref));
    try std.testing.expectEqual(PageStore.namespaceDomain("left"), left.domain);
    try std.testing.expect(try reclaimRoot(alloc, &left, left_ref, &.empty) > 0);
    const Reader = @import("page_reader.zig").Reader;
    const reader = try Reader.create(alloc, &artifacts_store, right_ref, .none, &reads, null);
    defer reader.destroy();
    try std.testing.expect(try reader.containsNode("a"));
    try std.testing.expect(try reader.containsNode("b"));
    // Domain addressing refuses to locate another namespace's page. Even a
    // copy placed at the wrong domain address fails authenticated decoding.
    try std.testing.expectError(error.FileNotFound, tree.Cursor.init(alloc, left.store(), right_root.page, "", null));
    const right_id = try PageStore.identity(right.domain, right_root.page.?);
    const copied_id = try PageStore.identity(left.domain, right_root.page.?);
    var copied = try TestArtifacts.save(&backing, alloc, &copied_id, backing.memory.pages.get(TestArtifacts.address(&right_id)).?);
    defer copied.deinit(alloc);
    try std.testing.expectError(error.GraphPageDomainMismatch, tree.Cursor.init(alloc, left.store(), right_root.page, "", null));
}

test "serverless graph attempt identities isolate recreated content and retain reused old pages" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 10 * 1024 * 1024;
    var writes: u64 = 10 * 1024 * 1024;
    var pages: PageStore = .{ .domain = PageStore.namespaceDomain("test"), .attempt = @splat(1), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const value = [_]u8{42} ** 16000;
    const mutations = &[_]tree.Mutation{ .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value }, .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value } };
    const old: graph.Root = .{ .domain = pages.domain, .nodes = 4, .page = try tree.apply(alloc, pages.store(), null, mutations) };
    const old_ref = try pages.publishRoot(alloc, old, "graph");
    defer freeRef(alloc, old_ref);
    pages.attempt = @splat(2);
    const recreated: graph.Root = .{ .domain = pages.domain, .nodes = 4, .page = try tree.apply(alloc, pages.store(), null, mutations) };
    const recreated_ref = try pages.publishRoot(alloc, recreated, "graph");
    defer freeRef(alloc, recreated_ref);
    var changed = old;
    changed.page = try tree.apply(alloc, pages.store(), old.page, &.{.{ .key = "a", .value = "changed" }});
    const changed_ref = try pages.publishRoot(alloc, changed, "graph");
    defer freeRef(alloc, changed_ref);
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var it = retained.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        retained.deinit(alloc);
    }
    try retainRoot(alloc, &pages, changed_ref, &retained);
    try std.testing.expect(try reclaimRoot(alloc, &pages, old_ref, &retained) > 0);
    for ([_]graph.Root{ recreated, changed }) |root| {
        var cursor = try tree.Cursor.init(alloc, pages.store(), root.page, "", null);
        defer cursor.deinit();
        var count: usize = 0;
        while (try cursor.next()) |_| count += 1;
        try std.testing.expectEqual(4, count);
    }
}

test "serverless graph page artifact roots authenticate budget and replay interrupted shared-page GC" {
    const alloc = std.testing.allocator;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var read_bytes: u64 = 10 * 1024 * 1024;
    var write_bytes: u64 = 10 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes };
    const value = [_]u8{42} ** 16000;
    const root: graph.Root = .{ .domain = pages.domain, .nodes = 6, .edges = 0, .page = try tree.apply(alloc, pages.store(), null, &.{
        .{ .key = "a", .value = &value }, .{ .key = "b", .value = &value },
        .{ .key = "c", .value = &value }, .{ .key = "d", .value = &value },
        .{ .key = "e", .value = &value }, .{ .key = "f", .value = &value },
    }) };
    const old = try pages.publishRoot(alloc, root, "graph");
    defer freeRef(alloc, old);
    try std.testing.expect(root.eql(try pages.loadRoot(alloc, old)));
    var changed = root;
    changed.page = try tree.apply(alloc, pages.store(), root.page, &.{.{ .key = "c", .value = "changed" }});
    const current = try pages.publishRoot(alloc, changed, "graph");
    defer freeRef(alloc, current);
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var iterator = retained.keyIterator();
        while (iterator.next()) |key| alloc.free(key.*);
        retained.deinit(alloc);
    }
    try retainRoot(alloc, &pages, current, &retained);
    backing.fail_delete_after = 1;
    try std.testing.expectError(error.InjectedGcInterruption, reclaimRoot(alloc, &pages, old, &retained));
    try std.testing.expect(root.eql(try pages.loadRoot(alloc, old)));
    backing.fail_delete_after = null;
    try std.testing.expect(try reclaimRoot(alloc, &pages, old, &retained) > 0);
    try std.testing.expectEqual(0, try reclaimRoot(alloc, &pages, old, &retained));
    try std.testing.expect(changed.eql(try pages.loadRoot(alloc, current)));
    var cursor = try tree.Cursor.init(alloc, pages.store(), changed.page, "", null);
    defer cursor.deinit();
    var count: usize = 0;
    while (try cursor.next()) |_| count += 1;
    try std.testing.expectEqual(6, count);
    read_bytes = graph.Root.encoded_bytes - 1;
    const reads = backing.memory.reads;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, pages.loadRoot(alloc, current));
    try std.testing.expectEqual(reads, backing.memory.reads);
    write_bytes = graph.Root.encoded_bytes - 1;
    try std.testing.expectError(error.GraphPageWriteBudgetExceeded, pages.publishRoot(alloc, changed, "graph"));
}

test "serverless paged metric preparation matches packed canonical topology and type checksums" {
    const alloc = std.testing.allocator;
    const indexed = @import("topology_reader.zig");
    const Edge = @import("page_keys.zig").Edge;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = alloc } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const edges = [_]Edge{
        .{ .source = "z", .target = "a", .kind = "link", .weight = 2 },
        .{ .source = "z", .target = "a", .kind = "link", .weight = 1 },
        .{ .source = "z", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "z", .target = "other", .kind = "other" },
        .{ .source = "z", .target = "z", .kind = "self" },
    };
    var plan = try graph.plan(alloc, pages.store(), .{}, &.{.{ .id = "z", .edges = &edges }});
    defer plan.deinit();
    const root = try plan.publish(pages.store(), .{});
    const paged_ref = try pages.publishRoot(alloc, root, "graph");
    defer freeRef(alloc, paged_ref);
    var packed_builder = @import("builder.zig").Builder{ .alloc = alloc };
    defer packed_builder.deinit();
    for (edges) |edge| try packed_builder.addEdge(edge.source, edge.target, edge.kind, edge.weight, edge.table);
    const payload = try packed_builder.encodeAlloc(16 * 1024 * 1024, .none);
    defer alloc.free(payload);
    var metadata = try artifacts_store.put(payload);
    defer metadata.deinit(alloc);
    const packed_ref: refs.ArtifactRef = .{ .kind = .graph_segment, .artifact_id = metadata.artifact_id, .byte_len = metadata.byte_len, .checksum = metadata.checksum };
    const Config = struct { edge_filter: struct { mode: enum { all, types }, types: []const []const u8 = &.{} } };
    for ([_]Config{ .{ .edge_filter = .{ .mode = .all } }, .{ .edge_filter = .{ .mode = .types, .types = &.{"link"} } } }) |config| {
        const configs = [_]Config{config};
        var expected = (try indexed.readOracleAlloc(alloc, &artifacts_store, packed_ref, &configs, .{ .max_nodes = 100, .max_edges = 100 }, .none, &reads)).?;
        defer expected.deinit(alloc);
        var actual = (try indexed.readAlloc(alloc, &artifacts_store, paged_ref, &configs, .{ .max_nodes = 100, .max_edges = 100 }, .none, &reads)).?;
        defer actual.deinit(alloc);
        try std.testing.expectEqualDeep(expected.node_ids, actual.node_ids);
        try std.testing.expectEqualDeep(expected.edge_types, actual.edge_types);
        try std.testing.expectEqualSlices(indexed.Edge, expected.edges, actual.edges);
        try std.testing.expectEqualSlices(u32, expected.edge_type_offsets, actual.edge_type_offsets);
        try std.testing.expectEqualSlices([32]u8, expected.type_checksums, actual.type_checksums);
        try std.testing.expectEqual(expected.source_node_count, actual.source_node_count);
        try std.testing.expectEqual(expected.source_edge_count, actual.source_edge_count);
    }
}

fn exercisePageReader(alloc: std.mem.Allocator) !void {
    const setup = std.testing.allocator;
    const public = @import("adjacency_reader.zig");
    const Edge = @import("page_keys.zig").Edge;
    var backing: TestArtifacts = .{ .memory = .{ .alloc = setup } };
    defer backing.memory.deinit();
    var artifacts_store = backing.capability();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: PageStore = .{ .attempt = @splat(1), .domain = PageStore.namespaceDomain("test"), .artifacts = &artifacts_store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const outgoing = [_]Edge{
        .{ .source = "a", .target = "b", .kind = "link", .weight = 2 },
        .{ .source = "a", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "a", .target = "a", .kind = "other" },
    };
    const incoming = [_]Edge{.{ .source = "b", .target = "a", .kind = "link" }};
    var plan = try graph.plan(setup, pages.store(), .{}, &.{ .{ .id = "a", .edges = &outgoing }, .{ .id = "b", .edges = &incoming } });
    defer plan.deinit();
    const root = try plan.publish(pages.store(), .{});
    const ref = try pages.publishRoot(setup, root, "graph");
    defer freeRef(setup, ref);
    var reader = (try public.Reader.init(alloc, &artifacts_store, ref, .none, &reads)).?;
    defer reader.deinit();
    try std.testing.expect(try reader.containsNode("a"));
    try std.testing.expect(!try reader.containsNode("missing"));
    const id = (try reader.ordinal("a")).?;
    const node_name = try reader.nodeNameAlloc(id);
    defer alloc.free(node_name);
    try std.testing.expectEqualStrings("a", node_name);
    const selected = try reader.resolveTypes(&.{ "other", "link", "link" });
    defer if (selected) |ids| alloc.free(ids);
    var work: usize = 100;
    var cursor = try reader.cursorOrdinal(id, selected, false, &work);
    defer cursor.deinit();
    var count: usize = 0;
    while (try cursor.next()) |owned| {
        var edge = owned;
        defer edge.deinit(alloc);
        if (edge.neighbor_table_id) |table| {
            const metadata = reader.dynamicTableMetadata(table).?;
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("remote", parsed.value.object.get("target_table").?.string);
        }
        count += 1;
    }
    try std.testing.expectEqual(3, count);
    var found = (try reader.probe("a", "link", "foreign", &work)).?;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("foreign", found.neighbor_id);
    try std.testing.expect(found.neighbor_table_id != null);
    var adjacency = (try reader.adjacencyFiltered("a", &.{}, .both, 10, &work, false, true)).?;
    defer adjacency.deinit(alloc);
    try std.testing.expectEqual(2, adjacency.out_edges.len);
    try std.testing.expectEqual(1, adjacency.in_edges.len);
    try std.testing.expectEqualStrings("b", adjacency.in_edges[0].neighbor_id);
}

test "serverless paged public adjacency reader preserves query-local identities and qualified metadata" {
    try exercisePageReader(std.testing.allocator);
}

test "serverless paged public adjacency reader cleans every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exercisePageReader, .{});
}
