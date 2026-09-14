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

//! Request-local document point reads over immutable facts pages. Stable heap
//! ownership is required because PageStore, its cache and cancellation borrow
//! pointers. The session and read allowance must outlive this reader.
const std = @import("std");
const Allocator = std.mem.Allocator;
const facts = @import("../build/document_facts.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const runtime = @import("runtime.zig");
const materializer = @import("materializer.zig");

pub const Fact = facts.Fact;
pub const max_materialized_bytes: usize = 512 * 1024 * 1024;

fn readFailure(err: anyerror) anyerror {
    return switch (err) {
        error.ArtifactReadBudgetExceeded, error.GraphMetricBuildBudgetExceeded => error.QueryCandidateBudgetExceeded,
        else => err,
    };
}

pub const Reader = struct {
    alloc: Allocator,
    session: *runtime.QuerySession,
    root: facts.Root,
    pages: PageStore,
    cache: tree.Cache,
    no_writes: u64 = 0,

    pub fn create(alloc: Allocator, session: *runtime.QuerySession, remaining: *u64) !?*Reader {
        try session.checkCancellation();
        const index = session.findArtifactIndex(.document_facts) orelse return null;
        const source = session.artifactRef(index).?;
        for (session.manifest.artifacts[index + 1 ..]) |artifact| {
            if (artifact.kind == .document_facts) return error.InvalidDocumentFactsRoot;
        }
        const self = try alloc.create(Reader);
        errdefer alloc.destroy(self);
        self.* = .{
            .alloc = alloc,
            .session = session,
            .root = undefined,
            .pages = .{ .artifacts = session.artifacts, .cancellation = session.readCancellation(), .remaining_read_bytes = remaining, .remaining_write_bytes = &self.no_writes, .read_cache = session.graphAdjacencyCache() },
            .cache = undefined,
        };
        self.root = facts.loadRoot(alloc, &self.pages, source) catch |err| return readFailure(err);
        if (!std.mem.eql(u8, &self.root.domain, &PageStore.namespaceDomain(session.namespace())) or
            self.root.wal_end_lsn != session.manifest.wal_end_lsn or
            self.root.document_count != session.manifest.stats.document_count) return error.DocumentFactsSourceChanged;
        self.cache = .{ .alloc = alloc, .underlying = self.pages.store() };
        return self;
    }

    pub fn destroy(self: *Reader) void {
        self.cache.deinit();
        self.alloc.destroy(self);
    }

    pub fn lookup(self: *Reader, id: []const u8) !?Fact {
        try self.session.checkCancellation();
        if (id.len == 0) return null;
        if (id.len > tree.max_key_bytes) return error.InvalidDocumentFact;
        return facts.lookup(self.alloc, self.cache.store(), self.root, id) catch |err| return readFailure(err);
    }

    pub fn readBodyAlloc(self: *Reader, fact: Fact) ![]u8 {
        try self.session.checkCancellation();
        if (fact.body.bytes > max_materialized_bytes) return error.QueryCandidateBudgetExceeded;
        return facts.readBodyAlloc(self.alloc, &self.pages, fact.body) catch |err| return readFailure(err);
    }

    /// Compatibility with callers that genuinely require a full snapshot.
    /// Point-query callers use lookup/readBodyAlloc and never scan all pages.
    /// Output owns each ID/body and is ordered by document ID.
    pub fn materializeAlloc(self: *Reader) ![]materializer.Document {
        try self.session.checkCancellation();
        const count = std.math.cast(usize, self.root.document_count) orelse return error.QueryCandidateBudgetExceeded;
        const array_bytes = std.math.mul(usize, count, @sizeOf(materializer.Document)) catch return error.QueryCandidateBudgetExceeded;
        if (array_bytes > max_materialized_bytes) return error.QueryCandidateBudgetExceeded;
        const documents = try self.alloc.alloc(materializer.Document, count);
        var initialized: usize = 0;
        errdefer {
            for (documents[0..initialized]) |*document| document.deinit(self.alloc);
            self.alloc.free(documents);
        }
        var cursor = tree.Cursor.init(self.alloc, self.cache.store(), self.root.page, "", null) catch |err| return readFailure(err);
        defer cursor.deinit();
        var owned_bytes = array_bytes;
        while (cursor.next() catch |err| return readFailure(err)) |record| {
            try self.session.checkCancellation();
            if (initialized >= count or record.key.len == 0) return error.InvalidDocumentFactsRoot;
            const fact = try Fact.decode(record.value);
            const body_len = std.math.cast(usize, fact.body.bytes) orelse return error.QueryCandidateBudgetExceeded;
            const doc_bytes = std.math.add(usize, record.key.len, body_len) catch return error.QueryCandidateBudgetExceeded;
            owned_bytes = std.math.add(usize, owned_bytes, doc_bytes) catch return error.QueryCandidateBudgetExceeded;
            if (owned_bytes > max_materialized_bytes) return error.QueryCandidateBudgetExceeded;
            const id = try self.alloc.dupe(u8, record.key);
            errdefer self.alloc.free(id);
            const body = try self.readBodyAlloc(fact);
            documents[initialized] = .{ .doc_id = id, .body = body, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns };
            initialized += 1;
        }
        if (initialized != count) return error.InvalidDocumentFactsRoot;
        try self.session.checkCancellation();
        return documents;
    }
};

test "serverless document facts point reads authenticate bodies and materialize sorted owned documents" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var impl = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "artifacts", "tenant");
    var artifacts = impl.artifactStore();
    defer artifacts.deinit();
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = reads;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const first_body = try facts.putBody(a, &pages, "{\"name\":\"a\"}");
    const second_body = try facts.putBody(a, &pages, "{\"name\":\"b\"}");
    const empty = facts.Root{ .domain = pages.domain, .policy_fingerprint = @splat(1) };
    var plan = try facts.planAlloc(a, pages.store(), empty, &.{
        .{ .id = "b", .value = .{ .body = second_body, .last_lsn = 7, .last_timestamp_ns = 70 } },
        .{ .id = "a", .value = .{ .body = first_body, .last_lsn = 3, .last_timestamp_ns = 30 } },
    }, 7);
    defer plan.deinit();
    const root = try plan.publish(pages.store(), empty);
    const root_ref = try facts.publishRoot(a, &pages, root);
    defer a.free(root_ref.artifact_id);
    defer a.free(root_ref.checksum);
    var refs = [_]@import("../manifest/mod.zig").ArtifactRef{root_ref};
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &artifacts, .owns_manifest = false, .manifest = .{
        .namespace = "docs",
        .version = 1,
        .built_at_ns = 1,
        .wal_start_lsn = 0,
        .wal_end_lsn = 7,
        .stats = .{ .document_count = 2, .document_base_version = 1 },
        .artifacts = &refs,
    } };
    defer session.deinit();
    var no_reads: u64 = 0;
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, Reader.create(a, &session, &no_reads));
    const reader = (try Reader.create(a, &session, &reads)).?;
    defer reader.destroy();
    try std.testing.expect((try reader.lookup("absent")) == null);
    const found = (try reader.lookup("b")).?;
    try std.testing.expectEqual(@as(u64, 7), found.last_lsn);
    const saved_reads = reads;
    reads = 0;
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, reader.readBodyAlloc(found));
    reads = saved_reads;
    const body = try reader.readBodyAlloc(found);
    defer a.free(body);
    try std.testing.expectEqualStrings("{\"name\":\"b\"}", body);
    const docs = try reader.materializeAlloc();
    defer materializer.freeDocuments(a, docs);
    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqualStrings("a", docs[0].doc_id);
    try std.testing.expectEqualStrings("{\"name\":\"a\"}", docs[0].body);
    try std.testing.expectEqualStrings("b", docs[1].doc_id);
    try std.testing.expectEqual(@as(u64, 30), docs[0].last_timestamp_ns);
    var corrupt = found;
    corrupt.body.digest[0] ^= 1;
    try std.testing.expectError(error.FileNotFound, reader.readBodyAlloc(corrupt));
    session.read_lease = .{ .unix_deadline = 1, .authority_deadline = 1 };
    try std.testing.expectError(error.DeadlineExceeded, reader.lookup("a"));
    try std.testing.expectError(error.DeadlineExceeded, reader.readBodyAlloc(found));
    session.read_lease = null;
    const Failures = struct {
        fn run(alloc: Allocator, query: *runtime.QuerySession) !void {
            var remaining: u64 = 16 * 1024 * 1024;
            const local = (try Reader.create(alloc, query, &remaining)).?;
            defer local.destroy();
            const owned = try local.materializeAlloc();
            defer materializer.freeDocuments(alloc, owned);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failures.run, .{&session});
    session.manifest.wal_end_lsn = 8;
    try std.testing.expectError(error.DocumentFactsSourceChanged, Reader.create(a, &session, &reads));
}
