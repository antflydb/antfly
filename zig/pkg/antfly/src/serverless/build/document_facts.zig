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

//! Source-fenced document point index and exact scheduling summaries. Body
//! blobs have independent immutable identities: rewriting a flat document
//! segment cannot invalidate every routing entry in this copy-on-write tree.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("../graph_segment/page_tree.zig");
const page_store = @import("../graph_segment/page_store.zig");
const artifacts = @import("../artifacts/store.zig");
const refs = @import("../manifest/artifact_ref.zig");

pub const BodyRef = struct {
    digest: [32]u8,
    attempt: [16]u8,
    bytes: u64,

    pub fn identity(self: BodyRef, domain: [32]u8) ![175]u8 {
        return (artifacts.UploadScope{ .domain = domain, .attempt = self.attempt }).artifactId(&std.fmt.bytesToHex(&self.digest, .lower));
    }
};

pub const Fact = struct {
    body: BodyRef,
    last_lsn: u64,
    last_timestamp_ns: u64,
    /// chunk_preview, chunk_embeddings, rerank_terms.
    present: u3 = 0,
    /// lexical_sparse, chunk_preview, chunk_embeddings, rerank_terms.
    pending: u4 = 0,

    pub const encoded_bytes = 80;
    pub fn encode(self: Fact) [encoded_bytes]u8 {
        var bytes = [_]u8{0} ** encoded_bytes;
        @memcpy(bytes[0..32], &self.body.digest);
        @memcpy(bytes[32..48], &self.body.attempt);
        std.mem.writeInt(u64, bytes[48..56], self.body.bytes, .little);
        std.mem.writeInt(u64, bytes[56..64], self.last_lsn, .little);
        std.mem.writeInt(u64, bytes[64..72], self.last_timestamp_ns, .little);
        bytes[72] = self.present;
        bytes[73] = self.pending;
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Fact {
        if (bytes.len != encoded_bytes or bytes[72] > 7 or bytes[73] > 15 or
            !std.mem.allEqual(u8, bytes[74..80], 0)) return error.InvalidDocumentFact;
        return .{
            .body = .{ .digest = bytes[0..32].*, .attempt = bytes[32..48].*, .bytes = std.mem.readInt(u64, bytes[48..56], .little) },
            .last_lsn = std.mem.readInt(u64, bytes[56..64], .little),
            .last_timestamp_ns = std.mem.readInt(u64, bytes[64..72], .little),
            .present = @intCast(bytes[72]),
            .pending = @intCast(bytes[73]),
        };
    }
    fn bits(self: Fact) u7 {
        return @as(u7, self.present) | (@as(u7, self.pending) << 3);
    }
};

pub const Root = struct {
    domain: [32]u8,
    policy_fingerprint: [32]u8,
    wal_end_lsn: u64 = 0,
    page: ?tree.Ref = null,
    document_count: u64 = 0,
    /// Presence counters followed by pending counters, in Fact bit order.
    counts: [7]u64 = @splat(0),

    pub const encoded_bytes = 256;
    pub const metadata_version = 1;
    pub fn eql(a: Root, b: Root) bool {
        return std.mem.eql(u8, &a.encode(), &b.encode());
    }
    pub fn encode(self: Root) [encoded_bytes]u8 {
        var out = [_]u8{0} ** encoded_bytes;
        @memcpy(out[0..8], "AFDFACT1");
        @memcpy(out[16..48], &self.domain);
        @memcpy(out[48..80], &self.policy_fingerprint);
        std.mem.writeInt(u64, out[80..88], self.wal_end_lsn, .little);
        std.mem.writeInt(u64, out[88..96], self.document_count, .little);
        for (self.counts, 0..) |count, i| std.mem.writeInt(u64, out[96 + i * 8 ..][0..8], count, .little);
        if (self.page) |page| {
            out[8] = 1;
            @memcpy(out[152..184], &page.digest);
            @memcpy(out[184..200], &page.attempt);
            std.mem.writeInt(u32, out[200..204], page.bytes, .little);
            out[204] = page.height;
            std.mem.writeInt(u64, out[208..216], page.records, .little);
        }
        return out;
    }
    pub fn decode(bytes: []const u8) !Root {
        if (bytes.len != encoded_bytes or !std.mem.eql(u8, bytes[0..8], "AFDFACT1") or
            !std.mem.allEqual(u8, bytes[9..16], 0) or !std.mem.allEqual(u8, bytes[205..208], 0) or
            !std.mem.allEqual(u8, bytes[216..256], 0)) return error.InvalidDocumentFactsRoot;
        var root = Root{
            .domain = bytes[16..48].*,
            .policy_fingerprint = bytes[48..80].*,
            .wal_end_lsn = std.mem.readInt(u64, bytes[80..88], .little),
            .document_count = std.mem.readInt(u64, bytes[88..96], .little),
        };
        for (&root.counts, 0..) |*count, i| {
            count.* = std.mem.readInt(u64, bytes[96 + i * 8 ..][0..8], .little);
            if (count.* > root.document_count) return error.InvalidDocumentFactsRoot;
        }
        if (bytes[8] == 0) {
            if (root.document_count != 0 or !std.mem.allEqual(u8, bytes[152..216], 0)) return error.InvalidDocumentFactsRoot;
        } else if (bytes[8] == 1) {
            root.page = .{
                .digest = bytes[152..184].*,
                .attempt = bytes[184..200].*,
                .bytes = std.mem.readInt(u32, bytes[200..204], .little),
                .height = bytes[204],
                .records = std.mem.readInt(u64, bytes[208..216], .little),
            };
            try root.page.?.validate();
            if (root.page.?.records != root.document_count) return error.InvalidDocumentFactsRoot;
        } else return error.InvalidDocumentFactsRoot;
        return root;
    }
};

pub fn lookup(alloc: Allocator, store: tree.Store, root: Root, id: []const u8) !?Fact {
    if (!std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    var cursor = try tree.Cursor.init(alloc, store, root.page, id, null);
    defer cursor.deinit();
    const record = (try cursor.next()) orelse return null;
    if (!std.mem.eql(u8, record.key, id)) return null;
    const fact = try Fact.decode(record.value);
    if (fact.last_lsn > root.wal_end_lsn) return error.InvalidDocumentFact;
    return fact;
}

pub const Replacement = struct { id: []const u8, value: ?Fact };
pub const Plan = struct {
    alloc: Allocator,
    source: Root,
    next: Root,
    changes: []tree.Mutation,

    pub fn deinit(self: *Plan) void {
        for (self.changes) |change| {
            self.alloc.free(change.key);
            if (change.value) |value| self.alloc.free(value);
        }
        self.alloc.free(self.changes);
        self.* = undefined;
    }
    pub fn publish(self: *const Plan, store: tree.Store, current: Root) !Root {
        if (!current.eql(self.source)) return error.DocumentFactsSourceChanged;
        if (!std.mem.eql(u8, &store.domain, &current.domain)) return error.GraphPageDomainMismatch;
        var next = self.next;
        next.page = try tree.apply(self.alloc, store, current.page, self.changes);
        _ = try Root.decode(&next.encode());
        return next;
    }
};

pub fn planAlloc(alloc: Allocator, store: tree.Store, source: Root, replacements: []const Replacement, next_wal_lsn: u64) !Plan {
    if (next_wal_lsn < source.wal_end_lsn) return error.DocumentFactsSourceChanged;
    if (!std.mem.eql(u8, &store.domain, &source.domain)) return error.GraphPageDomainMismatch;
    var changes = std.ArrayListUnmanaged(tree.Mutation).empty;
    errdefer {
        for (changes.items) |change| {
            alloc.free(change.key);
            if (change.value) |value| alloc.free(value);
        }
        changes.deinit(alloc);
    }
    var next = source;
    next.wal_end_lsn = next_wal_lsn;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    for (replacements) |replacement| {
        try store.check(store.ptr);
        if (replacement.id.len == 0 or replacement.id.len > tree.max_key_bytes) return error.InvalidDocumentFact;
        const slot = try seen.getOrPut(alloc, replacement.id);
        if (slot.found_existing) return error.DuplicateDocumentFactReplacement;
        const prior = try lookup(alloc, store, source, replacement.id);
        if (prior) |fact| {
            if (next.document_count == 0) return error.InvalidDocumentFactsRoot;
            next.document_count -= 1;
            for (&next.counts, 0..) |*count, i| if (fact.bits() & (@as(u7, 1) << @intCast(i)) != 0) {
                if (count.* == 0) return error.InvalidDocumentFactsRoot;
                count.* -= 1;
            };
        }
        if (replacement.value) |fact| {
            if (fact.last_lsn > next_wal_lsn) return error.DocumentFactsSourceChanged;
            next.document_count = std.math.add(u64, next.document_count, 1) catch return error.InvalidDocumentFactsRoot;
            for (&next.counts, 0..) |*count, i| if (fact.bits() & (@as(u7, 1) << @intCast(i)) != 0) {
                count.* += 1;
            };
        }
        const key = try alloc.dupe(u8, replacement.id);
        errdefer alloc.free(key);
        const value = if (replacement.value) |fact| try alloc.dupe(u8, &fact.encode()) else null;
        errdefer if (value) |bytes| alloc.free(bytes);
        try changes.append(alloc, .{ .key = key, .value = value });
    }
    std.mem.sort(tree.Mutation, changes.items, {}, struct {
        fn less(_: void, a: tree.Mutation, b: tree.Mutation) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.less);
    return .{ .alloc = alloc, .source = source, .next = next, .changes = try changes.toOwnedSlice(alloc) };
}

pub const body_header_bytes = 16;

pub fn bodyDigest(bytes: []const u8) [32]u8 {
    var header: [body_header_bytes]u8 = undefined;
    @memcpy(header[0..8], "AFDBODY1");
    std.mem.writeInt(u64, header[8..16], bytes.len, .little);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&header);
    hash.update(bytes);
    return hash.finalResult();
}

pub fn putBody(alloc: Allocator, pages: *page_store.PageStore, bytes: []const u8) !BodyRef {
    try pages.cancellation.check();
    const len = std.math.add(usize, bytes.len, body_header_bytes) catch return error.ArtifactTooLarge;
    if (len > pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
    const encoded = try alloc.alloc(u8, len);
    defer alloc.free(encoded);
    @memcpy(encoded[0..8], "AFDBODY1");
    std.mem.writeInt(u64, encoded[8..16], bytes.len, .little);
    @memcpy(encoded[16..], bytes);
    return putEncoded(pages, encoded);
}

fn putEncoded(pages: *page_store.PageStore, bytes: []const u8) !BodyRef {
    if (bytes.len > pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
    pages.remaining_write_bytes.* -= bytes.len;
    var metadata = try pages.artifacts.putScoped(.{ .domain = pages.domain, .attempt = pages.attempt }, bytes, pages.cancellation);
    defer metadata.deinit(pages.artifacts.allocator);
    const ref = BodyRef{ .digest = try artifacts.sha256DigestFromChecksum(metadata.checksum), .attempt = pages.attempt, .bytes = bytes.len };
    if (metadata.byte_len != bytes.len or !std.mem.eql(u8, metadata.artifact_id, &try ref.identity(pages.domain))) return error.ArtifactIntegrityMismatch;
    try artifacts.validatePayloadSha256WithCancellation(bytes, metadata.checksum, pages.cancellation);
    return ref;
}

pub fn readBodyAlloc(alloc: Allocator, pages: *page_store.PageStore, ref: BodyRef) ![]u8 {
    const id = try ref.identity(pages.domain);
    const len = std.math.cast(usize, ref.bytes) orelse return error.ArtifactReadBudgetExceeded;
    const bytes = if (pages.read_cache) |cache|
        try cache.read(cache.ptr, alloc, pages.artifacts, .{ .kind = .document_facts, .artifact_id = &id, .checksum = id[7..71], .byte_len = ref.bytes }, 0, len, ref.digest, pages.cancellation, pages.remaining_read_bytes)
    else bytes: {
        try artifacts.chargeReadBudget(pages.remaining_read_bytes, ref.bytes);
        break :bytes try pages.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, &id, 0, len, pages.cancellation);
    };
    errdefer alloc.free(bytes);
    if (bytes.len != len) return error.ArtifactIntegrityMismatch;
    try artifacts.validatePayloadSha256WithCancellation(bytes, id[7..71], pages.cancellation);
    // The envelope is a type-domain separator. An arbitrary document body
    // must never alias a routing page and bypass its descendant GC walk.
    if (len < body_header_bytes or !std.mem.eql(u8, bytes[0..8], "AFDBODY1") or
        std.mem.readInt(u64, bytes[8..16], .little) != len - body_header_bytes) return error.InvalidDocumentBody;
    const body_len = len - body_header_bytes;
    std.mem.copyForwards(u8, bytes[0..body_len], bytes[body_header_bytes..]);
    return alloc.realloc(bytes, body_len);
}

pub fn loadRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef) !Root {
    if (source.kind != .document_facts or source.metadata_version != Root.metadata_version or source.byte_len != Root.encoded_bytes)
        return error.InvalidDocumentFactsRoot;
    try artifacts.validateSha256ArtifactIdentity(source.artifact_id, source.checksum);
    const scope = (try artifacts.uploadScopeFromArtifactId(source.artifact_id)) orelse return error.InvalidDocumentFactsRoot;
    const bytes = if (pages.read_cache) |cache|
        try cache.read(cache.ptr, alloc, pages.artifacts, source, 0, Root.encoded_bytes, try artifacts.sha256DigestFromChecksum(source.checksum), pages.cancellation, pages.remaining_read_bytes)
    else bytes: {
        try artifacts.chargeReadBudget(pages.remaining_read_bytes, source.byte_len);
        break :bytes try pages.artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, source.artifact_id, 0, Root.encoded_bytes, pages.cancellation);
    };
    defer alloc.free(bytes);
    try artifacts.validatePayloadSha256WithCancellation(bytes, source.checksum, pages.cancellation);
    const root = try Root.decode(bytes);
    if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.GraphPageDomainMismatch;
    if (!std.mem.allEqual(u8, &pages.domain, 0) and !std.mem.eql(u8, &root.domain, &pages.domain)) return error.GraphPageDomainMismatch;
    pages.domain = root.domain;
    return root;
}

pub fn publishRoot(alloc: Allocator, pages: *page_store.PageStore, root: Root) !refs.ArtifactRef {
    if (!std.mem.eql(u8, &root.domain, &pages.domain)) return error.GraphPageDomainMismatch;
    const bytes = root.encode();
    _ = try Root.decode(&bytes);
    const body = try putEncoded(pages, &bytes);
    const id = try body.identity(root.domain);
    const owned_id = try alloc.dupe(u8, &id);
    errdefer alloc.free(owned_id);
    return .{ .kind = .document_facts, .artifact_id = owned_id, .checksum = try alloc.dupe(u8, id[7..71]), .byte_len = bytes.len, .metadata_version = Root.metadata_version };
}

pub fn retainRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef, retained: *std.StringHashMapUnmanaged(void)) !void {
    if (retained.contains(source.artifact_id)) return;
    const root = try loadRoot(alloc, pages, source);
    var visitor = Reachability{ .alloc = alloc, .pages = pages, .retained = retained };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, false);
    try visitor.mark(source.artifact_id);
}

pub fn reclaimRoot(alloc: Allocator, pages: *page_store.PageStore, source: refs.ArtifactRef, retained: *const std.StringHashMapUnmanaged(void)) !usize {
    if (retained.contains(source.artifact_id)) return 0;
    const root = loadRoot(alloc, pages, source) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    var visitor = Reachability{ .alloc = alloc, .pages = pages, .retained = @constCast(retained), .reclaim = true };
    if (root.page) |page| try tree.walkPostOrder(alloc, pages.store(), page, &visitor, true);
    try visitor.mark(source.artifact_id);
    return visitor.deleted;
}

const Reachability = struct {
    alloc: Allocator,
    pages: *page_store.PageStore,
    retained: *std.StringHashMapUnmanaged(void),
    reclaim: bool = false,
    deleted: usize = 0,

    fn mark(self: *@This(), id: []const u8) !void {
        if (self.retained.contains(id)) return;
        if (self.reclaim) {
            try self.pages.cancellation.check();
            self.pages.artifacts.delete(id) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            self.deleted += 1;
        } else {
            const owned = try self.alloc.dupe(u8, id);
            errdefer self.alloc.free(owned);
            try self.retained.put(self.alloc, owned, {});
        }
    }
    pub fn skip(self: *@This(), ref: tree.Ref) !bool {
        return self.retained.contains(&try page_store.PageStore.identity(self.pages.domain, ref));
    }
    pub fn visitRecord(self: *@This(), _: tree.Ref, _: []const u8, value: []const u8) !void {
        const fact = try Fact.decode(value);
        try self.mark(&try fact.body.identity(self.pages.domain));
    }
    pub fn visit(self: *@This(), ref: tree.Ref) !void {
        try self.mark(&try page_store.PageStore.identity(self.pages.domain, ref));
    }
};

test "serverless document facts update only touched paths and maintain exact source-fenced counters" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const empty = Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 12 }, .last_lsn = 1, .last_timestamp_ns = 10, .present = 5, .pending = 3 };
    var first = try planAlloc(a, store, empty, &.{ .{ .id = "a", .value = fact }, .{ .id = "b", .value = fact } }, 1);
    defer first.deinit();
    const root = try first.publish(store, empty);
    try std.testing.expectEqual(@as(u64, 2), root.document_count);
    try std.testing.expectEqual([7]u64{ 2, 0, 2, 2, 2, 0, 0 }, root.counts);
    try std.testing.expect(root.eql(try Root.decode(&root.encode())));
    var changed = fact;
    changed.present = 2;
    changed.pending = 12;
    var second = try planAlloc(a, store, root, &.{ .{ .id = "a", .value = changed }, .{ .id = "b", .value = null } }, 2);
    defer second.deinit();
    try std.testing.expectError(error.DocumentFactsSourceChanged, second.publish(store, empty));
    const updated = try second.publish(store, root);
    try std.testing.expectEqual([7]u64{ 0, 1, 0, 0, 0, 1, 1 }, updated.counts);
    try std.testing.expectEqual(@as(u64, 1), updated.document_count);
    try std.testing.expect((try lookup(a, store, updated, "b")) == null);
    try std.testing.expectEqual(@as(u3, 5), (try lookup(a, store, root, "a")).?.present);
    try std.testing.expectEqual(@as(u3, 2), (try lookup(a, store, updated, "a")).?.present);
    try std.testing.expectError(error.DuplicateDocumentFactReplacement, planAlloc(a, store, root, &.{ .{ .id = "a", .value = fact }, .{ .id = "a", .value = null } }, 2));
}

test "serverless document facts allocation failures preserve source and release plan ownership" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const source = Root{ .domain = memory.store().domain, .policy_fingerprint = @splat(1) };
    const Exercise = struct {
        fn run(alloc: Allocator, store: tree.Store, empty: Root) !void {
            const fact = Fact{ .body = .{ .digest = @splat(3), .attempt = @splat(1), .bytes = 1 }, .last_lsn = 1, .last_timestamp_ns = 0, .present = 7, .pending = 15 };
            var plan = try planAlloc(alloc, store, empty, &.{ .{ .id = "a", .value = fact }, .{ .id = "b", .value = fact } }, 1);
            defer plan.deinit();
            const root = try plan.publish(store, empty);
            var next = try planAlloc(alloc, store, root, &.{ .{ .id = "a", .value = null }, .{ .id = "c", .value = fact } }, 2);
            defer next.deinit();
            const updated = try next.publish(store, root);
            try std.testing.expectEqual(root.counts, updated.counts);
            try std.testing.expect((try lookup(alloc, store, root, "a")) != null);
            try std.testing.expect((try lookup(alloc, store, updated, "a")) == null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{ memory.store(), source });
}
