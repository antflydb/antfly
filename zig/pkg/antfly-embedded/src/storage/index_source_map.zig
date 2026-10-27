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

//! Immutable exact-source map pages. Segment-local document IDs address one
//! bounded page; the publication root authenticates every page plus the exact
//! segment digest. Neither lookup nor acknowledgment loads document bodies.
const std = @import("std");
const claims = @import("index_source_claim.zig");
const publication = claims.publication;
const directory = @import("index_source_directory.zig");
const A = std.mem.Allocator;
const Digest = publication.Digest;
const root_prefix = "index_source_map_v1:";
const page_prefix = "index_source_map_page_v1:";
const RootKey = [root_prefix.len + 64]u8;
const PageKey = [page_prefix.len + 68]u8;
pub const page_capacity = 64;
pub const legacy_maximum_documents = 65536;
pub const maximum_documents = std.math.maxInt(u32);
const entry_bytes = 105;
pub fn hasRoot(reader: anytype, binding: publication.Binding) !bool {
    _ = reader.get(&rootKey(binding)) catch |err| if (err == error.NotFound) return false else return err;
    return true;
}
fn rootKey(binding: publication.Binding) RootKey {
    var out: RootKey = undefined;
    @memcpy(out[0..root_prefix.len], root_prefix);
    @memcpy(out[root_prefix.len..][0..32], &binding.owner);
    @memcpy(out[root_prefix.len + 32 ..], &binding.publication);
    return out;
}
fn pageKey(binding: publication.Binding, page: u32) PageKey {
    var out: PageKey = undefined;
    @memcpy(out[0..page_prefix.len], page_prefix);
    @memcpy(out[page_prefix.len..][0..32], &binding.owner);
    @memcpy(out[page_prefix.len + 32 ..][0..32], &binding.publication);
    std.mem.writeInt(u32, out[page_prefix.len + 64 ..], page, .big);
    return out;
}
fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}
fn boundDigest(key: []const u8, bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(key);
    hash.update(bytes);
    return hash.finalResult();
}
fn encodeEntry(out: *[entry_bytes]u8, ref: claims.Reference) void {
    @memcpy(out[0..32], &ref.document);
    out[32] = @backingInt(ref.revision.domain);
    std.mem.writeInt(u64, out[33..41], ref.revision.value, .little);
    @memcpy(out[41..73], &ref.root);
    @memcpy(out[73..105], &ref.snapshot);
}
fn decodeEntry(binding: publication.Binding, raw: *const [entry_bytes]u8) !claims.Reference {
    const domain: @FieldType(claims.Revision, "domain") = switch (raw[32]) {
        0 => .mutation,
        1 => .retained,
        else => return error.InvalidIndexSourceMap,
    };
    const revision = std.mem.readInt(u64, raw[33..41], .little);
    if (revision == 0 or std.mem.allEqual(u8, raw[0..32], 0) or std.mem.allEqual(u8, raw[73..105], 0)) return error.InvalidIndexSourceMap;
    return .{ .binding = binding, .document = raw[0..32].*, .revision = .{ .domain = domain, .value = revision }, .root = raw[41..73].*, .snapshot = raw[73..105].* };
}
pub const maximum_page_bytes = 38 + page_capacity * entry_bytes;
fn encodePage(binding: publication.Binding, entries: []const claims.Reference, number: u32, scratch: *[maximum_page_bytes]u8) ![]const u8 {
    if (entries.len == 0 or entries.len > page_capacity) return error.InvalidIndexSourceMap;
    const page = scratch[0 .. 38 + entries.len * entry_bytes];
    @memcpy(page[0..4], "ISG1");
    std.mem.writeInt(u16, page[4..6], @intCast(entries.len), .little);
    for (entries, 0..) |ref, n| {
        if (!std.meta.eql(ref.binding, binding)) return error.IndexPublicationFenceMismatch;
        const raw = page[6 + n * entry_bytes ..][0..entry_bytes];
        encodeEntry(raw, ref);
        _ = try decodeEntry(binding, raw);
    }
    @memcpy(page[page.len - 32 ..], &boundDigest(&pageKey(binding, number), page[0 .. page.len - 32]));
    return page;
}
/// Holds a bounded hash directory and borrows immutable references through
/// stage/deinit. Encoding uses one page of scratch instead of retaining a
/// second complete representation of every source reference.
fn pageCount(documents: u32) u32 {
    return (documents - 1) / page_capacity + 1;
}
const Manifest = struct {
    identity: Digest,
    documents: u32,
    pages: u32,
    hashes: []const u8 = &.{},
    tree: ?Digest = null,
};
fn readManifest(reader: anytype, binding: publication.Binding, expected: Digest) !Manifest {
    const raw = try reader.get(&rootKey(binding));
    if (raw.len < 104 or raw.len > 74 + (legacy_maximum_documents / page_capacity) * 32 or
        !std.mem.eql(u8, raw[raw.len - 32 ..], &expected) or
        !std.mem.eql(u8, &expected, &boundDigest(&rootKey(binding), raw[0 .. raw.len - 32]))) return error.InvalidIndexSourceMap;
    const documents = std.mem.readInt(u32, raw[36..40], .little);
    if (documents == 0) return error.InvalidIndexSourceMap;
    const pages = pageCount(documents);
    if (std.mem.eql(u8, raw[0..4], "ISM2")) {
        if (raw.len != 104 or documents <= legacy_maximum_documents) return error.InvalidIndexSourceMap;
        _ = try directory.levels(pages);
        return .{ .identity = raw[4..36].*, .documents = documents, .pages = pages, .tree = raw[40..72].* };
    }
    if (!std.mem.eql(u8, raw[0..4], "ISM1") or documents > legacy_maximum_documents or
        pages != std.mem.readInt(u16, raw[40..42], .little) or raw.len != 74 + @as(usize, pages) * 32) return error.InvalidIndexSourceMap;
    return .{ .identity = raw[4..36].*, .documents = documents, .pages = pages, .hashes = raw[42 .. raw.len - 32] };
}
/// Owned, allocation-free authenticated manifest coordinates. The identity is
/// a physical segment digest for legacy maps, a typed layout/range digest for
/// source-family leaves; only the publication's role chooses its meaning.
pub fn summary(reader: anytype, binding: publication.Binding, expected: Digest) !struct { identity: Digest, documents: u32 } {
    const manifest = try readManifest(reader, binding, expected);
    return .{ .identity = manifest.identity, .documents = manifest.documents };
}
/// Streaming preparation/staging shares one leaf page and four directory
/// buckets. Writer calls consume bytes synchronously; null performs preparation.
fn streamDirectory(binding: publication.Binding, refs: []const claims.Reference, writer: anytype) !Digest {
    var tree = try directory.Builder.init(binding, pageCount(@intCast(refs.len)));
    var scratch: [maximum_page_bytes]u8 = undefined;
    var first: usize = 0;
    var number: u32 = 0;
    while (first < refs.len) : (number += 1) {
        const end = @min(refs.len, first + page_capacity);
        const page = try encodePage(binding, refs[first..end], number, &scratch);
        if (comptime @TypeOf(writer) != @TypeOf(null)) try writer.put(&pageKey(binding, number), page);
        try tree.append(digest(page), writer);
        first = end;
    }
    return tree.finish(writer);
}
pub const Plan = struct {
    binding: publication.Binding,
    manifest: []u8,
    references: []const claims.Reference,
    pub fn deinit(self: Plan, a: A) void {
        a.free(self.manifest);
    }
    pub fn root(self: Plan) Digest {
        return self.manifest[self.manifest.len - 32 ..][0..32].*;
    }
    pub fn pageCount(self: Plan) usize {
        return (self.references.len + page_capacity - 1) / page_capacity;
    }
    /// Recheck borrowed coordinates against the prepared root. A changed
    /// input cannot silently publish a map different from its receipt.
    pub fn pageInto(self: Plan, number: u32, scratch: *[maximum_page_bytes]u8) ![]const u8 {
        if (number >= self.pageCount()) return error.InvalidIndexSourceMap;
        const first = @as(usize, number) * page_capacity;
        const page = try encodePage(self.binding, self.references[first..@min(self.references.len, first + page_capacity)], number, scratch);
        if (self.references.len > legacy_maximum_documents) {
            // Cold inspection helper. Publication streams once through stage;
            // it does not invoke this full-root validation per leaf.
            const tree = try streamDirectory(self.binding, self.references, null);
            if (!std.mem.eql(u8, &tree, self.manifest[40..72])) return error.IndexPublicationConflict;
        } else if (!std.mem.eql(u8, &digest(page), self.manifest[42 + @as(usize, number) * 32 ..][0..32])) return error.IndexPublicationConflict;
        return page;
    }
    /// Entries are in segment-local document order, including deleted entries.
    /// The segment builder must preserve that mapping, never sort this array
    /// independently from its document IDs.
    pub fn prepare(a: A, binding: publication.Binding, segment_digest: Digest, refs: []const claims.Reference) !Plan {
        if (refs.len == 0 or refs.len > maximum_documents) return error.InvalidIndexSourceMap;
        if (refs.len > legacy_maximum_documents) {
            const tree = try streamDirectory(binding, refs, null);
            const manifest = try a.alloc(u8, 104);
            @memcpy(manifest[0..4], "ISM2");
            @memcpy(manifest[4..36], &segment_digest);
            std.mem.writeInt(u32, manifest[36..40], @intCast(refs.len), .little);
            @memcpy(manifest[40..72], &tree);
            @memcpy(manifest[72..104], &boundDigest(&rootKey(binding), manifest[0..72]));
            return .{ .binding = binding, .manifest = manifest, .references = refs };
        }
        const count = std.math.divCeil(usize, refs.len, page_capacity) catch unreachable;
        const manifest = try a.alloc(u8, 74 + count * 32);
        errdefer a.free(manifest);
        @memcpy(manifest[0..4], "ISM1");
        @memcpy(manifest[4..36], &segment_digest);
        std.mem.writeInt(u32, manifest[36..40], @intCast(refs.len), .little);
        std.mem.writeInt(u16, manifest[40..42], @intCast(count), .little);
        var scratch: [maximum_page_bytes]u8 = undefined;
        for (0..count) |i| {
            const entries = refs[i * page_capacity .. @min(refs.len, (i + 1) * page_capacity)];
            const page = try encodePage(binding, entries, @intCast(i), &scratch);
            @memcpy(manifest[42 + i * 32 ..][0..32], &digest(page));
        }
        @memcpy(manifest[manifest.len - 32 ..], &boundDigest(&rootKey(binding), manifest[0 .. manifest.len - 32]));
        return .{ .binding = binding, .manifest = manifest, .references = refs };
    }
    /// Same transaction as the active segment and its publication receipt.
    /// Writers copy a page before returning, like every metadata put here.
    /// Every failure poisons the enclosing transaction, including a final root
    /// mismatch after streaming pages. Catching it cannot publish partial data.
    pub fn stage(self: Plan, writer: anytype) !void {
        errdefer writer.poison();
        if (self.references.len > legacy_maximum_documents) {
            const tree = try streamDirectory(self.binding, self.references, writer);
            if (!std.mem.eql(u8, &tree, self.manifest[40..72])) return error.IndexPublicationConflict;
            try writer.put(&rootKey(self.binding), self.manifest);
            return;
        }
        var scratch: [maximum_page_bytes]u8 = undefined;
        for (0..self.pageCount()) |i| {
            const page = try self.pageInto(@intCast(i), &scratch);
            try writer.put(&pageKey(self.binding, @intCast(i)), page);
        }
        try writer.put(&rootKey(self.binding), self.manifest);
    }
};

pub fn lookup(reader: anytype, binding: publication.Binding, expected_root: Digest, document: u32) !claims.Reference {
    const manifest = try readManifest(reader, binding, expected_root);
    const count = manifest.documents;
    if (document >= count) return error.InvalidIndexSourceMap;
    const page_number = document / page_capacity;
    // Copy all borrowed root evidence before the next lookup retires it.
    const expected_page = if (manifest.tree) |tree|
        try directory.lookup(reader, binding, manifest.pages, tree, page_number)
    else
        manifest.hashes[@as(usize, page_number) * 32 ..][0..32].*;
    const page = try reader.get(&pageKey(binding, page_number));
    const entries: usize = @min(page_capacity, count - page_number * page_capacity);
    if (page.len != 38 + entries * entry_bytes or !std.mem.eql(u8, page[0..4], "ISG1") or std.mem.readInt(u16, page[4..6], .little) != entries or
        !std.mem.eql(u8, &expected_page, &digest(page)) or
        !std.mem.eql(u8, page[page.len - 32 ..], &boundDigest(&pageKey(binding, page_number), page[0 .. page.len - 32]))) return error.InvalidIndexSourceMap;
    return decodeEntry(binding, page[6 + @as(usize, document % page_capacity) * entry_bytes ..][0..entry_bytes]);
}

/// One retained source-map root. Window lookup validates each page once per
/// cohort instead of reopening and hashing its manifest for every document.
pub const ReadView = struct {
    binding: publication.Binding,
    documents: u32,
    pages: []Digest,
    directory_root: ?Digest = null,
    pub fn open(a: A, reader: anytype, binding: publication.Binding, expected_root: Digest) !ReadView {
        const manifest = try readManifest(reader, binding, expected_root);
        return .{ .binding = binding, .documents = manifest.documents, .directory_root = manifest.tree, .pages = if (manifest.tree != null) &.{} else try a.dupe(Digest, std.mem.bytesAsSlice(Digest, manifest.hashes)) };
    }
    pub fn deinit(self: ReadView, a: A) void {
        a.free(self.pages);
    }
    pub fn readWindow(self: ReadView, reader: anytype, first: u32, refs: []claims.Reference) !void {
        return self.readWindowInto(reader, first, refs);
    }
    /// A held publication already owns the binding once; caches need only the
    /// compact document/revision/root coordinates, with identical validation.
    pub fn readCompactWindow(self: ReadView, reader: anytype, first: u32, refs: []claims.SnapshotEntry) !void {
        return self.readWindowInto(reader, first, refs);
    }
    fn readWindowInto(self: ReadView, reader: anytype, first: u32, refs: anytype) !void {
        if (refs.len > page_capacity or first > self.documents or refs.len > self.documents - first) return error.InvalidIndexSourceMap;
        var used: usize = 0;
        while (used < refs.len) {
            const document = first + @as(u32, @intCast(used));
            const page_number = document / page_capacity;
            const expected_page = if (self.directory_root) |tree|
                try directory.lookup(reader, self.binding, pageCount(self.documents), tree, page_number)
            else
                self.pages[page_number];
            const page = try reader.get(&pageKey(self.binding, page_number));
            const entries: usize = @min(page_capacity, self.documents - page_number * page_capacity);
            if (page.len != 38 + entries * entry_bytes or !std.mem.eql(u8, page[0..4], "ISG1") or std.mem.readInt(u16, page[4..6], .little) != entries or
                !std.mem.eql(u8, &expected_page, &digest(page)) or !std.mem.eql(u8, page[page.len - 32 ..], &boundDigest(&pageKey(self.binding, page_number), page[0 .. page.len - 32]))) return error.InvalidIndexSourceMap;
            const within = document % page_capacity;
            const take = @min(refs.len - used, entries - within);
            for (refs[used..][0..take], 0..) |*ref, i| {
                const decoded = try decodeEntry(self.binding, page[6 + (@as(usize, within) + i) * entry_bytes ..][0..entry_bytes]);
                ref.* = if (comptime @TypeOf(ref.*) == claims.Reference) decoded else claims.SnapshotEntry.fromReference(decoded);
            }
            used += take;
        }
    }
};

pub const ReclaimProgress = struct { next_page: u32, done: bool };
const gc_prefix = "index_source_map_gc_v1:";
fn gcKey(binding: publication.Binding) [gc_prefix.len + 64]u8 {
    var out: [gc_prefix.len + 64]u8 = undefined;
    @memcpy(out[0..gc_prefix.len], gc_prefix);
    @memcpy(out[gc_prefix.len..][0..32], &binding.owner);
    @memcpy(out[gc_prefix.len + 32 ..], &binding.publication);
    return out;
}
/// Caller proves durable retirement and reader drainage first. Progress and
/// page deletion commit together, so a retry resumes from the durable frontier
/// without trusting a caller cursor or rescanning already-reclaimed pages.
/// Terminal publication tombstones stay intact.
pub fn reclaimPages(writer: anytype, binding: publication.Binding, expected_root: Digest, limit: u32) !ReclaimProgress {
    errdefer writer.poison();
    if (limit == 0 or limit > 128) return error.InvalidIndexSourceMap;
    const manifest = readManifest(writer, binding, expected_root) catch |err| switch (err) {
        error.NotFound => return .{ .next_page = 0, .done = true },
        else => return err,
    };
    const leaves = manifest.pages;
    const pages = leaves + if (manifest.tree != null) try directory.nodeCount(leaves) else @as(u32, 0);
    const stored_progress = writer.get(&gcKey(binding)) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var after_page: u32 = 0;
    if (stored_progress) |raw| {
        if (raw.len != 72 or !std.mem.eql(u8, raw[0..4], "ISQ1") or !std.mem.eql(u8, raw[4..36], &expected_root) or
            !std.mem.eql(u8, raw[40..72], &boundDigest(&gcKey(binding), raw[0..40]))) return error.InvalidIndexSourceMap;
        after_page = std.mem.readInt(u32, raw[36..40], .little);
        if (after_page >= pages) return error.InvalidIndexSourceMap;
    }
    const end: u32 = @min(@as(u32, pages), after_page + limit);
    for (after_page..end) |page| {
        if (page < leaves) {
            writer.delete(&pageKey(binding, @intCast(page))) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        } else try directory.deleteAt(writer, binding, leaves, @intCast(page - leaves));
    }
    if (end == pages) {
        try writer.delete(&rootKey(binding));
        writer.delete(&gcKey(binding)) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    } else {
        var progress: [72]u8 = undefined;
        @memcpy(progress[0..4], "ISQ1");
        @memcpy(progress[4..36], &expected_root);
        std.mem.writeInt(u32, progress[36..40], end, .little);
        @memcpy(progress[40..72], &boundDigest(&gcKey(binding), progress[0..40]));
        try writer.put(&gcKey(binding), &progress);
    }
    return .{ .next_page = end, .done = end == pages };
}
