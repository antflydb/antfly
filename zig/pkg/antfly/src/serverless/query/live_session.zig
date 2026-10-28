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

//! Pinned published facts plus the canonical bounded WAL overlay. Heap-stable
//! ownership keeps the facts reader's session and budget pointers valid.
const std = @import("std");
const A = std.mem.Allocator;
const runtime = @import("runtime.zig");
const facts = @import("document_facts_reader.zig");
const tree = @import("../graph_segment/page_tree.zig");
const WalStore = @import("../wal/store.zig").WalStore;
const View = @import("live_view.zig").View;
const Overlay = @import("live_overlay.zig").Overlay;
const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub const Session = struct {
    alloc: A,
    published: ?runtime.QuerySession = null,
    reader: ?*facts.Reader = null,
    remaining_read_bytes: u64,
    view: View = undefined,
    cancellation: CancellationToken,
    io: ?std.Io,

    pub const Options = struct {
        view: View.Options = .{},
        version: ?u64 = null,
        max_read_bytes: u64 = 16 * 1024 * 1024,
        cancellation: CancellationToken = .none,
        io: ?std.Io = null,
    };
    pub fn open(alloc: A, query: *runtime.QueryRuntime, wal: *WalStore, namespace: []const u8, options: Options) !*Session {
        const self = try alloc.create(Session);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .remaining_read_bytes = options.max_read_bytes, .cancellation = options.cancellation, .io = options.io };
        errdefer if (self.published) |*published| published.deinit();
        self.published = if (options.version) |version|
            if (version == 0) null else try query.openVersionSessionWithCancellation(namespace, version, options.cancellation)
        else
            query.openHeadSessionWithCancellation(namespace, options.cancellation) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
        if (self.published) |*published| {
            self.reader = (try facts.Reader.create(alloc, published, &self.remaining_read_bytes)) orelse return error.HostedDocumentFactsRequired;
        }
        errdefer if (self.reader) |reader| reader.destroy();
        var view_options = options.view;
        view_options.body_resolver = .{ .ptr = self, .resolve = resolveCheckpoint };
        self.view = try View.open(alloc, wal, namespace, if (self.published) |published| published.manifest.wal_end_lsn else 0, view_options);
        return self;
    }
    fn resolveCheckpoint(raw: *anyopaque, alloc: A, bytes: []const u8) ![]u8 {
        const self: *Session = @ptrCast(@alignCast(raw));
        const reader = self.reader orelse return error.HostedDocumentFactsRequired;
        try self.cancellation.check();
        return @import("../build/document_facts.zig").resolveCheckpointBodyAlloc(alloc, &reader.pages, bytes);
    }
    pub fn deinit(self: *Session) void {
        self.view.deinit();
        if (self.reader) |reader| reader.destroy();
        if (self.published) |*published| published.deinit();
        self.alloc.destroy(self);
    }
    pub fn lookupAlloc(self: *Session, id: []const u8) !?[]u8 {
        const Base = struct {
            session: *Session,
            pub fn lookupAlloc(base: @This(), _: A, key: []const u8) !?[]u8 {
                const reader = base.session.reader orelse return null;
                return try reader.lookupBodyAlloc(key);
            }
        };
        return self.view.lookupAlloc(id, Base{ .session = self });
    }
    pub fn list(self: *Session, after: []const u8, limit: usize, max_output_bytes: usize, predicate: anytype, options: Overlay.ListOptions) !Overlay.Page {
        if (self.view.point_id != null) return error.PointReadIdentityMismatch;
        var base = try Cursor.init(self, after);
        defer base.deinit();
        return self.view.overlay.list(.{ .ptr = &base, .next = Cursor.next }, after, limit, max_output_bytes, predicate, options);
    }
    /// Text candidates are merged before body fetches and limiting. The
    /// existing native text scorer uses document-local term frequencies, so
    /// these scores remain comparable across the published and recent sets.
    /// Corpus-dependent ranking, vector/hybrid queries and aggregations need
    /// their own visibility contracts and are deliberately rejected here.
    pub fn searchTextAlloc(self: *Session, req: @import("request.zig").QueryRequest, max_output_bytes: usize) ![]@import("request.zig").SearchHit {
        if (self.view.point_id != null) return error.PointReadIdentityMismatch;
        const All = struct {
            pub fn matches(_: @This(), _: Overlay.Document) !bool {
                return true;
            }
        };
        return self.searchTextCore(req, max_output_bytes, All{}, null, null);
    }
    /// Filter current bodies before offset/limit and refill from an admitted
    /// ranked prefix. Exhausting that prefix is an explicit budget failure.
    pub fn searchTextFilteredAlloc(self: *Session, req: @import("request.zig").QueryRequest, max_output_bytes: usize, predicate: anytype, max_candidates: usize) ![]@import("request.zig").SearchHit {
        if (max_candidates == 0 or max_candidates > 4096) return error.QueryCandidateBudgetExceeded;
        return self.searchTextCore(req, max_output_bytes, predicate, max_candidates, null);
    }
    pub const RankPosition = struct { score: u32, id: []const u8 };
    pub fn searchTextPageAlloc(self: *Session, req: @import("request.zig").QueryRequest, max_output_bytes: usize, predicate: anytype, after: ?RankPosition) ![]@import("request.zig").SearchHit {
        return self.searchTextCore(req, max_output_bytes, predicate, 4096, after);
    }
    fn searchTextCore(self: *Session, req: @import("request.zig").QueryRequest, max_output_bytes: usize, predicate: anytype, candidate_budget: ?usize, after: ?RankPosition) ![]@import("request.zig").SearchHit {
        if (self.view.point_id != null) return error.PointReadIdentityMismatch;
        if (req.mode != .text or req.count_only or req.filter_text != null or req.exclusion_text != null or
            req.filter_prefix != null or req.fields != null or req.semantic_search != null or req.indexes != null or req.vector != null or req.sparse != null)
            return error.FreshQueryCapabilityUnsupported;
        if (req.limit == 0 or req.limit > 100 or req.offset > 4096) return error.QueryCandidateBudgetExceeded;
        const indexed = @import("indexed_reader.zig");
        const builder = @import("../build/builder.zig");
        const projections = @import("materializer.zig");
        const spec_owner = @import("antfly_local_sources").api_full_text_indexes;
        const specs = try builder.resolvePublishedTextIndexSpecsAlloc(self.alloc, .{
            .indexes_json = if (self.published) |published| published.manifest.stats.indexes_json else &.{},
        }, &.{});
        defer spec_owner.freeFullTextIndexSpecs(self.alloc, specs);
        const index_name = if (self.published) |*published|
            published.manifest.artifacts[try indexed.resolveTextArtifactIndex(published, req)].name
        else
            req.full_text_index orelse @import("../search_sources.zig").default_full_text_index_name;
        var selected_spec: ?spec_owner.FullTextIndexSpec = null;
        for (specs) |spec| if (std.mem.eql(u8, spec.name, index_name)) {
            selected_spec = spec;
            break;
        };
        const spec = selected_spec orelse return error.IndexNotFound;
        var docs: std.ArrayListUnmanaged(projections.Document) = .empty;
        defer docs.deinit(self.alloc);
        for (self.view.overlay.ordered) |id| switch (try self.view.overlay.lookup(id)) {
            .present => |body| try docs.append(self.alloc, .{ .doc_id = @constCast(id), .body = @constCast(body), .last_lsn = 0, .last_timestamp_ns = 0 }),
            else => {},
        };
        var segment = try builder.allocTextSegmentAlloc(self.alloc, docs.items, spec, if (self.io) |io| .{ .io = io, .cooperative = self.cancellation } else null);
        defer segment.deinit(self.alloc);
        const needed = req.offset + req.limit;
        const recent = try indexed.searchTextSegmentDocIdsWithCancellationAlloc(self.alloc, segment, req.text, req.operator, 0, candidate_budget orelse needed, req.min_score, self.cancellation);
        defer indexed.freeScoredDocs(self.alloc, recent);
        var candidates: std.ArrayListUnmanaged(indexed.ScoredDoc) = .empty;
        defer {
            for (candidates.items) |hit| self.alloc.free(hit.doc_id);
            candidates.deinit(self.alloc);
        }
        var base_exhausted = true;
        if (self.published) |*published| {
            var base_req = req;
            base_req.offset = 0;
            // At most one published candidate per changed identity can be
            // invalidated. Asking for this extra prefix guarantees page refill.
            base_req.limit = candidate_budget orelse (needed + self.view.overlay.by_id.count());
            const base = try indexed.searchTextAlloc(self.alloc, published, base_req);
            defer indexed.freeScoredDocs(self.alloc, base);
            base_exhausted = base.len < base_req.limit;
            for (base) |hit| if (!self.view.overlay.by_id.contains(hit.doc_id)) {
                const id = try self.alloc.dupe(u8, hit.doc_id);
                errdefer self.alloc.free(id);
                try candidates.append(self.alloc, .{ .doc_id = id, .score = hit.score });
            };
        }
        for (recent) |hit| {
            const id = try self.alloc.dupe(u8, hit.doc_id);
            errdefer self.alloc.free(id);
            try candidates.append(self.alloc, .{ .doc_id = id, .score = hit.score });
        }
        std.mem.sort(indexed.ScoredDoc, candidates.items, {}, struct {
            fn less(_: void, l: indexed.ScoredDoc, r: indexed.ScoredDoc) bool {
                return if (l.score != r.score) l.score > r.score else std.mem.order(u8, l.doc_id, r.doc_id) == .lt;
            }
        }.less);
        var hits: std.ArrayListUnmanaged(@import("request.zig").SearchHit) = .empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(self.alloc);
            hits.deinit(self.alloc);
        }
        var bytes: usize = 0;
        var matched: usize = 0;
        for (candidates.items) |candidate| {
            try self.cancellation.check();
            if (after) |position| if (candidate.score > position.score or (candidate.score == position.score and std.mem.order(u8, candidate.doc_id, position.id) != .gt)) continue;
            const body = (try self.lookupAlloc(candidate.doc_id)) orelse return error.DocumentBodyNotFound;
            defer self.alloc.free(body);
            if (!try predicate.matches(Overlay.Document{ .doc_id = candidate.doc_id, .body = body })) continue;
            matched += 1;
            if (matched <= req.offset) continue;
            bytes = std.math.add(usize, bytes, body.len + candidate.doc_id.len) catch return error.QueryCandidateBudgetExceeded;
            if (bytes > max_output_bytes) return error.QueryCandidateBudgetExceeded;
            const id = try self.alloc.dupe(u8, candidate.doc_id);
            errdefer self.alloc.free(id);
            const owned_body = try self.alloc.dupe(u8, body);
            errdefer self.alloc.free(owned_body);
            try hits.append(self.alloc, .{ .doc_id = id, .body = owned_body, .score = candidate.score });
            if (hits.items.len == req.limit) break;
        }
        if (candidate_budget != null and hits.items.len < req.limit and (!base_exhausted or recent.len == candidate_budget.?)) return error.QueryCandidateBudgetExceeded;
        return hits.toOwnedSlice(self.alloc);
    }

    pub fn countDocuments(self: *Session) !u64 {
        if (self.view.point_id != null) return error.PointReadIdentityMismatch;
        var count: u64 = if (self.published) |published| published.manifest.stats.document_count else 0;
        for (self.view.overlay.ordered) |id| {
            try self.cancellation.check();
            const existed = if (self.reader) |reader| (try reader.lookup(id)) != null else false;
            const mutation = self.view.overlay.by_id.get(id) orelse unreachable;
            if (mutation.kind == .delete) {
                if (existed) count -= 1;
            } else if (!existed) count = try std.math.add(u64, count, 1);
        }
        return count;
    }

    pub fn generation(self: *const Session) u64 {
        return if (self.published) |published| published.manifest.version else 0;
    }

    const Cursor = struct {
        session: *Session,
        cursor: ?tree.Cursor = null,
        after: []const u8,
        body: ?[]u8 = null,
        fn init(session: *Session, after: []const u8) !Cursor {
            return .{ .session = session, .after = after, .cursor = if (session.reader) |reader| try tree.Cursor.init(session.alloc, reader.cache.store(), reader.root.page, after, null) else null };
        }
        fn deinit(self: *Cursor) void {
            if (self.body) |body| self.session.alloc.free(body);
            if (self.cursor) |*cursor| cursor.deinit();
        }
        fn next(raw: *anyopaque) !?Overlay.Document {
            const self: *Cursor = @ptrCast(@alignCast(raw));
            if (self.body) |body| self.session.alloc.free(body);
            self.body = null;
            const cursor = if (self.cursor) |*cursor| cursor else return null;
            const reader = self.session.reader.?;
            while (try cursor.next()) |entry| {
                if (std.mem.order(u8, entry.key, self.after) != .gt) continue;
                const fact = try facts.decodeRecord(entry);
                if (fact.last_lsn > reader.root.wal_end_lsn) return error.InvalidDocumentFact;
                self.body = try reader.readRecordBodyAlloc(entry);
                return .{ .doc_id = entry.key, .body = self.body.? };
            }
            return null;
        }
    };
};
