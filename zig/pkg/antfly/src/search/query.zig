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

//! Query filters for bitmap-based document filtering.
//!
//! Filters produce RoaringBitmaps of matching doc IDs without scoring.
//! This separates filtering from scoring — the key improvement over bleve
//! which intermixes the two.
//!
//! Filters compose via bitmap AND/OR/ANDNOT (SIMD-accelerated):
//!   - TermFilter: exact term match from inverted index
//!   - BoolFilter: AND/OR/NOT composition of sub-filters
//!   - PrefixFilter: prefix match via FST range iteration
//!   - MatchAll: matches all documents in the segment

const std = @import("std");
const Allocator = std.mem.Allocator;
const roaring = @import("../encoding/roaring.zig");
const inverted = @import("../section/inverted.zig");
const segment_mod = @import("../segment.zig");
const index_mod = @import("../index.zig");
const vellum = @import("antfly_vellum");
const levenshtein = @import("levenshtein.zig");
const regex_mod = @import("regex.zig");
const typed_dv = @import("../section/typed_doc_values.zig");
const geo = @import("geo.zig");
const synonyms_mod = @import("../section/synonyms.zig");

pub const FilterError = error{
    OutOfMemory,
    InvalidData,
    InvalidMagic,
    UnsupportedVersion,
    InvalidFST,
    SnappyError,
    InvalidFormat,
    InvalidAddress,
    InvalidChunk,
    CorruptInput,
    InvalidSegment,
};

/// A filter that produces a bitmap of matching document IDs.
pub const Filter = union(enum) {
    term: TermFilter,
    bool_filter: BoolFilter,
    prefix: PrefixFilter,
    phrase: PhraseFilter,
    fuzzy: FuzzyFilter,
    range: RangeFilter,
    geo_distance: GeoDistanceFilter,
    geo_bbox: GeoBBoxFilter,
    wildcard: WildcardFilter,
    doc_id: DocIdFilter,
    doc_num: DocNumFilter,
    bool_field: BoolFieldFilter,
    multi_phrase: MultiPhraseFilter,
    date_range: DateRangeFilter,
    regexp: RegexpFilter,
    term_range: TermRangeFilter,
    ip_range: IPRangeFilter,
    geo_shape: GeoShapeFilter,
    match_none: void,
    match_all: void,

    /// Execute this filter against a single segment, returning matching doc IDs.
    pub fn execute(self: Filter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        return switch (self) {
            .term => |f| f.execute(alloc, seg),
            .bool_filter => |f| f.execute(alloc, seg),
            .prefix => |f| f.execute(alloc, seg),
            .phrase => |f| f.execute(alloc, seg),
            .fuzzy => |f| f.execute(alloc, seg),
            .range => |f| f.execute(alloc, seg),
            .geo_distance => |f| f.execute(alloc, seg),
            .geo_bbox => |f| f.execute(alloc, seg),
            .wildcard => |f| f.execute(alloc, seg),
            .doc_id => |f| f.execute(alloc, seg),
            .doc_num => |f| f.executeWithOffset(alloc, seg, 0),
            .bool_field => |f| f.execute(alloc, seg),
            .multi_phrase => |f| f.execute(alloc, seg),
            .date_range => |f| f.execute(alloc, seg),
            .regexp => |f| f.execute(alloc, seg),
            .term_range => |f| f.execute(alloc, seg),
            .ip_range => |f| f.execute(alloc, seg),
            .geo_shape => |f| f.execute(alloc, seg),
            .match_none => matchNone(alloc),
            .match_all => matchAll(alloc, seg),
        };
    }

    pub fn executeWithOffset(self: Filter, alloc: Allocator, seg: *const index_mod.SegmentEntry, doc_offset: u32) FilterError!roaring.RoaringBitmap {
        return switch (self) {
            .bool_filter => |f| f.executeWithOffset(alloc, seg, doc_offset),
            .doc_num => |f| f.executeWithOffset(alloc, seg, doc_offset),
            else => self.execute(alloc, seg),
        };
    }

    fn matchNone(alloc: Allocator) roaring.RoaringBitmap {
        return roaring.RoaringBitmap.init(alloc);
    }

    fn matchAll(alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        var bm = roaring.RoaringBitmap.init(alloc);
        errdefer bm.deinit();
        for (0..seg.reader.doc_count) |i| {
            try bm.add(@intCast(i));
        }
        return bm;
    }
};

/// Exact term match: extracts the posting bitmap for a single term.
/// If the segment has a synonym section for the field, synonyms are automatically expanded.
pub const TermFilter = struct {
    field: []const u8,
    term: []const u8,
    boost: f32 = 1.0,

    pub fn execute(self: TermFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        // Look up the primary term
        if (inv_reader.lookup(self.term)) |lookup_result| {
            switch (lookup_result) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    defer bm.deinit();
                    try result.orWith(&bm);
                },
                .one_hit => |h| try result.add(h.doc_num),
            }
        }

        // Synonym expansion: check if field has a synonym section
        if (seg.reader.getSection(self.field, .synonym)) |syn_data| {
            var syn_reader = synonyms_mod.SynonymReader.init(alloc, syn_data) catch return result;
            defer syn_reader.deinit();

            const expanded = syn_reader.expandTerm(alloc, self.term) catch return result;
            if (expanded) |terms| {
                defer {
                    for (terms) |t| alloc.free(t);
                    alloc.free(terms);
                }
                for (terms) |syn_term| {
                    if (std.mem.eql(u8, syn_term, self.term)) continue; // skip primary
                    if (inv_reader.lookup(syn_term)) |syn_result| {
                        switch (syn_result) {
                            .postings => |p| {
                                var bm = try p.docBitmap(alloc);
                                defer bm.deinit();
                                try result.orWith(&bm);
                            },
                            .one_hit => |h| try result.add(h.doc_num),
                        }
                    }
                }
            }
        }

        return result;
    }
};

/// Boolean composition of sub-filters.
///
/// Semantics:
///   - must: AND — all must match (intersect bitmaps)
///   - should: OR — at least min_should_match must match (union bitmaps)
///   - must_not: NOT — none may match (remove from result)
///
/// If both must and should are empty, matches nothing.
/// If must is empty but should is non-empty, should results are the base.
pub const BoolFilter = struct {
    must: []const Filter,
    should: []const Filter,
    must_not: []const Filter,
    min_should_match: u32 = 1,
    boost: f32 = 1.0,

    pub fn execute(self: BoolFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        return self.executeWithOffset(alloc, seg, 0);
    }

    pub fn executeWithOffset(self: BoolFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry, doc_offset: u32) FilterError!roaring.RoaringBitmap {
        var result: ?roaring.RoaringBitmap = null;
        errdefer if (result) |*r| r.deinit();

        // Process must clauses (AND)
        for (self.must) |clause| {
            var clause_bm = try clause.executeWithOffset(alloc, seg, doc_offset);
            if (result) |*r| {
                r.andWith(&clause_bm);
                clause_bm.deinit();
            } else {
                result = clause_bm;
            }
        }

        // Process should clauses (OR)
        if (self.should.len > 0) {
            const required_should: u32 = if (self.min_should_match == 0 and result == null) 1 else self.min_should_match;
            if (required_should > 0) {
                var should_bm = try executeShouldClauses(alloc, self.should, required_should, seg, doc_offset);
                errdefer should_bm.deinit();
                if (result) |*r| {
                    r.andWith(&should_bm);
                    should_bm.deinit();
                } else {
                    result = should_bm;
                }
            }
        }

        // If no must or should, return empty
        if (result == null) {
            return roaring.RoaringBitmap.init(alloc);
        }

        // Process must_not clauses (ANDNOT)
        for (self.must_not) |clause| {
            var clause_bm = try clause.executeWithOffset(alloc, seg, doc_offset);
            defer clause_bm.deinit();
            result.?.andNotWith(&clause_bm);
        }

        return result.?;
    }
};

fn executeShouldClauses(
    alloc: Allocator,
    should: []const Filter,
    min_should_match: u32,
    seg: *const index_mod.SegmentEntry,
    doc_offset: u32,
) FilterError!roaring.RoaringBitmap {
    if (min_should_match > should.len) {
        return roaring.RoaringBitmap.init(alloc);
    }

    if (min_should_match <= 1) {
        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();
        for (should) |clause| {
            var clause_bm = try clause.executeWithOffset(alloc, seg, doc_offset);
            defer clause_bm.deinit();
            try result.orWith(&clause_bm);
        }
        return result;
    }

    var counts = std.AutoHashMapUnmanaged(u32, u32).empty;
    defer counts.deinit(alloc);

    for (should) |clause| {
        var clause_bm = try clause.executeWithOffset(alloc, seg, doc_offset);
        defer clause_bm.deinit();

        var iter = clause_bm.iterator();
        while (iter.next()) |doc_id| {
            const entry = try counts.getOrPut(alloc, doc_id);
            if (!entry.found_existing) entry.value_ptr.* = 0;
            entry.value_ptr.* += 1;
        }
    }

    var result = roaring.RoaringBitmap.init(alloc);
    errdefer result.deinit();
    var iter = counts.iterator();
    while (iter.next()) |entry| {
        if (entry.value_ptr.* >= min_should_match) {
            try result.add(entry.key_ptr.*);
        }
    }
    return result;
}

/// Prefix match: finds all terms starting with a prefix via FST range iteration.
/// Returns the union of posting bitmaps for all matching terms.
pub const PrefixFilter = struct {
    field: []const u8,
    prefix: []const u8,
    boost: f32 = 1.0,

    pub fn execute(self: PrefixFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Use the term iterator to find all terms with the given prefix
        var term_iter = try inv_reader.termIterator();
        defer term_iter.deinit();

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        while (try term_iter.next()) |entry| {
            if (entry.term.len < self.prefix.len) continue;
            if (!std.mem.startsWith(u8, entry.term, self.prefix)) {
                // FST terms are sorted; if we've passed the prefix range, stop
                if (std.mem.order(u8, entry.term[0..self.prefix.len], self.prefix) == .gt) break;
                continue;
            }
            // Term matches prefix — union its postings
            switch (entry.result) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    defer bm.deinit();
                    try result.orWith(&bm);
                },
                .one_hit => |h| {
                    try result.add(h.doc_num);
                },
            }
        }

        return result;
    }
};

/// Phrase match: finds documents where all terms appear at adjacent positions.
/// Optionally allows `slop` positions of gap between terms.
pub const PhraseFilter = struct {
    field: []const u8,
    terms: []const []const u8,
    slop: u32 = 0,
    max_edits: u8 = 0,
    auto_fuzzy: bool = false,
    boost: f32 = 1.0,

    pub fn execute(self: PhraseFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        if (self.terms.len == 0) return roaring.RoaringBitmap.init(alloc);
        if (self.terms.len == 1) {
            // Single-term phrase is just a term filter
            if (!self.auto_fuzzy and self.max_edits == 0) {
                const tf = TermFilter{ .field = self.field, .term = self.terms[0] };
                return tf.execute(alloc, seg);
            }
        }

        if (self.auto_fuzzy or self.max_edits > 0) {
            var alternatives = try alloc.alloc([]const []const u8, self.terms.len);
            var initialized: usize = 0;
            defer {
                for (alternatives[0..initialized]) |position| alloc.free(position);
                alloc.free(alternatives);
            }
            for (self.terms, 0..) |term, i| {
                const one = try alloc.alloc([]const u8, 1);
                one[0] = term;
                alternatives[i] = one;
                initialized = i + 1;
            }
            const multi = MultiPhraseFilter{
                .field = self.field,
                .term_alternatives = alternatives,
                .slop = self.slop,
                .max_edits = self.max_edits,
                .auto_fuzzy = self.auto_fuzzy,
            };
            return multi.execute(alloc, seg);
        }

        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Step 1: Intersect posting bitmaps of all terms to get candidate docs
        var candidate_bm: ?roaring.RoaringBitmap = null;
        defer if (candidate_bm) |*bm| bm.deinit();

        // Collect lookup results for all terms
        var lookups = std.ArrayListUnmanaged(inverted.LookupResult).empty;
        defer lookups.deinit(alloc);

        for (self.terms) |term| {
            const lr = inv_reader.lookup(term) orelse {
                // Term not found — no phrase match possible
                return roaring.RoaringBitmap.init(alloc);
            };
            try lookups.append(alloc, lr);

            switch (lr) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    if (candidate_bm) |*cb| {
                        cb.andWith(&bm);
                        bm.deinit();
                    } else {
                        candidate_bm = bm;
                    }
                },
                .one_hit => |h| {
                    var bm = roaring.RoaringBitmap.init(alloc);
                    try bm.add(h.doc_num);
                    if (candidate_bm) |*cb| {
                        cb.andWith(&bm);
                        bm.deinit();
                    } else {
                        candidate_bm = bm;
                    }
                },
            }
        }

        const candidates = candidate_bm orelse return roaring.RoaringBitmap.init(alloc);

        // Step 2: For each candidate doc, load positions for each term and check adjacency
        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        // Build position maps: for each term, doc_id → positions
        var term_positions = try alloc.alloc(std.AutoHashMapUnmanaged(u32, []const u32), self.terms.len);
        defer {
            for (term_positions) |*tp| {
                var it = tp.valueIterator();
                while (it.next()) |v| alloc.free(v.*);
                tp.deinit(alloc);
            }
            alloc.free(term_positions);
        }

        for (lookups.items, 0..) |lr, i| {
            term_positions[i] = .empty;
            var post_iter = try lr.iterator(alloc);
            defer post_iter.deinit();
            while (try post_iter.next()) |hit| {
                if (candidates.contains(hit.doc_id)) {
                    const pos_copy = try alloc.dupe(u32, hit.positions);
                    try term_positions[i].put(alloc, hit.doc_id, pos_copy);
                }
            }
        }

        // Step 3: Check each candidate for phrase adjacency
        var cand_iter = candidates.iterator();
        while (cand_iter.next()) |doc_id| {
            if (self.checkPhraseMatch(term_positions, doc_id)) {
                try result.add(doc_id);
            }
        }

        return result;
    }

    fn checkPhraseMatch(self: PhraseFilter, term_positions: []const std.AutoHashMapUnmanaged(u32, []const u32), doc_id: u32) bool {
        // Get positions for first term
        const first_positions = term_positions[0].get(doc_id) orelse return false;

        // For each starting position in term[0], check if subsequent terms
        // appear at position+1, position+2, etc. (within slop)
        for (first_positions) |start_pos| {
            var matched = true;
            for (1..self.terms.len) |ti| {
                const positions = term_positions[ti].get(doc_id) orelse {
                    matched = false;
                    break;
                };
                const expected: u32 = start_pos + @as(u32, @intCast(ti));
                if (!positionWithinSlop(positions, expected, self.slop)) {
                    matched = false;
                    break;
                }
            }
            if (matched) return true;
        }
        return false;
    }
};

fn positionWithinSlop(positions: []const u32, expected: u32, slop: u32) bool {
    for (positions) |pos| {
        const diff = if (pos >= expected) pos - expected else expected - pos;
        if (diff <= slop) return true;
    }
    return false;
}

/// Fuzzy match: finds all terms within Levenshtein edit distance via FST automaton search.
pub const FuzzyFilter = struct {
    field: []const u8,
    term: []const u8,
    max_edits: u8 = 1,
    prefix_len: u8 = 0,
    boost: f32 = 1.0,

    pub fn execute(self: FuzzyFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Use Levenshtein automaton with FST.search for efficient traversal.
        // The automaton prunes non-matching FST branches early.
        var lev = levenshtein.LevenshteinAutomaton{ .term = self.term, .max_distance = self.max_edits, .alloc = alloc };
        defer lev.deinit();
        var term_iter = try inv_reader.fstSearchIterator(lev.automaton());
        defer term_iter.deinit();

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        while (try term_iter.next()) |entry| {
            if (!fuzzyPrefixMatches(self.term, entry.term, self.prefix_len)) continue;
            switch (entry.result) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    defer bm.deinit();
                    try result.orWith(&bm);
                },
                .one_hit => |h| {
                    try result.add(h.doc_num);
                },
            }
        }

        return result;
    }
};

/// Compute Levenshtein edit distance between two strings.
fn editDistance(a: []const u8, b: []const u8) u32 {
    if (a.len == 0) return @intCast(b.len);
    if (b.len == 0) return @intCast(a.len);
    if (a.len > 64 or b.len > 64) return @intCast(@max(a.len, b.len)); // bail on very long strings

    // Use two rows of the DP matrix
    var prev: [65]u32 = undefined;
    var curr: [65]u32 = undefined;
    for (0..b.len + 1) |j| prev[j] = @intCast(j);

    for (a, 0..) |ca, i| {
        curr[0] = @intCast(i + 1);
        for (b, 0..) |cb, j| {
            const cost: u32 = if (ca == cb) 0 else 1;
            curr[j + 1] = @min(@min(curr[j] + 1, prev[j + 1] + 1), prev[j] + cost);
        }
        prev = curr;
    }
    return prev[b.len];
}

/// Regex filter: matches terms against a regular expression using FST automaton search.
pub const RegexpFilter = struct {
    field: []const u8,
    pattern: []const u8,
    boost: f32 = 1.0,

    pub fn execute(self: RegexpFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Compile regex → automaton for efficient FST traversal
        var regex = regex_mod.compile(alloc, self.pattern) catch
            return roaring.RoaringBitmap.init(alloc);
        defer regex.deinit();

        var term_iter = try inv_reader.fstSearchIterator(regex.automaton());
        defer term_iter.deinit();

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        while (try term_iter.next()) |entry| {
            switch (entry.result) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    defer bm.deinit();
                    try result.orWith(&bm);
                },
                .one_hit => |h| {
                    try result.add(h.doc_num);
                },
            }
        }

        return result;
    }
};

/// Lexicographic term range filter: matches docs containing any term in [min, max].
/// Unlike RangeFilter (numeric on doc values), this operates on FST term strings.
pub const TermRangeFilter = struct {
    field: []const u8,
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
    boost: f32 = 1.0,

    pub fn execute(self: TermRangeFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Compute FST bounds: iterator uses [start, end) half-open interval
        const start = self.min;
        // For inclusive_max, we need to include max itself. FST iterator uses [start, end),
        // so append \xff to make end exclusive past max.
        var end_buf: [1024]u8 = undefined;
        const end: ?[]const u8 = if (self.max) |m| blk: {
            if (self.inclusive_max) {
                if (m.len + 1 > end_buf.len) break :blk null; // too long, unbounded
                @memcpy(end_buf[0..m.len], m);
                end_buf[m.len] = 0xff;
                break :blk end_buf[0 .. m.len + 1];
            } else {
                break :blk m;
            }
        } else null;

        var term_iter = try inv_reader.rangeTermIterator(start, end);
        defer term_iter.deinit();

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        while (try term_iter.next()) |entry| {
            // Handle exclusive min: skip the min term itself
            if (!self.inclusive_min) {
                if (self.min) |m| {
                    if (std.mem.eql(u8, entry.term, m)) continue;
                }
            }
            switch (entry.result) {
                .postings => |p| {
                    var bm = try p.docBitmap(alloc);
                    defer bm.deinit();
                    try result.orWith(&bm);
                },
                .one_hit => |h| try result.add(h.doc_num),
            }
        }

        return result;
    }
};

/// CIDR-based IP range filter. Matches docs with IP address terms in the given subnet.
/// IPs must be indexed as dotted-quad strings (e.g. "192.168.1.1").
pub const IPRangeFilter = struct {
    field: []const u8,
    cidr: []const u8,

    pub fn execute(self: IPRangeFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const parsed = parseCIDR(self.cidr);
        const exact_ip = if (parsed == null) parseIPv4(self.cidr) else null;
        if (parsed == null and exact_ip == null) return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        if (try seg.reader.invertedIndex(self.field)) |inv_reader| {
            var term_iter = try inv_reader.termIterator();
            defer term_iter.deinit();

            while (try term_iter.next()) |entry| {
                const ip = parseIPv4(entry.term) orelse continue;
                const matched = if (parsed) |cidr|
                    ipInRange(ip, cidr.network, cidr.prefix_len)
                else if (exact_ip) |wanted|
                    std.mem.eql(u8, wanted[0..], ip[0..])
                else
                    false;
                if (matched) {
                    switch (entry.result) {
                        .postings => |p| {
                            var bm = try p.docBitmap(alloc);
                            defer bm.deinit();
                            try result.orWith(&bm);
                        },
                        .one_hit => |h| try result.add(h.doc_num),
                    }
                }
            }
        }

        // Default text indexing may analyze string fields in a way that loses exact dotted-quad
        // terms. Fall back to stored JSON field inspection when the inverted path yields nothing.
        if (result.isEmpty()) {
            for (0..seg.reader.doc_count) |doc_id| {
                const stored = (try seg.reader.storedDocDecompressed(@intCast(doc_id))) orelse continue;
                defer alloc.free(stored.data);
                if (try storedDocMatchesIPRange(alloc, stored.data, self.field, parsed, exact_ip)) {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }

    const CIDRParsed = struct { network: [4]u8, prefix_len: u8 };

    fn parseCIDR(cidr: []const u8) ?CIDRParsed {
        // Split on '/'
        const slash_pos = std.mem.indexOfScalar(u8, cidr, '/') orelse return null;
        const ip = parseIPv4(cidr[0..slash_pos]) orelse return null;
        const prefix_len = std.fmt.parseInt(u8, cidr[slash_pos + 1 ..], 10) catch return null;
        if (prefix_len > 32) return null;
        // Apply mask to get network address
        const mask = ipMask(prefix_len);
        return .{
            .network = .{ ip[0] & mask[0], ip[1] & mask[1], ip[2] & mask[2], ip[3] & mask[3] },
            .prefix_len = prefix_len,
        };
    }

    fn parseIPv4(s: []const u8) ?[4]u8 {
        var octets: [4]u8 = undefined;
        var it = std.mem.splitScalar(u8, s, '.');
        for (&octets) |*o| {
            const part = it.next() orelse return null;
            o.* = std.fmt.parseInt(u8, part, 10) catch return null;
        }
        if (it.next() != null) return null; // extra parts
        return octets;
    }

    fn ipMask(prefix_len: u8) [4]u8 {
        if (prefix_len == 0) return .{ 0, 0, 0, 0 };
        if (prefix_len >= 32) return .{ 0xff, 0xff, 0xff, 0xff };
        const shift: u5 = @intCast(32 - prefix_len);
        const mask: u32 = ~(@as(u32, 0)) << shift;
        return .{
            @intCast((mask >> 24) & 0xff),
            @intCast((mask >> 16) & 0xff),
            @intCast((mask >> 8) & 0xff),
            @intCast(mask & 0xff),
        };
    }

    fn ipInRange(ip: [4]u8, network: [4]u8, prefix_len: u8) bool {
        const mask = ipMask(prefix_len);
        return (ip[0] & mask[0]) == network[0] and
            (ip[1] & mask[1]) == network[1] and
            (ip[2] & mask[2]) == network[2] and
            (ip[3] & mask[3]) == network[3];
    }

    fn storedDocMatchesIPRange(
        alloc: Allocator,
        stored: []const u8,
        field: []const u8,
        parsed: ?CIDRParsed,
        exact_ip: ?[4]u8,
    ) !bool {
        const doc = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch return false;
        defer doc.deinit();
        if (doc.value != .object) return false;
        const field_value = doc.value.object.get(field) orelse return false;
        if (field_value != .string) return false;
        const ip = parseIPv4(field_value.string) orelse return false;
        if (parsed) |cidr| return ipInRange(ip, cidr.network, cidr.prefix_len);
        if (exact_ip) |wanted| return std.mem.eql(u8, wanted[0..], ip[0..]);
        return false;
    }
};

/// GeoShape filter: point-in-polygon test on geo_point typed doc values.
pub const GeoShapeFilter = struct {
    field: []const u8,
    polygons: []const []const geo.GeoPoint,

    pub fn execute(self: GeoShapeFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);
        var reader = try typed_dv.TypedDocValuesReader.init(alloc, section_data);
        if (reader.value_type != .geo_point) return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const point = (try reader.getGeoPoint(@intCast(doc_id))) orelse continue;
            for (self.polygons) |polygon| {
                if (geo.pointInPolygon(.{ .lat = point.lat, .lon = point.lon }, polygon)) {
                    try result.add(@intCast(doc_id));
                    break;
                }
            }
        }

        return result;
    }
};

/// Numeric range filter: matches documents with f64 typed doc values in a range.
/// Default: [min, max) — inclusive min, exclusive max.
pub const RangeFilter = struct {
    field: []const u8,
    min_val: ?f64 = null,
    max_val: ?f64 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
    boost: f32 = 1.0,

    pub fn execute(self: RangeFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);

        const reader = typed_dv.TypedDocValuesReader.init(alloc, section_data) catch
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const val = reader.getF64(@intCast(doc_id)) catch continue;
            if (val) |v| {
                const above_min = if (self.min_val) |min|
                    (if (self.inclusive_min) v >= min else v > min)
                else
                    true;
                const below_max = if (self.max_val) |max|
                    (if (self.inclusive_max) v <= max else v < max)
                else
                    true;
                if (above_min and below_max) {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }
};

/// Geo distance filter: finds docs within radius_meters of center point.
pub const GeoDistanceFilter = struct {
    field: []const u8,
    center: geo.GeoPoint,
    radius_meters: f64,

    pub fn execute(self: GeoDistanceFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);

        const reader = typed_dv.TypedDocValuesReader.init(alloc, section_data) catch
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const point = reader.getGeoPoint(@intCast(doc_id)) catch continue;
            if (point) |p| {
                const gp = geo.GeoPoint{ .lat = p.lat, .lon = p.lon };
                if (geo.haversineDistance(self.center, gp) <= self.radius_meters) {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }
};

/// Geo bounding box filter: finds docs within a lat/lon rectangle.
pub const GeoBBoxFilter = struct {
    field: []const u8,
    min_lat: f64,
    min_lon: f64,
    max_lat: f64,
    max_lon: f64,

    pub fn execute(self: GeoBBoxFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);

        const reader = typed_dv.TypedDocValuesReader.init(alloc, section_data) catch
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const point = reader.getGeoPoint(@intCast(doc_id)) catch continue;
            if (point) |p| {
                if (p.lat >= self.min_lat and p.lat <= self.max_lat and
                    p.lon >= self.min_lon and p.lon <= self.max_lon)
                {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }
};

/// Multi-phrase filter: like PhraseFilter but allows alternative terms at each position.
/// e.g., [["hello","hi"], ["world","earth"]] matches "hello world" or "hi earth" etc.
pub const MultiPhraseFilter = struct {
    field: []const u8,
    term_alternatives: []const []const []const u8,
    slop: u32 = 0,
    max_edits: u8 = 0,
    auto_fuzzy: bool = false,

    pub fn execute(self: MultiPhraseFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        if (self.term_alternatives.len == 0) return roaring.RoaringBitmap.init(alloc);

        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        // Step 1: For each position, collect all alternative terms' postings and intersect candidates
        var candidate_bm: ?roaring.RoaringBitmap = null;
        defer if (candidate_bm) |*bm| bm.deinit();

        // Collect all lookups grouped by position
        const num_positions = self.term_alternatives.len;
        var position_lookups = try alloc.alloc(std.ArrayListUnmanaged(inverted.LookupResult), num_positions);
        defer {
            for (position_lookups) |*pl| pl.deinit(alloc);
            alloc.free(position_lookups);
        }
        for (position_lookups) |*pl| pl.* = .empty;

        for (self.term_alternatives, 0..) |alternatives, pos_idx| {
            var pos_bm = roaring.RoaringBitmap.init(alloc);
            errdefer pos_bm.deinit();
            var any_found = false;
            var seen_terms: std.StringHashMapUnmanaged(void) = .empty;
            var owned_seen_terms = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (owned_seen_terms.items) |owned_term| alloc.free(owned_term);
                owned_seen_terms.deinit(alloc);
                seen_terms.deinit(alloc);
            }

            for (alternatives) |term| {
                const fuzziness = if (self.auto_fuzzy) autoFuzziness(term) else self.max_edits;
                if (fuzziness == 0) {
                    if (seen_terms.contains(term)) continue;
                    const owned_term = try alloc.dupe(u8, term);
                    errdefer alloc.free(owned_term);
                    try seen_terms.put(alloc, owned_term, {});
                    try owned_seen_terms.append(alloc, owned_term);
                    const lr = inv_reader.lookup(term) orelse continue;
                    try position_lookups[pos_idx].append(alloc, lr);
                    any_found = true;
                    switch (lr) {
                        .postings => |p| {
                            var bm = try p.docBitmap(alloc);
                            defer bm.deinit();
                            try pos_bm.orWith(&bm);
                        },
                        .one_hit => |h| try pos_bm.add(h.doc_num),
                    }
                    continue;
                }

                const expanded_terms = try collectFuzzyCandidateTerms(alloc, inv_reader, term, fuzziness, 0);
                defer {
                    for (expanded_terms) |expanded_term| alloc.free(expanded_term);
                    alloc.free(expanded_terms);
                }
                for (expanded_terms) |expanded_term| {
                    if (seen_terms.contains(expanded_term)) continue;
                    const owned_term = try alloc.dupe(u8, expanded_term);
                    errdefer alloc.free(owned_term);
                    try seen_terms.put(alloc, owned_term, {});
                    try owned_seen_terms.append(alloc, owned_term);
                    const lr = inv_reader.lookup(expanded_term) orelse continue;
                    try position_lookups[pos_idx].append(alloc, lr);
                    any_found = true;
                    switch (lr) {
                        .postings => |p| {
                            var bm = try p.docBitmap(alloc);
                            defer bm.deinit();
                            try pos_bm.orWith(&bm);
                        },
                        .one_hit => |h| try pos_bm.add(h.doc_num),
                    }
                }
            }

            if (!any_found) {
                pos_bm.deinit();
                return roaring.RoaringBitmap.init(alloc);
            }

            if (candidate_bm) |*cb| {
                cb.andWith(&pos_bm);
                pos_bm.deinit();
            } else {
                candidate_bm = pos_bm;
            }
        }

        const candidates = candidate_bm orelse return roaring.RoaringBitmap.init(alloc);

        // Step 2: Build position maps for each lookup
        const all_position_maps = try alloc.alloc(std.ArrayListUnmanaged(std.AutoHashMapUnmanaged(u32, []const u32)), num_positions);
        defer {
            for (all_position_maps) |*pos_maps| {
                for (pos_maps.items) |*pm| {
                    var vit = pm.valueIterator();
                    while (vit.next()) |v| alloc.free(v.*);
                    pm.deinit(alloc);
                }
                pos_maps.deinit(alloc);
            }
            alloc.free(all_position_maps);
        }

        for (all_position_maps, 0..) |*pos_maps, pos_idx| {
            pos_maps.* = .empty;
            for (position_lookups[pos_idx].items) |lr| {
                var pm: std.AutoHashMapUnmanaged(u32, []const u32) = .empty;
                var post_iter = try lr.iterator(alloc);
                defer post_iter.deinit();
                while (try post_iter.next()) |hit| {
                    if (candidates.contains(hit.doc_id)) {
                        const pos_copy = try alloc.dupe(u32, hit.positions);
                        try pm.put(alloc, hit.doc_id, pos_copy);
                    }
                }
                try pos_maps.append(alloc, pm);
            }
        }

        // Step 3: Check phrase adjacency for each candidate
        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        var cand_iter = candidates.iterator();
        while (cand_iter.next()) |doc_id| {
            if (self.checkMultiPhraseMatch(all_position_maps, doc_id)) {
                try result.add(doc_id);
            }
        }

        return result;
    }

    fn checkMultiPhraseMatch(
        self: MultiPhraseFilter,
        all_position_maps: []const std.ArrayListUnmanaged(std.AutoHashMapUnmanaged(u32, []const u32)),
        doc_id: u32,
    ) bool {
        // Get all starting positions from position 0's alternatives
        const first_maps = all_position_maps[0].items;
        for (first_maps) |pm| {
            const first_positions = pm.get(doc_id) orelse continue;
            for (first_positions) |start_pos| {
                if (self.matchFromStart(all_position_maps, doc_id, start_pos)) return true;
            }
        }
        return false;
    }

    fn matchFromStart(
        self: MultiPhraseFilter,
        all_position_maps: []const std.ArrayListUnmanaged(std.AutoHashMapUnmanaged(u32, []const u32)),
        doc_id: u32,
        start_pos: u32,
    ) bool {
        for (1..self.term_alternatives.len) |ti| {
            const expected: u32 = start_pos + @as(u32, @intCast(ti));
            var found = false;
            for (all_position_maps[ti].items) |pm| {
                const positions = pm.get(doc_id) orelse continue;
                if (positionWithinSlop(positions, expected, self.slop)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }
};

fn autoFuzziness(term: []const u8) u8 {
    const len = term.len;
    if (len > 5) return 2;
    if (len > 2) return 1;
    return 0;
}

fn fuzzyPrefixMatches(term: []const u8, candidate: []const u8, prefix_len: u8) bool {
    if (prefix_len == 0) return true;
    const prefix: usize = @intCast(prefix_len);
    if (term.len < prefix or candidate.len < prefix) return false;
    return std.mem.eql(u8, term[0..prefix], candidate[0..prefix]);
}

fn collectFuzzyCandidateTerms(
    alloc: Allocator,
    inv_reader: inverted.InvertedIndexReader,
    term: []const u8,
    max_edits: u8,
    prefix_len: u8,
) FilterError![]const []const u8 {
    if (max_edits == 0) {
        const out = try alloc.alloc([]const u8, 1);
        out[0] = try alloc.dupe(u8, term);
        return out;
    }

    var lev = levenshtein.LevenshteinAutomaton{ .term = term, .max_distance = max_edits, .alloc = alloc };
    defer lev.deinit();
    var term_iter = try inv_reader.fstSearchIterator(lev.automaton());
    defer term_iter.deinit();

    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);

    while (try term_iter.next()) |entry| {
        if (!fuzzyPrefixMatches(term, entry.term, prefix_len)) continue;
        var seen = false;
        for (out.items) |existing| {
            if (std.mem.eql(u8, existing, entry.term)) {
                seen = true;
                break;
            }
        }
        if (!seen) try out.append(alloc, try alloc.dupe(u8, entry.term));
    }

    if (out.items.len == 0) try out.append(alloc, try alloc.dupe(u8, term));
    return out.toOwnedSlice(alloc);
}

/// Date range filter: matches documents by u64 timestamp in typed doc values.
/// Start/end are unix nanoseconds (caller parses ISO8601 before constructing).
pub const DateRangeFilter = struct {
    field: []const u8,
    start_ns: ?u64 = null,
    end_ns: ?u64 = null,
    inclusive_start: bool = true,
    inclusive_end: bool = false,
    boost: f32 = 1.0,

    pub fn execute(self: DateRangeFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);

        const reader = typed_dv.TypedDocValuesReader.init(alloc, section_data) catch
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const val = reader.getU64(@intCast(doc_id)) catch continue;
            if (val) |v| {
                const above_start = if (self.start_ns) |s|
                    (if (self.inclusive_start) v >= s else v > s)
                else
                    true;
                const below_end = if (self.end_ns) |e|
                    (if (self.inclusive_end) v <= e else v < e)
                else
                    true;
                if (above_start and below_end) {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }
};

/// Wildcard match: glob-style `*` (any chars) and `?` (single char) matching.
/// Iterates FST terms and matches against the pattern.
pub const WildcardFilter = struct {
    field: []const u8,
    pattern: []const u8,
    boost: f32 = 1.0,

    pub fn execute(self: WildcardFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const inv_reader = (try seg.reader.invertedIndex(self.field)) orelse
            return roaring.RoaringBitmap.init(alloc);

        var term_iter = try inv_reader.termIterator();
        defer term_iter.deinit();

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        while (try term_iter.next()) |entry| {
            if (wildcardMatch(self.pattern, entry.term)) {
                switch (entry.result) {
                    .postings => |p| {
                        var bm = try p.docBitmap(alloc);
                        defer bm.deinit();
                        try result.orWith(&bm);
                    },
                    .one_hit => |h| {
                        try result.add(h.doc_num);
                    },
                }
            }
        }

        return result;
    }
};

/// Match a glob pattern against a string.
/// `*` matches zero or more characters, `?` matches exactly one character.
fn wildcardMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == '?' or pattern[pi] == text[ti])) {
            pi += 1;
            ti += 1;
        } else if (pi < pattern.len and pattern[pi] == '*') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Consume trailing *'s
    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

/// Document ID filter: matches documents by their stored document ID.
pub const DocIdFilter = struct {
    doc_ids: []const []const u8,

    pub fn execute(self: DocIdFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        // Scan stored docs and match IDs
        for (0..seg.reader.doc_count) |doc_num| {
            const stored = seg.reader.storedDoc(@intCast(doc_num)) orelse continue;
            for (self.doc_ids) |wanted| {
                if (std.mem.eql(u8, stored.id, wanted)) {
                    try result.add(@intCast(doc_num));
                    break;
                }
            }
        }

        return result;
    }
};

/// Global numeric document filter: matches documents by snapshot-global doc ID.
pub const DocNumFilter = struct {
    doc_nums: []const u32,

    pub fn executeWithOffset(self: DocNumFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry, doc_offset: u32) FilterError!roaring.RoaringBitmap {
        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        const upper = doc_offset + seg.reader.doc_count;
        for (self.doc_nums) |doc_num| {
            if (doc_num < doc_offset or doc_num >= upper) continue;
            try result.add(doc_num - doc_offset);
        }

        return result;
    }
};

/// Boolean field filter: matches documents by a boolean typed doc value.
pub const BoolFieldFilter = struct {
    field: []const u8,
    value: bool,

    pub fn execute(self: BoolFieldFilter, alloc: Allocator, seg: *const index_mod.SegmentEntry) FilterError!roaring.RoaringBitmap {
        const section_data = seg.reader.getSection(self.field, .typed_doc_values) orelse
            return roaring.RoaringBitmap.init(alloc);

        const reader = typed_dv.TypedDocValuesReader.init(alloc, section_data) catch
            return roaring.RoaringBitmap.init(alloc);

        var result = roaring.RoaringBitmap.init(alloc);
        errdefer result.deinit();

        for (0..seg.reader.doc_count) |doc_id| {
            const val = reader.getBool(@intCast(doc_id)) catch continue;
            if (val) |v| {
                if (v == self.value) {
                    try result.add(@intCast(doc_id));
                }
            }
        }

        return result;
    }
};

// ============================================================================
// Multi-segment filter execution
// ============================================================================

/// Execute a filter across all segments in a snapshot.
/// Returns matching global doc IDs (with per-segment offsets applied).
pub fn executeFilter(
    alloc: Allocator,
    snap: *const index_mod.IndexSnapshot,
    filter: Filter,
) ![]u32 {
    var all_ids = std.ArrayListUnmanaged(u32).empty;
    defer all_ids.deinit(alloc);

    var doc_offset: u32 = 0;
    for (snap.segments) |*seg| {
        var bm = try filter.executeWithOffset(alloc, seg, doc_offset);
        defer bm.deinit();

        // Remove deleted docs
        if (seg.deleted) |d| bm.andNotWith(&d);

        // Collect with offset
        var iter = bm.iterator();
        while (iter.next()) |doc_id| {
            try all_ids.append(alloc, doc_id + doc_offset);
        }

        doc_offset += seg.reader.doc_count;
    }

    return try alloc.dupe(u32, all_ids.items);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

fn buildTestSegmentWithTerms(alloc: Allocator, docs: []const struct { terms: []const inverted.InvertedIndexBuilder.TermHit }) ![]u8 {
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();

    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), doc.terms);
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);

    for (0..docs.len) |i| {
        var id_buf: [16]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{i}) catch unreachable;
        try seg_writer.addStoredDoc(id_str, "{}");
    }

    return seg_writer.build();
}

test "term filter finds matching docs" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{ .{ .term = "hello", .freq = 2, .norm = 15 }, .{ .term = "world", .freq = 1, .norm = 15 } } },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 8 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Filter for "hello" — should match docs 0, 1
    const filter = Filter{ .term = .{ .field = "body", .term = "hello" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "bool filter AND" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{ .{ .term = "hello", .freq = 1, .norm = 10 }, .{ .term = "world", .freq = 1, .norm = 10 } } },
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // AND: "hello" AND "world" — only doc 0
    const filter = Filter{ .bool_filter = .{
        .must = &.{
            .{ .term = .{ .field = "body", .term = "hello" } },
            .{ .term = .{ .field = "body", .term = "world" } },
        },
        .should = &.{},
        .must_not = &.{},
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 1), bm.cardinality());
    try testing.expect(bm.contains(0));
}

test "bool filter OR" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "other", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // OR: "hello" OR "world" — docs 0, 1
    const filter = Filter{ .bool_filter = .{
        .must = &.{},
        .should = &.{
            .{ .term = .{ .field = "body", .term = "hello" } },
            .{ .term = .{ .field = "body", .term = "world" } },
        },
        .must_not = &.{},
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
}

test "bool filter respects min_should_match" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{ .{ .term = "alpha", .freq = 1, .norm = 10 }, .{ .term = "beta", .freq = 1, .norm = 10 } } },
        .{ .terms = &.{ .{ .term = "alpha", .freq = 1, .norm = 10 }, .{ .term = "gamma", .freq = 1, .norm = 10 } } },
        .{ .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .bool_filter = .{
        .must = &.{},
        .should = &.{
            .{ .term = .{ .field = "body", .term = "alpha" } },
            .{ .term = .{ .field = "body", .term = "beta" } },
            .{ .term = .{ .field = "body", .term = "gamma" } },
        },
        .must_not = &.{},
        .min_should_match = 2,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "bool filter treats should clauses as optional when must matches and min_should_match is zero" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{ .{ .term = "alpha", .freq = 1, .norm = 10 }, .{ .term = "beta", .freq = 1, .norm = 10 } } },
        .{ .terms = &.{.{ .term = "alpha", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "beta", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .bool_filter = .{
        .must = &.{.{ .term = .{ .field = "body", .term = "alpha" } }},
        .should = &.{.{ .term = .{ .field = "body", .term = "beta" } }},
        .must_not = &.{},
        .min_should_match = 0,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "bool filter NOT" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{ .{ .term = "hello", .freq = 1, .norm = 10 }, .{ .term = "world", .freq = 1, .norm = 10 } } },
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // "hello" AND NOT "world" — only doc 1
    const filter = Filter{ .bool_filter = .{
        .must = &.{
            .{ .term = .{ .field = "body", .term = "hello" } },
        },
        .should = &.{},
        .must_not = &.{
            .{ .term = .{ .field = "body", .term = "world" } },
        },
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 1), bm.cardinality());
    try testing.expect(bm.contains(1));
}

test "match all filter" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .match_all = {} };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
}

test "prefix filter" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "apple", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "application", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "banana", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Prefix "app" — should match docs 0 (apple) and 1 (application)
    const filter = Filter{ .prefix = .{ .field = "body", .prefix = "app" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 2), bm.cardinality());
    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "multi-segment filter execution" {
    const alloc = testing.allocator;

    const seg1 = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg1);

    const seg2 = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "other", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg2);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg1);
    try writer.addSegment(seg2);

    const snap = writer.snapshot();
    const filter = Filter{ .term = .{ .field = "body", .term = "hello" } };
    const results = try executeFilter(alloc, snap, filter);
    defer alloc.free(results);

    // Doc 0 from seg1, doc 0 from seg2 (offset by 2)
    try testing.expectEqual(@as(usize, 2), results.len);
    try testing.expectEqual(@as(u32, 0), results[0]);
    try testing.expectEqual(@as(u32, 2), results[1]);
}

test "fuzzy filter finds similar terms" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "hello", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "world", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "hallo", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Fuzzy "helo" with distance 1 should match "hello" (substitution l→o)
    const filter = Filter{ .fuzzy = .{ .field = "body", .term = "helo", .max_edits = 1 } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    // Should find "hello" (edit distance 1 from "helo")
    try testing.expect(bm.contains(0));
    // "world" is too far
    try testing.expect(!bm.contains(1));
}

test "edit distance computation" {
    try testing.expectEqual(@as(u32, 0), editDistance("hello", "hello"));
    try testing.expectEqual(@as(u32, 1), editDistance("hello", "helo"));
    try testing.expectEqual(@as(u32, 1), editDistance("hello", "hallo"));
    try testing.expectEqual(@as(u32, 1), editDistance("hello", "helloo"));
    try testing.expectEqual(@as(u32, 2), editDistance("hello", "haxlo"));
    try testing.expectEqual(@as(u32, 4), editDistance("hello", "world"));
}

test "range filter on typed doc values" {
    const alloc = testing.allocator;

    // Build a segment with typed doc values
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    // Create typed doc values for "price" field
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 10.0 });
    try dv_writer.add(1, .{ .f64_val = 25.0 });
    try dv_writer.add(2, .{ .f64_val = 50.0 });

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("price");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);

    // Add stored docs
    try seg_writer.addStoredDoc("a", "{}");
    try seg_writer.addStoredDoc("b", "{}");
    try seg_writer.addStoredDoc("c", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Range [15, 40) should match doc 1 (price=25)
    const filter = Filter{ .range = .{ .field = "price", .min_val = 15.0, .max_val = 40.0 } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expectEqual(@as(usize, 1), bm.cardinality());
    try testing.expect(bm.contains(1));

    // Range [0, 100) should match all
    const filter2 = Filter{ .range = .{ .field = "price", .min_val = 0.0, .max_val = 100.0 } };
    var bm2 = try filter2.execute(alloc, seg);
    defer bm2.deinit();
    try testing.expectEqual(@as(usize, 3), bm2.cardinality());
}

test "geo distance filter" {
    const alloc = testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    // Create typed doc values for "location" field with geo points
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .geo_point, 128);
    defer dv_writer.deinit();
    // San Francisco
    try dv_writer.add(0, .{ .geo_point = .{ .lat = 37.7749, .lon = -122.4194 } });
    // New York
    try dv_writer.add(1, .{ .geo_point = .{ .lat = 40.7128, .lon = -74.0060 } });
    // Oakland (near SF)
    try dv_writer.add(2, .{ .geo_point = .{ .lat = 37.8044, .lon = -122.2712 } });

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("location");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);

    try seg_writer.addStoredDoc("sf", "{}");
    try seg_writer.addStoredDoc("nyc", "{}");
    try seg_writer.addStoredDoc("oak", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // 20km radius from SF center — should match SF and Oakland, not NYC
    const filter = Filter{ .geo_distance = .{
        .field = "location",
        .center = .{ .lat = 37.7749, .lon = -122.4194 },
        .radius_meters = 20_000,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // SF
    try testing.expect(!bm.contains(1)); // NYC is far
    try testing.expect(bm.contains(2)); // Oakland is close
}

test "phrase filter exact adjacency" {
    const alloc = testing.allocator;

    // Doc 0: "hello world" — hello@0, world@1
    // Doc 1: "world hello" — world@0, hello@1
    // Doc 2: "hello foo world" — hello@0, foo@1, world@2
    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .terms = &.{
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "foo", .freq = 1, .norm = 10, .positions = &.{1} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{2} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Phrase "hello world" (slop=0) — only doc 0
    const filter = Filter{ .phrase = .{
        .field = "body",
        .terms = &.{ "hello", "world" },
        .slop = 0,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // hello@0, world@1 ✓
    try testing.expect(!bm.contains(1)); // world@0, hello@1 — wrong order
    try testing.expect(!bm.contains(2)); // hello@0, world@2 — gap too big
}

test "phrase filter with slop" {
    const alloc = testing.allocator;

    // Doc 0: "hello foo world" — hello@0, foo@1, world@2
    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "foo", .freq = 1, .norm = 10, .positions = &.{1} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{2} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Phrase "hello world" slop=0 — should NOT match (gap of 1)
    const filter0 = Filter{ .phrase = .{
        .field = "body",
        .terms = &.{ "hello", "world" },
        .slop = 0,
    } };
    var bm0 = try filter0.execute(alloc, seg);
    defer bm0.deinit();
    try testing.expect(!bm0.contains(0));

    // Phrase "hello world" slop=1 — should match (world@2 is 1 away from expected@1)
    const filter1 = Filter{ .phrase = .{
        .field = "body",
        .terms = &.{ "hello", "world" },
        .slop = 1,
    } };
    var bm1 = try filter1.execute(alloc, seg);
    defer bm1.deinit();
    try testing.expect(bm1.contains(0));
}

test "wildcard filter with prefix star" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "foobar", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "foobaz", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "barfoo", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // "foo*" matches foobar and foobaz
    const filter = Filter{ .wildcard = .{ .field = "body", .pattern = "foo*" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "wildcard filter with suffix star" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "foobar", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "bazbar", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "foobaz", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // "*bar" matches foobar and bazbar
    const filter = Filter{ .wildcard = .{ .field = "body", .pattern = "*bar" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
    try testing.expect(!bm.contains(2));
}

test "wildcard filter with question mark" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "foo", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "fao", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "fooo", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // "f?o" matches foo and fao (single char wildcard), not fooo (too long)
    const filter = Filter{ .wildcard = .{ .field = "body", .pattern = "f?o" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // foo
    try testing.expect(bm.contains(1)); // fao
    try testing.expect(!bm.contains(2)); // fooo — too long
}

test "doc_id filter finds specific documents" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "a", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "b", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "c", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Docs are stored with IDs "0", "1", "2" (from buildTestSegmentWithTerms)
    const filter = Filter{ .doc_id = .{ .doc_ids = &.{ "0", "2" } } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0));
    try testing.expect(!bm.contains(1));
    try testing.expect(bm.contains(2));
}

test "doc_num filter matches snapshot global document numbers" {
    const alloc = testing.allocator;

    const seg_a = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "a", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "b", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_a);
    const seg_b = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{.{ .term = "c", .freq = 1, .norm = 10 }} },
        .{ .terms = &.{.{ .term = "d", .freq = 1, .norm = 10 }} },
    });
    defer alloc.free(seg_b);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_a);
    try writer.addSegment(seg_b);

    const doc_ids = try writer.snapshot().executeFilter(alloc, .{ .doc_num = .{ .doc_nums = &.{ 1, 2 } } });
    defer alloc.free(doc_ids);

    try testing.expectEqual(@as(usize, 2), doc_ids.len);
    try testing.expectEqual(@as(u32, 1), doc_ids[0]);
    try testing.expectEqual(@as(u32, 2), doc_ids[1]);
}

test "bool_field filter matches true/false" {
    const alloc = testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .bool_val, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .bool_val = true });
    try dv_writer.add(1, .{ .bool_val = false });
    try dv_writer.add(2, .{ .bool_val = true });

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("active");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);

    try seg_writer.addStoredDoc("a", "{}");
    try seg_writer.addStoredDoc("b", "{}");
    try seg_writer.addStoredDoc("c", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Filter for active=true
    const filter_true = Filter{ .bool_field = .{ .field = "active", .value = true } };
    var bm_true = try filter_true.execute(alloc, seg);
    defer bm_true.deinit();

    try testing.expect(bm_true.contains(0));
    try testing.expect(!bm_true.contains(1));
    try testing.expect(bm_true.contains(2));

    // Filter for active=false
    const filter_false = Filter{ .bool_field = .{ .field = "active", .value = false } };
    var bm_false = try filter_false.execute(alloc, seg);
    defer bm_false.deinit();

    try testing.expect(!bm_false.contains(0));
    try testing.expect(bm_false.contains(1));
    try testing.expect(!bm_false.contains(2));
}

test "wildcard match function" {
    // Basic patterns
    try testing.expect(wildcardMatch("foo*", "foobar"));
    try testing.expect(wildcardMatch("foo*", "foo"));
    try testing.expect(!wildcardMatch("foo*", "bar"));
    try testing.expect(wildcardMatch("*bar", "foobar"));
    try testing.expect(wildcardMatch("*bar", "bar"));
    try testing.expect(!wildcardMatch("*bar", "baz"));
    try testing.expect(wildcardMatch("f?o", "foo"));
    try testing.expect(wildcardMatch("f?o", "fao"));
    try testing.expect(!wildcardMatch("f?o", "fooo"));
    try testing.expect(wildcardMatch("*", "anything"));
    try testing.expect(wildcardMatch("*", ""));
    try testing.expect(wildcardMatch("a*b*c", "abc"));
    try testing.expect(wildcardMatch("a*b*c", "aXbYc"));
    try testing.expect(!wildcardMatch("a*b*c", "aXbY"));
}

test "range filter inclusive_max includes boundary" {
    const alloc = testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 10.0 });
    try dv_writer.add(1, .{ .f64_val = 50.0 }); // boundary value

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("price");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("a", "{}");
    try seg_writer.addStoredDoc("b", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Default: [0, 50) exclusive max — should NOT include 50
    const filter_excl = Filter{ .range = .{ .field = "price", .min_val = 0.0, .max_val = 50.0 } };
    var bm1 = try filter_excl.execute(alloc, seg);
    defer bm1.deinit();
    try testing.expect(bm1.contains(0));
    try testing.expect(!bm1.contains(1)); // 50 excluded

    // inclusive_max=true: [0, 50] — should include 50
    const filter_incl = Filter{ .range = .{ .field = "price", .min_val = 0.0, .max_val = 50.0, .inclusive_max = true } };
    var bm2 = try filter_incl.execute(alloc, seg);
    defer bm2.deinit();
    try testing.expect(bm2.contains(0));
    try testing.expect(bm2.contains(1)); // 50 included
}

test "range filter inclusive_min false excludes boundary" {
    const alloc = testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .f64_val, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .f64_val = 10.0 }); // boundary value
    try dv_writer.add(1, .{ .f64_val = 20.0 });

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("price");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("a", "{}");
    try seg_writer.addStoredDoc("b", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // inclusive_min=false: (10, 100) — 10 excluded
    const filter = Filter{ .range = .{ .field = "price", .min_val = 10.0, .max_val = 100.0, .inclusive_min = false } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();
    try testing.expect(!bm.contains(0)); // 10 excluded
    try testing.expect(bm.contains(1)); // 20 included
}

test "multi phrase filter matches alternatives" {
    const alloc = testing.allocator;

    // Doc 0: "hello world" — hello@0, world@1
    // Doc 1: "hi earth" — hi@0, earth@1
    // Doc 2: "hello earth" — hello@0, earth@1
    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "world", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .terms = &.{
            .{ .term = "hi", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "earth", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "earth", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Multi-phrase: [["hello","hi"], ["world","earth"]] — should match all 3
    const filter = Filter{ .multi_phrase = .{
        .field = "body",
        .term_alternatives = &.{
            &.{ "hello", "hi" },
            &.{ "world", "earth" },
        },
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // hello world
    try testing.expect(bm.contains(1)); // hi earth
    try testing.expect(bm.contains(2)); // hello earth
}

test "phrase filter supports fuzzy alternatives" {
    const alloc = testing.allocator;

    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{
            .{ .term = "alpha", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "beta", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
        .{ .terms = &.{
            .{ .term = "alphi", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "beta", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .phrase = .{
        .field = "body",
        .terms = &.{ "alpha", "beta" },
        .max_edits = 1,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0));
    try testing.expect(bm.contains(1));
}

test "multi phrase filter rejects non-matching position" {
    const alloc = testing.allocator;

    // Doc 0: "hello foo" — hello@0, foo@1 (no world/earth at position 1)
    const seg_bytes = try buildTestSegmentWithTerms(alloc, &.{
        .{ .terms = &.{
            .{ .term = "hello", .freq = 1, .norm = 10, .positions = &.{0} },
            .{ .term = "foo", .freq = 1, .norm = 10, .positions = &.{1} },
        } },
    });
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .multi_phrase = .{
        .field = "body",
        .term_alternatives = &.{
            &.{ "hello", "hi" },
            &.{ "world", "earth" },
        },
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(!bm.contains(0)); // no world/earth at pos 1
}

test "date range filter on u64 timestamps" {
    const alloc = testing.allocator;

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();

    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .u64_val, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .u64_val = 1_000_000_000 }); // 1s
    try dv_writer.add(1, .{ .u64_val = 5_000_000_000 }); // 5s
    try dv_writer.add(2, .{ .u64_val = 10_000_000_000 }); // 10s

    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    const field_idx = try seg_writer.addField("timestamp");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);
    try seg_writer.addStoredDoc("a", "{}");
    try seg_writer.addStoredDoc("b", "{}");
    try seg_writer.addStoredDoc("c", "{}");

    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // [3s, 8s) — should match doc 1 (5s)
    const filter = Filter{ .date_range = .{
        .field = "timestamp",
        .start_ns = 3_000_000_000,
        .end_ns = 8_000_000_000,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(!bm.contains(0)); // 1s < 3s
    try testing.expect(bm.contains(1)); // 5s in [3s, 8s)
    try testing.expect(!bm.contains(2)); // 10s >= 8s
}

test "term range filter [b, d)" {
    const alloc = testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "apple", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "banana", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "cherry", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(3, &.{.{ .term = "date", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(4, &.{.{ .term = "elderberry", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("fruit");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..5) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // [banana, date) → matches banana(1), cherry(2)
    const filter = Filter{ .term_range = .{
        .field = "fruit",
        .min = "banana",
        .max = "date",
        .inclusive_min = true,
        .inclusive_max = false,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(!bm.contains(0)); // apple
    try testing.expect(bm.contains(1)); // banana
    try testing.expect(bm.contains(2)); // cherry
    try testing.expect(!bm.contains(3)); // date (exclusive)
    try testing.expect(!bm.contains(4)); // elderberry
}

test "term range filter inclusive max [b, d]" {
    const alloc = testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "banana", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "cherry", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "date", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("fruit");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..3) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // [banana, date] inclusive_max → matches all three
    const filter = Filter{ .term_range = .{
        .field = "fruit",
        .min = "banana",
        .max = "date",
        .inclusive_min = true,
        .inclusive_max = true,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // banana
    try testing.expect(bm.contains(1)); // cherry
    try testing.expect(bm.contains(2)); // date (inclusive)
}

test "term range filter unbounded lower" {
    const alloc = testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "apple", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "banana", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "cherry", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("fruit");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..3) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // [null, cherry) → matches apple, banana
    const filter = Filter{ .term_range = .{
        .field = "fruit",
        .min = null,
        .max = "cherry",
        .inclusive_max = false,
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // apple
    try testing.expect(bm.contains(1)); // banana
    try testing.expect(!bm.contains(2)); // cherry (exclusive)
}

test "IP range filter /24 subnet" {
    const alloc = testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "192.168.1.1", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "192.168.1.100", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "192.168.2.1", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(3, &.{.{ .term = "10.0.0.1", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("ip");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..4) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .ip_range = .{ .field = "ip", .cidr = "192.168.1.0/24" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // 192.168.1.1
    try testing.expect(bm.contains(1)); // 192.168.1.100
    try testing.expect(!bm.contains(2)); // 192.168.2.1
    try testing.expect(!bm.contains(3)); // 10.0.0.1
}

test "IP range filter /32 single host" {
    const alloc = testing.allocator;
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "10.0.0.1", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "10.0.0.2", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("ip");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (0..2) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    const filter = Filter{ .ip_range = .{ .field = "ip", .cidr = "10.0.0.1/32" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // exact match
    try testing.expect(!bm.contains(1)); // different host
}

test "geo shape filter point in polygon" {
    const alloc = testing.allocator;

    // Build geo_point doc values
    var dv_writer = typed_dv.TypedDocValuesWriter.init(alloc, .geo_point, 128);
    defer dv_writer.deinit();
    try dv_writer.add(0, .{ .geo_point = .{ .lat = 5.0, .lon = 5.0 } }); // inside
    try dv_writer.add(1, .{ .geo_point = .{ .lat = 15.0, .lon = 5.0 } }); // outside
    try dv_writer.add(2, .{ .geo_point = .{ .lat = 3.0, .lon = 3.0 } }); // inside
    const dv_data = try dv_writer.build();
    defer alloc.free(dv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("location");
    try seg_writer.addSection(field_idx, .typed_doc_values, dv_data);
    for (0..3) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Rectangle polygon: (0,0)-(0,10)-(10,10)-(10,0)-(0,0)
    const filter = Filter{ .geo_shape = .{
        .field = "location",
        .polygons = &.{&.{
            .{ .lat = 0, .lon = 0 },
            .{ .lat = 0, .lon = 10 },
            .{ .lat = 10, .lon = 10 },
            .{ .lat = 10, .lon = 0 },
            .{ .lat = 0, .lon = 0 },
        }},
    } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // (5,5) inside
    try testing.expect(!bm.contains(1)); // (15,5) outside
    try testing.expect(bm.contains(2)); // (3,3) inside
}

test "term filter with synonym expansion" {
    const alloc = testing.allocator;

    // Build inverted index: doc0 has "fast", doc1 has "quick"
    var inv_builder = inverted.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = "fast", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(1, &.{.{ .term = "quick", .freq = 1, .norm = 10 }});
    try inv_builder.addDocument(2, &.{.{ .term = "slow", .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    // Build synonym section: "fast" and "quick" are synonyms
    var syn_writer = synonyms_mod.SynonymWriter.init(alloc);
    defer syn_writer.deinit();
    try syn_writer.addGroup(&.{ "fast", "quick" }, &.{ 0, 1 });
    const syn_data = try syn_writer.build();
    defer alloc.free(syn_data);

    // Build segment with both sections
    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("text");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    try seg_writer.addSection(field_idx, .synonym, syn_data);
    for (0..3) |_| try seg_writer.addStoredDoc("d", "{}");
    const seg_bytes = try seg_writer.build();
    defer alloc.free(seg_bytes);

    var writer = try index_mod.IndexWriter.init(alloc);
    defer writer.deinit();
    try writer.addSegment(seg_bytes);

    const snap = writer.snapshot();
    const seg = &snap.segments[0];

    // Search for "fast" — should also match "quick" via synonym expansion
    const filter = Filter{ .term = .{ .field = "text", .term = "fast" } };
    var bm = try filter.execute(alloc, seg);
    defer bm.deinit();

    try testing.expect(bm.contains(0)); // "fast" — direct match
    try testing.expect(bm.contains(1)); // "quick" — synonym expansion
    try testing.expect(!bm.contains(2)); // "slow" — not a synonym
}
