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

const std = @import("std");
const vellum = @import("antfly_vellum");
const pathfact = @import("pathfact.zig");
const token = @import("token.zig");

pub const DimensionSemantics = enum {
    analyzed_text,
    canonical_scalar,
};

pub const LabelKind = enum {
    analyzed_term,
    canonical_scalar,
    sparse_token,
    graph_label,
};

pub const SelectorKind = enum {
    term,
    prefix,
    range,
    wildcard,
    regexp,
    fuzzy,
};

pub const DictionaryLayoutKind = enum {
    fst_postings,
    lexicon_postings_rows,
    path_lookup_rows,
    sparse_vector_terms,
    graph_label_rows,
};

pub const RegistryEntry = struct {
    owner: []const u8,
    layout: DictionaryLayoutKind,
    state: []const u8 = "ready",

    pub fn encodeAlloc(self: RegistryEntry, alloc: std.mem.Allocator) ![]u8 {
        return try token.canonicalTupleAlloc(alloc, &.{
            "dictionary_registry_entry:v1",
            self.owner,
            @tagName(self.layout),
            self.state,
        });
    }

    pub fn decodeAlloc(alloc: std.mem.Allocator, encoded: []const u8) !OwnedRegistryEntry {
        const parts = token.decodeTupleAlloc(alloc, encoded) catch return error.InvalidDictionaryRegistryEntry;
        defer {
            for (parts) |part| alloc.free(part);
            if (parts.len > 0) alloc.free(parts);
        }
        if (parts.len != 4 or !std.mem.eql(u8, parts[0], "dictionary_registry_entry:v1")) return error.InvalidDictionaryRegistryEntry;
        const layout = std.meta.stringToEnum(DictionaryLayoutKind, parts[2]) orelse return error.InvalidDictionaryRegistryEntry;
        return .{
            .owner = try alloc.dupe(u8, parts[1]),
            .layout = layout,
            .state = try alloc.dupe(u8, parts[3]),
        };
    }
};

pub const OwnedRegistryEntry = struct {
    owner: []u8,
    layout: DictionaryLayoutKind,
    state: []u8,

    pub fn deinit(self: *OwnedRegistryEntry, alloc: std.mem.Allocator) void {
        alloc.free(self.owner);
        alloc.free(self.state);
        self.* = undefined;
    }
};

pub const RegistryClaim = enum {
    claimed,
    already_owned,
    owned_by_other,
};

pub fn claimRegistryOwnerTxn(
    alloc: std.mem.Allocator,
    txn: anytype,
    identity: DictionaryIdentity,
    owner: []const u8,
    layout: DictionaryLayoutKind,
    state: []const u8,
) !RegistryClaim {
    const key = try identity.registryKeyAlloc(alloc);
    defer alloc.free(key);

    const existing_payload = txn.get(key) catch null;
    if (existing_payload) |payload| {
        var existing = try RegistryEntry.decodeAlloc(alloc, payload);
        defer existing.deinit(alloc);
        const same_physical_owner = existing.layout == layout and std.mem.eql(u8, existing.owner, owner);
        if (!same_physical_owner) return .owned_by_other;
        if (std.mem.eql(u8, existing.state, state)) return .already_owned;
    }

    const payload = try (RegistryEntry{
        .owner = owner,
        .layout = layout,
        .state = state,
    }).encodeAlloc(alloc);
    defer alloc.free(payload);
    try txn.put(key, payload);
    return .claimed;
}

pub const DictionaryIdentity = struct {
    scope: []const u8,
    field_or_path: []const u8,
    label_kind: LabelKind,
    analyzer_or_canonicalization: []const u8,
    value_kind: []const u8,
    coercion_policy: []const u8,

    pub fn analyzedText(scope: []const u8, field: []const u8, analyzer: []const u8) DictionaryIdentity {
        return .{
            .scope = scope,
            .field_or_path = field,
            .label_kind = .analyzed_term,
            .analyzer_or_canonicalization = analyzer,
            .value_kind = "term",
            .coercion_policy = "analyzer",
        };
    }

    pub fn canonicalScalar(scope: []const u8, path: []const u8, kind: pathfact.Kind, canonicalization: []const u8, coercion_policy: []const u8) DictionaryIdentity {
        return .{
            .scope = scope,
            .field_or_path = path,
            .label_kind = .canonical_scalar,
            .analyzer_or_canonicalization = canonicalization,
            .value_kind = kind.tag(),
            .coercion_policy = coercion_policy,
        };
    }

    pub fn sparseToken(scope: []const u8, field: []const u8, token_space: []const u8, value_kind: []const u8) DictionaryIdentity {
        return .{
            .scope = scope,
            .field_or_path = field,
            .label_kind = .sparse_token,
            .analyzer_or_canonicalization = token_space,
            .value_kind = value_kind,
            .coercion_policy = "token-space",
        };
    }

    pub fn graphLabel(scope: []const u8, label_path: []const u8, canonicalization: []const u8, value_kind: []const u8) DictionaryIdentity {
        return .{
            .scope = scope,
            .field_or_path = label_path,
            .label_kind = .graph_label,
            .analyzer_or_canonicalization = canonicalization,
            .value_kind = value_kind,
            .coercion_policy = "graph-label",
        };
    }

    pub fn eql(left: DictionaryIdentity, right: DictionaryIdentity) bool {
        return std.mem.eql(u8, left.scope, right.scope) and
            std.mem.eql(u8, left.field_or_path, right.field_or_path) and
            left.label_kind == right.label_kind and
            std.mem.eql(u8, left.analyzer_or_canonicalization, right.analyzer_or_canonicalization) and
            std.mem.eql(u8, left.value_kind, right.value_kind) and
            std.mem.eql(u8, left.coercion_policy, right.coercion_policy);
    }

    pub fn keyAlloc(self: DictionaryIdentity, alloc: std.mem.Allocator) ![]u8 {
        return try token.canonicalTupleAlloc(alloc, &.{
            "dictionary:v1",
            self.scope,
            self.field_or_path,
            @tagName(self.label_kind),
            self.analyzer_or_canonicalization,
            self.value_kind,
            self.coercion_policy,
        });
    }

    pub fn registryKeyAlloc(self: DictionaryIdentity, alloc: std.mem.Allocator) ![]u8 {
        const identity_key = try self.keyAlloc(alloc);
        defer alloc.free(identity_key);
        return try token.canonicalTupleAlloc(alloc, &.{ "dictionary_registry:v1", identity_key });
    }
};

pub const AccessPath = struct {
    dictionary: DictionaryIdentity,
    selector: SelectorKind,

    pub fn analyzedText(field: []const u8, analyzer: []const u8, selector: SelectorKind) AccessPath {
        return analyzedTextScoped("default", field, analyzer, selector);
    }

    pub fn analyzedTextScoped(scope: []const u8, field: []const u8, analyzer: []const u8, selector: SelectorKind) AccessPath {
        return .{
            .dictionary = DictionaryIdentity.analyzedText(scope, field, analyzer),
            .selector = selector,
        };
    }

    pub fn canonicalScalar(path: []const u8, kind: pathfact.Kind, selector: SelectorKind) AccessPath {
        return canonicalScalarScoped("default", path, kind, selector);
    }

    pub fn canonicalScalarScoped(scope: []const u8, path: []const u8, kind: pathfact.Kind, selector: SelectorKind) AccessPath {
        return .{
            .dictionary = DictionaryIdentity.canonicalScalar(scope, path, kind, "json-scalar-v1", "kind-qualified"),
            .selector = selector,
        };
    }
};

pub const AccessPathRejectReason = enum {
    scope_mismatch,
    label_kind_mismatch,
    field_or_path_mismatch,
    analyzer_or_canonicalization_mismatch,
    value_kind_mismatch,
    coercion_policy_mismatch,
};

pub const AccessPathProof = union(enum) {
    proven,
    rejected: AccessPathRejectReason,

    pub fn safe(self: AccessPathProof) bool {
        return self == .proven;
    }
};

pub fn sharedDictionaryProof(left: AccessPath, right: AccessPath) AccessPathProof {
    if (!std.mem.eql(u8, left.dictionary.scope, right.dictionary.scope)) return .{ .rejected = .scope_mismatch };
    if (left.dictionary.label_kind != right.dictionary.label_kind) return .{ .rejected = .label_kind_mismatch };
    if (!std.mem.eql(u8, left.dictionary.field_or_path, right.dictionary.field_or_path)) return .{ .rejected = .field_or_path_mismatch };
    if (!std.mem.eql(u8, left.dictionary.analyzer_or_canonicalization, right.dictionary.analyzer_or_canonicalization)) return .{ .rejected = .analyzer_or_canonicalization_mismatch };
    if (!std.mem.eql(u8, left.dictionary.value_kind, right.dictionary.value_kind)) return .{ .rejected = .value_kind_mismatch };
    if (!std.mem.eql(u8, left.dictionary.coercion_policy, right.dictionary.coercion_policy)) return .{ .rejected = .coercion_policy_mismatch };
    return .proven;
}

pub fn canShareDictionary(left: AccessPath, right: AccessPath) bool {
    return sharedDictionaryProof(left, right).safe();
}

pub fn buildFstAlloc(alloc: std.mem.Allocator, labels: []const []const u8) ![]u8 {
    if (labels.len == 0) return try alloc.alloc(u8, 0);
    const sorted = try alloc.dupe([]const u8, labels);
    defer alloc.free(sorted);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn lessThan(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.lessThan);

    var builder = try vellum.Builder.init(alloc, .{
        .registry_table_size = std.math.clamp(labels.len, 64, 65_536),
    });
    defer builder.deinit();
    var last: ?[]const u8 = null;
    for (sorted) |label| {
        if (last) |prior| {
            if (std.mem.eql(u8, prior, label)) continue;
        }
        try builder.insert(label, 0);
        last = label;
    }
    return try builder.finish();
}

pub fn fstContains(fst_bytes: []const u8, label: []const u8) !bool {
    if (fst_bytes.len == 0) return false;
    const fst = try vellum.FST.load(fst_bytes);
    return try fst.contains(label);
}

pub fn fstLabelsWithPrefixAlloc(alloc: std.mem.Allocator, fst_bytes: []const u8, prefix: []const u8) ![][]u8 {
    if (fst_bytes.len == 0) return try alloc.alloc([]u8, 0);
    const fst = try vellum.FST.load(fst_bytes);
    var starts = vellum.StartsWith{ .prefix = prefix };
    var iter = try fst.search(alloc, starts.automaton(), prefix, null);
    defer iter.deinit();
    return try collectFstIteratorLabelsAlloc(alloc, &iter);
}

pub fn fstLabelsInRangeAlloc(
    alloc: std.mem.Allocator,
    fst_bytes: []const u8,
    min: ?[]const u8,
    max: ?[]const u8,
    inclusive_min: bool,
    inclusive_max: bool,
) ![][]u8 {
    if (fst_bytes.len == 0) return try alloc.alloc([]u8, 0);
    const fst = try vellum.FST.load(fst_bytes);
    var iter = try fst.iterator(alloc, min, null);
    defer iter.deinit();
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |label| alloc.free(label);
        out.deinit(alloc);
    }
    while (iter.current()) |entry| : (_ = try iter.nextEntry()) {
        if (min) |lower| {
            const order = std.mem.order(u8, entry.key, lower);
            if (order == .lt or (order == .eq and !inclusive_min)) continue;
        }
        if (max) |upper| {
            const order = std.mem.order(u8, entry.key, upper);
            if (order == .gt or (order == .eq and !inclusive_max)) break;
        }
        try out.append(alloc, try alloc.dupe(u8, entry.key));
    }
    return try out.toOwnedSlice(alloc);
}

pub fn fstLabelsMatchingAutomatonAlloc(
    alloc: std.mem.Allocator,
    fst_bytes: []const u8,
    automaton: vellum.Automaton,
    start: ?[]const u8,
) ![][]u8 {
    if (fst_bytes.len == 0) return try alloc.alloc([]u8, 0);
    const fst = try vellum.FST.load(fst_bytes);
    var iter = try fst.search(alloc, automaton, start, null);
    defer iter.deinit();
    return try collectFstIteratorLabelsAlloc(alloc, &iter);
}

fn collectFstIteratorLabelsAlloc(alloc: std.mem.Allocator, iter: *vellum.FSTIterator) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |label| alloc.free(label);
        out.deinit(alloc);
    }
    while (iter.current()) |entry| : (_ = try iter.nextEntry()) {
        try out.append(alloc, try alloc.dupe(u8, entry.key));
    }
    return try out.toOwnedSlice(alloc);
}

test "lexical access paths keep analyzed text and canonical scalar semantics distinct" {
    const text = AccessPath.analyzedText("body", "default", .prefix);
    const scalar = AccessPath.canonicalScalar("/body", .string, .prefix);

    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .label_kind_mismatch },
        sharedDictionaryProof(text, scalar),
    );
}

test "lexical access paths can share dictionaries only for matching dimensions" {
    const left = AccessPath.canonicalScalar("/customer", .string, .prefix);
    const right = AccessPath.canonicalScalar("/customer", .string, .range);
    const wrong_kind = AccessPath.canonicalScalar("/customer", .number, .range);
    const wrong_path = AccessPath.canonicalScalar("/segment", .string, .range);

    try std.testing.expect(canShareDictionary(left, right));
    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .value_kind_mismatch },
        sharedDictionaryProof(left, wrong_kind),
    );
    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .field_or_path_mismatch },
        sharedDictionaryProof(left, wrong_path),
    );
}

test "dictionary identity prevents duplicate FST ownership for identical semantic label spaces" {
    const alloc = std.testing.allocator;
    const full_text = AccessPath.analyzedTextScoped("docs/body/default", "body", "default", .prefix);
    const algebraic_candidate = AccessPath.analyzedTextScoped("docs/body/default", "body", "default", .regexp);
    const different_scope = AccessPath.analyzedTextScoped("docs/body/v2", "body", "default", .prefix);
    const full_text_key = try full_text.dictionary.keyAlloc(alloc);
    defer alloc.free(full_text_key);
    const algebraic_candidate_key = try algebraic_candidate.dictionary.keyAlloc(alloc);
    defer alloc.free(algebraic_candidate_key);
    const different_scope_key = try different_scope.dictionary.keyAlloc(alloc);
    defer alloc.free(different_scope_key);
    const registry_key = try full_text.dictionary.registryKeyAlloc(alloc);
    defer alloc.free(registry_key);

    try std.testing.expect(DictionaryIdentity.eql(full_text.dictionary, algebraic_candidate.dictionary));
    try std.testing.expect(canShareDictionary(full_text, algebraic_candidate));
    try std.testing.expectEqualStrings(full_text_key, algebraic_candidate_key);
    try std.testing.expect(!std.mem.eql(u8, full_text_key, different_scope_key));
    try std.testing.expect(std.mem.indexOf(u8, registry_key, full_text_key) != null);
    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .scope_mismatch },
        sharedDictionaryProof(full_text, different_scope),
    );
}

test "dictionary registry entry records one physical owner per identity" {
    const alloc = std.testing.allocator;
    const access_path = AccessPath.analyzedTextScoped("docs/body/default", "body", "default", .prefix);
    const registry_key = try access_path.dictionary.registryKeyAlloc(alloc);
    defer alloc.free(registry_key);
    const registry_parts = try token.decodeTupleAlloc(alloc, registry_key);
    defer {
        for (registry_parts) |part| alloc.free(part);
        if (registry_parts.len > 0) alloc.free(registry_parts);
    }
    const entry = RegistryEntry{
        .owner = "fulltext:body:default",
        .layout = .fst_postings,
        .state = "ready",
    };
    const encoded = try entry.encodeAlloc(alloc);
    defer alloc.free(encoded);
    var decoded = try RegistryEntry.decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), registry_parts.len);
    try std.testing.expectEqualStrings("dictionary_registry:v1", registry_parts[0]);
    try std.testing.expectEqualStrings("fulltext:body:default", decoded.owner);
    try std.testing.expectEqual(DictionaryLayoutKind.fst_postings, decoded.layout);
    try std.testing.expectEqualStrings("ready", decoded.state);
}

test "dictionary registry claim preserves a ready owner" {
    const FakeTxn = struct {
        alloc: std.mem.Allocator,
        rows: std.StringHashMapUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            var iter = self.rows.iterator();
            while (iter.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            self.rows.deinit(self.alloc);
        }

        fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }

        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.rows.fetchRemove(key)) |removed| {
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
            }
            const owned_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(owned_key);
            const owned_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(owned_value);
            try self.rows.put(self.alloc, owned_key, owned_value);
        }
    };

    const alloc = std.testing.allocator;
    var txn = FakeTxn{ .alloc = alloc };
    defer txn.deinit();
    const identity = DictionaryIdentity.analyzedText("docs/body/default", "body", "default");

    try std.testing.expectEqual(
        RegistryClaim.claimed,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "fulltext:body:default", .fst_postings, "ready"),
    );
    try std.testing.expectEqual(
        RegistryClaim.already_owned,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "fulltext:body:default", .fst_postings, "ready"),
    );
    try std.testing.expectEqual(
        RegistryClaim.owned_by_other,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "algebraic:body:default", .lexicon_postings_rows, "ready"),
    );

    const key = try identity.registryKeyAlloc(alloc);
    defer alloc.free(key);
    var decoded = try RegistryEntry.decodeAlloc(alloc, try txn.get(key));
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings("fulltext:body:default", decoded.owner);
    try std.testing.expectEqual(DictionaryLayoutKind.fst_postings, decoded.layout);
}

test "dictionary registry claim preserves non-ready owners" {
    const FakeTxn = struct {
        alloc: std.mem.Allocator,
        rows: std.StringHashMapUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            var iter = self.rows.iterator();
            while (iter.next()) |entry| {
                self.alloc.free(entry.key_ptr.*);
                self.alloc.free(entry.value_ptr.*);
            }
            self.rows.deinit(self.alloc);
        }

        fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }

        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.rows.fetchRemove(key)) |removed| {
                self.alloc.free(removed.key);
                self.alloc.free(removed.value);
            }
            const owned_key = try self.alloc.dupe(u8, key);
            errdefer self.alloc.free(owned_key);
            const owned_value = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(owned_value);
            try self.rows.put(self.alloc, owned_key, owned_value);
        }
    };

    const alloc = std.testing.allocator;
    var txn = FakeTxn{ .alloc = alloc };
    defer txn.deinit();
    const identity = DictionaryIdentity.analyzedText("docs/body/default", "body", "default");

    try std.testing.expectEqual(
        RegistryClaim.claimed,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "fulltext:body:default", .fst_postings, "building"),
    );
    try std.testing.expectEqual(
        RegistryClaim.owned_by_other,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "algebraic:body:default", .lexicon_postings_rows, "ready"),
    );
    try std.testing.expectEqual(
        RegistryClaim.owned_by_other,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "fulltext:body:default", .lexicon_postings_rows, "ready"),
    );
    try std.testing.expectEqual(
        RegistryClaim.claimed,
        try claimRegistryOwnerTxn(alloc, &txn, identity, "fulltext:body:default", .fst_postings, "ready"),
    );

    const key = try identity.registryKeyAlloc(alloc);
    defer alloc.free(key);
    var decoded = try RegistryEntry.decodeAlloc(alloc, try txn.get(key));
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings("fulltext:body:default", decoded.owner);
    try std.testing.expectEqual(DictionaryLayoutKind.fst_postings, decoded.layout);
    try std.testing.expectEqualStrings("ready", decoded.state);
}

test "canonical scalar dictionaries remain separate from analyzed terms and coercion policy changes" {
    const strict = AccessPath{
        .dictionary = DictionaryIdentity.canonicalScalar("docs/path/customer", "/customer", .string, "json-scalar-v1", "kind-qualified"),
        .selector = .term,
    };
    const coerced = AccessPath{
        .dictionary = DictionaryIdentity.canonicalScalar("docs/path/customer", "/customer", .string, "json-scalar-v1", "string-coerce"),
        .selector = .term,
    };

    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .coercion_policy_mismatch },
        sharedDictionaryProof(strict, coerced),
    );
}

test "dictionary identity covers sparse token and graph label spaces without sharing analyzed text" {
    const sparse = AccessPath{
        .dictionary = DictionaryIdentity.sparseToken("docs/sparse/body", "body", "splade-v1", "token"),
        .selector = .term,
    };
    const same_sparse = AccessPath{
        .dictionary = DictionaryIdentity.sparseToken("docs/sparse/body", "body", "splade-v1", "token"),
        .selector = .prefix,
    };
    const graph = AccessPath{
        .dictionary = DictionaryIdentity.graphLabel("graph/edges", "edge_type", "graph-label-v1", "string"),
        .selector = .term,
    };
    const analyzed = AccessPath.analyzedTextScoped("docs/sparse/body", "body", "splade-v1", .term);
    const analyzed_graph_label = AccessPath.analyzedTextScoped("graph/edges", "edge_type", "graph-label-v1", .term);

    try std.testing.expect(canShareDictionary(sparse, same_sparse));
    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .label_kind_mismatch },
        sharedDictionaryProof(sparse, analyzed),
    );
    try std.testing.expectEqual(
        AccessPathProof{ .rejected = .label_kind_mismatch },
        sharedDictionaryProof(graph, analyzed_graph_label),
    );
}

test "dictionary registry claim fails closed for malformed existing owner rows" {
    const FakeTxn = struct {
        payload: []const u8,

        fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.payload;
        }

        fn put(_: *@This(), _: []const u8, _: []const u8) anyerror!void {
            return error.UnexpectedPut;
        }
    };

    const alloc = std.testing.allocator;
    var txn = FakeTxn{ .payload = "not-a-registry-entry" };
    const identity = DictionaryIdentity.analyzedText("docs/body/default", "body", "default");

    try std.testing.expectError(
        error.InvalidDictionaryRegistryEntry,
        claimRegistryOwnerTxn(alloc, &txn, identity, "algebraic:body:default", .lexicon_postings_rows, "ready"),
    );
}

test "lexical FST artifact supports exact prefix range and automaton traversal" {
    const alloc = std.testing.allocator;
    const labels = [_][]const u8{ "gold", "silver", "bronze", "gold", "goose" };
    const fst_bytes = try buildFstAlloc(alloc, labels[0..]);
    defer alloc.free(fst_bytes);

    try std.testing.expect(try fstContains(fst_bytes, "gold"));
    try std.testing.expect(!(try fstContains(fst_bytes, "platinum")));

    const prefix = try fstLabelsWithPrefixAlloc(alloc, fst_bytes, "go");
    defer {
        for (prefix) |label| alloc.free(label);
        alloc.free(prefix);
    }
    try std.testing.expectEqual(@as(usize, 2), prefix.len);
    try std.testing.expectEqualStrings("gold", prefix[0]);
    try std.testing.expectEqualStrings("goose", prefix[1]);

    const ranged = try fstLabelsInRangeAlloc(alloc, fst_bytes, "go", "h", true, false);
    defer {
        for (ranged) |label| alloc.free(label);
        alloc.free(ranged);
    }
    try std.testing.expectEqual(@as(usize, 2), ranged.len);

    var starts = vellum.StartsWith{ .prefix = "br" };
    const matched = try fstLabelsMatchingAutomatonAlloc(alloc, fst_bytes, starts.automaton(), "br");
    defer {
        for (matched) |label| alloc.free(label);
        alloc.free(matched);
    }
    try std.testing.expectEqual(@as(usize, 1), matched.len);
    try std.testing.expectEqualStrings("bronze", matched[0]);
}
