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

//! Named context sources for asset-producer enrichments.
//!
//! A context source declares input that lives outside the enrichment's source
//! row: one referenced document (`document`) or the bounded matching set of a
//! parameterized equality query (`query`). The runtime renders the projected
//! values into a canonical `{"context":{...}}` block that participates in the
//! producer skip state, and registers *subscriptions* so a later change to an
//! input invalidates the dependent artifact.
//!
//! This module is pure: it owns the declaration grammar, typed `$source`
//! bindings, predicate evaluation, canonical projection/rendering, input
//! fingerprints, and subscription tuples. Storage of subscriptions, freshness,
//! and manifests lives in `context_dependency_store.zig`.
//!
//! Subscription tuples are the indexed invalidation strategy. A dependent row
//! subscribes to the digests of its *bound* predicate tuples; a changed input
//! row computes the tuples it matches before and after the change. Equal
//! digests identify exactly the dependents whose matching set may differ, so
//! invalidation never scans dependent artifacts. Document sources subscribe to
//! the referenced key itself, which also detects a missing optional document
//! becoming present.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const Digest = [Sha256.digest_length]u8;

pub const max_sources: usize = 16;
pub const max_conjuncts: usize = 16;
pub const max_fields: usize = 64;
pub const max_set_values: usize = 256;
pub const max_name_bytes: usize = 128;
pub const max_pointer_bytes: usize = 512;
pub const default_max_documents: u32 = 100;
pub const max_max_documents: u32 = 10_000;
pub const default_max_bytes: u64 = 1024 * 1024;
pub const max_max_bytes: u64 = 16 * 1024 * 1024;
pub const max_debounce_ms: u64 = 24 * 60 * 60 * 1000;
/// A changed input row whose array-valued predicate fields expand to more
/// tuples than this invalidates every subscriber of the source instead. That
/// stays correct (over-invalidation is safe, skip state suppresses identical
/// inputs) while bounding per-change work.
pub const max_row_tuples: usize = 256;

pub const Error = error{
    InvalidContextSourcesConfig,
    UnsupportedContextPredicate,
    ContextBudgetExceeded,
    RequiredContextMissing,
    InvalidContextBinding,
    UnknownContextSourceTable,
    OutOfMemory,
};

pub const SourceKind = enum { document, query };
pub const RefreshMode = enum { background, manual };
pub const Overflow = enum { @"error" };

/// One predicate conjunct. `term` requires the candidate field to equal one
/// value; `terms` requires membership in a set. A value is either a literal
/// (canonical JSON scalar bytes) or a typed binding from the source document
/// (`{"$source":"/json/pointer"}`); bindings are never text-interpolated.
pub const Conjunct = struct {
    field: []const u8,
    op: Op,
    /// Literal canonical scalar values (one for `term`, any for `terms`).
    literals: []const []const u8 = &.{},
    /// JSON pointer into the source document when bound.
    source_pointer: []const u8 = "",

    pub const Op = enum { term, terms };

    pub fn isBound(self: Conjunct) bool {
        return self.source_pointer.len != 0;
    }
};

pub const Source = struct {
    name: []const u8,
    kind: SourceKind,
    /// Public table name. Empty means the enrichment's own table.
    table: []const u8 = "",
    /// Immutable table identity resolved at admission. Empty means the
    /// enrichment's own table. Renaming or recreating a table changes the
    /// identity, so a dependency can never silently retarget.
    table_id: []const u8 = "",
    /// `document` only: JSON pointer yielding the referenced key.
    key_from: []const u8 = "",
    /// `query` only.
    conjuncts: []const Conjunct = &.{},
    /// Projected fields consumed by the producer, sorted and unique.
    fields: []const []const u8 = &.{},
    /// `document` only: a missing referenced document fails explicitly.
    required: bool = false,
    max_documents: u32 = default_max_documents,
    max_bytes: u64 = default_max_bytes,
    overflow: Overflow = .@"error",

    /// Stable identity of the declaration (name, target, predicate shape, and
    /// projection). Subscriptions are keyed by it, so changing a source's
    /// meaning naturally orphans the old subscriptions instead of matching
    /// them against a different predicate.
    pub fn specDigest(self: Source) Digest {
        var h = Sha256.init(.{});
        hashField(&h, "antfly.context_source.v1");
        hashField(&h, self.name);
        hashField(&h, @tagName(self.kind));
        hashField(&h, self.table_id);
        hashField(&h, self.key_from);
        for (self.conjuncts) |c| {
            hashField(&h, c.field);
            hashField(&h, @tagName(c.op));
            hashField(&h, c.source_pointer);
            for (c.literals) |lit| hashField(&h, lit);
            hashField(&h, "|");
        }
        for (self.fields) |field| hashField(&h, field);
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }

    pub fn targetsOwnTable(self: Source) bool {
        return self.table_id.len == 0;
    }
};

pub const Refresh = struct {
    mode: RefreshMode = .background,
    debounce_ms: u64 = 0,
};

/// Parsed, canonical declaration. Every string is owned by `arena`.
pub const Config = struct {
    arena: std.heap.ArenaAllocator,
    sources: []const Source,
    refresh: Refresh,

    pub fn deinit(self: *Config) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn find(self: *const Config, name: []const u8) ?*const Source {
        for (self.sources) |*source| if (std.mem.eql(u8, source.name, name)) return source;
        return null;
    }

    /// Canonical stored form: `{"sources":{...sorted...},"refresh":{...}}`.
    pub fn canonicalJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"sources\":{");
        for (self.sources, 0..) |source, i| {
            if (i != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, source.name);
            try out.append(alloc, ':');
            try appendSourceJson(alloc, &out, source, true);
        }
        try out.appendSlice(alloc, "},\"refresh\":{\"mode\":");
        try appendJsonString(alloc, &out, @tagName(self.refresh.mode));
        try out.appendSlice(alloc, ",\"debounce_ms\":");
        try out.print(alloc, "{d}", .{self.refresh.debounce_ms});
        try out.appendSlice(alloc, "}}");
        return try out.toOwnedSlice(alloc);
    }

    /// Public form of `context_sources` (without identities).
    pub fn publicSourcesJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.append(alloc, '{');
        for (self.sources, 0..) |source, i| {
            if (i != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, source.name);
            try out.append(alloc, ':');
            try appendSourceJson(alloc, &out, source, false);
        }
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    pub fn publicRefreshJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"mode\":");
        try appendJsonString(alloc, &out, @tagName(self.refresh.mode));
        if (self.refresh.debounce_ms != 0) {
            try out.appendSlice(alloc, ",\"debounce\":");
            var buf: [32]u8 = undefined;
            try appendJsonString(alloc, &out, formatDurationMs(&buf, self.refresh.debounce_ms));
        }
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    pub fn digest(self: *const Config) Digest {
        var h = Sha256.init(.{});
        for (self.sources) |source| h.update(&source.specDigest());
        hashField(&h, @tagName(self.refresh.mode));
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }
};

fn appendSourceJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), source: Source, with_identity: bool) !void {
    try out.append(alloc, '{');
    switch (source.kind) {
        .document => {
            try out.appendSlice(alloc, "\"document\":{");
            try appendTableJson(alloc, out, source, with_identity);
            try out.appendSlice(alloc, "\"key_from\":");
            try appendJsonString(alloc, out, source.key_from);
            try out.append(alloc, '}');
        },
        .query => {
            try out.appendSlice(alloc, "\"query\":{");
            try appendTableJson(alloc, out, source, with_identity);
            try out.appendSlice(alloc, "\"filter_query\":{\"conjuncts\":[");
            for (source.conjuncts, 0..) |c, i| {
                if (i != 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, "{\"field\":");
                try appendJsonString(alloc, out, c.field);
                try out.append(alloc, ',');
                try appendJsonString(alloc, out, @tagName(c.op));
                try out.append(alloc, ':');
                if (c.isBound()) {
                    try out.appendSlice(alloc, "{\"$source\":");
                    try appendJsonString(alloc, out, c.source_pointer);
                    try out.append(alloc, '}');
                } else if (c.op == .term) {
                    try out.appendSlice(alloc, c.literals[0]);
                } else {
                    try out.append(alloc, '[');
                    for (c.literals, 0..) |lit, j| {
                        if (j != 0) try out.append(alloc, ',');
                        try out.appendSlice(alloc, lit);
                    }
                    try out.append(alloc, ']');
                }
                try out.append(alloc, '}');
            }
            try out.appendSlice(alloc, "]}}");
        },
    }
    try out.appendSlice(alloc, ",\"fields\":[");
    for (source.fields, 0..) |field, i| {
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
    if (source.kind == .document) {
        try out.appendSlice(alloc, if (source.required) ",\"required\":true" else ",\"required\":false");
    } else {
        try out.print(alloc, ",\"max_documents\":{d}", .{source.max_documents});
    }
    try out.print(alloc, ",\"max_bytes\":{d},\"overflow\":\"error\"}}", .{source.max_bytes});
}

fn appendTableJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), source: Source, with_identity: bool) !void {
    if (source.table.len != 0) {
        try out.appendSlice(alloc, "\"table\":");
        try appendJsonString(alloc, out, source.table);
        try out.append(alloc, ',');
    }
    if (with_identity and source.table_id.len != 0) {
        try out.appendSlice(alloc, "\"table_id\":");
        try appendJsonString(alloc, out, source.table_id);
        try out.append(alloc, ',');
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse the canonical stored form produced by `Config.canonicalJsonAlloc`.
pub fn parseStoredJson(alloc: Allocator, raw: []const u8) Error!Config {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidContextSourcesConfig;
    const sources = parsed.value.object.get("sources") orelse return error.InvalidContextSourcesConfig;
    const refresh = parsed.value.object.get("refresh");
    return parsePublicValue(alloc, sources, refresh, true);
}

/// Parse public `context_sources` and optional `refresh` objects. Unknown
/// fields and unsupported predicates are rejected at admission so a typo can
/// never silently widen or drop a dependency.
pub fn parsePublicValue(alloc: Allocator, sources_value: std.json.Value, refresh_value: ?std.json.Value, allow_identity: bool) Error!Config {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    if (sources_value != .object) return error.InvalidContextSourcesConfig;
    const count = sources_value.object.count();
    if (count == 0 or count > max_sources) return error.InvalidContextSourcesConfig;
    var sources = try a.alloc(Source, count);
    var it = sources_value.object.iterator();
    var i: usize = 0;
    while (it.next()) |entry| : (i += 1) {
        sources[i] = try parseSource(a, entry.key_ptr.*, entry.value_ptr.*, allow_identity);
    }
    std.mem.sort(Source, sources, {}, struct {
        fn lt(_: void, l: Source, r: Source) bool {
            return std.mem.lessThan(u8, l.name, r.name);
        }
    }.lt);
    var refresh = Refresh{};
    if (refresh_value) |value| switch (value) {
        .null => {},
        .object => |object| {
            var rit = object.iterator();
            while (rit.next()) |entry| {
                const key = entry.key_ptr.*;
                const field = entry.value_ptr.*;
                if (std.mem.eql(u8, key, "mode")) {
                    if (field != .string) return error.InvalidContextSourcesConfig;
                    refresh.mode = std.meta.stringToEnum(RefreshMode, field.string) orelse return error.InvalidContextSourcesConfig;
                } else if (std.mem.eql(u8, key, "debounce")) {
                    if (field != .string) return error.InvalidContextSourcesConfig;
                    refresh.debounce_ms = parseDurationMs(field.string) orelse return error.InvalidContextSourcesConfig;
                } else if (std.mem.eql(u8, key, "debounce_ms")) {
                    if (field != .integer or field.integer < 0) return error.InvalidContextSourcesConfig;
                    refresh.debounce_ms = @intCast(field.integer);
                } else return error.InvalidContextSourcesConfig;
            }
            if (refresh.debounce_ms > max_debounce_ms) return error.InvalidContextSourcesConfig;
        },
        else => return error.InvalidContextSourcesConfig,
    };
    return .{ .arena = arena, .sources = sources, .refresh = refresh };
}

pub fn parsePublicJson(alloc: Allocator, sources_raw: []const u8, refresh_raw: []const u8) Error!Config {
    var sources = std.json.parseFromSlice(std.json.Value, alloc, sources_raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer sources.deinit();
    if (refresh_raw.len == 0) return parsePublicValue(alloc, sources.value, null, false);
    var refresh = std.json.parseFromSlice(std.json.Value, alloc, refresh_raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer refresh.deinit();
    return parsePublicValue(alloc, sources.value, refresh.value, false);
}

fn parseSource(a: Allocator, name: []const u8, value: std.json.Value, allow_identity: bool) Error!Source {
    if (!validName(name)) return error.InvalidContextSourcesConfig;
    if (value != .object) return error.InvalidContextSourcesConfig;
    var source = Source{ .name = try a.dupe(u8, name), .kind = .document };
    var seen_target = false;
    var seen_required = false;
    var seen_max_documents = false;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "document") or std.mem.eql(u8, key, "query")) {
            if (seen_target) return error.InvalidContextSourcesConfig;
            seen_target = true;
            if (field != .object) return error.InvalidContextSourcesConfig;
            source.kind = if (key[0] == 'd') .document else .query;
            try parseTarget(a, &source, field.object, allow_identity);
        } else if (std.mem.eql(u8, key, "fields")) {
            if (field != .array or field.array.items.len == 0 or field.array.items.len > max_fields)
                return error.InvalidContextSourcesConfig;
            for (field.array.items) |item| {
                if (item != .string or !validFieldPath(item.string)) return error.InvalidContextSourcesConfig;
                try fields.append(a, try a.dupe(u8, item.string));
            }
        } else if (std.mem.eql(u8, key, "required")) {
            if (field != .bool) return error.InvalidContextSourcesConfig;
            source.required = field.bool;
            seen_required = true;
        } else if (std.mem.eql(u8, key, "max_documents")) {
            if (field != .integer or field.integer < 1 or field.integer > max_max_documents)
                return error.InvalidContextSourcesConfig;
            source.max_documents = @intCast(field.integer);
            seen_max_documents = true;
        } else if (std.mem.eql(u8, key, "max_bytes")) {
            if (field != .integer or field.integer < 1 or field.integer > max_max_bytes)
                return error.InvalidContextSourcesConfig;
            source.max_bytes = @intCast(field.integer);
        } else if (std.mem.eql(u8, key, "overflow")) {
            // Truncated context must never publish as complete; `error` is the
            // only admitted overflow policy.
            if (field != .string or !std.mem.eql(u8, field.string, "error")) return error.InvalidContextSourcesConfig;
        } else return error.InvalidContextSourcesConfig;
    }
    if (!seen_target or fields.items.len == 0) return error.InvalidContextSourcesConfig;
    if (source.kind == .query and seen_required) return error.InvalidContextSourcesConfig;
    if (source.kind == .document and seen_max_documents) return error.InvalidContextSourcesConfig;
    std.mem.sort([]const u8, fields.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    for (fields.items[1..], 1..) |field, idx| {
        if (std.mem.eql(u8, field, fields.items[idx - 1])) return error.InvalidContextSourcesConfig;
    }
    source.fields = fields.items;
    return source;
}

fn parseTarget(a: Allocator, source: *Source, object: std.json.ObjectMap, allow_identity: bool) Error!void {
    var it = object.iterator();
    var seen_filter = false;
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "table")) {
            if (field != .string or !validName(field.string)) return error.InvalidContextSourcesConfig;
            source.table = try a.dupe(u8, field.string);
        } else if (allow_identity and std.mem.eql(u8, key, "table_id")) {
            if (field != .string or field.string.len == 0 or field.string.len > max_name_bytes) return error.InvalidContextSourcesConfig;
            source.table_id = try a.dupe(u8, field.string);
        } else if (source.kind == .document and std.mem.eql(u8, key, "key_from")) {
            if (field != .string or !validPointer(field.string)) return error.InvalidContextSourcesConfig;
            source.key_from = try a.dupe(u8, field.string);
        } else if (source.kind == .query and std.mem.eql(u8, key, "filter_query")) {
            seen_filter = true;
            source.conjuncts = try parseFilter(a, field);
        } else return error.InvalidContextSourcesConfig;
    }
    switch (source.kind) {
        .document => if (source.key_from.len == 0) return error.InvalidContextSourcesConfig,
        .query => if (!seen_filter) return error.InvalidContextSourcesConfig,
    }
}

/// Admitted grammar: `{"conjuncts":[...]}` of term/terms conjuncts, or a
/// single bare term/terms conjunct. Anything else (ranges, disjunctions,
/// full-text, semantic top-k) is rejected so the runtime never advertises
/// invalidation it cannot index.
fn parseFilter(a: Allocator, value: std.json.Value) Error![]const Conjunct {
    if (value != .object) return error.UnsupportedContextPredicate;
    var list = std.ArrayListUnmanaged(Conjunct).empty;
    if (value.object.get("conjuncts")) |conjuncts| {
        if (value.object.count() != 1) return error.UnsupportedContextPredicate;
        if (conjuncts != .array or conjuncts.array.items.len == 0 or conjuncts.array.items.len > max_conjuncts)
            return error.UnsupportedContextPredicate;
        for (conjuncts.array.items) |item| try list.append(a, try parseConjunct(a, item));
    } else {
        try list.append(a, try parseConjunct(a, value));
    }
    // Canonical member order: conjunction is commutative, so sort by field
    // then operator so equivalent declarations share one identity.
    std.mem.sort(Conjunct, list.items, {}, struct {
        fn lt(_: void, l: Conjunct, r: Conjunct) bool {
            return switch (std.mem.order(u8, l.field, r.field)) {
                .lt => true,
                .gt => false,
                .eq => @backingInt(l.op) < @backingInt(r.op),
            };
        }
    }.lt);
    var bound_sets: usize = 0;
    for (list.items) |c| if (c.op == .terms and c.isBound()) {
        bound_sets += 1;
    };
    // A dependent subscribes to the cartesian product of its bound sets; one
    // bound set keeps that linear in the bound values.
    if (bound_sets > 1) return error.UnsupportedContextPredicate;
    return list.items;
}

fn parseConjunct(a: Allocator, value: std.json.Value) Error!Conjunct {
    if (value != .object) return error.UnsupportedContextPredicate;
    var field_name: ?[]const u8 = null;
    var conjunct = Conjunct{ .field = "", .op = .term };
    var seen_value = false;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "field")) {
            if (field != .string or !validFieldPath(field.string)) return error.InvalidContextSourcesConfig;
            field_name = try a.dupe(u8, field.string);
        } else if (std.mem.eql(u8, key, "term") or std.mem.eql(u8, key, "terms")) {
            if (seen_value) return error.UnsupportedContextPredicate;
            seen_value = true;
            conjunct.op = if (key.len == 4) .term else .terms;
            if (try sourceBinding(a, field)) |pointer| {
                conjunct.source_pointer = pointer;
            } else if (conjunct.op == .term) {
                const lit = (try canonicalScalarAlloc(a, field)) orelse return error.UnsupportedContextPredicate;
                const lits = try a.alloc([]const u8, 1);
                lits[0] = lit;
                conjunct.literals = lits;
            } else {
                if (field != .array or field.array.items.len == 0 or field.array.items.len > max_set_values)
                    return error.UnsupportedContextPredicate;
                var lits = std.ArrayListUnmanaged([]const u8).empty;
                for (field.array.items) |item| {
                    const lit = (try canonicalScalarAlloc(a, item)) orelse return error.UnsupportedContextPredicate;
                    try lits.append(a, lit);
                }
                sortUniqueStrings(&lits);
                conjunct.literals = lits.items;
            }
        } else return error.UnsupportedContextPredicate;
    }
    conjunct.field = field_name orelse return error.UnsupportedContextPredicate;
    if (!seen_value) return error.UnsupportedContextPredicate;
    return conjunct;
}

fn sourceBinding(a: Allocator, value: std.json.Value) Error!?[]const u8 {
    if (value != .object) return null;
    const pointer = value.object.get("$source") orelse return null;
    if (value.object.count() != 1) return error.UnsupportedContextPredicate;
    if (pointer != .string or !validPointer(pointer.string)) return error.InvalidContextSourcesConfig;
    return try a.dupe(u8, pointer.string);
}

fn validName(name: []const u8) bool {
    if (name.len == 0 or name.len > max_name_bytes) return false;
    for (name) |c| switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9', '_', '-', '.' => {},
        else => return false,
    };
    return true;
}

/// Field paths are dotted top-level names. Fields starting with `_` are
/// reserved for engine-owned metadata (artifacts, embeddings, chunks); reading
/// them would let a context source observe generated output and form a hidden
/// producer feedback loop, so they are rejected.
fn validFieldPath(path: []const u8) bool {
    if (path.len == 0 or path.len > max_pointer_bytes) return false;
    if (path[0] == '_') return false;
    var segments = std.mem.splitScalar(u8, path, '.');
    while (segments.next()) |segment| if (segment.len == 0) return false;
    return true;
}

fn validPointer(pointer: []const u8) bool {
    return pointer.len > 1 and pointer.len <= max_pointer_bytes and pointer[0] == '/';
}

fn sortUniqueStrings(list: *std.ArrayListUnmanaged([]const u8)) void {
    std.mem.sort([]const u8, list.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    var w: usize = 0;
    for (list.items) |item| {
        if (w != 0 and std.mem.eql(u8, list.items[w - 1], item)) continue;
        list.items[w] = item;
        w += 1;
    }
    list.items.len = w;
}

pub fn parseDurationMs(text: []const u8) ?u64 {
    if (text.len < 2) return null;
    const Unit = struct { suffix: []const u8, ms: u64 };
    const units = [_]Unit{ .{ .suffix = "ms", .ms = 1 }, .{ .suffix = "s", .ms = 1000 }, .{ .suffix = "m", .ms = 60_000 }, .{ .suffix = "h", .ms = 3_600_000 } };
    for (units) |unit| {
        if (!std.mem.endsWith(u8, text, unit.suffix)) continue;
        const digits = text[0 .. text.len - unit.suffix.len];
        if (digits.len == 0) return null;
        for (digits) |c| if (c < '0' or c > '9') {
            // "ms" also ends with "s": let the loop try the next unit.
            if (unit.ms != 1000 or !std.mem.endsWith(u8, text, "ms")) return null;
            break;
        };
        const n = std.fmt.parseInt(u64, digits, 10) catch continue;
        return std.math.mul(u64, n, unit.ms) catch null;
    }
    return null;
}

fn formatDurationMs(buf: []u8, ms: u64) []const u8 {
    if (ms % 3_600_000 == 0) return std.fmt.bufPrint(buf, "{d}h", .{ms / 3_600_000}) catch unreachable;
    if (ms % 60_000 == 0) return std.fmt.bufPrint(buf, "{d}m", .{ms / 60_000}) catch unreachable;
    if (ms % 1000 == 0) return std.fmt.bufPrint(buf, "{d}s", .{ms / 1000}) catch unreachable;
    return std.fmt.bufPrint(buf, "{d}ms", .{ms}) catch unreachable;
}

/// Host hook that maps a public table name to its immutable identity. Hosts
/// return null for unknown or unauthorized tables (admission then fails), and
/// should map the enrichment's own table to its own identity.
pub const TableResolver = struct {
    ptr: *anyopaque,
    resolve_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table: []const u8) anyerror!?[]u8,

    pub fn resolve(self: TableResolver, alloc: Allocator, table: []const u8) !?[]u8 {
        return self.resolve_fn(self.ptr, alloc, table);
    }
};

/// Canonicalize public `context_sources` (and optional `refresh`) into the
/// stored form, resolving every named table through `resolver`. Without a
/// resolver, named tables stay unresolved and catalog admission rejects them;
/// unnamed sources target the enrichment's own table.
pub fn canonicalFromPublicAlloc(alloc: Allocator, sources_value: std.json.Value, refresh_value: ?std.json.Value, resolver: ?TableResolver) ![]u8 {
    var config = try parsePublicValue(alloc, sources_value, refresh_value, false);
    defer config.deinit();
    try resolveTables(alloc, &config, resolver);
    return config.canonicalJsonAlloc(alloc);
}

/// Re-resolve table identities of an already stored declaration from table
/// names (restore, import, or a host re-validating a client-supplied stored
/// form). Client-provided identities are never trusted.
pub fn reresolveStoredAlloc(alloc: Allocator, stored: []const u8, resolver: ?TableResolver) ![]u8 {
    var config = try parseStoredJson(alloc, stored);
    defer config.deinit();
    for (config.sources) |*source| @constCast(source).table_id = "";
    try resolveTables(alloc, &config, resolver);
    return config.canonicalJsonAlloc(alloc);
}

fn resolveTables(alloc: Allocator, config: *Config, resolver: ?TableResolver) !void {
    const r = resolver orelse return;
    const a = config.arena.allocator();
    for (config.sources) |*source| {
        if (source.table.len == 0) continue;
        const id = (try r.resolve(alloc, source.table)) orelse return error.UnknownContextSourceTable;
        defer alloc.free(id);
        if (id.len == 0 or id.len > max_name_bytes) return error.UnknownContextSourceTable;
        @constCast(source).table_id = try a.dupe(u8, id);
    }
}

/// Distinct foreign table identities referenced by a declaration.
pub fn foreignTableIdsAlloc(alloc: Allocator, config: *const Config, own_table_id: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);
    for (config.sources) |source| {
        if (source.table_id.len == 0 or std.mem.eql(u8, source.table_id, own_table_id)) continue;
        var seen = false;
        for (out.items) |id| if (std.mem.eql(u8, id, source.table_id)) {
            seen = true;
        };
        if (!seen) try out.append(alloc, source.table_id);
    }
    return try out.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// Canonical scalars, pointers, projection
// ---------------------------------------------------------------------------

/// Canonical bytes for a JSON scalar. Integral floats collapse to integers so
/// `1` and `1.0` bind identically. Objects/arrays/null are not scalars.
pub fn canonicalScalarAlloc(alloc: Allocator, value: std.json.Value) Allocator.Error!?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    switch (value) {
        .string => |s| try appendJsonString(alloc, &out, s),
        .integer => |n| try out.print(alloc, "{d}", .{n}),
        .number_string => |s| {
            const f = std.fmt.parseFloat(f64, s) catch return null;
            try appendCanonicalFloat(alloc, &out, f);
        },
        .float => |f| try appendCanonicalFloat(alloc, &out, f),
        .bool => |b| try out.appendSlice(alloc, if (b) "true" else "false"),
        else => return null,
    }
    return try out.toOwnedSlice(alloc);
}

fn appendCanonicalFloat(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), f: f64) !void {
    if (std.math.isFinite(f) and @floor(f) == f and @abs(f) < 9.007199254740992e15) {
        try out.print(alloc, "{d}", .{@as(i64, @intFromFloat(f))});
    } else {
        try out.print(alloc, "{e}", .{f});
    }
}

/// RFC 6901 JSON pointer lookup.
pub fn resolvePointer(root: std.json.Value, pointer: []const u8) ?std.json.Value {
    if (pointer.len == 0) return root;
    if (pointer[0] != '/') return null;
    var current = root;
    var tokens = std.mem.splitScalar(u8, pointer[1..], '/');
    var buf: [max_pointer_bytes]u8 = undefined;
    while (tokens.next()) |raw_token| {
        const token = unescapePointerToken(&buf, raw_token) orelse return null;
        switch (current) {
            .object => |object| current = object.get(token) orelse return null,
            .array => |array| {
                const index = std.fmt.parseInt(usize, token, 10) catch return null;
                if (index >= array.items.len) return null;
                current = array.items[index];
            },
            else => return null,
        }
    }
    return current;
}

fn unescapePointerToken(buf: []u8, token: []const u8) ?[]const u8 {
    if (std.mem.indexOfScalar(u8, token, '~') == null) return token;
    var w: usize = 0;
    var i: usize = 0;
    while (i < token.len) : (i += 1) {
        if (w >= buf.len) return null;
        if (token[i] == '~') {
            if (i + 1 >= token.len) return null;
            buf[w] = switch (token[i + 1]) {
                '0' => '~',
                '1' => '/',
                else => return null,
            };
            i += 1;
        } else buf[w] = token[i];
        w += 1;
    }
    return buf[0..w];
}

pub fn resolveFieldPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var segments = std.mem.splitScalar(u8, path, '.');
    while (segments.next()) |segment| {
        if (current != .object) return null;
        current = current.object.get(segment) orelse return null;
    }
    return current;
}

/// Canonical projection of the consumed fields: an object with sorted keys
/// (fields are pre-sorted at admission), missing fields omitted, and values
/// serialized with sorted object keys at every depth. Two rows with equal
/// projections are equivalent inputs regardless of unconsumed fields.
pub fn projectAlloc(alloc: Allocator, row: std.json.Value, fields: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    for (fields) |field| {
        const value = resolveFieldPath(row, field) orelse continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, field);
        try out.append(alloc, ':');
        try appendCanonicalValue(alloc, &out, value);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn appendCanonicalValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    switch (value) {
        .null => try out.appendSlice(alloc, "null"),
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var it = object.iterator();
            var i: usize = 0;
            while (it.next()) |entry| : (i += 1) keys[i] = entry.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn lt(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.lt);
            try out.append(alloc, '{');
            for (keys, 0..) |key, j| {
                if (j != 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendCanonicalValue(alloc, out, object.get(key).?);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, j| {
                if (j != 0) try out.append(alloc, ',');
                try appendCanonicalValue(alloc, out, item);
            }
            try out.append(alloc, ']');
        },
        else => {
            const scalar = (try canonicalScalarAlloc(alloc, value)).?;
            defer alloc.free(scalar);
            try out.appendSlice(alloc, scalar);
        },
    }
}

pub fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, s, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn hashField(h: *Sha256, bytes: []const u8) void {
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, bytes.len, .little);
    h.update(&len);
    h.update(bytes);
}

// ---------------------------------------------------------------------------
// Binding (dependent side)
// ---------------------------------------------------------------------------

/// One context source bound against a dependent source document.
pub const Bound = struct {
    source: *const Source,
    spec: Digest,
    /// `document`: the referenced key, null when `key_from` is missing.
    key: ?[]const u8 = null,
    /// `query`: one resolved value list per conjunct (canonical scalars). A
    /// bound set conjunct carries every member value. Empty slice for a
    /// conjunct means the binding was missing: the query matches nothing.
    values: []const []const []const u8 = &.{},
    /// `query`: true when a missing/non-scalar binding makes the match set
    /// statically empty.
    unsatisfiable: bool = false,
    /// Subscription tuples this dependent must register before reading.
    tuples: []const Digest = &.{},
    /// Canonical identity of the bound query (spec plus resolved values).
    query_identity: Digest = @splat(0),
};

pub const Bindings = struct {
    arena: std.heap.ArenaAllocator,
    items: []const Bound,

    pub fn deinit(self: *Bindings) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Union of every tuple this document subscribes to, sorted and unique.
    pub fn allTuplesAlloc(self: *const Bindings, alloc: Allocator) ![]SubscriptionTuple {
        var list = std.ArrayListUnmanaged(SubscriptionTuple).empty;
        errdefer list.deinit(alloc);
        for (self.items) |item| for (item.tuples) |tuple| try list.append(alloc, .{ .spec = item.spec, .tuple = tuple });
        sortUniqueTuples(&list);
        return try list.toOwnedSlice(alloc);
    }
};

pub const SubscriptionTuple = struct {
    spec: Digest,
    tuple: Digest,

    pub fn lessThan(_: void, l: SubscriptionTuple, r: SubscriptionTuple) bool {
        return switch (std.mem.order(u8, &l.spec, &r.spec)) {
            .lt => true,
            .gt => false,
            .eq => std.mem.lessThan(u8, &l.tuple, &r.tuple),
        };
    }

    pub fn eql(l: SubscriptionTuple, r: SubscriptionTuple) bool {
        return std.mem.eql(u8, &l.spec, &r.spec) and std.mem.eql(u8, &l.tuple, &r.tuple);
    }
};

pub fn sortUniqueTuples(list: *std.ArrayListUnmanaged(SubscriptionTuple)) void {
    std.mem.sort(SubscriptionTuple, list.items, {}, SubscriptionTuple.lessThan);
    var w: usize = 0;
    for (list.items) |item| {
        if (w != 0 and list.items[w - 1].eql(item)) continue;
        list.items[w] = item;
        w += 1;
    }
    list.items.len = w;
}

pub fn documentTuple(spec: Digest, key: []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "doc");
    h.update(&spec);
    hashField(&h, key);
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

fn queryTuple(spec: Digest, values: []const []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "query");
    h.update(&spec);
    for (values) |value| hashField(&h, value);
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

/// Bind every source against the dependent document. Missing optional bindings
/// are not errors: a document source with no key and a query with an unbound
/// predicate both render as empty. Required documents are checked when the
/// context is resolved, since presence is only known after the read.
pub fn bindAlloc(alloc: Allocator, config: *const Config, source_doc: std.json.Value) Error!Bindings {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const items = try a.alloc(Bound, config.sources.len);
    for (config.sources, 0..) |*source, i| {
        var bound = Bound{ .source = source, .spec = source.specDigest() };
        switch (source.kind) {
            .document => {
                if (resolvePointer(source_doc, source.key_from)) |value| switch (value) {
                    .string => |s| if (s.len != 0) {
                        bound.key = try a.dupe(u8, s);
                    },
                    .integer, .float, .number_string => {
                        const scalar = (try canonicalScalarAlloc(a, value)).?;
                        bound.key = scalar;
                    },
                    else => {},
                };
                if (bound.key) |key| {
                    const tuples = try a.alloc(Digest, 1);
                    tuples[0] = documentTuple(bound.spec, key);
                    bound.tuples = tuples;
                }
            },
            .query => {
                const values = try a.alloc([]const []const u8, source.conjuncts.len);
                for (source.conjuncts, 0..) |c, j| {
                    if (!c.isBound()) {
                        values[j] = c.literals;
                        continue;
                    }
                    const resolved = resolvePointer(source_doc, c.source_pointer) orelse {
                        values[j] = &.{};
                        bound.unsatisfiable = true;
                        continue;
                    };
                    var list = std.ArrayListUnmanaged([]const u8).empty;
                    switch (resolved) {
                        .array => |array| {
                            if (c.op == .term) {
                                bound.unsatisfiable = true;
                            } else {
                                if (array.items.len > max_set_values) return error.ContextBudgetExceeded;
                                for (array.items) |item| {
                                    const scalar = (try canonicalScalarAlloc(a, item)) orelse return error.InvalidContextBinding;
                                    try list.append(a, scalar);
                                }
                            }
                        },
                        .null => bound.unsatisfiable = true,
                        else => {
                            const scalar = (try canonicalScalarAlloc(a, resolved)) orelse return error.InvalidContextBinding;
                            try list.append(a, scalar);
                        },
                    }
                    sortUniqueStrings(&list);
                    if (list.items.len == 0) bound.unsatisfiable = true;
                    values[j] = list.items;
                }
                bound.values = values;
                bound.query_identity = boundQueryIdentity(bound.spec, values);
                if (!bound.unsatisfiable) bound.tuples = try expandTuplesAlloc(a, bound.spec, values, max_set_values);
            },
        }
        items[i] = bound;
    }
    return .{ .arena = arena, .items = items };
}

fn boundQueryIdentity(spec: Digest, values: []const []const []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "query-identity");
    h.update(&spec);
    for (values) |set| {
        for (set) |value| hashField(&h, value);
        hashField(&h, "|");
    }
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

/// Cartesian product of per-conjunct value sets. Literal `terms` sets on the
/// dependent side are expanded too so that a row matches exactly the tuples
/// whose every position it satisfies.
fn expandTuplesAlloc(a: Allocator, spec: Digest, values: []const []const []const u8, cap: usize) Error![]const Digest {
    var total: usize = 1;
    for (values) |set| {
        if (set.len == 0) return &.{};
        total = std.math.mul(usize, total, set.len) catch return error.ContextBudgetExceeded;
        if (total > cap) return error.ContextBudgetExceeded;
    }
    const out = try a.alloc(Digest, total);
    const cursor = try a.alloc(usize, values.len);
    defer a.free(cursor);
    @memset(cursor, 0);
    const scratch = try a.alloc([]const u8, values.len);
    defer a.free(scratch);
    for (out) |*digest| {
        for (values, 0..) |set, j| scratch[j] = set[cursor[j]];
        digest.* = queryTuple(spec, scratch);
        var j = values.len;
        while (j > 0) {
            j -= 1;
            cursor[j] += 1;
            if (cursor[j] < values[j].len) break;
            cursor[j] = 0;
        }
    }
    std.mem.sort(Digest, out, {}, struct {
        fn lt(_: void, l: Digest, r: Digest) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.lt);
    return out;
}

/// Evaluate the bound query predicate against a candidate row.
pub fn rowMatches(bound: *const Bound, row: std.json.Value) bool {
    if (bound.unsatisfiable) return false;
    for (bound.source.conjuncts, bound.values) |c, set| {
        const value = resolveFieldPath(row, c.field) orelse return false;
        if (!valueInSet(value, set)) return false;
    }
    return true;
}

fn valueInSet(value: std.json.Value, set: []const []const u8) bool {
    var buf: [256]u8 = undefined;
    switch (value) {
        .array => |array| {
            for (array.items) |item| if (scalarInSet(&buf, item, set)) return true;
            return false;
        },
        else => return scalarInSet(&buf, value, set),
    }
}

fn scalarInSet(buf: []u8, value: std.json.Value, set: []const []const u8) bool {
    var fba = std.heap.FixedBufferAllocator.init(buf);
    const scalar = (canonicalScalarAlloc(fba.allocator(), value) catch return slowScalarInSet(value, set)) orelse return false;
    for (set) |candidate| if (std.mem.eql(u8, candidate, scalar)) return true;
    return false;
}

fn slowScalarInSet(value: std.json.Value, set: []const []const u8) bool {
    const scalar = (canonicalScalarAlloc(std.heap.page_allocator, value) catch return false) orelse return false;
    defer std.heap.page_allocator.free(scalar);
    for (set) |candidate| if (std.mem.eql(u8, candidate, scalar)) return true;
    return false;
}

// ---------------------------------------------------------------------------
// Change side: which subscriptions does an input row change touch?
// ---------------------------------------------------------------------------

/// Tuples of a candidate row for one query source: for every conjunct, the
/// row values that could satisfy *some* dependent binding (any value for bound
/// conjuncts, only matching literals for literal conjuncts).
pub const RowTuples = union(enum) {
    /// Row matches no dependent (a literal conjunct failed or a field is missing).
    none,
    tuples: []Digest,
    /// Expansion exceeded `max_row_tuples`: notify every subscriber.
    all,

    pub fn deinit(self: *RowTuples, alloc: Allocator) void {
        switch (self.*) {
            .tuples => |t| alloc.free(t),
            else => {},
        }
        self.* = .none;
    }
};

pub fn rowTuplesAlloc(alloc: Allocator, source: *const Source, spec: Digest, row: std.json.Value) !RowTuples {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const sets = try a.alloc([]const []const u8, source.conjuncts.len);
    for (source.conjuncts, 0..) |c, j| {
        const value = resolveFieldPath(row, c.field) orelse return .none;
        var list = std.ArrayListUnmanaged([]const u8).empty;
        const items: []const std.json.Value = switch (value) {
            .array => |array| array.items,
            else => (&value)[0..1],
        };
        for (items) |item| {
            const scalar = (try canonicalScalarAlloc(a, item)) orelse continue;
            if (!c.isBound()) {
                var hit = false;
                for (c.literals) |lit| if (std.mem.eql(u8, lit, scalar)) {
                    hit = true;
                };
                if (!hit) continue;
            }
            try list.append(a, scalar);
        }
        sortUniqueStrings(&list);
        if (list.items.len == 0) return .none;
        sets[j] = list.items;
    }
    const expanded = expandTuplesAlloc(a, spec, sets, max_row_tuples) catch |err| switch (err) {
        error.ContextBudgetExceeded => return .all,
        else => |e| return e,
    };
    return .{ .tuples = try alloc.dupe(Digest, expanded) };
}

pub const ChangeKind = enum {
    member_added,
    member_removed,
    member_changed,
    document_created,
    document_deleted,
    document_changed,

    pub fn jsonName(self: ChangeKind) []const u8 {
        return @tagName(self);
    }
};

pub const TouchedTuple = struct {
    tuple: Digest,
    change: ChangeKind,
};

pub const Touched = struct {
    /// Every subscriber of the source must be notified (row expansion overflow).
    all: bool = false,
    all_change: ChangeKind = .member_changed,
    tuples: []TouchedTuple = &.{},

    pub fn deinit(self: *Touched, alloc: Allocator) void {
        alloc.free(self.tuples);
        self.* = .{};
    }
};

/// Compute the subscription tuples a single input row change touches, given
/// its previous and next values (null when absent). Changes that keep the row
/// in the same tuple with an identical projection of consumed fields touch
/// nothing: irrelevant field updates never invalidate.
pub fn touchedTuplesAlloc(
    alloc: Allocator,
    source: *const Source,
    key: []const u8,
    previous: ?std.json.Value,
    next: ?std.json.Value,
) !Touched {
    const spec = source.specDigest();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const old_projection: ?[]u8 = if (previous) |row| try projectAlloc(a, row, source.fields) else null;
    const new_projection: ?[]u8 = if (next) |row| try projectAlloc(a, row, source.fields) else null;
    const projection_equal = old_projection != null and new_projection != null and
        std.mem.eql(u8, old_projection.?, new_projection.?);

    var out = std.ArrayListUnmanaged(TouchedTuple).empty;
    errdefer out.deinit(alloc);
    switch (source.kind) {
        .document => {
            if (previous == null and next == null) return .{};
            if (projection_equal) return .{};
            const change: ChangeKind = if (previous == null) .document_created else if (next == null) .document_deleted else .document_changed;
            try out.append(alloc, .{ .tuple = documentTuple(spec, key), .change = change });
        },
        .query => {
            var old_tuples: RowTuples = if (previous) |row| try rowTuplesAlloc(a, source, spec, row) else .none;
            var new_tuples: RowTuples = if (next) |row| try rowTuplesAlloc(a, source, spec, row) else .none;
            if (old_tuples == .all or new_tuples == .all) {
                const change: ChangeKind = if (old_tuples == .none) .member_added else if (new_tuples == .none) .member_removed else .member_changed;
                if (projection_equal and old_tuples == .all and new_tuples == .all) {
                    // Same expansion, same projection: membership is unchanged
                    // only if the predicate fields are equal; the projection
                    // does not prove that, so stay conservative.
                }
                return .{ .all = true, .all_change = change };
            }
            const old_list: []const Digest = if (old_tuples == .tuples) old_tuples.tuples else &.{};
            const new_list: []const Digest = if (new_tuples == .tuples) new_tuples.tuples else &.{};
            var i: usize = 0;
            var j: usize = 0;
            while (i < old_list.len or j < new_list.len) {
                const take_old = j >= new_list.len or (i < old_list.len and std.mem.lessThan(u8, &old_list[i], &new_list[j]));
                const take_new = i >= old_list.len or (j < new_list.len and std.mem.lessThan(u8, &new_list[j], &old_list[i]));
                if (take_old) {
                    try out.append(alloc, .{ .tuple = old_list[i], .change = .member_removed });
                    i += 1;
                } else if (take_new) {
                    try out.append(alloc, .{ .tuple = new_list[j], .change = .member_added });
                    j += 1;
                } else {
                    if (!projection_equal) try out.append(alloc, .{ .tuple = old_list[i], .change = .member_changed });
                    i += 1;
                    j += 1;
                }
            }
            old_tuples.deinit(a);
            new_tuples.deinit(a);
        },
    }
    return .{ .tuples = try out.toOwnedSlice(alloc) };
}

// ---------------------------------------------------------------------------
// Resolved context, rendering, fingerprints
// ---------------------------------------------------------------------------

/// One input actually consumed. `revision` is the content digest of the
/// projected values, an engine-neutral revision that is stable across
/// replicas and identical for equivalent inputs.
pub const ConsumedInput = struct {
    key: []const u8,
    projection: []const u8,

    pub fn revision(self: ConsumedInput) Digest {
        var h = Sha256.init(.{});
        hashField(&h, self.key);
        hashField(&h, self.projection);
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }
};

pub const ResolvedSource = struct {
    bound: *const Bound,
    /// Members sorted by key; for `document` at most one.
    members: []const ConsumedInput,
    /// `document`: whether the referenced document existed.
    present: bool = false,
    /// Opaque reader frontier position for the source table/shard(s).
    frontier: []const u8 = "",
};

pub const Rendered = struct {
    /// `{"organization":{...}|null,"policies":{"count":n,"members":[...]}}`.
    json: []u8,
    fingerprint: Digest,
    bytes: u64,

    pub fn deinit(self: *Rendered, alloc: Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

/// Canonically render resolved context and enforce declared budgets. Query
/// members must already be complete: callers pass at most `max_documents + 1`
/// members so overflow is detected rather than silently truncated.
pub fn renderAlloc(alloc: Allocator, resolved: []const ResolvedSource) Error!Rendered {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    for (resolved, 0..) |item, i| {
        const source = item.bound.source;
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, source.name);
        try out.append(alloc, ':');
        const start = out.items.len;
        switch (source.kind) {
            .document => {
                if (!item.present or item.members.len == 0) {
                    if (source.required) return error.RequiredContextMissing;
                    try out.appendSlice(alloc, "null");
                } else {
                    const member = item.members[0];
                    try out.appendSlice(alloc, "{\"key\":");
                    try appendJsonString(alloc, &out, member.key);
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, member.projection);
                    try out.append(alloc, '}');
                }
            },
            .query => {
                if (item.members.len > source.max_documents) return error.ContextBudgetExceeded;
                try out.print(alloc, "{{\"count\":{d},\"members\":[", .{item.members.len});
                var previous: ?[]const u8 = null;
                for (item.members, 0..) |member, j| {
                    if (previous) |p| if (!std.mem.lessThan(u8, p, member.key)) return error.InvalidContextBinding;
                    previous = member.key;
                    if (j != 0) try out.append(alloc, ',');
                    try out.appendSlice(alloc, "{\"key\":");
                    try appendJsonString(alloc, &out, member.key);
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, member.projection);
                    try out.append(alloc, '}');
                }
                try out.appendSlice(alloc, "]}");
            },
        }
        if (out.items.len - start > source.max_bytes) return error.ContextBudgetExceeded;
    }
    try out.append(alloc, '}');
    var fingerprint: Digest = undefined;
    Sha256.hash(out.items, &fingerprint, .{});
    const bytes = out.items.len;
    return .{ .json = try out.toOwnedSlice(alloc), .fingerprint = fingerprint, .bytes = bytes };
}

/// The documented producer input contract appended to rendered source text:
/// a marker line carrying the fingerprint, then `{"context":{...}}`.
pub fn producerBlockAlloc(alloc: Allocator, rendered: *const Rendered) ![]u8 {
    return std.fmt.allocPrint(alloc, "[context sha256:{x}]\n{{\"context\":{s}}}", .{ &rendered.fingerprint, rendered.json });
}

pub fn digestHex(digest: Digest) [Sha256.digest_length * 2]u8 {
    return std.fmt.bytesToHex(digest, .lower);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const issue_example =
    \\{
    \\  "organization": {
    \\    "document": {"table": "organizations", "key_from": "/organization_id"},
    \\    "fields": ["voice", "guidance"],
    \\    "required": true
    \\  },
    \\  "policies": {
    \\    "query": {
    \\      "table": "policies",
    \\      "filter_query": {"conjuncts": [
    \\        {"field": "tenant_id", "term": {"$source": "/tenant_id"}},
    \\        {"field": "organization_id", "term": {"$source": "/organization_id"}},
    \\        {"field": "locale", "term": {"$source": "/locale"}},
    \\        {"field": "status", "term": "approved"}
    \\      ]}
    \\    },
    \\    "fields": ["policy_id", "revision", "payload"],
    \\    "max_documents": 500,
    \\    "max_bytes": 1048576,
    \\    "overflow": "error"
    \\  }
    \\}
;

fn parseTestConfig(alloc: Allocator) !Config {
    return parsePublicJson(alloc, issue_example, "{\"mode\":\"background\",\"debounce\":\"2s\"}");
}

fn parseValue(arena: Allocator, raw: []const u8) !std.json.Value {
    return (try std.json.parseFromSliceLeaky(std.json.Value, arena, raw, .{}));
}

test "context sources parse, canonicalize, and round trip through stored form" {
    const alloc = testing.allocator;
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    try testing.expectEqual(@as(usize, 2), config.sources.len);
    try testing.expectEqualStrings("organization", config.sources[0].name);
    try testing.expectEqual(SourceKind.document, config.sources[0].kind);
    try testing.expect(config.sources[0].required);
    try testing.expectEqualStrings("guidance", config.sources[0].fields[0]);
    const policies = config.find("policies").?;
    try testing.expectEqual(@as(usize, 4), policies.conjuncts.len);
    // Conjuncts are canonicalized by field name.
    try testing.expectEqualStrings("locale", policies.conjuncts[0].field);
    try testing.expectEqualStrings("status", policies.conjuncts[2].field);
    try testing.expectEqualStrings("tenant_id", policies.conjuncts[3].field);
    try testing.expectEqualStrings("\"approved\"", policies.conjuncts[2].literals[0]);
    try testing.expectEqual(@as(u64, 2000), config.refresh.debounce_ms);

    const stored = try config.canonicalJsonAlloc(alloc);
    defer alloc.free(stored);
    var reparsed = try parseStoredJson(alloc, stored);
    defer reparsed.deinit();
    const again = try reparsed.canonicalJsonAlloc(alloc);
    defer alloc.free(again);
    try testing.expectEqualStrings(stored, again);
    try testing.expectEqualSlices(u8, &config.digest(), &reparsed.digest());

    // Reordered declaration has an identical identity.
    var reordered = try parsePublicJson(alloc,
        \\{"policies":{"fields":["revision","payload","policy_id"],"max_documents":500,"query":{"table":"policies","filter_query":{"conjuncts":[
        \\ {"field":"status","term":"approved"},{"field":"locale","term":{"$source":"/locale"}},
        \\ {"field":"organization_id","term":{"$source":"/organization_id"}},{"field":"tenant_id","term":{"$source":"/tenant_id"}}]}}},
        \\ "organization":{"required":true,"fields":["guidance","voice"],"document":{"key_from":"/organization_id","table":"organizations"}}}
    , "{\"debounce\":\"2000ms\"}");
    defer reordered.deinit();
    try testing.expectEqualSlices(u8, &config.digest(), &reordered.digest());

    const public = try config.publicRefreshJsonAlloc(alloc);
    defer alloc.free(public);
    try testing.expectEqualStrings("{\"mode\":\"background\",\"debounce\":\"2s\"}", public);
}

test "context sources reject unsupported predicates and malformed declarations" {
    const alloc = testing.allocator;
    const bad = [_][]const u8{
        "{}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"}}}", // no fields
        "{\"a\":{\"document\":{\"key_from\":\"k\"},\"fields\":[\"x\"]}}", // bad pointer
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"_artifacts\"]}}", // reserved field
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\",\"x\"]}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"],\"max_documents\":3}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"term\":\"v\"}},\"fields\":[\"x\"],\"required\":true}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"range\":{\"gt\":1}}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"disjuncts\":[]}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"conjuncts\":[{\"field\":\"f\",\"terms\":{\"$source\":\"/a\"}},{\"field\":\"g\",\"terms\":{\"$source\":\"/b\"}}]}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"term\":{\"$source\":\"/a\",\"x\":1}}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"],\"overflow\":\"truncate\"}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\",\"table_id\":\"7\"},\"fields\":[\"x\"]}}", // identity is engine-owned
        "{\"bad name\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"]}}",
    };
    for (bad) |raw| {
        if (parsePublicJson(alloc, raw, "")) |parsed| {
            var config = parsed;
            config.deinit();
            std.debug.print("unexpectedly admitted: {s}\n", .{raw});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc, "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"]}}", "{\"mode\":\"eventually\"}"));
}

test "bindings resolve typed source values and subscription tuples" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    const doc = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    var bindings = try bindAlloc(alloc, &config, doc);
    defer bindings.deinit();
    const org = bindings.items[0];
    try testing.expectEqualStrings("org-1", org.key.?);
    try testing.expectEqual(@as(usize, 1), org.tuples.len);
    const policies = bindings.items[1];
    try testing.expect(!policies.unsatisfiable);
    try testing.expectEqual(@as(usize, 1), policies.tuples.len);

    // The policy row's own tuple equals the dependent's subscription tuple.
    const row = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"p\"}");
    try testing.expect(rowMatches(&policies, row));
    var row_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, row);
    defer row_tuples.deinit(alloc);
    try testing.expectEqualSlices(u8, &policies.tuples[0], &row_tuples.tuples[0]);

    // A draft row matches no dependent; another organization's row has a different tuple.
    const draft = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"draft\"}");
    try testing.expect(!rowMatches(&policies, draft));
    var draft_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, draft);
    defer draft_tuples.deinit(alloc);
    try testing.expect(draft_tuples == .none);
    const other = try parseValue(arena.allocator(), "{\"tenant_id\":\"t2\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    var other_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, other);
    defer other_tuples.deinit(alloc);
    try testing.expect(!std.mem.eql(u8, &policies.tuples[0], &other_tuples.tuples[0]));

    // Missing bindings: empty query, absent document key, no subscriptions.
    const sparse = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\"}");
    var unbound = try bindAlloc(alloc, &config, sparse);
    defer unbound.deinit();
    try testing.expect(unbound.items[0].key == null);
    try testing.expect(unbound.items[1].unsatisfiable);
    const all = try unbound.allTuplesAlloc(alloc);
    defer alloc.free(all);
    try testing.expectEqual(@as(usize, 0), all.len);
}

test "set membership bindings expand one tuple per bound value" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var config = try parsePublicJson(alloc,
        \\{"terms":{"query":{"filter_query":{"conjuncts":[{"field":"locale","terms":{"$source":"/locales"}},{"field":"kind","terms":["glossary","style"]}]}},"fields":["text"]}}
    , "");
    defer config.deinit();
    const doc = try parseValue(arena.allocator(), "{\"locales\":[\"en-GB\",\"fr-FR\",\"en-GB\"]}");
    var bindings = try bindAlloc(alloc, &config, doc);
    defer bindings.deinit();
    // 2 distinct locales x 2 literal kinds.
    try testing.expectEqual(@as(usize, 4), bindings.items[0].tuples.len);
    const row = try parseValue(arena.allocator(), "{\"locale\":\"fr-FR\",\"kind\":[\"style\",\"other\"],\"text\":\"t\"}");
    try testing.expect(rowMatches(&bindings.items[0], row));
    var tuples = try rowTuplesAlloc(alloc, bindings.items[0].source, bindings.items[0].spec, row);
    defer tuples.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), tuples.tuples.len);
    var found = false;
    for (bindings.items[0].tuples) |t| {
        if (std.mem.eql(u8, &t, &tuples.tuples[0])) found = true;
    }
    try testing.expect(found);
}

test "touched tuples ignore irrelevant updates and report membership transitions" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    const policies = config.find("policies").?;
    const approved = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v1\",\"note\":\"x\"}");
    const approved_note = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v1\",\"note\":\"y\"}");
    const approved_v2 = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v2\"}");
    const withdrawn = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"withdrawn\",\"payload\":\"v2\"}");

    var irrelevant = try touchedTuplesAlloc(alloc, policies, "p1", approved, approved_note);
    defer irrelevant.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), irrelevant.tuples.len);

    var changed = try touchedTuplesAlloc(alloc, policies, "p1", approved, approved_v2);
    defer changed.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), changed.tuples.len);
    try testing.expectEqual(ChangeKind.member_changed, changed.tuples[0].change);

    var left = try touchedTuplesAlloc(alloc, policies, "p1", approved_v2, withdrawn);
    defer left.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), left.tuples.len);
    try testing.expectEqual(ChangeKind.member_removed, left.tuples[0].change);

    var inserted = try touchedTuplesAlloc(alloc, policies, "p2", null, approved);
    defer inserted.deinit(alloc);
    try testing.expectEqual(ChangeKind.member_added, inserted.tuples[0].change);

    var deleted = try touchedTuplesAlloc(alloc, policies, "p2", approved, null);
    defer deleted.deinit(alloc);
    try testing.expectEqual(ChangeKind.member_removed, deleted.tuples[0].change);

    var never = try touchedTuplesAlloc(alloc, policies, "p3", null, withdrawn);
    defer never.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), never.tuples.len);

    // Document sources: creation of a missing optional reference is a touch.
    const org = config.find("organization").?;
    const org_row = try parseValue(a, "{\"voice\":\"warm\",\"guidance\":\"g\",\"logo\":\"a.png\"}");
    const org_logo = try parseValue(a, "{\"voice\":\"warm\",\"guidance\":\"g\",\"logo\":\"b.png\"}");
    var created = try touchedTuplesAlloc(alloc, org, "org-1", null, org_row);
    defer created.deinit(alloc);
    try testing.expectEqual(ChangeKind.document_created, created.tuples[0].change);
    try testing.expectEqualSlices(u8, &documentTuple(org.specDigest(), "org-1"), &created.tuples[0].tuple);
    var logo = try touchedTuplesAlloc(alloc, org, "org-1", org_row, org_logo);
    defer logo.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), logo.tuples.len);
}

test "rendering is canonical, fingerprinted, and enforces budgets explicitly" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"org":{"document":{"key_from":"/org"},"fields":["voice"],"required":true},
        \\ "rules":{"query":{"filter_query":{"field":"org","term":{"$source":"/org"}}},"fields":["text"],"max_documents":2,"max_bytes":200}}
    , "");
    defer config.deinit();
    var bindings = try bindAlloc(alloc, &config, try parseValue(a, "{\"org\":\"o1\"}"));
    defer bindings.deinit();
    const org_proj = try projectAlloc(a, try parseValue(a, "{\"voice\":{\"b\":1,\"a\":[1.0,\"x\"]},\"ignored\":true}"), config.sources[0].fields);
    try testing.expectEqualStrings("{\"voice\":{\"a\":[1,\"x\"],\"b\":1}}", org_proj);
    const members = [_]ConsumedInput{ .{ .key = "r1", .projection = "{\"text\":\"a\"}" }, .{ .key = "r2", .projection = "{\"text\":\"b\"}" } };
    var rendered = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &members },
    });
    defer rendered.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"org\":{\"key\":\"o1\",\"value\":{\"voice\":{\"a\":[1,\"x\"],\"b\":1}}},\"rules\":{\"count\":2,\"members\":[{\"key\":\"r1\",\"value\":{\"text\":\"a\"}},{\"key\":\"r2\",\"value\":{\"text\":\"b\"}}]}}",
        rendered.json,
    );
    var again = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &members },
    });
    defer again.deinit(alloc);
    try testing.expectEqualSlices(u8, &rendered.fingerprint, &again.fingerprint);
    const block = try producerBlockAlloc(alloc, &rendered);
    defer alloc.free(block);
    try testing.expect(std.mem.startsWith(u8, block, "[context sha256:"));

    // Empty result is complete and distinct from a missing optional document.
    var empty = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &.{} },
    });
    defer empty.deinit(alloc);
    try testing.expect(std.mem.endsWith(u8, empty.json, "\"rules\":{\"count\":0,\"members\":[]}}"));

    // Required missing document and member/byte overflow fail explicitly.
    try testing.expectError(error.RequiredContextMissing, renderAlloc(alloc, &.{.{ .bound = &bindings.items[0], .members = &.{}, .present = false }}));
    const three = [_]ConsumedInput{ .{ .key = "r1", .projection = "{}" }, .{ .key = "r2", .projection = "{}" }, .{ .key = "r3", .projection = "{}" } };
    try testing.expectError(error.ContextBudgetExceeded, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &three }}));
    const filler: [300]u8 = @splat('x');
    const big = [_]ConsumedInput{.{ .key = "r1", .projection = "{\"text\":\"" ++ filler ++ "\"}" }};
    try testing.expectError(error.ContextBudgetExceeded, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &big }}));
    // Unordered members are a caller bug, not silently reordered output.
    const unordered = [_]ConsumedInput{ .{ .key = "r2", .projection = "{}" }, .{ .key = "r1", .projection = "{}" } };
    try testing.expectError(error.InvalidContextBinding, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &unordered }}));
}

test "table names resolve to immutable identities and client identities are not trusted" {
    const alloc = testing.allocator;
    const Map = struct {
        fn resolve(_: *anyopaque, a: Allocator, table: []const u8) anyerror!?[]u8 {
            if (std.mem.eql(u8, table, "policies")) return try a.dupe(u8, "tbl-42");
            return null;
        }
    };
    var dummy: u8 = 0;
    const resolver = TableResolver{ .ptr = &dummy, .resolve_fn = Map.resolve };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const public = try parseValue(arena.allocator(), "{\"p\":{\"query\":{\"table\":\"policies\",\"filter_query\":{\"field\":\"org\",\"term\":{\"$source\":\"/org\"}}},\"fields\":[\"text\"]}}");
    const stored = try canonicalFromPublicAlloc(alloc, public, null, resolver);
    defer alloc.free(stored);
    try testing.expect(std.mem.indexOf(u8, stored, "\"table_id\":\"tbl-42\"") != null);
    // A forged identity in a stored form is replaced by re-resolution.
    const forged = try std.mem.replaceOwned(u8, alloc, stored, "tbl-42", "tbl-evil");
    defer alloc.free(forged);
    const fixed = try reresolveStoredAlloc(alloc, forged, resolver);
    defer alloc.free(fixed);
    try testing.expectEqualStrings(stored, fixed);
    const unknown = try parseValue(arena.allocator(), "{\"p\":{\"document\":{\"table\":\"nope\",\"key_from\":\"/k\"},\"fields\":[\"x\"]}}");
    try testing.expectError(error.UnknownContextSourceTable, canonicalFromPublicAlloc(alloc, unknown, null, resolver));
    var config = try parseStoredJson(alloc, stored);
    defer config.deinit();
    const foreign = try foreignTableIdsAlloc(alloc, &config, "tbl-1");
    defer alloc.free(foreign);
    try testing.expectEqual(@as(usize, 1), foreign.len);
}

test "duration parsing" {
    try testing.expectEqual(@as(?u64, 2000), parseDurationMs("2s"));
    try testing.expectEqual(@as(?u64, 250), parseDurationMs("250ms"));
    try testing.expectEqual(@as(?u64, 120_000), parseDurationMs("2m"));
    try testing.expectEqual(@as(?u64, 3_600_000), parseDurationMs("1h"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("s"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("2x"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("-2s"));
}
