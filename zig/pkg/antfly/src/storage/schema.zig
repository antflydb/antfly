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

//! Schema management: TableSchema, DocumentSchema, field type validation.
//!
//! Matches Go antfly's lib/schema/ types:
//!   - AntflyType: text, keyword, numeric, embedding, link, boolean, datetime, geopoint, etc.
//!   - FieldMapping: type + index/store/doc_values/sortable/analyzer settings
//!   - DynamicTemplate: glob-based pattern matching for field names
//!   - TableSchema: version, TTL config, default type, dynamic templates

const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_erased = @import("backend_erased.zig");
const backend_scan = @import("backend_scan.zig");
const docstore = @import("docstore.zig");
const DocStore = docstore.DocStore;
const lsm_backend = @import("lsm_backend.zig");
const lmdb = @import("lmdb.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("antfly_platform").time;

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

var temp_test_path_nonce: u64 = 0;

fn tempTestPath(alloc: Allocator, label: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &temp_test_path_nonce, .Add, 1, .monotonic);
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-{s}-{d}-{d}", .{
        label,
        platform_time.monotonicNs(),
        nonce,
    });
    defer alloc.free(path);
    return try alloc.dupeZ(u8, path);
}

// ============================================================================
// Types
// ============================================================================

pub const AntflyType = enum(u8) {
    text = 0,
    keyword = 1,
    numeric = 2,
    embedding = 3,
    link = 4,
    boolean = 5,
    datetime = 6,
    geopoint = 7,
    geoshape = 8,
    blob = 9,
    html = 10,
    search_as_you_type = 11,
};

pub const MissingNullPolicy = enum(u8) {
    missing_rejected = 0,
};

pub fn missingNullPolicyName(policy: MissingNullPolicy) []const u8 {
    return switch (policy) {
        .missing_rejected => "missing_rejected",
    };
}

pub fn parseMissingNullPolicy(value: []const u8) ?MissingNullPolicy {
    if (std.mem.eql(u8, value, "missing_rejected")) return .missing_rejected;
    return null;
}

pub const FieldMapping = struct {
    field_type: AntflyType = .text,
    do_index: bool = true,
    store: bool = true,
    doc_values: bool = false,
    sortable: bool = false,
    missing_null_policy: MissingNullPolicy = .missing_rejected,
    include_in_all: bool = false,
    analyzer: []const u8 = "standard",
};

pub const DynamicTemplate = struct {
    name: []const u8,
    match_pattern: ?[]const u8 = null,
    unmatch_pattern: ?[]const u8 = null,
    path_match: ?[]const u8 = null,
    path_unmatch: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    mapping: FieldMapping = .{},
};

pub const FullTextField = struct {
    path: []const u8,
    emitted_name: []const u8,
    analyzer: []const u8,
    include_in_all: bool = false,
};

pub const FullTextDynamicVariant = struct {
    suffix: []const u8,
    analyzer: []const u8,
    include_in_all: bool = false,
};

pub const FullTextDynamicRule = struct {
    parent_path: []const u8,
    segment_pattern: ?[]const u8 = null,
    relative_path: []const u8 = "",
    variants: []const FullTextDynamicVariant = &.{},
};

pub const FullTextDocument = struct {
    name: []const u8,
    fields: []const FullTextField = &.{},
    dynamic_rules: []const FullTextDynamicRule = &.{},
    open_dynamic_paths: []const []const u8 = &.{},
    infer_type_dynamic_paths: []const []const u8 = &.{},
};

pub const IndexSortField = struct {
    field: []const u8,
    desc: bool = false,
};

pub const TableSchema = struct {
    version: u32 = 0,
    default_type: []const u8 = "_default",
    ttl_duration_ns: u64 = 0,
    ttl_field: []const u8 = "_timestamp",
    enforce_types: bool = false,
    dynamic_templates: []const DynamicTemplate = &.{},
    full_text_documents: []const FullTextDocument = &.{},
    index_sort: []const IndexSortField = &.{},
};

// ============================================================================
// Schema storage key
// ============================================================================

const schema_key = "\x00\x00__metadata__:schema";
const schema_version_prefix = "\x00\x00__metadata__:schema_v";

// ============================================================================
// Serialization
// ============================================================================

/// Serialize a TableSchema to bytes. Caller owns the returned slice.
pub fn serializeSchema(alloc: Allocator, schema: TableSchema) ![]u8 {
    var buf = std.ArrayListUnmanaged(u8).empty;
    errdefer buf.deinit(alloc);

    // Header
    try buf.appendSlice(alloc, "ASCH"); // magic
    try appendU32(&buf, alloc, 11); // format version
    try appendU32(&buf, alloc, schema.version);
    try appendStr(&buf, alloc, schema.default_type);
    try appendU64(&buf, alloc, schema.ttl_duration_ns);
    try appendStr(&buf, alloc, schema.ttl_field);
    try buf.append(alloc, if (schema.enforce_types) 1 else 0);

    // Dynamic templates
    try appendU32(&buf, alloc, @intCast(schema.dynamic_templates.len));
    for (schema.dynamic_templates) |tmpl| {
        try appendStr(&buf, alloc, tmpl.name);
        try appendOptStr(&buf, alloc, tmpl.match_pattern);
        try appendOptStr(&buf, alloc, tmpl.unmatch_pattern);
        try appendOptStr(&buf, alloc, tmpl.path_match);
        try appendOptStr(&buf, alloc, tmpl.path_unmatch);
        try appendOptStr(&buf, alloc, tmpl.match_mapping_type);
        try buf.append(alloc, @intFromEnum(tmpl.mapping.field_type));
        try buf.append(alloc, if (tmpl.mapping.do_index) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.store) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.doc_values) 1 else 0);
        try buf.append(alloc, if (tmpl.mapping.sortable) 1 else 0);
        try buf.append(alloc, @intFromEnum(tmpl.mapping.missing_null_policy));
        try buf.append(alloc, if (tmpl.mapping.include_in_all) 1 else 0);
        try appendStr(&buf, alloc, tmpl.mapping.analyzer);
    }

    try appendU32(&buf, alloc, @intCast(schema.full_text_documents.len));
    for (schema.full_text_documents) |doc| {
        try appendStr(&buf, alloc, doc.name);
        try appendU32(&buf, alloc, @intCast(doc.fields.len));
        for (doc.fields) |field| {
            try appendStr(&buf, alloc, field.path);
            try appendStr(&buf, alloc, field.emitted_name);
            try appendStr(&buf, alloc, field.analyzer);
            try buf.append(alloc, if (field.include_in_all) 1 else 0);
        }
        try appendU32(&buf, alloc, @intCast(doc.dynamic_rules.len));
        for (doc.dynamic_rules) |rule| {
            try appendStr(&buf, alloc, rule.parent_path);
            try appendOptStr(&buf, alloc, rule.segment_pattern);
            try appendStr(&buf, alloc, rule.relative_path);
            try appendU32(&buf, alloc, @intCast(rule.variants.len));
            for (rule.variants) |variant| {
                try appendStr(&buf, alloc, variant.suffix);
                try appendStr(&buf, alloc, variant.analyzer);
                try buf.append(alloc, if (variant.include_in_all) 1 else 0);
            }
        }
        try appendU32(&buf, alloc, @intCast(doc.open_dynamic_paths.len));
        for (doc.open_dynamic_paths) |path| try appendStr(&buf, alloc, path);
        try appendU32(&buf, alloc, @intCast(doc.infer_type_dynamic_paths.len));
        for (doc.infer_type_dynamic_paths) |path| try appendStr(&buf, alloc, path);
    }

    try appendU32(&buf, alloc, @intCast(schema.index_sort.len));
    for (schema.index_sort) |field| {
        try appendStr(&buf, alloc, field.field);
        try buf.append(alloc, if (field.desc) 1 else 0);
    }

    const result = try alloc.dupe(u8, buf.items);
    buf.deinit(alloc);
    return result;
}

/// Deserialize a TableSchema from bytes. Dupes all string data so the result
/// is independent of the source buffer. Call `freeSchema` to release.
pub fn deserializeSchema(alloc: Allocator, data: []const u8) !TableSchema {
    if (data.len < 4) return error.InvalidFormat;
    if (!std.mem.eql(u8, data[0..4], "ASCH")) return error.InvalidFormat;

    var pos: usize = 4;
    const fmt_version = readU32(data, &pos);
    if (fmt_version < 1 or fmt_version > 11) return error.UnsupportedVersion;

    const version = readU32(data, &pos);
    const default_type = try alloc.dupe(u8, readStr(data, &pos));
    errdefer alloc.free(default_type);
    const ttl_duration_ns = readU64(data, &pos);
    const ttl_field = try alloc.dupe(u8, readStr(data, &pos));
    errdefer alloc.free(ttl_field);
    const enforce_types = data[pos] == 1;
    pos += 1;

    const num_templates = readU32(data, &pos);
    const templates = try alloc.alloc(DynamicTemplate, num_templates);
    errdefer {
        for (templates[0..num_templates]) |t| {
            alloc.free(t.name);
            if (t.match_pattern) |p| alloc.free(p);
            if (t.unmatch_pattern) |p| alloc.free(p);
            if (t.path_match) |p| alloc.free(p);
            if (t.path_unmatch) |p| alloc.free(p);
            if (t.match_mapping_type) |p| alloc.free(p);
            alloc.free(t.mapping.analyzer);
        }
        alloc.free(templates);
    }

    for (templates) |*tmpl| {
        const name = try alloc.dupe(u8, readStr(data, &pos));
        errdefer alloc.free(name);

        const has_match = data[pos] == 1;
        pos += 1;
        const match_pattern: ?[]const u8 = if (has_match) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (match_pattern) |p| alloc.free(p);

        const has_unmatch = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const unmatch_pattern: ?[]const u8 = if (has_unmatch) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (unmatch_pattern) |p| alloc.free(p);

        const has_path = data[pos] == 1;
        pos += 1;
        const path_match: ?[]const u8 = if (has_path) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (path_match) |p| alloc.free(p);

        const has_path_unmatch = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const path_unmatch: ?[]const u8 = if (has_path_unmatch) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (path_unmatch) |p| alloc.free(p);

        const has_match_mapping_type = if (fmt_version >= 7) data[pos] == 1 else false;
        if (fmt_version >= 7) pos += 1;
        const match_mapping_type: ?[]const u8 = if (has_match_mapping_type) try alloc.dupe(u8, readStr(data, &pos)) else null;
        errdefer if (match_mapping_type) |p| alloc.free(p);

        const field_type: AntflyType = @enumFromInt(data[pos]);
        pos += 1;
        const do_index = data[pos] == 1;
        pos += 1;
        const store_val = data[pos] == 1;
        pos += 1;
        const doc_values = data[pos] == 1;
        pos += 1;
        const sortable = if (fmt_version >= 9) blk: {
            const value = data[pos] == 1;
            pos += 1;
            break :blk value;
        } else defaultSortableForMapping(field_type, doc_values);
        const missing_null_policy: MissingNullPolicy = if (fmt_version >= 11) blk: {
            const value: MissingNullPolicy = switch (data[pos]) {
                0 => .missing_rejected,
                else => return error.InvalidSchema,
            };
            pos += 1;
            break :blk value;
        } else .missing_rejected;
        const include_in_all = data[pos] == 1;
        pos += 1;
        const analyzer = try alloc.dupe(u8, readStr(data, &pos));

        tmpl.* = .{
            .name = name,
            .match_pattern = match_pattern,
            .unmatch_pattern = unmatch_pattern,
            .path_match = path_match,
            .path_unmatch = path_unmatch,
            .match_mapping_type = match_mapping_type,
            .mapping = .{
                .field_type = field_type,
                .do_index = do_index,
                .store = store_val,
                .doc_values = doc_values,
                .sortable = sortable,
                .missing_null_policy = missing_null_policy,
                .include_in_all = include_in_all,
                .analyzer = analyzer,
            },
        };
    }

    const full_text_documents: []FullTextDocument = if (fmt_version >= 2) blk: {
        const doc_count = readU32(data, &pos);
        const docs = try alloc.alloc(FullTextDocument, doc_count);
        var docs_initialized: usize = 0;
        errdefer {
            for (docs[0..docs_initialized]) |doc| {
                alloc.free(doc.name);
                for (doc.fields) |field| {
                    alloc.free(field.path);
                    alloc.free(field.emitted_name);
                    alloc.free(field.analyzer);
                }
                if (doc.fields.len > 0) alloc.free(doc.fields);
                for (doc.dynamic_rules) |rule| {
                    alloc.free(rule.parent_path);
                    if (rule.segment_pattern) |pattern| alloc.free(pattern);
                    alloc.free(rule.relative_path);
                    for (rule.variants) |variant| {
                        alloc.free(variant.suffix);
                        alloc.free(variant.analyzer);
                    }
                    if (rule.variants.len > 0) alloc.free(rule.variants);
                }
                if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
                for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
                if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
                for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
                if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
            }
            alloc.free(docs);
        }

        for (docs) |*doc| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);

            const field_count = readU32(data, &pos);
            const fields = try alloc.alloc(FullTextField, field_count);
            var fields_initialized: usize = 0;
            errdefer {
                for (fields[0..fields_initialized]) |field| {
                    alloc.free(field.path);
                    alloc.free(field.emitted_name);
                    alloc.free(field.analyzer);
                }
                alloc.free(fields);
            }
            for (fields) |*field| {
                field.* = .{
                    .path = try alloc.dupe(u8, readStr(data, &pos)),
                    .emitted_name = try alloc.dupe(u8, readStr(data, &pos)),
                    .analyzer = try alloc.dupe(u8, readStr(data, &pos)),
                    .include_in_all = data[pos] == 1,
                };
                pos += 1;
                fields_initialized += 1;
            }

            doc.* = .{
                .name = name,
                .fields = fields,
                .dynamic_rules = &.{},
                .open_dynamic_paths = &.{},
                .infer_type_dynamic_paths = &.{},
            };
            if (fmt_version >= 3) {
                const dynamic_rule_count = readU32(data, &pos);
                const dynamic_rules = try alloc.alloc(FullTextDynamicRule, dynamic_rule_count);
                var dynamic_rules_initialized: usize = 0;
                errdefer {
                    for (dynamic_rules[0..dynamic_rules_initialized]) |rule| {
                        alloc.free(rule.parent_path);
                        if (rule.segment_pattern) |pattern| alloc.free(pattern);
                        alloc.free(rule.relative_path);
                        for (rule.variants) |variant| {
                            alloc.free(variant.suffix);
                            alloc.free(variant.analyzer);
                        }
                        if (rule.variants.len > 0) alloc.free(rule.variants);
                    }
                    alloc.free(dynamic_rules);
                }
                for (dynamic_rules) |*rule| {
                    const parent_path = try alloc.dupe(u8, readStr(data, &pos));
                    errdefer alloc.free(parent_path);
                    const has_segment_pattern = if (fmt_version >= 5) data[pos] == 1 else false;
                    if (fmt_version >= 5) pos += 1;
                    const segment_pattern = if (has_segment_pattern)
                        try alloc.dupe(u8, readStr(data, &pos))
                    else
                        null;
                    errdefer if (segment_pattern) |pattern| alloc.free(pattern);
                    const relative_path = if (fmt_version >= 4)
                        try alloc.dupe(u8, readStr(data, &pos))
                    else
                        try alloc.dupe(u8, "");
                    errdefer alloc.free(relative_path);

                    const variant_count = readU32(data, &pos);
                    const variants = try alloc.alloc(FullTextDynamicVariant, variant_count);
                    var variants_initialized: usize = 0;
                    errdefer {
                        for (variants[0..variants_initialized]) |variant| {
                            alloc.free(variant.suffix);
                            alloc.free(variant.analyzer);
                        }
                        alloc.free(variants);
                    }
                    for (variants) |*variant| {
                        variant.* = .{
                            .suffix = try alloc.dupe(u8, readStr(data, &pos)),
                            .analyzer = try alloc.dupe(u8, readStr(data, &pos)),
                            .include_in_all = data[pos] == 1,
                        };
                        pos += 1;
                        variants_initialized += 1;
                    }

                    rule.* = .{
                        .parent_path = parent_path,
                        .segment_pattern = segment_pattern,
                        .relative_path = relative_path,
                        .variants = variants,
                    };
                    dynamic_rules_initialized += 1;
                }
                doc.dynamic_rules = dynamic_rules;
            }
            if (fmt_version >= 6) {
                const open_dynamic_path_count = readU32(data, &pos);
                const open_dynamic_paths = try alloc.alloc([]const u8, open_dynamic_path_count);
                var open_dynamic_paths_initialized: usize = 0;
                errdefer {
                    for (open_dynamic_paths[0..open_dynamic_paths_initialized]) |open_path| alloc.free(open_path);
                    alloc.free(open_dynamic_paths);
                }
                for (open_dynamic_paths) |*open_path| {
                    open_path.* = try alloc.dupe(u8, readStr(data, &pos));
                    open_dynamic_paths_initialized += 1;
                }
                doc.open_dynamic_paths = open_dynamic_paths;
            }
            if (fmt_version >= 8) {
                const infer_type_dynamic_path_count = readU32(data, &pos);
                const infer_type_dynamic_paths = try alloc.alloc([]const u8, infer_type_dynamic_path_count);
                var infer_type_dynamic_paths_initialized: usize = 0;
                errdefer {
                    for (infer_type_dynamic_paths[0..infer_type_dynamic_paths_initialized]) |infer_path| alloc.free(infer_path);
                    alloc.free(infer_type_dynamic_paths);
                }
                for (infer_type_dynamic_paths) |*infer_path| {
                    infer_path.* = try alloc.dupe(u8, readStr(data, &pos));
                    infer_type_dynamic_paths_initialized += 1;
                }
                doc.infer_type_dynamic_paths = infer_type_dynamic_paths;
            }
            docs_initialized += 1;
        }
        break :blk docs;
    } else &.{};

    const index_sort: []IndexSortField = if (fmt_version >= 10) blk: {
        const field_count = readU32(data, &pos);
        const fields = try alloc.alloc(IndexSortField, field_count);
        var fields_initialized: usize = 0;
        errdefer {
            for (fields[0..fields_initialized]) |field| alloc.free(field.field);
            alloc.free(fields);
        }
        for (fields) |*field| {
            field.* = .{
                .field = try alloc.dupe(u8, readStr(data, &pos)),
                .desc = data[pos] == 1,
            };
            pos += 1;
            fields_initialized += 1;
        }
        break :blk fields;
    } else &.{};

    return .{
        .version = version,
        .default_type = default_type,
        .ttl_duration_ns = ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = enforce_types,
        .dynamic_templates = templates,
        .full_text_documents = full_text_documents,
        .index_sort = index_sort,
    };
}

/// Free a schema returned by deserializeSchema.
pub fn freeSchema(alloc: Allocator, s: TableSchema) void {
    alloc.free(s.default_type);
    alloc.free(s.ttl_field);
    for (s.dynamic_templates) |t| {
        alloc.free(t.name);
        if (t.match_pattern) |p| alloc.free(p);
        if (t.unmatch_pattern) |p| alloc.free(p);
        if (t.path_match) |p| alloc.free(p);
        if (t.path_unmatch) |p| alloc.free(p);
        if (t.match_mapping_type) |p| alloc.free(p);
        alloc.free(t.mapping.analyzer);
    }
    if (s.dynamic_templates.len > 0) alloc.free(s.dynamic_templates);
    for (s.full_text_documents) |doc| {
        alloc.free(doc.name);
        for (doc.fields) |field| {
            alloc.free(field.path);
            alloc.free(field.emitted_name);
            alloc.free(field.analyzer);
        }
        if (doc.fields.len > 0) alloc.free(doc.fields);
        for (doc.dynamic_rules) |rule| {
            alloc.free(rule.parent_path);
            if (rule.segment_pattern) |pattern| alloc.free(pattern);
            alloc.free(rule.relative_path);
            for (rule.variants) |variant| {
                alloc.free(variant.suffix);
                alloc.free(variant.analyzer);
            }
            if (rule.variants.len > 0) alloc.free(rule.variants);
        }
        if (doc.dynamic_rules.len > 0) alloc.free(doc.dynamic_rules);
        for (doc.open_dynamic_paths) |open_path| alloc.free(open_path);
        if (doc.open_dynamic_paths.len > 0) alloc.free(doc.open_dynamic_paths);
        for (doc.infer_type_dynamic_paths) |infer_path| alloc.free(infer_path);
        if (doc.infer_type_dynamic_paths.len > 0) alloc.free(doc.infer_type_dynamic_paths);
    }
    if (s.full_text_documents.len > 0) alloc.free(s.full_text_documents);
    for (s.index_sort) |field| alloc.free(field.field);
    if (s.index_sort.len > 0) alloc.free(s.index_sort);
}

pub fn cloneSchema(alloc: Allocator, schema: TableSchema) !TableSchema {
    const encoded = try serializeSchema(alloc, schema);
    defer alloc.free(encoded);
    return try deserializeSchema(alloc, encoded);
}

/// Save a schema to DocStore. Returns whether durable state changed.
pub fn saveSchema(store: anytype, alloc: Allocator, schema: TableSchema) !bool {
    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);
    if (try activeSchemaDataMatches(store, alloc, data)) return false;

    const versioned_key = try schemaVersionKeyAlloc(alloc, schema.version);
    defer alloc.free(versioned_key);
    const previous_schema = try loadSchema(store, alloc);
    defer if (previous_schema) |loaded| freeSchema(alloc, loaded);

    const previous_versioned_data = blk: {
        const loaded = previous_schema orelse break :blk null;
        if (loaded.version == schema.version) break :blk null;
        const existing_version = try loadSchemaVersion(store, alloc, loaded.version);
        defer if (existing_version) |existing| freeSchema(alloc, existing);
        if (existing_version != null) break :blk null;
        break :blk try serializeSchema(alloc, loaded);
    };
    defer if (previous_versioned_data) |encoded| alloc.free(encoded);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    if (previous_schema) |loaded| {
        if (previous_versioned_data) |encoded| {
            const previous_versioned_key = try schemaVersionKeyAlloc(alloc, loaded.version);
            defer alloc.free(previous_versioned_key);
            try txn.put(previous_versioned_key, encoded);
        }
    }
    try txn.put(schema_key, data);
    try txn.put(versioned_key, data);
    try txn.commit();
    return true;
}

fn activeSchemaDataMatches(store: anytype, alloc: Allocator, expected: []const u8) !bool {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(schema_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return std.mem.eql(u8, raw, expected);
}

/// Load a schema from DocStore. Returns null if no schema exists.
pub fn loadSchema(store: anytype, alloc: Allocator) !?TableSchema {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(schema_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const data = try alloc.dupe(u8, raw);
    defer alloc.free(data);
    return try deserializeSchema(alloc, data);
}

pub fn loadSchemaVersion(store: anytype, alloc: Allocator, version: u32) !?TableSchema {
    const versioned_key = try schemaVersionKeyAlloc(alloc, version);
    defer alloc.free(versioned_key);
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const raw = txn.get(versioned_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const data = try alloc.dupe(u8, raw);
    defer alloc.free(data);
    return try deserializeSchema(alloc, data);
}

pub fn copySchemas(source_store: anytype, dest_store: anytype, alloc: Allocator) !void {
    var source_runtime = try initRuntimeStore(alloc, source_store);
    defer source_runtime.deinit();
    var source_txn = try source_runtime.store.beginProbe();
    defer source_txn.abort();

    var dest_runtime = try initRuntimeStore(alloc, dest_store);
    defer dest_runtime.deinit();
    var dest_txn = try dest_runtime.store.beginWrite();
    errdefer dest_txn.abort();

    if (source_txn.get(schema_key)) |raw| {
        try dest_txn.put(schema_key, raw);
    } else |err| switch (err) {
        error.NotFound => {},
        else => return err,
    }

    const entries = try backend_scan.scanPrefixCurrent(alloc, &source_runtime.store, schema_version_prefix);
    defer backend_scan.freeResults(alloc, entries);
    for (entries) |entry| try dest_txn.put(entry.key, entry.value);

    try dest_txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }
    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn schemaVersionKeyAlloc(alloc: Allocator, version: u32) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{d}", .{ schema_version_prefix, version });
}

// ============================================================================
// Field type resolution
// ============================================================================

/// Resolve the field type for a field/path using dynamic templates without a
/// runtime value. Templates using `match_mapping_type` will not match.
pub fn resolveFieldType(schema: TableSchema, field_name: []const u8) ?FieldMapping {
    return resolveFieldTypeForValue(schema, field_name, null);
}

/// Resolve a declared mapping for a concrete field path without requiring a
/// sample value for `match_mapping_type`. Use this only for schema/config
/// validation paths that still require physical coverage before queryability.
pub fn resolveDeclaredFieldType(schema: TableSchema, path: []const u8) ?FieldMapping {
    const field_name = fieldNameFromPath(path);
    for (schema.dynamic_templates) |tmpl| {
        if (dynamicTemplateMatchesDeclaredPath(tmpl, path, field_name)) return tmpl.mapping;
    }
    return null;
}

/// Resolve the field type for a field/path using dynamic templates and an
/// optional runtime value for `match_mapping_type` matching.
pub fn resolveFieldTypeForValue(schema: TableSchema, path: []const u8, value: ?std.json.Value) ?FieldMapping {
    const field_name = fieldNameFromPath(path);
    for (schema.dynamic_templates) |tmpl| {
        if (dynamicTemplateMatches(tmpl, path, field_name, value)) return tmpl.mapping;
    }
    return null;
}

pub fn defaultSortableForMapping(field_type: AntflyType, doc_values: bool) bool {
    if (!doc_values) return false;
    return fieldTypeIsSortableScalar(field_type);
}

pub fn fieldTypeIsSortableScalar(field_type: AntflyType) bool {
    return switch (field_type) {
        .keyword, .numeric, .boolean, .datetime, .link => true,
        else => false,
    };
}

pub fn mappingIsFilterable(mapping: FieldMapping) bool {
    return switch (mapping.field_type) {
        .geopoint => mapping.doc_values,
        else => fieldTypeIsSortableScalar(mapping.field_type) and (mapping.do_index or mapping.doc_values),
    };
}

pub fn mappingIsAggregatable(mapping: FieldMapping) bool {
    return fieldTypeIsSortableScalar(mapping.field_type) and mapping.doc_values;
}

pub fn mappingHasNativeDocValues(mapping: FieldMapping) bool {
    if (!mapping.doc_values) return false;
    return switch (mapping.field_type) {
        .keyword, .numeric, .boolean, .datetime, .link, .geopoint => true,
        else => false,
    };
}

pub fn mappingIsSortable(mapping: FieldMapping) bool {
    return fieldTypeIsSortableScalar(mapping.field_type) and mapping.doc_values and mapping.sortable;
}

pub fn mappingQueryabilityStateName(mapping: FieldMapping) []const u8 {
    if (mappingIsSortable(mapping)) return "declared";
    if (mapping.field_type == .geopoint) {
        if (mapping.doc_values) return "declared";
        return "missing_doc_values";
    }
    if (!fieldTypeIsSortableScalar(mapping.field_type)) return "non_scalar";
    if (!mapping.doc_values) return "missing_doc_values";
    if (!mapping.sortable) return "non_sortable";
    return "unsupported";
}

pub fn conservativeDocValueCoverage(left: []const u8, right: []const u8) []const u8 {
    return if (docValueCoverageRank(left) <= docValueCoverageRank(right)) left else right;
}

fn docValueCoverageRank(value: []const u8) u8 {
    if (std.mem.eql(u8, value, "identity_metadata")) return 5;
    if (std.mem.eql(u8, value, "covered")) return 4;
    if (std.mem.eql(u8, value, "schema_declared")) return 3;
    if (std.mem.eql(u8, value, "observed_declared")) return 2;
    if (std.mem.eql(u8, value, "not_declared")) return 1;
    return 0;
}

pub fn conservativeQueryabilityState(left: []const u8, right: []const u8) []const u8 {
    return if (queryabilityStateRank(left) <= queryabilityStateRank(right)) left else right;
}

fn queryabilityStateRank(value: []const u8) u8 {
    if (std.mem.eql(u8, value, "queryable")) return 6;
    if (std.mem.eql(u8, value, "declared")) return 5;
    if (std.mem.eql(u8, value, "text_search_only")) return 4;
    if (std.mem.eql(u8, value, "missing_doc_values")) return 3;
    if (std.mem.eql(u8, value, "non_sortable")) return 2;
    if (std.mem.eql(u8, value, "non_scalar")) return 1;
    return 0;
}

pub fn sortLifecycleStateName(capability: FieldCapability) []const u8 {
    if (!capability.sortable) return "unsupported";
    if (std.mem.eql(u8, capability.queryability_state, "queryable")) {
        return if (capability.index_sort != null) "accelerated" else "queryable";
    }
    if (std.mem.eql(u8, capability.doc_value_coverage, "covered") or
        std.mem.eql(u8, capability.doc_value_coverage, "identity_metadata"))
    {
        return "covered";
    }
    if (capability.doc_values and std.mem.eql(u8, capability.doc_value_coverage, "observed_declared")) return "indexed";
    return "declared";
}

pub fn refreshSortLifecycleState(capability: *FieldCapability) void {
    capability.sort_lifecycle_state = sortLifecycleStateName(capability.*);
}

pub fn conservativeSortLifecycleState(left: []const u8, right: []const u8) []const u8 {
    return if (sortLifecycleStateRank(left) <= sortLifecycleStateRank(right)) left else right;
}

fn sortLifecycleStateRank(value: []const u8) u8 {
    if (std.mem.eql(u8, value, "accelerated")) return 5;
    if (std.mem.eql(u8, value, "queryable")) return 4;
    if (std.mem.eql(u8, value, "covered")) return 3;
    if (std.mem.eql(u8, value, "indexed")) return 2;
    if (std.mem.eql(u8, value, "declared")) return 1;
    return 0;
}

pub const IndexSortMembership = struct {
    position: usize,
    desc: bool,
};

pub const FieldCapability = struct {
    name: ?[]const u8 = null,
    field: ?[]const u8 = null,
    path_pattern: ?[]const u8 = null,
    field_pattern: ?[]const u8 = null,
    match_mapping_type: ?[]const u8 = null,
    emitted_name: ?[]const u8 = null,
    document_schema: ?[]const u8 = null,
    field_type: AntflyType,
    searchable: bool,
    filterable: bool,
    aggregatable: bool,
    doc_values: bool,
    sortable: bool,
    doc_value_coverage: []const u8,
    provenance: []const u8,
    missing_null_policy: []const u8,
    queryability_state: []const u8,
    sort_lifecycle_state: []const u8,
    analyzer: ?[]const u8 = null,
    index_sort: ?IndexSortMembership = null,
};

pub fn indexSortMembership(schema: TableSchema, field: []const u8) ?IndexSortMembership {
    for (schema.index_sort, 0..) |sort_field, idx| {
        if (std.mem.eql(u8, sort_field.field, field)) {
            return .{ .position = idx, .desc = sort_field.desc };
        }
    }
    return null;
}

pub fn reservedIdFieldCapability(schema: TableSchema) FieldCapability {
    const index_sort = indexSortMembership(schema, "_id");
    return .{
        .field = "_id",
        .field_type = .keyword,
        .searchable = true,
        .filterable = true,
        .aggregatable = false,
        .doc_values = false,
        .sortable = true,
        .doc_value_coverage = "identity_metadata",
        .provenance = "reserved",
        .missing_null_policy = "not_null",
        .queryability_state = "queryable",
        .sort_lifecycle_state = lifecycleStateFromParts(true, false, "identity_metadata", "queryable", index_sort),
        .index_sort = index_sort,
    };
}

pub fn dynamicTemplateFieldCapability(schema: TableSchema, tmpl: DynamicTemplate) FieldCapability {
    const mapping = tmpl.mapping;
    const exact_path = exactDynamicTemplatePath(tmpl);
    const index_sort = if (exact_path) |field| indexSortMembership(schema, field) else null;
    const sortable = mappingIsSortable(mapping);
    const queryability_state = mappingQueryabilityStateName(mapping);
    const doc_value_coverage = if (mapping.doc_values) "schema_declared" else "not_declared";
    return .{
        .name = tmpl.name,
        .field = exact_path,
        .path_pattern = tmpl.path_match,
        .field_pattern = tmpl.match_pattern,
        .match_mapping_type = tmpl.match_mapping_type,
        .field_type = mapping.field_type,
        .searchable = mapping.do_index,
        .filterable = mappingIsFilterable(mapping),
        .aggregatable = mappingIsAggregatable(mapping),
        .doc_values = mapping.doc_values,
        .sortable = sortable,
        .doc_value_coverage = doc_value_coverage,
        .provenance = "dynamic_template",
        .missing_null_policy = missingNullPolicyName(mapping.missing_null_policy),
        .queryability_state = queryability_state,
        .sort_lifecycle_state = lifecycleStateFromParts(sortable, mapping.doc_values, doc_value_coverage, queryability_state, index_sort),
        .analyzer = mapping.analyzer,
        .index_sort = index_sort,
    };
}

pub fn fullTextFieldCapability(schema: TableSchema, document_name: []const u8, field: FullTextField) FieldCapability {
    const exact_keyword = std.mem.eql(u8, field.analyzer, "keyword");
    const capability_field = if (exact_keyword) field.emitted_name else field.path;
    return .{
        .field = capability_field,
        .emitted_name = field.emitted_name,
        .document_schema = document_name,
        .field_type = if (exact_keyword) .keyword else .text,
        .searchable = true,
        .filterable = exact_keyword,
        .aggregatable = false,
        .doc_values = false,
        .sortable = false,
        .doc_value_coverage = "not_declared",
        .provenance = "document_schema",
        .missing_null_policy = "not_applicable",
        .queryability_state = if (exact_keyword) "missing_doc_values" else "text_search_only",
        .sort_lifecycle_state = "unsupported",
        .analyzer = field.analyzer,
        .index_sort = indexSortMembership(schema, capability_field),
    };
}

pub fn observedDynamicFieldCapability(schema: ?TableSchema, field: []const u8, mapping: FieldMapping) FieldCapability {
    const index_sort = if (schema) |runtime_schema| indexSortMembership(runtime_schema, field) else null;
    const sortable = mappingIsSortable(mapping);
    const queryability_state = mappingQueryabilityStateName(mapping);
    const doc_value_coverage = if (mapping.doc_values) "observed_declared" else "not_declared";
    return .{
        .field = field,
        .field_type = mapping.field_type,
        .searchable = mapping.do_index,
        .filterable = mappingIsFilterable(mapping),
        .aggregatable = mappingIsAggregatable(mapping),
        .doc_values = mapping.doc_values,
        .sortable = sortable,
        .doc_value_coverage = doc_value_coverage,
        .provenance = "observed_dynamic",
        .missing_null_policy = missingNullPolicyName(mapping.missing_null_policy),
        .queryability_state = queryability_state,
        .sort_lifecycle_state = lifecycleStateFromParts(sortable, mapping.doc_values, doc_value_coverage, queryability_state, index_sort),
        .analyzer = mapping.analyzer,
        .index_sort = index_sort,
    };
}

fn lifecycleStateFromParts(
    sortable: bool,
    doc_values: bool,
    doc_value_coverage: []const u8,
    queryability_state: []const u8,
    index_sort: ?IndexSortMembership,
) []const u8 {
    if (!sortable) return "unsupported";
    if (std.mem.eql(u8, queryability_state, "queryable")) {
        return if (index_sort != null) "accelerated" else "queryable";
    }
    if (std.mem.eql(u8, doc_value_coverage, "covered") or
        std.mem.eql(u8, doc_value_coverage, "identity_metadata"))
    {
        return "covered";
    }
    if (doc_values and std.mem.eql(u8, doc_value_coverage, "observed_declared")) return "indexed";
    return "declared";
}

pub fn fieldCapabilitiesAlloc(alloc: Allocator, schema: TableSchema) ![]FieldCapability {
    var count: usize = 1 + schema.dynamic_templates.len;
    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.path, "_id")) continue;
            if (resolveFieldType(schema, field.path) != null) continue;
            count += 1;
        }
    }

    const capabilities = try alloc.alloc(FieldCapability, count);
    errdefer alloc.free(capabilities);

    var index: usize = 0;
    capabilities[index] = reservedIdFieldCapability(schema);
    index += 1;

    for (schema.dynamic_templates) |tmpl| {
        capabilities[index] = dynamicTemplateFieldCapability(schema, tmpl);
        index += 1;
    }

    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.path, "_id")) continue;
            if (resolveFieldType(schema, field.path) != null) continue;
            capabilities[index] = fullTextFieldCapability(schema, doc.name, field);
            index += 1;
        }
    }

    std.debug.assert(index == capabilities.len);
    return capabilities;
}

pub fn freeFieldCapabilities(alloc: Allocator, capabilities: []FieldCapability) void {
    if (capabilities.len > 0) alloc.free(capabilities);
}

pub fn cloneFieldCapabilityAlloc(alloc: Allocator, capability: FieldCapability) !FieldCapability {
    var cloned = capability;
    cloned.name = if (capability.name) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.name) |value| alloc.free(value);
    cloned.field = if (capability.field) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.field) |value| alloc.free(value);
    cloned.path_pattern = if (capability.path_pattern) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.path_pattern) |value| alloc.free(value);
    cloned.field_pattern = if (capability.field_pattern) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.field_pattern) |value| alloc.free(value);
    cloned.match_mapping_type = if (capability.match_mapping_type) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.match_mapping_type) |value| alloc.free(value);
    cloned.emitted_name = if (capability.emitted_name) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.emitted_name) |value| alloc.free(value);
    cloned.document_schema = if (capability.document_schema) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cloned.document_schema) |value| alloc.free(value);
    cloned.doc_value_coverage = try alloc.dupe(u8, capability.doc_value_coverage);
    errdefer alloc.free(cloned.doc_value_coverage);
    cloned.provenance = try alloc.dupe(u8, capability.provenance);
    errdefer alloc.free(cloned.provenance);
    cloned.missing_null_policy = try alloc.dupe(u8, capability.missing_null_policy);
    errdefer alloc.free(cloned.missing_null_policy);
    cloned.queryability_state = try alloc.dupe(u8, capability.queryability_state);
    errdefer alloc.free(cloned.queryability_state);
    cloned.sort_lifecycle_state = try alloc.dupe(u8, capability.sort_lifecycle_state);
    errdefer alloc.free(cloned.sort_lifecycle_state);
    cloned.analyzer = if (capability.analyzer) |value| try alloc.dupe(u8, value) else null;
    return cloned;
}

pub fn cloneFieldCapabilitiesAlloc(alloc: Allocator, capabilities: []const FieldCapability) ![]FieldCapability {
    const cloned = try alloc.alloc(FieldCapability, capabilities.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |item| freeOwnedFieldCapability(alloc, item);
        alloc.free(cloned);
    }

    for (capabilities, 0..) |capability, i| {
        cloned[i] = try cloneFieldCapabilityAlloc(alloc, capability);
        initialized += 1;
    }
    return cloned;
}

pub fn freeOwnedFieldCapability(alloc: Allocator, capability: FieldCapability) void {
    if (capability.name) |value| alloc.free(value);
    if (capability.field) |value| alloc.free(value);
    if (capability.path_pattern) |value| alloc.free(value);
    if (capability.field_pattern) |value| alloc.free(value);
    if (capability.match_mapping_type) |value| alloc.free(value);
    if (capability.emitted_name) |value| alloc.free(value);
    if (capability.document_schema) |value| alloc.free(value);
    alloc.free(capability.doc_value_coverage);
    alloc.free(capability.provenance);
    alloc.free(capability.missing_null_policy);
    alloc.free(capability.queryability_state);
    alloc.free(capability.sort_lifecycle_state);
    if (capability.analyzer) |value| alloc.free(value);
}

pub fn freeOwnedFieldCapabilities(alloc: Allocator, capabilities: []FieldCapability) void {
    for (capabilities) |capability| freeOwnedFieldCapability(alloc, capability);
    if (capabilities.len > 0) alloc.free(capabilities);
}

pub fn exactDynamicTemplatePath(tmpl: DynamicTemplate) ?[]const u8 {
    const path_match = tmpl.path_match orelse return null;
    if (std.mem.indexOfAny(u8, path_match, "*?") != null) return null;
    if (tmpl.path_unmatch != null) return null;
    if (tmpl.match_pattern) |pattern| {
        if (std.mem.indexOfAny(u8, pattern, "*?") != null) return null;
        if (!std.mem.eql(u8, pattern, fieldNameFromPath(path_match))) return null;
    }
    if (tmpl.unmatch_pattern) |pattern| {
        if (std.mem.eql(u8, pattern, fieldNameFromPath(path_match))) return null;
    }
    return path_match;
}

fn dynamicTemplateMatches(
    tmpl: DynamicTemplate,
    path: []const u8,
    field_name: []const u8,
    value: ?std.json.Value,
) bool {
    if (tmpl.match_pattern) |pattern| {
        if (!globMatch(pattern, field_name)) return false;
    }
    if (tmpl.unmatch_pattern) |pattern| {
        if (globMatch(pattern, field_name)) return false;
    }
    if (tmpl.path_match) |pattern| {
        if (!globMatch(pattern, path)) return false;
    }
    if (tmpl.path_unmatch) |pattern| {
        if (globMatch(pattern, path)) return false;
    }
    if (tmpl.match_mapping_type) |expected| {
        const actual = if (value) |v| inferDynamicTemplateMatchType(v) else null;
        if (actual == null or !std.mem.eql(u8, expected, actual.?)) return false;
    }
    return true;
}

fn dynamicTemplateMatchesDeclaredPath(
    tmpl: DynamicTemplate,
    path: []const u8,
    field_name: []const u8,
) bool {
    if (tmpl.match_pattern) |pattern| {
        if (!globMatch(pattern, field_name)) return false;
    }
    if (tmpl.unmatch_pattern) |pattern| {
        if (globMatch(pattern, field_name)) return false;
    }
    if (tmpl.path_match) |pattern| {
        if (!globMatch(pattern, path)) return false;
    }
    if (tmpl.path_unmatch) |pattern| {
        if (globMatch(pattern, path)) return false;
    }
    return true;
}

fn inferDynamicTemplateMatchType(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| if (parseRfc3339ToNs(text) != null or isValidDate(text)) "date" else "string",
        .integer, .float, .number_string => "number",
        .bool => "boolean",
        .object => "object",
        else => null,
    };
}

pub fn fieldNameFromPath(path: []const u8) []const u8 {
    const last_dot = std.mem.lastIndexOfScalar(u8, path, '.') orelse return path;
    return path[last_dot + 1 ..];
}

/// Simple glob matching: supports '*' (any chars) and '?' (single char).
pub fn globMatch(pattern: []const u8, text: []const u8) bool {
    var pi: usize = 0;
    var ti: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;

    while (ti < text.len) {
        if (pi < pattern.len and (pattern[pi] == text[ti] or pattern[pi] == '?')) {
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

    while (pi < pattern.len and pattern[pi] == '*') pi += 1;
    return pi == pattern.len;
}

/// Validate that all field names resolve to known types (when enforce_types=true).
pub fn validateFields(schema: TableSchema, field_names: []const []const u8) !void {
    if (!schema.enforce_types) return;
    for (field_names) |name| {
        if (resolveFieldType(schema, name) == null) {
            return error.UnknownFieldType;
        }
    }
}

pub fn parseDateTimeToNs(text: []const u8) ?u64 {
    if (parseRfc3339ToNs(text)) |ns| return ns;
    return parseDateToNs(text);
}

pub fn formatDateTimeNsAlloc(alloc: Allocator, ns: u64) ![]u8 {
    const seconds = ns / std.time.ns_per_s;
    const nanos = ns % std.time.ns_per_s;
    const days: i64 = @intCast(seconds / 86_400);
    const seconds_of_day = seconds % 86_400;
    const civil = civilFromDays(days);
    if (civil.year < 0 or civil.year > 9999) return error.InvalidDateTime;
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        @as(u64, @intCast(civil.year)),
        civil.month,
        civil.day,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
        nanos,
    });
}

fn parseRfc3339ToNs(text: []const u8) ?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn parseDateToNs(value: []const u8) ?u64 {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return null;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return null;
    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    return @as(u64, @intCast(days * 86_400)) * std.time.ns_per_s;
}

fn isValidDate(value: []const u8) bool {
    return parseDateToNs(value) != null;
}

const CivilDate = struct {
    year: i64,
    month: u8,
    day: u8,
};

fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var year = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + if (mp < 10) @as(i64, 3) else @as(i64, -9);
    year += if (month <= 2) @as(i64, 1) else @as(i64, 0);
    return .{
        .year = year,
        .month = @intCast(month),
        .day = @intCast(day),
    };
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

// ============================================================================
// Serialization helpers
// ============================================================================

fn appendU32(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u32) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, val, .little);
    try buf.appendSlice(alloc, &bytes);
}

fn appendU64(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, val: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, val, .little);
    try buf.appendSlice(alloc, &bytes);
}

fn appendStr(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, s: []const u8) !void {
    try appendU32(buf, alloc, @intCast(s.len));
    try buf.appendSlice(alloc, s);
}

fn appendOptStr(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, s: ?[]const u8) !void {
    if (s) |str| {
        try buf.append(alloc, 1);
        try appendStr(buf, alloc, str);
    } else {
        try buf.append(alloc, 0);
    }
}

fn readU32(data: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return val;
}

fn readU64(data: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

fn readStr(data: []const u8, pos: *usize) []const u8 {
    const len = readU32(data, pos);
    const s = data[pos.*..][0..len];
    pos.* += len;
    return s;
}

// ============================================================================
// Tests
// ============================================================================

test "schema serialize/deserialize round-trip" {
    const alloc = std.testing.allocator;

    const schema = TableSchema{
        .version = 42,
        .default_type = "my_type",
        .ttl_duration_ns = 86400_000_000_000,
        .ttl_field = "_created",
        .enforce_types = true,
        .dynamic_templates = &.{
            .{
                .name = "dates",
                .match_pattern = "*_at",
                .unmatch_pattern = "skip_*",
                .path_match = "meta.*",
                .path_unmatch = "meta.private.*",
                .match_mapping_type = "date",
                .mapping = .{
                    .field_type = .datetime,
                    .do_index = false,
                    .store = false,
                    .doc_values = true,
                    .sortable = true,
                    .missing_null_policy = .missing_rejected,
                    .include_in_all = false,
                    .analyzer = "keyword",
                },
            },
        },
        .full_text_documents = &.{
            .{
                .name = "my_type",
                .fields = &.{
                    .{
                        .path = "title",
                        .emitted_name = "title",
                        .analyzer = "standard",
                        .include_in_all = true,
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._2gram",
                        .analyzer = "search_as_you_type_2gram",
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._3gram",
                        .analyzer = "search_as_you_type_3gram",
                    },
                    .{
                        .path = "title",
                        .emitted_name = "title._index_prefix",
                        .analyzer = "search_as_you_type_index_prefix",
                    },
                },
                .dynamic_rules = &.{
                    .{
                        .parent_path = "meta",
                        .segment_pattern = "^tag_[a-z]+$",
                        .relative_path = "title",
                        .variants = &.{
                            .{
                                .suffix = "",
                                .analyzer = "standard",
                            },
                            .{
                                .suffix = "._2gram",
                                .analyzer = "search_as_you_type_2gram",
                            },
                            .{
                                .suffix = "._3gram",
                                .analyzer = "search_as_you_type_3gram",
                            },
                            .{
                                .suffix = "._index_prefix",
                                .analyzer = "search_as_you_type_index_prefix",
                            },
                        },
                    },
                },
                .open_dynamic_paths = &.{ "", "meta" },
                .infer_type_dynamic_paths = &.{"typed"},
            },
        },
        .index_sort = &.{
            .{ .field = "created_at", .desc = true },
            .{ .field = "_id" },
        },
    };

    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);

    const loaded = try deserializeSchema(alloc, data);
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 42), loaded.version);
    try std.testing.expectEqualStrings("my_type", loaded.default_type);
    try std.testing.expectEqual(@as(u64, 86400_000_000_000), loaded.ttl_duration_ns);
    try std.testing.expectEqualStrings("_created", loaded.ttl_field);
    try std.testing.expect(loaded.enforce_types);
    try std.testing.expectEqual(@as(usize, 1), loaded.dynamic_templates.len);
    try std.testing.expectEqualStrings("dates", loaded.dynamic_templates[0].name);
    try std.testing.expectEqualStrings("skip_*", loaded.dynamic_templates[0].unmatch_pattern.?);
    try std.testing.expectEqualStrings("meta.private.*", loaded.dynamic_templates[0].path_unmatch.?);
    try std.testing.expectEqualStrings("date", loaded.dynamic_templates[0].match_mapping_type.?);
    try std.testing.expectEqual(AntflyType.datetime, loaded.dynamic_templates[0].mapping.field_type);
    try std.testing.expect(!loaded.dynamic_templates[0].mapping.do_index);
    try std.testing.expect(loaded.dynamic_templates[0].mapping.doc_values);
    try std.testing.expect(loaded.dynamic_templates[0].mapping.sortable);
    try std.testing.expectEqual(MissingNullPolicy.missing_rejected, loaded.dynamic_templates[0].mapping.missing_null_policy);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents.len);
    try std.testing.expectEqualStrings("my_type", loaded.full_text_documents[0].name);
    try std.testing.expectEqual(@as(usize, 4), loaded.full_text_documents[0].fields.len);
    try std.testing.expectEqualStrings("title._2gram", loaded.full_text_documents[0].fields[1].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_2gram", loaded.full_text_documents[0].fields[1].analyzer);
    try std.testing.expectEqualStrings("title._3gram", loaded.full_text_documents[0].fields[2].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_3gram", loaded.full_text_documents[0].fields[2].analyzer);
    try std.testing.expectEqualStrings("title._index_prefix", loaded.full_text_documents[0].fields[3].emitted_name);
    try std.testing.expectEqualStrings("search_as_you_type_index_prefix", loaded.full_text_documents[0].fields[3].analyzer);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents[0].dynamic_rules.len);
    try std.testing.expectEqualStrings("meta", loaded.full_text_documents[0].dynamic_rules[0].parent_path);
    try std.testing.expectEqualStrings("^tag_[a-z]+$", loaded.full_text_documents[0].dynamic_rules[0].segment_pattern.?);
    try std.testing.expectEqualStrings("title", loaded.full_text_documents[0].dynamic_rules[0].relative_path);
    try std.testing.expectEqual(@as(usize, 4), loaded.full_text_documents[0].dynamic_rules[0].variants.len);
    try std.testing.expectEqualStrings("._2gram", loaded.full_text_documents[0].dynamic_rules[0].variants[1].suffix);
    try std.testing.expectEqualStrings("._3gram", loaded.full_text_documents[0].dynamic_rules[0].variants[2].suffix);
    try std.testing.expectEqualStrings("._index_prefix", loaded.full_text_documents[0].dynamic_rules[0].variants[3].suffix);
    try std.testing.expectEqual(@as(usize, 2), loaded.full_text_documents[0].open_dynamic_paths.len);
    try std.testing.expectEqualStrings("", loaded.full_text_documents[0].open_dynamic_paths[0]);
    try std.testing.expectEqualStrings("meta", loaded.full_text_documents[0].open_dynamic_paths[1]);
    try std.testing.expectEqual(@as(usize, 1), loaded.full_text_documents[0].infer_type_dynamic_paths.len);
    try std.testing.expectEqualStrings("typed", loaded.full_text_documents[0].infer_type_dynamic_paths[0]);
    try std.testing.expectEqual(@as(usize, 2), loaded.index_sort.len);
    try std.testing.expectEqualStrings("created_at", loaded.index_sort[0].field);
    try std.testing.expect(loaded.index_sort[0].desc);
    try std.testing.expectEqualStrings("_id", loaded.index_sort[1].field);
    try std.testing.expect(!loaded.index_sort[1].desc);
}

test "schema save/load via DocStore" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-store");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    // No schema initially
    const none = try loadSchema(&store, alloc);
    try std.testing.expect(none == null);

    // Save and reload
    const schema = TableSchema{ .version = 7, .default_type = "doc" };
    try std.testing.expect(try saveSchema(&store, alloc, schema));
    const saved_sequence = store.lastReplaySequence(0);
    try std.testing.expect(!try saveSchema(&store, alloc, schema));
    try std.testing.expectEqual(saved_sequence, store.lastReplaySequence(0));

    const loaded = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 7), loaded.version);
    try std.testing.expectEqualStrings("doc", loaded.default_type);

    const loaded_v7 = (try loadSchemaVersion(&store, alloc, 7)).?;
    defer freeSchema(alloc, loaded_v7);
    try std.testing.expectEqual(@as(u32, 7), loaded_v7.version);
}

test "schema preserves versioned history in DocStore" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-history");
    defer alloc.free(path);
    cleanupTestDir(path);
    var store = try DocStore.open(alloc, path, .{});
    defer store.close();
    defer cleanupTestDir(path);

    _ = try saveSchema(&store, alloc, .{ .version = 0, .default_type = "doc_v0" });
    _ = try saveSchema(&store, alloc, .{ .version = 1, .default_type = "doc_v1" });

    const active = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("doc_v1", active.default_type);

    const previous = (try loadSchemaVersion(&store, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("doc_v0", previous.default_type);
}

test "schema copy includes versioned history" {
    const alloc = std.testing.allocator;
    const src_path = try tempTestPath(alloc, "schema-copy-src");
    defer alloc.free(src_path);
    cleanupTestDir(src_path);
    defer cleanupTestDir(src_path);

    const dst_path = try tempTestPath(alloc, "schema-copy-dst");
    defer alloc.free(dst_path);
    cleanupTestDir(dst_path);
    defer cleanupTestDir(dst_path);

    var src = try DocStore.open(alloc, src_path, .{});
    defer src.close();
    var dst = try DocStore.open(alloc, dst_path, .{});
    defer dst.close();

    _ = try saveSchema(&src, alloc, .{ .version = 0, .default_type = "doc_v0" });
    _ = try saveSchema(&src, alloc, .{ .version = 1, .default_type = "doc_v1" });
    try copySchemas(&src, &dst, alloc);

    const active = (try loadSchema(&dst, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("doc_v1", active.default_type);

    const previous = (try loadSchemaVersion(&dst, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("doc_v0", previous.default_type);
}

test "schema save upgrades legacy active-only schema into versioned history" {
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, "schema-legacy-upgrade");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);

    var store = try DocStore.open(alloc, path, .{});
    defer store.close();

    const legacy_data = try serializeSchema(alloc, .{ .version = 0, .default_type = "legacy_v0" });
    defer alloc.free(legacy_data);
    try store.put(schema_key, legacy_data);

    _ = try saveSchema(&store, alloc, .{ .version = 1, .default_type = "next_v1" });

    const active = (try loadSchema(&store, alloc)).?;
    defer freeSchema(alloc, active);
    try std.testing.expectEqual(@as(u32, 1), active.version);
    try std.testing.expectEqualStrings("next_v1", active.default_type);

    const previous = (try loadSchemaVersion(&store, alloc, 0)).?;
    defer freeSchema(alloc, previous);
    try std.testing.expectEqual(@as(u32, 0), previous.version);
    try std.testing.expectEqualStrings("legacy_v0", previous.default_type);
}

test "schema save/load via memory backend store" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    const none = try loadSchema(runtime, alloc);
    try std.testing.expect(none == null);

    const schema = TableSchema{ .version = 11, .default_type = "memdoc" };
    _ = try saveSchema(runtime, alloc, schema);

    const loaded = (try loadSchema(runtime, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 11), loaded.version);
    try std.testing.expectEqualStrings("memdoc", loaded.default_type);
}

test "schema save/load via lsm backend store" {
    const alloc = std.testing.allocator;
    var backend = lsm_backend.Backend.init(alloc, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();

    const none = try loadSchema(runtime, alloc);
    try std.testing.expect(none == null);

    const schema = TableSchema{ .version = 12, .default_type = "lsmdoc" };
    _ = try saveSchema(runtime, alloc, schema);

    const loaded = (try loadSchema(runtime, alloc)).?;
    defer freeSchema(alloc, loaded);
    try std.testing.expectEqual(@as(u32, 12), loaded.version);
    try std.testing.expectEqualStrings("lsmdoc", loaded.default_type);
}

test "glob matching" {
    // Exact
    try std.testing.expect(globMatch("hello", "hello"));
    try std.testing.expect(!globMatch("hello", "world"));

    // Wildcard *
    try std.testing.expect(globMatch("*_embedding", "title_embedding"));
    try std.testing.expect(globMatch("*_embedding", "desc_embedding"));
    try std.testing.expect(!globMatch("*_embedding", "title_text"));

    // Wildcard ?
    try std.testing.expect(globMatch("doc?", "doc1"));
    try std.testing.expect(globMatch("doc?", "docA"));
    try std.testing.expect(!globMatch("doc?", "doc12"));

    // Mixed
    try std.testing.expect(globMatch("*.embedding.*", "field.embedding.vector"));
    try std.testing.expect(!globMatch("*.embedding.*", "field.text.vector"));
}

test "runtime schema field capability helpers classify mapped sortability" {
    const keyword = FieldMapping{
        .field_type = .keyword,
        .do_index = true,
        .doc_values = true,
        .sortable = true,
        .analyzer = "keyword",
    };
    const text = FieldMapping{
        .field_type = .text,
        .do_index = true,
        .doc_values = true,
        .sortable = true,
        .analyzer = "standard",
    };
    const missing_doc_values = FieldMapping{
        .field_type = .numeric,
        .do_index = true,
        .doc_values = false,
        .sortable = true,
        .analyzer = "keyword",
    };
    const non_sortable = FieldMapping{
        .field_type = .datetime,
        .do_index = true,
        .doc_values = true,
        .sortable = false,
        .analyzer = "keyword",
    };
    const geo = FieldMapping{
        .field_type = .geopoint,
        .do_index = true,
        .doc_values = true,
        .sortable = false,
        .analyzer = "standard",
    };
    const geo_missing_doc_values = FieldMapping{
        .field_type = .geopoint,
        .do_index = true,
        .doc_values = false,
        .sortable = false,
        .analyzer = "standard",
    };

    try std.testing.expect(fieldTypeIsSortableScalar(.keyword));
    try std.testing.expect(fieldTypeIsSortableScalar(.datetime));
    try std.testing.expect(!fieldTypeIsSortableScalar(.text));
    try std.testing.expect(!fieldTypeIsSortableScalar(.geopoint));
    try std.testing.expect(mappingIsFilterable(keyword));
    try std.testing.expect(mappingIsFilterable(geo));
    try std.testing.expect(!mappingIsFilterable(geo_missing_doc_values));
    try std.testing.expect(mappingIsAggregatable(keyword));
    try std.testing.expect(mappingIsSortable(keyword));
    try std.testing.expect(!mappingIsSortable(text));
    try std.testing.expect(!mappingIsSortable(geo));
    try std.testing.expect(!mappingIsAggregatable(text));
    try std.testing.expect(mappingHasNativeDocValues(keyword));
    try std.testing.expect(mappingHasNativeDocValues(geo));
    try std.testing.expect(!mappingHasNativeDocValues(geo_missing_doc_values));
    try std.testing.expectEqualStrings("declared", mappingQueryabilityStateName(keyword));
    try std.testing.expectEqualStrings("declared", mappingQueryabilityStateName(geo));
    try std.testing.expectEqualStrings("non_scalar", mappingQueryabilityStateName(text));
    try std.testing.expectEqualStrings("missing_doc_values", mappingQueryabilityStateName(missing_doc_values));
    try std.testing.expectEqualStrings("missing_doc_values", mappingQueryabilityStateName(geo_missing_doc_values));
    try std.testing.expectEqualStrings("non_sortable", mappingQueryabilityStateName(non_sortable));
}

test "runtime schema exact dynamic template path is conservative" {
    const exact = DynamicTemplate{
        .name = "created",
        .path_match = "meta.created_at",
        .match_pattern = "created_at",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true },
    };
    const wildcard = DynamicTemplate{
        .name = "dates",
        .path_match = "meta.*_at",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true },
    };
    const excluded = DynamicTemplate{
        .name = "excluded",
        .path_match = "meta.created_at",
        .path_unmatch = "meta.private.*",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true },
    };
    const mismatched_field = DynamicTemplate{
        .name = "mismatch",
        .path_match = "meta.created_at",
        .match_pattern = "updated_at",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true },
    };

    try std.testing.expectEqualStrings("meta.created_at", exactDynamicTemplatePath(exact).?);
    try std.testing.expect(exactDynamicTemplatePath(wildcard) == null);
    try std.testing.expect(exactDynamicTemplatePath(excluded) == null);
    try std.testing.expect(exactDynamicTemplatePath(mismatched_field) == null);
}

test "runtime schema field capability model carries provenance and index sort membership" {
    const index_sort = [_]IndexSortField{
        .{ .field = "meta.created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    };
    const tmpl = DynamicTemplate{
        .name = "created",
        .path_match = "meta.created_at",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true, .analyzer = "keyword" },
    };
    const text_field = FullTextField{
        .path = "title",
        .emitted_name = "title",
        .analyzer = "english",
    };
    const templates = [_]DynamicTemplate{tmpl};
    const schema = TableSchema{
        .dynamic_templates = &templates,
        .index_sort = &index_sort,
    };

    const id_capability = reservedIdFieldCapability(schema);
    try std.testing.expectEqualStrings("_id", id_capability.field.?);
    try std.testing.expectEqual(AntflyType.keyword, id_capability.field_type);
    try std.testing.expect(id_capability.sortable);
    try std.testing.expectEqualStrings("identity_metadata", id_capability.doc_value_coverage);
    try std.testing.expectEqualStrings("accelerated", id_capability.sort_lifecycle_state);
    try std.testing.expectEqual(@as(usize, 1), id_capability.index_sort.?.position);
    try std.testing.expect(!id_capability.index_sort.?.desc);

    const dynamic_capability = dynamicTemplateFieldCapability(schema, tmpl);
    try std.testing.expectEqualStrings("created", dynamic_capability.name.?);
    try std.testing.expectEqualStrings("meta.created_at", dynamic_capability.field.?);
    try std.testing.expectEqual(AntflyType.datetime, dynamic_capability.field_type);
    try std.testing.expect(dynamic_capability.filterable);
    try std.testing.expect(dynamic_capability.aggregatable);
    try std.testing.expect(dynamic_capability.sortable);
    try std.testing.expectEqualStrings("dynamic_template", dynamic_capability.provenance);
    try std.testing.expectEqualStrings("declared", dynamic_capability.queryability_state);
    try std.testing.expectEqualStrings("declared", dynamic_capability.sort_lifecycle_state);
    try std.testing.expectEqual(@as(usize, 0), dynamic_capability.index_sort.?.position);
    try std.testing.expect(dynamic_capability.index_sort.?.desc);

    const text_capability = fullTextFieldCapability(schema, "doc", text_field);
    try std.testing.expectEqualStrings("title", text_capability.field.?);
    try std.testing.expectEqualStrings("doc", text_capability.document_schema.?);
    try std.testing.expectEqual(AntflyType.text, text_capability.field_type);
    try std.testing.expect(text_capability.searchable);
    try std.testing.expect(!text_capability.sortable);
    try std.testing.expectEqualStrings("document_schema", text_capability.provenance);
    try std.testing.expectEqualStrings("text_search_only", text_capability.queryability_state);
    try std.testing.expectEqualStrings("unsupported", text_capability.sort_lifecycle_state);
    try std.testing.expect(text_capability.index_sort == null);

    const observed_capability = observedDynamicFieldCapability(schema, "meta.created_at", tmpl.mapping);
    try std.testing.expectEqualStrings("meta.created_at", observed_capability.field.?);
    try std.testing.expectEqual(AntflyType.datetime, observed_capability.field_type);
    try std.testing.expect(observed_capability.filterable);
    try std.testing.expect(observed_capability.aggregatable);
    try std.testing.expect(observed_capability.sortable);
    try std.testing.expectEqualStrings("observed_declared", observed_capability.doc_value_coverage);
    try std.testing.expectEqualStrings("observed_dynamic", observed_capability.provenance);
    try std.testing.expectEqualStrings("declared", observed_capability.queryability_state);
    try std.testing.expectEqualStrings("indexed", observed_capability.sort_lifecycle_state);
    try std.testing.expectEqual(@as(usize, 0), observed_capability.index_sort.?.position);
    try std.testing.expect(observed_capability.index_sort.?.desc);
}

test "runtime schema field capability matrix enumerates shared capabilities" {
    const alloc = std.testing.allocator;
    const templates = [_]DynamicTemplate{.{
        .name = "created",
        .path_match = "created_at",
        .mapping = .{ .field_type = .datetime, .doc_values = true, .sortable = true, .analyzer = "keyword" },
    }};
    const fields = [_]FullTextField{
        .{ .path = "created_at", .emitted_name = "created_at", .analyzer = "keyword" },
        .{ .path = "title", .emitted_name = "title", .analyzer = "english" },
        .{ .path = "title", .emitted_name = "title.keyword", .analyzer = "keyword" },
        .{ .path = "_id", .emitted_name = "_id", .analyzer = "keyword" },
    };
    const docs = [_]FullTextDocument{.{
        .name = "doc",
        .fields = &fields,
    }};
    const index_sort = [_]IndexSortField{
        .{ .field = "created_at", .desc = true },
        .{ .field = "_id", .desc = false },
    };
    const schema = TableSchema{
        .dynamic_templates = &templates,
        .full_text_documents = &docs,
        .index_sort = &index_sort,
    };

    const capabilities = try fieldCapabilitiesAlloc(alloc, schema);
    defer freeFieldCapabilities(alloc, capabilities);

    try std.testing.expectEqual(@as(usize, 4), capabilities.len);
    try std.testing.expectEqualStrings("_id", capabilities[0].field.?);
    try std.testing.expectEqualStrings("reserved", capabilities[0].provenance);
    try std.testing.expectEqualStrings("accelerated", capabilities[0].sort_lifecycle_state);
    try std.testing.expectEqual(@as(usize, 1), capabilities[0].index_sort.?.position);

    try std.testing.expectEqualStrings("created", capabilities[1].name.?);
    try std.testing.expectEqualStrings("created_at", capabilities[1].field.?);
    try std.testing.expectEqualStrings("dynamic_template", capabilities[1].provenance);
    try std.testing.expect(capabilities[1].sortable);
    try std.testing.expectEqualStrings("declared", capabilities[1].sort_lifecycle_state);
    try std.testing.expectEqual(@as(usize, 0), capabilities[1].index_sort.?.position);

    try std.testing.expectEqualStrings("title", capabilities[2].field.?);
    try std.testing.expectEqualStrings("document_schema", capabilities[2].provenance);
    try std.testing.expectEqual(AntflyType.text, capabilities[2].field_type);
    try std.testing.expectEqualStrings("text_search_only", capabilities[2].queryability_state);
    try std.testing.expectEqualStrings("unsupported", capabilities[2].sort_lifecycle_state);
    try std.testing.expect(capabilities[2].index_sort == null);

    try std.testing.expectEqualStrings("title.keyword", capabilities[3].field.?);
    try std.testing.expectEqualStrings("title.keyword", capabilities[3].emitted_name.?);
    try std.testing.expectEqualStrings("document_schema", capabilities[3].provenance);
    try std.testing.expectEqual(AntflyType.keyword, capabilities[3].field_type);
    try std.testing.expect(capabilities[3].filterable);
    try std.testing.expect(!capabilities[3].doc_values);
    try std.testing.expect(!capabilities[3].sortable);
    try std.testing.expectEqualStrings("missing_doc_values", capabilities[3].queryability_state);
    try std.testing.expectEqualStrings("unsupported", capabilities[3].sort_lifecycle_state);
}

test "dynamic template field resolution" {
    const templates = [_]DynamicTemplate{
        .{
            .name = "embeddings",
            .match_pattern = "*_embedding",
            .mapping = .{ .field_type = .embedding, .doc_values = true, .sortable = false },
        },
        .{
            .name = "keywords",
            .match_pattern = "*_id",
            .mapping = .{ .field_type = .keyword, .doc_values = true, .sortable = true },
        },
    };

    const schema = TableSchema{
        .dynamic_templates = &templates,
        .enforce_types = true,
    };

    const emb = resolveFieldType(schema, "title_embedding");
    try std.testing.expect(emb != null);
    try std.testing.expectEqual(AntflyType.embedding, emb.?.field_type);
    try std.testing.expect(emb.?.doc_values);

    const kw = resolveFieldType(schema, "user_id");
    try std.testing.expect(kw != null);
    try std.testing.expectEqual(AntflyType.keyword, kw.?.field_type);
    try std.testing.expect(kw.?.sortable);

    const unknown = resolveFieldType(schema, "random_field");
    try std.testing.expect(unknown == null);

    // Validation: enforce_types rejects unknown fields
    const result = validateFields(schema, &.{"random_field"});
    try std.testing.expectError(error.UnknownFieldType, result);

    // Known fields pass validation
    try validateFields(schema, &.{"title_embedding"});
}

test "dynamic template selector and mapping-option resolution" {
    const templates = [_]DynamicTemplate{
        .{
            .name = "dates",
            .match_pattern = "*_at",
            .unmatch_pattern = "skip_*",
            .path_match = "meta.*",
            .path_unmatch = "meta.private.*",
            .match_mapping_type = "date",
            .mapping = .{
                .field_type = .datetime,
                .do_index = false,
                .store = false,
                .doc_values = true,
                .sortable = true,
                .include_in_all = false,
                .analyzer = "keyword",
            },
        },
        .{
            .name = "keywords",
            .path_match = "meta.tags.*",
            .match_mapping_type = "string",
            .mapping = .{
                .field_type = .keyword,
                .include_in_all = true,
                .analyzer = "keyword",
            },
        },
    };

    const schema = TableSchema{ .dynamic_templates = &templates };

    const created = resolveFieldTypeForValue(schema, "meta.created_at", .{ .string = "2026-01-03T00:00:00Z" });
    try std.testing.expect(created != null);
    try std.testing.expectEqual(AntflyType.datetime, created.?.field_type);
    try std.testing.expect(!created.?.do_index);
    try std.testing.expect(created.?.doc_values);
    try std.testing.expect(created.?.sortable);
    try std.testing.expectEqualStrings("keyword", created.?.analyzer);

    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.skip_created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.private.created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.created_at", .{ .string = "not-a-date" }) == null);
    try std.testing.expect(resolveFieldType(schema, "meta.created_at") == null);

    const declared_created = resolveDeclaredFieldType(schema, "meta.created_at");
    try std.testing.expect(declared_created != null);
    try std.testing.expectEqual(AntflyType.datetime, declared_created.?.field_type);
    try std.testing.expect(declared_created.?.doc_values);
    try std.testing.expect(declared_created.?.sortable);
    try std.testing.expect(resolveDeclaredFieldType(schema, "meta.skip_created_at") == null);
    try std.testing.expect(resolveDeclaredFieldType(schema, "meta.private.created_at") == null);

    const tag = resolveFieldTypeForValue(schema, "meta.tags.primary", .{ .string = "alpha" });
    try std.testing.expect(tag != null);
    try std.testing.expectEqual(AntflyType.keyword, tag.?.field_type);
    try std.testing.expect(tag.?.include_in_all);
}

test "parseDateTimeToNs accepts rfc3339 and date-only values" {
    try std.testing.expectEqual(@as(?u64, 15), parseDateTimeToNs("1970-01-01T00:00:00.000000015Z"));
    try std.testing.expectEqual(@as(?u64, 0), parseDateTimeToNs("1970-01-01"));
    try std.testing.expect(parseDateTimeToNs("not-a-date") == null);

    const formatted = try formatDateTimeNsAlloc(std.testing.allocator, 15);
    defer std.testing.allocator.free(formatted);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00.000000015Z", formatted);
    try std.testing.expectEqual(@as(?u64, 15), parseDateTimeToNs(formatted));
}
