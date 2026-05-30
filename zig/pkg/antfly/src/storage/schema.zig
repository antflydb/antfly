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
//!   - FieldMapping: type + index/store/doc_values/analyzer settings
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
const platform_time = @import("../platform/time.zig");

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
    json = 12,
};

pub const FieldMapping = struct {
    field_type: AntflyType = .text,
    do_index: bool = true,
    store: bool = true,
    doc_values: bool = false,
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

/// Storage profile for a table. See zig/RELATIONAL.md.
pub const StorageMode = enum(u8) {
    document = 0,
    relational = 1,
};

/// A declared typed column of a relational table. `json` columns
/// (field_type == .json) are indexed as document subtrees rather than as a
/// typed column.
pub const RelationalColumn = struct {
    name: []const u8,
    path: []const u8,
    field_type: AntflyType = .text,
    nullable: bool = true,
};

pub const TableSchema = struct {
    version: u32 = 0,
    default_type: []const u8 = "_default",
    ttl_duration_ns: u64 = 0,
    ttl_field: []const u8 = "_timestamp",
    enforce_types: bool = false,
    storage_mode: StorageMode = .document,
    dynamic_templates: []const DynamicTemplate = &.{},
    full_text_documents: []const FullTextDocument = &.{},
    relational_columns: []const RelationalColumn = &.{},
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
    try appendU32(&buf, alloc, 9); // format version
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

    // Storage mode + relational column catalog (format version 9+).
    try buf.append(alloc, @intFromEnum(schema.storage_mode));
    try appendU32(&buf, alloc, @intCast(schema.relational_columns.len));
    for (schema.relational_columns) |column| {
        try appendStr(&buf, alloc, column.name);
        try appendStr(&buf, alloc, column.path);
        try buf.append(alloc, @intFromEnum(column.field_type));
        try buf.append(alloc, if (column.nullable) 1 else 0);
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
    if (fmt_version != 1 and fmt_version != 2 and fmt_version != 3 and fmt_version != 4 and fmt_version != 5 and fmt_version != 6 and fmt_version != 7 and fmt_version != 8 and fmt_version != 9) return error.UnsupportedVersion;

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
    errdefer freeFullTextDocumentsSlice(alloc, full_text_documents);

    const storage_mode: StorageMode = if (fmt_version >= 9) blk: {
        const mode: StorageMode = @enumFromInt(data[pos]);
        pos += 1;
        break :blk mode;
    } else .document;

    const relational_columns: []RelationalColumn = if (fmt_version >= 9) blk: {
        const column_count = readU32(data, &pos);
        const columns = try alloc.alloc(RelationalColumn, column_count);
        var columns_initialized: usize = 0;
        errdefer {
            for (columns[0..columns_initialized]) |column| {
                alloc.free(column.name);
                alloc.free(column.path);
            }
            alloc.free(columns);
        }
        for (columns) |*column| {
            const name = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(name);
            const path = try alloc.dupe(u8, readStr(data, &pos));
            errdefer alloc.free(path);
            const field_type: AntflyType = @enumFromInt(data[pos]);
            pos += 1;
            const nullable = data[pos] == 1;
            pos += 1;
            column.* = .{ .name = name, .path = path, .field_type = field_type, .nullable = nullable };
            columns_initialized += 1;
        }
        break :blk columns;
    } else &.{};

    return .{
        .version = version,
        .default_type = default_type,
        .ttl_duration_ns = ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = enforce_types,
        .storage_mode = storage_mode,
        .dynamic_templates = templates,
        .full_text_documents = full_text_documents,
        .relational_columns = relational_columns,
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
    freeFullTextDocumentsSlice(alloc, s.full_text_documents);
    for (s.relational_columns) |column| {
        alloc.free(column.name);
        alloc.free(column.path);
    }
    if (s.relational_columns.len > 0) alloc.free(s.relational_columns);
}

fn freeFullTextDocumentsSlice(alloc: Allocator, docs: []const FullTextDocument) void {
    for (docs) |doc| {
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
    if (docs.len > 0) alloc.free(docs);
}

/// Save a schema to DocStore.
pub fn saveSchema(store: anytype, alloc: Allocator, schema: TableSchema) !void {
    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);
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
}

/// Load a schema from DocStore. Returns null if no schema exists.
pub fn loadSchema(store: anytype, alloc: Allocator) !?TableSchema {
    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginRead();
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
    var txn = try runtime.store.beginRead();
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
    var source_txn = try source_runtime.store.beginRead();
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

    const entries = try backend_scan.scanPrefix(alloc, &source_runtime.store, schema_version_prefix);
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

/// Resolve the field type for a field/path using dynamic templates and an
/// optional runtime value for `match_mapping_type` matching.
pub fn resolveFieldTypeForValue(schema: TableSchema, path: []const u8, value: ?std.json.Value) ?FieldMapping {
    const field_name = fieldNameFromPath(path);
    for (schema.dynamic_templates) |tmpl| {
        if (dynamicTemplateMatches(tmpl, path, field_name, value)) return tmpl.mapping;
    }
    return null;
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

fn inferDynamicTemplateMatchType(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |text| if (parseRfc3339ToNs(text) != null or isValidDate(text)) "date" else "string",
        .integer, .float, .number_string => "number",
        .bool => "boolean",
        .object => "object",
        else => null,
    };
}

fn fieldNameFromPath(path: []const u8) []const u8 {
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

fn isValidDate(value: []const u8) bool {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return false;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return false;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return false;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return false;
    return daysFromCivil(year, month, day) >= 0;
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

    // Default document mode round-trips with no relational columns.
    try std.testing.expectEqual(StorageMode.document, loaded.storage_mode);
    try std.testing.expectEqual(@as(usize, 0), loaded.relational_columns.len);
}

test "schema serialize/deserialize round-trips relational storage mode and columns" {
    const alloc = std.testing.allocator;

    const schema = TableSchema{
        .version = 7,
        .default_type = "row",
        .enforce_types = true,
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .nullable = false },
            .{ .name = "created_at", .path = "created_at", .field_type = .datetime, .nullable = true },
            .{ .name = "payload", .path = "payload", .field_type = .json, .nullable = true },
        },
    };

    const data = try serializeSchema(alloc, schema);
    defer alloc.free(data);

    const loaded = try deserializeSchema(alloc, data);
    defer freeSchema(alloc, loaded);

    try std.testing.expectEqual(StorageMode.relational, loaded.storage_mode);
    try std.testing.expectEqual(@as(usize, 4), loaded.relational_columns.len);
    try std.testing.expectEqualStrings("id", loaded.relational_columns[0].name);
    try std.testing.expectEqualStrings("id", loaded.relational_columns[0].path);
    try std.testing.expectEqual(AntflyType.keyword, loaded.relational_columns[0].field_type);
    try std.testing.expect(!loaded.relational_columns[0].nullable);
    try std.testing.expectEqual(AntflyType.numeric, loaded.relational_columns[1].field_type);
    try std.testing.expectEqual(AntflyType.datetime, loaded.relational_columns[2].field_type);
    try std.testing.expect(loaded.relational_columns[2].nullable);
    try std.testing.expectEqual(AntflyType.json, loaded.relational_columns[3].field_type);
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
    try saveSchema(&store, alloc, schema);

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

    try saveSchema(&store, alloc, .{ .version = 0, .default_type = "doc_v0" });
    try saveSchema(&store, alloc, .{ .version = 1, .default_type = "doc_v1" });

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

    try saveSchema(&src, alloc, .{ .version = 0, .default_type = "doc_v0" });
    try saveSchema(&src, alloc, .{ .version = 1, .default_type = "doc_v1" });
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

    try saveSchema(&store, alloc, .{ .version = 1, .default_type = "next_v1" });

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
    try saveSchema(runtime, alloc, schema);

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
    try saveSchema(runtime, alloc, schema);

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

test "dynamic template field resolution" {
    const templates = [_]DynamicTemplate{
        .{
            .name = "embeddings",
            .match_pattern = "*_embedding",
            .mapping = .{ .field_type = .embedding, .doc_values = true },
        },
        .{
            .name = "keywords",
            .match_pattern = "*_id",
            .mapping = .{ .field_type = .keyword },
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
    try std.testing.expectEqualStrings("keyword", created.?.analyzer);

    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.skip_created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.private.created_at", .{ .string = "2026-01-03T00:00:00Z" }) == null);
    try std.testing.expect(resolveFieldTypeForValue(schema, "meta.created_at", .{ .string = "not-a-date" }) == null);

    const tag = resolveFieldTypeForValue(schema, "meta.tags.primary", .{ .string = "alpha" });
    try std.testing.expect(tag != null);
    try std.testing.expectEqual(AntflyType.keyword, tag.?.field_type);
    try std.testing.expect(tag.?.include_in_all);
}
