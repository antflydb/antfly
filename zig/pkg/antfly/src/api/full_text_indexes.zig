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
const builtin = @import("builtin");
const chunker_mod = if (builtin.os.tag == .freestanding)
    @import("../storage/db/enrichment/chunker_stub.zig")
else
    @import("../storage/db/enrichment/chunker.zig");
const template = if (builtin.os.tag == .freestanding)
    @import("../storage/db/template_stub.zig")
else
    @import("../template.zig");
const template_remote = if (builtin.os.tag == .freestanding)
    @import("../storage/db/template_remote_stub.zig")
else
    @import("../template_remote.zig");

pub const default_full_text_index_name = "full_text_index_v0";

pub const FullTextSourceMode = enum {
    document,
    artifact_only,
    document_plus_artifact,
};

pub const FullTextIndexSpec = struct {
    name: []u8,
    config_json: []u8,
    source_artifact_name: ?[]u8 = null,
    source_mode: FullTextSourceMode = .document,
    chunked_sources: []ChunkedFullTextSource = &.{},

    pub fn deinit(self: *FullTextIndexSpec, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.config_json);
        if (self.source_artifact_name) |source_artifact_name| alloc.free(source_artifact_name);
        freeChunkedFullTextSources(alloc, self.chunked_sources);
        self.* = undefined;
    }
};

pub const ChunkedFullTextSource = struct {
    source_field: []u8,
    source_template: []u8,
    artifact_name: []u8,
    chunker_json: []u8,

    pub fn deinit(self: *ChunkedFullTextSource, alloc: std.mem.Allocator) void {
        alloc.free(self.source_field);
        if (self.source_template.len > 0) alloc.free(self.source_template);
        alloc.free(self.artifact_name);
        alloc.free(self.chunker_json);
        self.* = undefined;
    }
};

pub fn listFullTextIndexNamesAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![][]u8 {
    if (indexes_json.len == 0) return try alloc.alloc([]u8, 0);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        try names.append(alloc, try alloc.dupe(u8, entry.key_ptr.*));
    }

    std.mem.sort([]u8, names.items, {}, lessString);
    return try names.toOwnedSlice(alloc);
}

pub fn listFullTextIndexSpecsAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]FullTextIndexSpec {
    if (indexes_json.len == 0) return try alloc.alloc(FullTextIndexSpec, 0);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var specs = std.ArrayListUnmanaged(FullTextIndexSpec).empty;
    errdefer {
        for (specs.items) |*spec| spec.deinit(alloc);
        specs.deinit(alloc);
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        errdefer alloc.free(encoded);
        const source_artifact_name = try extractSourceArtifactNameAlloc(alloc, entry.value_ptr.*);
        try specs.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .config_json = encoded,
            .source_artifact_name = source_artifact_name,
            .source_mode = if (source_artifact_name != null) .artifact_only else .document,
        });
    }

    std.mem.sort(FullTextIndexSpec, specs.items, {}, lessSpec);
    return try specs.toOwnedSlice(alloc);
}

pub fn hasChunkedFullTextSourceAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !bool {
    if (indexes_json.len == 0) return false;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!indexUsesChunkedFullText(entry.value_ptr.*)) continue;
        return true;
    }
    return false;
}

pub fn freeFullTextIndexSpecs(alloc: std.mem.Allocator, specs: []FullTextIndexSpec) void {
    for (specs) |*spec| spec.deinit(alloc);
    if (specs.len > 0) alloc.free(specs);
}

pub fn listChunkedFullTextSourcesAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]ChunkedFullTextSource {
    if (indexes_json.len == 0) return try alloc.alloc(ChunkedFullTextSource, 0);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var sources = std.ArrayListUnmanaged(ChunkedFullTextSource).empty;
    errdefer {
        for (sources.items) |*source| source.deinit(alloc);
        sources.deinit(alloc);
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (!indexUsesChunkedFullText(entry.value_ptr.*)) continue;
        try sources.append(alloc, try extractChunkedFullTextSourceAlloc(alloc, entry.key_ptr.*, entry.value_ptr.*));
    }

    std.mem.sort(ChunkedFullTextSource, sources.items, {}, lessChunkedSource);
    return try sources.toOwnedSlice(alloc);
}

pub fn cloneChunkedFullTextSourcesAlloc(
    alloc: std.mem.Allocator,
    sources: []const ChunkedFullTextSource,
) ![]ChunkedFullTextSource {
    const cloned = try alloc.alloc(ChunkedFullTextSource, sources.len);
    errdefer alloc.free(cloned);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*source| source.deinit(alloc);
    }
    for (sources, 0..) |source, idx| {
        cloned[idx] = .{
            .source_field = try alloc.dupe(u8, source.source_field),
            .source_template = if (source.source_template.len > 0) try alloc.dupe(u8, source.source_template) else &.{},
            .artifact_name = try alloc.dupe(u8, source.artifact_name),
            .chunker_json = try alloc.dupe(u8, source.chunker_json),
        };
        initialized += 1;
    }
    return cloned;
}

pub fn freeChunkedFullTextSources(alloc: std.mem.Allocator, sources: []ChunkedFullTextSource) void {
    for (sources) |*source| source.deinit(alloc);
    if (sources.len > 0) alloc.free(sources);
}

pub fn synthesizeChunkedFullTextAlloc(
    alloc: std.mem.Allocator,
    raw_doc: []const u8,
    sources: []const ChunkedFullTextSource,
) ![]u8 {
    if (sources.len == 0) return try alloc.dupe(u8, "");

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (sources) |source| {
        const source_text = try extractChunkedSourceTextAlloc(alloc, raw_doc, source);
        defer if (source_text) |value| alloc.free(value);
        const text = source_text orelse continue;
        if (text.len == 0) continue;

        const chunks = try chunker_mod.chunkTextWithConfigJson(alloc, text, source.chunker_json);
        defer chunker_mod.freeChunks(alloc, chunks);

        for (chunks) |chunk| {
            const chunk_text = chunk.text orelse continue;
            if (chunk_text.len == 0) continue;
            if (out.items.len > 0) try out.append(alloc, '\n');
            try out.appendSlice(alloc, chunk_text);
        }
    }

    return if (out.items.len == 0) try alloc.dupe(u8, "") else try out.toOwnedSlice(alloc);
}

pub fn selectActiveFullTextIndexNameAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
) !?[]u8 {
    const selected_version = if (read_schema_json.len > 0)
        try schemaVersion(read_schema_json)
    else
        try schemaVersion(schema_json);
    return try selectFullTextIndexNameForVersionAlloc(alloc, indexes_json, selected_version);
}

pub fn selectFullTextIndexNameForVersionAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) !?[]u8 {
    if (indexes_json.len == 0) return null;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    if (version == 0) {
        if (root.get(default_full_text_index_name)) |value| {
            if (isFullTextIndexConfig(value)) return try alloc.dupe(u8, default_full_text_index_name);
        }
        if (root.get("default")) |value| {
            if (isFullTextIndexConfig(value)) return try alloc.dupe(u8, "default");
        }
    }

    const versioned_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{version});
    defer alloc.free(versioned_name);
    if (root.get(versioned_name)) |value| {
        if (isFullTextIndexConfig(value)) return try alloc.dupe(u8, versioned_name);
    }

    var only_full_text_name: ?[]const u8 = null;
    const default_is_full_text = if (root.get("default")) |value| isFullTextIndexConfig(value) else false;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        if (only_full_text_name != null) return if (version == 0 and default_is_full_text)
            try alloc.dupe(u8, "default")
        else
            null;
        only_full_text_name = entry.key_ptr.*;
    }
    if (only_full_text_name) |name| return try alloc.dupe(u8, name);
    return null;
}

pub fn isFullTextIndexConfig(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return true;
    if (type_value != .string) return false;
    return std.mem.eql(u8, type_value.string, "full_text");
}

fn schemaVersion(schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const version_value = root.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| std.math.cast(u32, value) orelse error.InvalidSchemaUpdateRequest,
        else => error.InvalidSchemaUpdateRequest,
    };
}

fn lessString(_: void, lhs: []u8, rhs: []u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

fn lessSpec(_: void, lhs: FullTextIndexSpec, rhs: FullTextIndexSpec) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn lessChunkedSource(_: void, lhs: ChunkedFullTextSource, rhs: ChunkedFullTextSource) bool {
    if (std.mem.lessThan(u8, lhs.artifact_name, rhs.artifact_name)) return true;
    if (std.mem.lessThan(u8, rhs.artifact_name, lhs.artifact_name)) return false;
    return std.mem.lessThan(u8, lhs.source_field, rhs.source_field);
}

fn extractSourceArtifactNameAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?[]u8 {
    if (value != .object) return null;
    if (value.object.get("artifact_name")) |artifact_name| {
        if (artifact_name == .string and artifact_name.string.len > 0) return try alloc.dupe(u8, artifact_name.string);
        return null;
    }
    const chunk_name = value.object.get("chunk_name") orelse return null;
    if (chunk_name != .string or chunk_name.string.len == 0) return null;
    return try alloc.dupe(u8, chunk_name.string);
}

fn extractChunkedFullTextSourceAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    value: std.json.Value,
) !ChunkedFullTextSource {
    if (value != .object) return error.InvalidTableIndexMetadata;
    if (value.object.get("generator")) |generator| {
        if (generator != .object) return error.InvalidTableIndexMetadata;
        const source_field = generator.object.get("source_field") orelse return error.InvalidTableIndexMetadata;
        if (source_field != .string or source_field.string.len == 0) return error.InvalidTableIndexMetadata;
        const chunker = generator.object.get("chunker") orelse return error.InvalidTableIndexMetadata;
        const artifact_name = generator.object.get("artifact_name");
        const chunk_name = generator.object.get("chunk_name");
        return .{
            .source_field = try alloc.dupe(u8, source_field.string),
            .source_template = if (generator.object.get("source_template")) |template_value|
                if (template_value == .string and template_value.string.len > 0) try alloc.dupe(u8, template_value.string) else &.{}
            else
                &.{},
            .artifact_name = if (chunk_name) |chunk_name_value|
                if (chunk_name_value == .string and chunk_name_value.string.len > 0) try alloc.dupe(u8, chunk_name_value.string) else try alloc.dupe(u8, source_field.string)
            else if (artifact_name) |artifact_name_value|
                if (artifact_name_value == .string and artifact_name_value.string.len > 0) try alloc.dupe(u8, artifact_name_value.string) else try alloc.dupe(u8, source_field.string)
            else
                try alloc.dupe(u8, source_field.string),
            .chunker_json = try std.json.Stringify.valueAlloc(alloc, chunker, .{}),
        };
    }

    const chunker = value.object.get("chunker") orelse return error.InvalidTableIndexMetadata;
    const source_field = if (value.object.get("field")) |field_value|
        if (field_value == .string and field_value.string.len > 0) field_value.string else return error.InvalidTableIndexMetadata
    else
        "embedding";
    const source_template = if (value.object.get("template")) |template_value|
        if (template_value == .string and template_value.string.len > 0) template_value.string else ""
    else
        "";
    const artifact_name = try std.fmt.allocPrint(alloc, "{s}_chunks", .{index_name});
    errdefer alloc.free(artifact_name);
    return .{
        .source_field = try alloc.dupe(u8, source_field),
        .source_template = if (source_template.len > 0) try alloc.dupe(u8, source_template) else &.{},
        .artifact_name = artifact_name,
        .chunker_json = try std.json.Stringify.valueAlloc(alloc, chunker, .{}),
    };
}

fn extractChunkedSourceTextAlloc(
    alloc: std.mem.Allocator,
    raw_doc: []const u8,
    source: ChunkedFullTextSource,
) !?[]u8 {
    if (source.source_template.len > 0) {
        const rendered = template_remote.renderJsonToText(alloc, source.source_template, raw_doc) catch return null;
        if (rendered.len == 0) {
            alloc.free(rendered);
            return null;
        }
        return @constCast(rendered);
    }

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw_doc, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const field = parsed.value.object.get(source.source_field) orelse return null;
    if (field != .string or field.string.len == 0) return null;
    return try alloc.dupe(u8, field.string);
}

fn indexUsesChunkedFullText(value: std.json.Value) bool {
    if (value != .object) return false;
    if (value.object.get("type")) |type_value| {
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) return false;
    }
    const chunker = if (value.object.get("generator")) |generator|
        if (generator == .object) generator.object.get("chunker") orelse return false else return false
    else
        value.object.get("chunker") orelse return false;
    if (chunker != .object) return false;
    return chunker.object.get("full_text_index") != null;
}

test "list full text index names sorts deterministically" {
    const alloc = std.testing.allocator;
    const names = try listFullTextIndexNamesAlloc(
        alloc,
        "{\"semantic_idx\":{\"type\":\"embeddings\"},\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v0\":{\"type\":\"full_text\"}}",
    );
    defer {
        for (names) |name| alloc.free(name);
        alloc.free(names);
    }
    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expectEqualStrings("full_text_index_v0", names[0]);
    try std.testing.expectEqualStrings("full_text_index_v1", names[1]);
}

test "select active full text index prefers read schema version" {
    const alloc = std.testing.allocator;
    const name = try selectActiveFullTextIndexNameAlloc(
        alloc,
        "{\"version\":1}",
        "{\"version\":0}",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
    );
    defer if (name) |value| alloc.free(value);
    try std.testing.expectEqualStrings("full_text_index_v0", name.?);
}

test "list full text index specs captures source artifact names" {
    const alloc = std.testing.allocator;
    const specs = try listFullTextIndexSpecsAlloc(
        alloc,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"chunks\":{\"type\":\"full_text\",\"artifact_name\":\"serverless_chunk_preview\"}}",
    );
    defer freeFullTextIndexSpecs(alloc, specs);

    try std.testing.expectEqual(@as(usize, 2), specs.len);
    try std.testing.expectEqualStrings("chunks", specs[0].name);
    try std.testing.expectEqualStrings("serverless_chunk_preview", specs[0].source_artifact_name.?);
    try std.testing.expectEqual(FullTextSourceMode.artifact_only, specs[0].source_mode);
    try std.testing.expectEqualStrings("full_text_index_v0", specs[1].name);
    try std.testing.expect(specs[1].source_artifact_name == null);
    try std.testing.expectEqual(FullTextSourceMode.document, specs[1].source_mode);
}

test "list full text index specs still accepts legacy chunk name alias" {
    const alloc = std.testing.allocator;
    const specs = try listFullTextIndexSpecsAlloc(
        alloc,
        "{\"chunks\":{\"type\":\"full_text\",\"chunk_name\":\"serverless_chunk_preview\"}}",
    );
    defer freeFullTextIndexSpecs(alloc, specs);

    try std.testing.expectEqual(@as(usize, 1), specs.len);
    try std.testing.expectEqualStrings("serverless_chunk_preview", specs[0].source_artifact_name.?);
    try std.testing.expectEqual(FullTextSourceMode.artifact_only, specs[0].source_mode);
}

test "detects chunked full text sources from embedding chunker config" {
    try std.testing.expect(try hasChunkedFullTextSourceAlloc(
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"chunk_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8}}}}}",
    ));
    try std.testing.expect(!(try hasChunkedFullTextSourceAlloc(
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"chunk_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":8}}}}}",
    )));
    try std.testing.expect(try hasChunkedFullTextSourceAlloc(
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"chunk_idx\":{\"field\":\"body\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8}}}}}",
    ));
}

test "list chunked full text sources captures source metadata" {
    const alloc = std.testing.allocator;
    const sources = try listChunkedFullTextSourcesAlloc(
        alloc,
        "{\"chunk_idx\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dimension\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"title\",\"source_template\":\"{{title}} {{body}}\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":8}}}}}",
    );
    defer freeChunkedFullTextSources(alloc, sources);

    try std.testing.expectEqual(@as(usize, 1), sources.len);
    try std.testing.expectEqualStrings("title", sources[0].source_field);
    try std.testing.expectEqualStrings("{{title}} {{body}}", sources[0].source_template);
    try std.testing.expectEqualStrings("body_chunks_v1", sources[0].artifact_name);
    try std.testing.expect(std.mem.indexOf(u8, sources[0].chunker_json, "\"full_text_index\"") != null);
}

test "synthesize chunked full text renders source fields and templates" {
    const alloc = std.testing.allocator;
    const sources = try listChunkedFullTextSourcesAlloc(
        alloc,
        "{\"chunk_idx\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dimension\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"title\",\"source_template\":\"{{title}} {{body}}\",\"artifact_name\":\"body_chunks_v1\",\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":2,\"overlap_tokens\":0,\"separator\":\" \"}}}}}",
    );
    defer freeChunkedFullTextSources(alloc, sources);

    const chunked = try synthesizeChunkedFullTextAlloc(
        alloc,
        "{\"title\":\"alpha beta\",\"body\":\"gamma delta\"}",
        sources,
    );
    defer alloc.free(chunked);

    try std.testing.expect(std.mem.indexOf(u8, chunked, "alpha beta") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunked, "gamma delta") != null);
}
