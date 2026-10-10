// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded native enrichment units shared by archive and accepted-WAL builds.
const std = @import("std");
const local = @import("antfly_local_sources");
const managed = local.inference_managed_embedder;
const V = std.json.Value;
const A = std.mem.Allocator;
pub const Part = @import("antfly_template_content").ContentPart;
pub const Unit = struct {
    ordinal: u32 = 0,
    source_ordinal: u32 = 0,
    source_part: ?Part = null,
    source_json: ?[]const u8 = null,
    source_fingerprint: ?[]const u8 = null,
    parts: []const Part = &.{},
    materialized: ?V = null,
    start_offset: ?u32 = null,
    end_offset: ?u32 = null,
    start_time_ms: ?f32 = null,
    end_time_ms: ?f32 = null,
    frame_index: ?u32 = null,
    frame_delay_ms: ?u32 = null,
    chunked: bool = false,
};
pub fn configured(config: V, name: []const u8) ?V {
    const value = config.object.get(name) orelse return null;
    return if (value == .null) null else value;
}
fn renderCancelled(raw: *const anyopaque) bool {
    const opts: *const managed.InitOptions = @ptrCast(@alignCast(raw));
    if (opts.cancellation) |token| {
        token.check() catch return true;
    }
    return false;
}
pub const max_units: usize = 4096;
pub const marker = "\x1fu2:";
pub fn parent(key: []const u8) ![]const u8 {
    const position = std.mem.lastIndexOf(u8, key, marker) orelse return key;
    const suffix = key[position + marker.len ..];
    if (suffix.len != 8) return error.InvalidLakeUnitIdentity;
    _ = std.fmt.parseUnsigned(u32, suffix, 16) catch return error.InvalidLakeUnitIdentity;
    return key[0..position];
}
pub fn identity(a: A, key: []const u8, unit: Unit) ![]const u8 {
    if (!unit.chunked) return a.dupe(u8, key);
    return std.fmt.allocPrint(a, "{s}{s}{x:0>8}", .{ key, marker, unit.ordinal });
}
/// Rebind a private member to its public parent while retaining member identity.
pub fn rebind(a: A, key: []const u8, public_parent: []const u8) ![]u8 {
    const base = try parent(key);
    return std.fmt.allocPrint(a, "{s}{s}", .{ public_parent, key[base.len..] });
}
pub fn prepare(a: A, config: V, column: []const u8, row: V, options: ?managed.InitOptions) ![]const Unit {
    if (row != .object or config != .object) return error.InvalidEmbeddingInput;
    const value = row.object.get(column) orelse .null;
    if (configured(config, "embedder") == null) {
        if (value == .null) return &.{};
        const result = try a.alloc(Unit, 1);
        result[0] = .{ .materialized = value };
        return result;
    }
    const opts = options orelse return error.LakeEmbeddingProviderUnavailable;
    if (opts.cancellation) |token| try token.check();
    if (opts.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
    var parts: []const Part = if (configured(config, "template")) |source| templated: {
        if (source != .string) return error.InvalidEmbeddingInput;
        const bytes = try std.json.Stringify.valueAlloc(a, row, .{});
        defer a.free(bytes);
        break :templated try local.template_remote.renderJsonToPartsWithConfig(a, source.string, bytes, .{
            .io = opts.io,
            .deadline_ns = opts.deadline_ns,
            .cancellation = if (opts.cancellation != null) .{ .ptr = &opts, .is_cancelled_fn = renderCancelled } else null,
            .secret_store = opts.secret_store,
            .remote_content = opts.remote_content,
            .max_media_parts = max_units,
        });
    } else if (value == .null) return &.{} else if (value == .string)
        try local.template.textToParts(a, value.string)
    else
        return error.InvalidEmbeddingInput;
    if (parts.len > max_units) return error.LakeEnrichmentUnitBudgetExceeded;
    // Resolve every URL before memoization: cached provider output describes
    // captured bytes, rather than whatever a mutable URL returns tomorrow.
    var remaining: usize = 64 * 1024 * 1024;
    const resolved = try a.alloc(Part, parts.len);
    for (parts, 0..) |part, position| {
        if (opts.cancellation) |token| try token.check();
        resolved[position] = if (part == .media_url) fetched: {
            const outcome = try local.template_remote.downloadRemoteContentOutcomeAllocWithRenderConfig(a, .{
                .io = opts.io,
                .deadline_ns = opts.deadline_ns,
                .cancellation = if (opts.cancellation != null) .{ .ptr = &opts, .is_cancelled_fn = renderCancelled } else null,
                .secret_store = opts.secret_store,
                .remote_content = opts.remote_content,
            }, part.media_url, null);
            if (outcome != .ok) return error.RemoteMediaFetchFailed;
            break :fetched .{ .binary = .{ .data = outcome.ok.data, .mime_type = outcome.ok.content_type } };
        } else part;
        const size = switch (resolved[position]) {
            .text => |text| text.len,
            .binary => |binary| binary.data.len,
            .media_url => unreachable,
        };
        remaining = std.math.sub(usize, remaining, size) catch return error.LakeEnrichmentUnitBudgetExceeded;
    }
    parts = resolved;
    const chunker = configured(config, "chunker") orelse {
        if (parts.len == 0) return &.{};
        const result = try a.alloc(Unit, 1);
        result[0] = .{ .parts = parts };
        return result;
    };
    const chunking = local.chunking_mod;
    var cfg = try chunking.types.parseConfigFromSlice(a, try std.json.Stringify.valueAlloc(a, chunker, .{}));
    defer cfg.deinit(a);
    const provider: chunking.Provider = .{
        .ptr = if (opts.antfly_provider) |p| p.ptr else null,
        .boundary_dispatch = if (opts.antfly_provider) |p| p.boundary_dispatch else null,
        .chunk_input_callback = if (opts.antfly_provider) |p| if (p.chunk_input) |callback| @ptrCast(callback) else null else null,
        .chunk_input_with_context_callback = if (opts.antfly_provider) |p| if (p.chunk_input_with_context) |callback| @ptrCast(callback) else null else null,
        .execution = .{
            .default_endpoint = opts.inference_api_url,
            .capability_cache = opts.remote_capability_cache,
            .io = opts.io,
            .routing = .{ .source_table = opts.source_table },
            .deadline_ns = opts.deadline_ns,
            .cancellation = opts.cancellation orelse .none,
            .http_client = if (opts.provider_runtime) |runtime| try runtime.httpClient() else null,
        },
    };
    var units: std.ArrayList(Unit) = .empty;
    var input_remaining: usize = 64 * 1024 * 1024;
    var output_remaining: usize = 64 * 1024 * 1024;
    for (parts, 0..) |part, source_ordinal| {
        try provider.execution.check(@import("antfly_platform").time.monotonicNs());
        const bytes = switch (part) {
            .text => |text| text.len,
            .binary => |binary| binary.data.len,
            .media_url => |url| url.len,
        };
        input_remaining = std.math.sub(usize, input_remaining, bytes) catch return error.LakeEnrichmentUnitBudgetExceeded;
        if (part == .text) {
            const chunks = try local.storage_db_enrichment_chunker.chunkTextWithConfigJsonAndProvider(a, part.text, try std.json.Stringify.valueAlloc(a, chunker, .{}), provider);
            for (chunks) |chunk| {
                if (units.items.len == max_units) return error.LakeEnrichmentUnitBudgetExceeded;
                const owned_bytes = if (chunk.text) |text| text.len else 0;
                output_remaining = std.math.sub(usize, output_remaining, owned_bytes) catch return error.LakeEnrichmentUnitBudgetExceeded;
                const unit_parts = try a.alloc(Part, 1);
                unit_parts[0] = .{ .text = chunk.text orelse return error.InvalidChunkerResponse };
                try units.append(a, .{ .ordinal = @intCast(units.items.len), .source_ordinal = @intCast(source_ordinal), .source_part = part, .parts = unit_parts, .start_offset = chunk.start_offset, .end_offset = chunk.end_offset, .chunked = true });
            }
        } else {
            const media = switch (part) {
                .binary => |media| media,
                .media_url => |url| resolved: {
                    const fetched = try local.template_remote.downloadRemoteContentOutcomeAllocWithRenderConfig(a, .{
                        .io = opts.io,
                        .deadline_ns = opts.deadline_ns,
                        .cancellation = if (opts.cancellation != null) .{ .ptr = &opts, .is_cancelled_fn = renderCancelled } else null,
                        .secret_store = opts.secret_store,
                        .remote_content = opts.remote_content,
                    }, url, null);
                    if (fetched != .ok) return error.RemoteMediaFetchFailed;
                    break :resolved Part.BinaryContent{ .data = fetched.ok.data, .mime_type = fetched.ok.content_type };
                },
                .text => unreachable,
            };
            if (part == .media_url) input_remaining = std.math.sub(usize, input_remaining, media.data.len) catch return error.LakeEnrichmentUnitBudgetExceeded;
            if (media.data.len == 0) continue;
            const chunks = try chunking.inference.chunkInputWithProvider(a, cfg, .{ .binary = .{ .mime_type = media.mime_type, .data = media.data } }, provider);
            for (chunks) |chunk| {
                if (units.items.len == max_units) return error.LakeEnrichmentUnitBudgetExceeded;
                const owned_bytes = if (chunk.text) |text| text.len else if (chunk.data) |data| data.len else 0;
                output_remaining = std.math.sub(usize, output_remaining, owned_bytes) catch return error.LakeEnrichmentUnitBudgetExceeded;
                const unit_parts = try a.alloc(Part, 1);
                unit_parts[0] = if (chunk.text) |text| .{ .text = text } else .{ .binary = .{ .data = chunk.data orelse return error.InvalidChunkerResponse, .mime_type = chunk.mime_type } };
                try units.append(a, .{ .ordinal = @intCast(units.items.len), .source_ordinal = @intCast(source_ordinal), .source_part = .{ .binary = media }, .parts = unit_parts, .start_time_ms = chunk.start_time_ms, .end_time_ms = chunk.end_time_ms, .frame_index = chunk.frame_index, .frame_delay_ms = chunk.frame_delay_ms, .start_offset = chunk.start_char, .end_offset = chunk.end_char, .chunked = true });
            }
        }
    }
    var source_ordinal: ?u32 = null;
    var source_json: ?[]const u8 = null;
    var fingerprint: ?[]const u8 = null;
    for (units.items) |*unit| {
        if (source_ordinal == null or source_ordinal.? != unit.source_ordinal) {
            source_ordinal = unit.source_ordinal;
            fingerprint = try sourceFingerprint(a, unit.*);
            unit.source_fingerprint = fingerprint;
            source_json = try sourceJson(a, unit.*);
        }
        unit.source_fingerprint = fingerprint;
        unit.source_json = source_json;
    }
    return units.toOwnedSlice(a);
}

/// Binary-safe durable representation also used as the memo input identity.
pub fn partsJson(a: A, parts: []const Part) ![]u8 {
    var values: std.ArrayList(V) = .empty;
    for (parts) |part| {
        var value: V = .{ .object = .empty };
        switch (part) {
            .text => |text| try value.object.put(a, "text", .{ .string = text }),
            .media_url => |url| try value.object.put(a, "url", .{ .string = url }),
            .binary => |binary| {
                const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(binary.data.len));
                _ = std.base64.standard.Encoder.encode(encoded, binary.data);
                try value.object.put(a, "mime_type", .{ .string = binary.mime_type });
                try value.object.put(a, "data", .{ .string = encoded });
            },
        }
        try values.append(a, value);
    }
    return std.json.Stringify.valueAlloc(a, values.items, .{});
}
pub fn recordJson(a: A, unit: Unit) ![]u8 {
    return std.json.Stringify.valueAlloc(a, .{
        .chunk_id = unit.ordinal,
        .text = if (unit.parts.len == 1 and unit.parts[0] == .text) unit.parts[0].text else null,
        ._parent_unit_id = try std.fmt.allocPrint(a, "{d}", .{unit.source_ordinal}),
        ._source_artifact_name = "lake_sources",
        ._artifact_unit_fingerprint = try sourceFingerprint(a, unit),
        .start_offset = unit.start_offset,
        .end_offset = unit.end_offset,
        .start_time_ms = unit.start_time_ms,
        .end_time_ms = unit.end_time_ms,
        .frame_index = unit.frame_index,
        .frame_delay_ms = unit.frame_delay_ms,
        .parts = try std.json.parseFromSliceLeaky(V, a, try partsJson(a, unit.parts), .{}),
    }, .{});
}
pub fn sourceKey(a: A, key: []const u8, ordinal: u32) ![]u8 {
    return std.fmt.allocPrint(a, "{s}:unit:{d}", .{ key, ordinal });
}
pub fn sourceKeyFromRecord(a: A, key: []const u8, bytes: []const u8) ![]u8 {
    var record = try std.json.parseFromSlice(V, a, bytes, .{});
    defer record.deinit();
    const unit = record.value.object.get("_parent_unit_id") orelse return error.InvalidChunkArtifact;
    if (unit != .string) return error.InvalidChunkArtifact;
    return sourceKey(a, try parent(key), try std.fmt.parseUnsigned(u32, unit.string, 10));
}
pub fn sourceJson(a: A, unit: Unit) ![]const u8 {
    if (unit.source_json) |cached| return cached;
    return std.json.Stringify.valueAlloc(a, .{
        .text = if (unit.source_part.? == .text) unit.source_part.?.text else null,
        .parts = try std.json.parseFromSliceLeaky(V, a, try partsJson(a, &.{unit.source_part.?}), .{}),
        ._artifact_unit_fingerprint = try sourceFingerprint(a, unit),
    }, .{});
}
fn sourceFingerprint(a: A, unit: Unit) ![]const u8 {
    if (unit.source_fingerprint) |cached| return cached;
    const bytes = try partsJson(a, &.{unit.source_part.?});
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return std.fmt.allocPrint(a, "{s}", .{std.fmt.bytesToHex(digest, .lower)});
}
pub fn chunkName(a: A, name: []const u8, config_json: []const u8) !?[]u8 {
    var parsed = try std.json.parseFromSlice(V, a, config_json, .{});
    defer parsed.deinit();
    const config = parsed.value;
    const generator = config.object.get("generator") orelse config;
    if (generator != .object or configured(generator, "chunker") == null) return null;
    return try std.fmt.allocPrint(a, "{s}_chunks", .{name});
}
/// Decode the selected native column page without serializing a whole batch.
pub fn rowValue(a: A, page: local.sql_catalog.ColumnPage) !V {
    var row: V = .{ .object = .empty };
    for (page.batch.columns) |column| try row.object.put(a, column.name, (try page.cell(a, 0, column.name)).value);
    return row;
}

test "external lake unit identities retain their parent and reject malformed members" {
    const a = std.testing.allocator;
    const id = try identity(a, "lake-parent", .{ .ordinal = 7, .chunked = true });
    defer a.free(id);
    try std.testing.expectEqualStrings("lake-parent", try parent(id));
    const rebound = try rebind(a, id, "public-row");
    defer a.free(rebound);
    try std.testing.expectEqualStrings("public-row", try parent(rebound));
    try std.testing.expectError(error.InvalidLakeUnitIdentity, parent("row\x1fu2:garbage"));
}

test "external lake enrichment renders multimodal rows and preserves independent chunk inputs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const config = try std.json.parseFromSliceLeaky(V, a,
        \\{"embedder":{"provider":"mock"},"template":"{{title}} <<<dotprompt:media:url data:application/octet-stream;base64,AP8=>>>"}
    , .{});
    const row = try std.json.parseFromSliceLeaky(V, a, "{\"title\":\"header\",\"body\":\"alpha beta gamma delta\"}", .{});
    const media = try prepare(a, config, "unused", row, .{ .io = std.testing.io });
    try std.testing.expectEqual(@as(usize, 1), media.len);
    try std.testing.expectEqual(@as(usize, 2), media[0].parts.len);
    try std.testing.expectEqualStrings("header", media[0].parts[0].text);
    try std.testing.expectEqualSlices(u8, &.{ 0, 255 }, media[0].parts[1].binary.data);
    const durable = try partsJson(a, media[0].parts);
    try std.testing.expect(std.mem.indexOf(u8, durable, "AP8=") != null);
    const chunk_config = try std.json.parseFromSliceLeaky(V, a,
        \\{"embedder":{"provider":"mock"},"chunker":{"provider":"mock","text":{"target_tokens":1,"overlap_tokens":0}}}
    , .{});
    const chunks = try prepare(a, chunk_config, "body", row, .{ .io = std.testing.io });
    try std.testing.expect(chunks.len > 1);
    for (chunks, 0..) |chunk, ordinal| {
        try std.testing.expect(chunk.chunked);
        try std.testing.expectEqual(@as(u32, @intCast(ordinal)), chunk.ordinal);
        try std.testing.expectEqual(@as(usize, 1), chunk.parts.len);
        try std.testing.expect(chunk.parts[0] == .text);
    }
    try std.testing.expectError(error.DeadlineExceeded, prepare(a, chunk_config, "body", row, .{ .deadline_ns = 0 }));
}

test "external lake optional null generators do not manufacture chunk-backed indexes" {
    const a = std.testing.allocator;
    try std.testing.expectEqual(@as(?[]u8, null), try chunkName(a, "materialized", "{\"generator\":{\"chunker\":null}}"));
}

test "external lake media chunks preserve binary payloads provenance and request cancellation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const chunking = local.chunking_mod;
    const Provider = struct {
        calls: usize = 0,
        fn dense(_: *anyopaque, _: A, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }
        fn sparse(_: *anyopaque, _: A, _: []const u8, _: []const []const u8) ![]local.storage_db_enrichment_embedder.SparseEmbedding {
            return error.TestUnexpectedResult;
        }
        fn chunk(raw: *anyopaque, alloc: A, _: []const u8, input: chunking.inference.RemoteInput, _: chunking.types.Config, context: @import("antfly_inference_execution_context").RequestContext) ![]chunking.inference.RemoteChunk {
            try context.check();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqualStrings("video/mp4", input.binary.mime_type);
            try std.testing.expectEqualSlices(u8, &.{ 0, 255 }, input.binary.data);
            const result = try alloc.alloc(chunking.inference.RemoteChunk, 2);
            for (result, 0..) |*item, i| item.* = .{
                .id = @intCast(i),
                .mime_type = try alloc.dupe(u8, "image/png"),
                .data = try alloc.dupe(u8, &.{ 255, @intCast(i) }),
                .owns_mime_type = true,
                .owns_data = true,
                .frame_index = @intCast(i),
                .start_time_ms = @floatFromInt(i * 1000),
                .end_time_ms = @floatFromInt((i + 1) * 1000),
            };
            return result;
        }
    };
    var provider: Provider = .{};
    const config = try std.json.parseFromSliceLeaky(V, a,
        \\{"embedder":{"provider":"antfly"},"chunker":{"provider":"antfly","model":"frames"}}
    , .{});
    const row = try std.json.parseFromSliceLeaky(V, a,
        \\{"body":"<<<dotprompt:media:url data:video/mp4;base64,AP8=>>>"}
    , .{});
    const options: managed.InitOptions = .{ .io = std.testing.io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s, .antfly_provider = .{ .ptr = &provider, .embed_dense_texts = Provider.dense, .embed_sparse_texts = Provider.sparse, .chunk_input_with_context = Provider.chunk } };
    const chunks = try prepare(a, config, "body", row, options);
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    for (chunks, 0..) |unit, i| {
        try std.testing.expect(unit.chunked);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), unit.frame_index);
        try std.testing.expectEqual(@as(?f32, @floatFromInt(i * 1000)), unit.start_time_ms);
        try std.testing.expectEqualSlices(u8, &.{ 255, @intCast(i) }, unit.parts[0].binary.data);
        try std.testing.expectEqualStrings(chunks[0].source_fingerprint.?, unit.source_fingerprint.?);
        try std.testing.expect(std.mem.indexOf(u8, unit.source_json.?, "AP8=") != null);
    }
    var cancelled = options;
    cancelled.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, prepare(a, config, "body", row, cancelled));
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
}
