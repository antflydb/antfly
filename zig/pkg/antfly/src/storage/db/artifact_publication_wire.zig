// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Binary-safe bounded publication wire format. Artifact bodies are streamed
//! base64, while identity byte strings use the existing native JSON codec.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const binary = @import("relational_integrity_json.zig");
const WireMutation = struct { family: publication.Family, key: []const u8, value_base64: ?[]const u8, source_index: u32 };
const Wire = struct {
    mode: @FieldType(publication.Command, "mode") = .publish,
    producer_kind: @FieldType(publication.Command, "producer_kind") = .index,
    namespace: publication.Namespace,
    authority_epoch: u64,
    catalog_digest: publication.Digest,
    producer_name: []const u8,
    producer_generation: u64,
    producer_artifact_name: []const u8 = "",
    producer_scope_key: []const u8 = "",
    sources: []const publication.Source,
    artifact_sources: []const publication.ArtifactSource = &.{},
    mutation_preconditions: []const publication.ArtifactSource = &.{},
    mutations: []const WireMutation,
    publication_digest: publication.Digest,
    baseline: ?publication.BaselinePage = null,
    validation: ?publication.ValidationPage = null,
    census: ?publication.CensusPage = null,
    completion: ?publication.CompletionPage = null,
};
comptime {
    if (@typeInfo(Wire).@"struct".fields.len != @typeInfo(publication.Command).@"struct".fields.len) @compileError("update publication wire projection");
}
pub fn write(command: publication.Command, stream: anytype) @TypeOf(stream.*).Error!void {
    try stream.beginObject();
    inline for (@typeInfo(publication.Command).@"struct".fields) |field| {
        try stream.objectField(field.name);
        if (comptime std.mem.eql(u8, field.name, "mutations")) {
            try stream.beginArray();
            for (command.mutations) |effect| {
                try stream.beginObject();
                try stream.objectField("family");
                try stream.write(@tagName(effect.family));
                try stream.objectField("key");
                try binary.write(effect.key, stream);
                try stream.objectField("source_index");
                try stream.write(effect.source_index);
                try stream.objectField("value_base64");
                if (effect.value) |value| {
                    try stream.beginWriteRaw();
                    try stream.writer.writeByte('"');
                    var scratch: [4096]u8 = undefined;
                    var offset: usize = 0;
                    while (offset < value.len) {
                        const end = @min(value.len, offset + 3072);
                        try stream.writer.writeAll(std.base64.standard.Encoder.encode(&scratch, value[offset..end]));
                        offset = end;
                    }
                    try stream.writer.writeByte('"');
                    stream.endWriteRaw();
                } else try stream.write(null);
                try stream.endObject();
            }
            try stream.endArray();
        } else try binary.write(@field(command, field.name), stream);
    }
    try stream.endObject();
}
pub fn parse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) std.json.ParseError(@TypeOf(source.*))!publication.Command {
    return fromWire(alloc, try std.json.innerParse(Wire, alloc, source, options));
}
pub fn parseValue(alloc: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) std.json.ParseFromValueError!publication.Command {
    return fromWire(alloc, try std.json.innerParseFromValue(Wire, alloc, value, options));
}
fn fromWire(alloc: std.mem.Allocator, wire: Wire) std.json.ParseFromValueError!publication.Command {
    if (wire.sources.len > publication.max_source_documents or wire.artifact_sources.len > publication.max_source_documents or wire.mutations.len > publication.max_mutations) return error.LengthMismatch;
    if (wire.mutation_preconditions.len > publication.max_source_documents - wire.artifact_sources.len) return error.LengthMismatch;
    var remaining: usize = publication.max_payload_bytes;
    if (wire.baseline) |page| page.validate() catch return error.LengthMismatch;
    if (wire.validation) |page| page.validate() catch return error.LengthMismatch;
    if (wire.census) |page| page.validate() catch return error.LengthMismatch;
    if (wire.completion) |page| page.validate() catch return error.LengthMismatch;
    remaining = std.math.sub(usize, remaining, wire.producer_name.len) catch return error.LengthMismatch;
    remaining = std.math.sub(usize, remaining, wire.producer_artifact_name.len) catch return error.LengthMismatch;
    remaining = std.math.sub(usize, remaining, wire.producer_scope_key.len) catch return error.LengthMismatch;
    for (wire.sources) |source| remaining = std.math.sub(usize, remaining, source.document_key.len) catch return error.LengthMismatch;
    for (wire.artifact_sources) |source| remaining = std.math.sub(usize, remaining, source.key.len) catch return error.LengthMismatch;
    for (wire.mutation_preconditions) |source| remaining = std.math.sub(usize, remaining, source.key.len) catch return error.LengthMismatch;
    const effects = try alloc.alloc(publication.Mutation, wire.mutations.len);
    for (wire.mutations, effects) |effect, *out| {
        remaining = std.math.sub(usize, remaining, effect.key.len) catch return error.LengthMismatch;
        out.* = .{ .family = effect.family, .key = effect.key, .source_index = effect.source_index, .value = null };
        if (effect.value_base64) |encoded| {
            if (encoded.len > std.base64.standard.Encoder.calcSize(remaining)) return error.LengthMismatch;
            const size = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.UnexpectedToken;
            remaining = std.math.sub(usize, remaining, size) catch return error.LengthMismatch;
            const bytes = try alloc.alloc(u8, size);
            std.base64.standard.Decoder.decode(bytes, encoded) catch return error.UnexpectedToken;
            out.value = bytes;
        }
    }
    var result: publication.Command = undefined;
    inline for (@typeInfo(publication.Command).@"struct".fields) |field| {
        if (comptime std.mem.eql(u8, field.name, "mutations")) result.mutations = effects else @field(result, field.name) = @field(wire, field.name);
    }
    return result;
}

test "ordered artifact inventory unit reconciliation control preserves scope and bounded claims" {
    const alloc = std.testing.allocator;
    const codec = @import("artifact_publication_transport_codec.zig");
    var command: publication.Command = .{ .mode = .reconcile_units, .producer_kind = .enrichment, .namespace = @splat(1), .authority_epoch = 2, .catalog_digest = @splat(3), .producer_name = "child", .producer_artifact_name = "child", .producer_generation = 2, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .census = .{ .document_key = "doc\x00\xff", .chunk_name = "child", .visits = 128, .bytes = 4096, .before = @splat(4), .after = @splat(5) } };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(publication.Command, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(command, parsed.value);
    const bytes = try codec.encodeAlloc(alloc, command);
    defer alloc.free(bytes);
    try std.testing.expect(codec.controlAdmissionHint(bytes));
    var decoded = try codec.decodeBorrowed(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    var changed = command;
    changed.producer_kind = .index;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.producer_scope_key = "sender-cursor";
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.census.?.visits = 129;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
}

test "ordered artifact inventory completion control preserves bounded binary claims" {
    const alloc = std.testing.allocator;
    const codec = @import("artifact_publication_transport_codec.zig");
    var command: publication.Command = .{ .mode = .complete_streams, .namespace = @splat(1), .authority_epoch = 2, .catalog_digest = @splat(3), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0), .completion = .{ .document_key = "doc\x00\xff", .visits = 3, .bytes = 4096, .before = @splat(4), .after = @splat(5) } };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(publication.Command, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(command, parsed.value);
    const bytes = try codec.encodeAlloc(alloc, command);
    defer alloc.free(bytes);
    try std.testing.expect(codec.controlAdmissionHint(bytes));
    var decoded = try codec.decodeBorrowed(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    var changed = command;
    changed.completion.?.visits = 129;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.mode = .activate;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.producer_name = "not-a-provider-result";
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.completion.?.before[0] ^= 1;
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    const Harness = struct {
        fn run(a: std.mem.Allocator, raw: []const u8) !void {
            var value = try codec.decodeBorrowed(a, raw);
            defer value.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Harness.run, .{bytes});
}

test "ordered artifact inventory census control binds binary identity limits and receiver claims" {
    const alloc = std.testing.allocator;
    const codec = @import("artifact_publication_transport_codec.zig");
    var command: publication.Command = .{
        .mode = .census,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "index\x00\xff",
        .producer_generation = 4,
        .producer_artifact_name = "model\x80",
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .census = .{ .document_key = "doc\xff", .chunk_name = "chunk\x00", .visits = 3, .bytes = 4096, .before = @splat(5), .after = @splat(6) },
    };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(publication.Command, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(command, parsed.value);
    const raw = try codec.encodeAlloc(alloc, command);
    defer alloc.free(raw);
    try std.testing.expect(codec.controlAdmissionHint(raw));
    var decoded = try codec.decodeBorrowed(alloc, raw);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    var changed = command;
    changed.census.?.visits += 1;
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.census.?.after[0] ^= 1;
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.mode = .activate;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    const Check = struct {
        fn run(a: std.mem.Allocator, bytes: []const u8) !void {
            var value = try codec.decodeBorrowed(a, bytes);
            defer value.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{raw});
}

test "ordered artifact inventory publication wire preserves arbitrary binary values and native source positions" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "doc\xff", "model");
    defer alloc.free(key);
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "model", .producer_generation = 2, .sources = &.{.{ .document_key = "doc\xff", .content_digest = @splat(3), .timestamp = 4, .input_position = .{ .native = .{ .namespace = publication.namespaceFromBytes(@splat(1)), .sequence = 5 } } }}, .mutations = &.{.{ .family = .base_vector, .key = key, .value = "\x00\xff\xfe\x80bytes", .source_index = 0 }}, .publication_digest = @splat(0) };
    command.producer_artifact_name = "model";
    command.publication_digest = command.digest();
    const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(encoded);
    var decoded = try std.json.parseFromSlice(publication.Command, alloc, encoded, .{});
    defer decoded.deinit();
    try decoded.value.validate(alloc);
    try std.testing.expectEqualSlices(u8, command.mutations[0].value.?, decoded.value.mutations[0].value.?);
    try std.testing.expectEqualDeep(command.sources[0].input_position, decoded.value.sources[0].input_position);
}

test "ordered artifact inventory validation control binds mutation epoch repairs and cursor in both codecs" {
    const alloc = std.testing.allocator;
    const codec = @import("artifact_publication_transport_codec.zig");
    var command: publication.Command = .{
        .mode = .validate_inputs,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .validation = .{ .mutation_epoch = 9, .expected_cursor = "\x00\x80", .next_cursor = "\x00\xff", .at_end = true, .repair_documents = &.{"\xff\x00document"} },
    };
    command.publication_digest = command.digest();
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(publication.Command, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqualDeep(command, parsed.value);
    const bytes = try codec.encodeAlloc(alloc, command);
    defer alloc.free(bytes);
    try std.testing.expect(codec.controlAdmissionHint(bytes));
    var decoded = try codec.decodeBorrowed(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    var changed = command;
    changed.validation.?.mutation_epoch += 1;
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.validation.?.repair_documents = &.{};
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.mode = .baseline;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    const Failure = struct {
        fn decode(a: std.mem.Allocator, input: []const u8) !void {
            var value = try codec.decodeBorrowed(a, input);
            defer value.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Failure.decode, .{bytes});
}

test "ordered artifact inventory baseline control binds binary cursors rows and committed cut in both codecs" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "doc\x00\xff");
    defer alloc.free(key);
    var command: publication.Command = .{
        .mode = .baseline,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .baseline = .{ .observed_term = 4, .observed_index = 5, .expected_cursor = "", .next_cursor = key, .upper_bound = key, .row_keys = &.{key}, .at_end = true },
    };
    command.publication_digest = command.digest();
    try command.validate(alloc);
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(publication.Command, alloc, json, .{});
    defer parsed.deinit();
    try parsed.value.validate(alloc);
    try std.testing.expectEqualDeep(command, parsed.value);
    const codec = @import("artifact_publication_transport_codec.zig");
    var decoded = try codec.decodeOwned(alloc, try codec.encodeAlloc(alloc, command));
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command, decoded.command);
    var changed = command;
    changed.baseline.?.observed_index += 1;
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.baseline.?.expected_cursor = key;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
    changed = command;
    changed.mode = .publish;
    changed.publication_digest = changed.digest();
    try std.testing.expectError(error.InvalidBatchRequest, changed.validate(alloc));
}
