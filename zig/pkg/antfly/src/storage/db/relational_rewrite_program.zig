// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Pure immutable rewrite program binding shared by API admission and native
//! snapshot/tail evaluation. No DB owner or filesystem authority is imported.
const std = @import("std");
const contract = @import("relational_rewrite_contract.zig");
const transform = @import("relational_row_transform.zig");
const staging = @import("restore_staging_contract.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;
pub const LogicalRow = struct { json: []u8, timestamp: u64 };

pub const ProgramSet = struct {
    alloc: Allocator,
    programs: []transform.Program,
    identity: contract.Digest,
    target_runtime_digest: contract.Digest,
    document_validator: ?@import("../../schema/mod.zig").CompiledTableValidator = null,
    document_read_validator: ?@import("../../schema/mod.zig").CompiledTableValidator = null,
    document_active_digest: contract.Digest = @splat(0),
    document_read_digest: ?contract.Digest = null,

    pub fn initDocumentPreservation(alloc: Allocator, schema_json: []const u8) !ProgramSet {
        return initDocumentPreservationWithRead(alloc, schema_json, "");
    }

    pub fn initDocumentPreservationWithRead(alloc: Allocator, schema_json: []const u8, read_json: []const u8) !ProgramSet {
        if (schema_json.len == 0 or schema_json.len > contract.max_schema_bytes) return error.InvalidRestoreStagingCommand;
        if (schema_json.len +| read_json.len > contract.max_schema_bytes) return error.InvalidRestoreStagingCommand;
        try @import("../../schema/restore_migration.zig").validate(alloc, schema_json, read_json);
        const api = @import("../../schema/mod.zig");
        var validator = try api.CompiledTableValidator.init(alloc, schema_json);
        errdefer validator.deinit(alloc);
        if (validator.schema.storage_mode != .document) return error.InvalidRestoreStagingCommand;
        var read_validator = if (read_json.len != 0) try api.CompiledTableValidator.init(alloc, read_json) else null;
        errdefer if (read_validator) |*value| value.deinit(alloc);
        const native = try api.deriveRuntimeTableSchema(alloc, validator.schema);
        defer @import("../schema.zig").freeSchema(alloc, native);
        const encoded = try @import("../schema.zig").serializeSchema(alloc, native);
        defer alloc.free(encoded);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-rewrite-document-preservation-v2");
        hash.update(schema_json);
        hash.update(encoded);
        hash.update(read_json);
        var identity: contract.Digest = undefined;
        hash.final(&identity);
        return .{ .alloc = alloc, .programs = try alloc.alloc(transform.Program, 0), .identity = identity, .target_runtime_digest = staging.digest(encoded), .document_validator = validator, .document_read_validator = read_validator, .document_active_digest = try definitionDigest(alloc, schema_json), .document_read_digest = if (read_json.len != 0) try definitionDigest(alloc, read_json) else null };
    }

    pub fn init(alloc: Allocator, source_schemas: []const []const u8, target_schema: []const u8, policies: transform.Policies) !ProgramSet {
        if (source_schemas.len == 0 or source_schemas.len > contract.max_source_schemas) return error.InvalidRestoreStagingCommand;
        var bytes = target_schema.len;
        for (source_schemas) |source| bytes = std.math.add(usize, bytes, source.len) catch return error.InvalidRestoreStagingCommand;
        if (bytes > contract.max_schema_bytes) return error.InvalidRestoreStagingCommand;
        const programs = try alloc.alloc(transform.Program, source_schemas.len);
        errdefer alloc.free(programs);
        var initialized: usize = 0;
        errdefer for (programs[0..initialized]) |*program| program.deinit();
        for (source_schemas, programs) |source, *program| {
            program.* = if (initialized == 0) try transform.Program.init(alloc, source, target_schema, policies) else try transform.Program.initWithTarget(alloc, source, &programs[0]);
            initialized += 1;
            for (programs[0 .. initialized - 1]) |previous| if (previous.source.version() == program.source.version()) return error.InvalidRestoreStagingCommand;
        }
        std.mem.sort(transform.Program, programs, {}, struct {
            fn less(_: void, a: transform.Program, b: transform.Program) bool {
                return a.source.version() < b.source.version();
            }
        }.less);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-relational-rewrite-program-set-v1");
        for (programs) |program| hash.update(&program.identity);
        const encoded = try @import("../schema.zig").serializeSchema(alloc, programs[0].target.tableSchema().*);
        defer alloc.free(encoded);
        var identity: contract.Digest = undefined;
        hash.final(&identity);
        return .{ .alloc = alloc, .programs = programs, .identity = identity, .target_runtime_digest = staging.digest(encoded) };
    }

    pub fn initIntent(alloc: Allocator, intent: contract.Intent) !ProgramSet {
        try intent.validate();
        var value = if (intent.preserve_document) try initDocumentPreservationWithRead(alloc, intent.target_schema, intent.target_read_schema) else try init(alloc, intent.source_schemas, intent.target_schema, .{
            .defaults = if (intent.apply_defaults_to_absent) .apply_to_absent else .preserve_absence,
            .dropped_columns = if (intent.allow_column_drops) .allow else .reject,
        });
        errdefer value.deinit();
        if (!std.mem.eql(u8, &value.identity, &intent.program_digest)) return error.RestoreStagingScopeChanged;
        return value;
    }

    pub fn deinit(self: *ProgramSet) void {
        if (self.document_validator) |*validator| validator.deinit(self.alloc);
        if (self.document_read_validator) |*validator| validator.deinit(self.alloc);
        for (self.programs) |*program| program.deinit();
        self.alloc.free(self.programs);
        self.* = undefined;
    }

    pub fn requireScope(self: *const ProgramSet, scope: staging.Scope) !void {
        try scope.validate();
        const rewrite = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
        if (!std.mem.eql(u8, &rewrite.program_digest, &self.identity) or
            !std.mem.eql(u8, &scope.target_schema_digest, &self.target_runtime_digest)) return error.RestoreStagingScopeChanged;
    }

    /// Run once before publishing the certified, immutable source decoder.
    /// Native/public map equality is checked by relational_rewrite_manifest;
    /// this checks that its semantics match the admitted program, not merely
    /// an equal version or physically compatible ordinal layout.
    pub fn requireSourceManifest(self: *const ProgramSet, alloc: Allocator, definitions: []const []const u8, active: []const u8) !void {
        if (definitions.len == 0 or definitions.len > contract.max_source_schemas) return error.RestoreStagingScopeChanged;
        if (self.document_validator != null) {
            const actual_active = try definitionDigest(alloc, active);
            if (!std.mem.eql(u8, &actual_active, &self.document_active_digest)) return error.RestoreStagingScopeChanged;
        }
        for (definitions) |json| {
            var parsed = try std.json.parseFromSlice(struct { version: u32 = 0 }, alloc, json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (self.document_validator != null) {
                if (self.document_read_validator) |read| if (read.schema.version == parsed.value.version) {
                    const actual = try definitionDigest(alloc, json);
                    if (!std.mem.eql(u8, &actual, &self.document_read_digest.?)) return error.RestoreStagingScopeChanged;
                };
                continue;
            }
            const program = for (self.programs) |*candidate| {
                if (candidate.source.version() == parsed.value.version) break candidate;
            } else return error.RestoreStagingScopeChanged;
            try program.requireSourceDefinition(alloc, json);
        }
    }

    pub fn transformJson(self: *const ProgramSet, alloc: Allocator, row: []const u8) !LogicalRow {
        if (self.document_validator != null) return error.InvalidRestoreStagingCommand;
        const version = try codec.rowSchemaVersion(row);
        const program = for (self.programs) |*candidate| {
            if (candidate.source.version() == version) break candidate;
        } else return error.UnknownSchemaVersion;
        var result = try program.transform(alloc, row);
        defer result.deinit(alloc);
        const typed = try codec.ordinalRowViewSelective(result.packed_row, program.target.tableSchema().*, program.target.physicalLayout());
        return .{ .json = try typed.reconstructValueAlloc(alloc), .timestamp = typed.writeTimestampNs() };
    }

    pub fn preserveDocument(self: *const ProgramSet, alloc: Allocator, json: []const u8) ![]u8 {
        const validator = self.document_validator orelse return error.InvalidRestoreStagingCommand;
        if (json.len > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
        defer parsed.deinit();
        validator.validateValue(alloc, &parsed.value) catch |err| {
            if (err == error.OutOfMemory) return err;
            const previous = self.document_read_validator orelse return err;
            try previous.validateValue(alloc, &parsed.value);
        };
        return alloc.dupe(u8, json);
    }
};

fn definitionDigest(alloc: Allocator, json: []const u8) !contract.Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{ .parse_numbers = false });
    defer parsed.deinit();
    const canonical = try @import("document_content_hash.zig").canonicalJsonValueAlloc(alloc, parsed.value);
    defer alloc.free(canonical);
    return staging.digest(canonical);
}

test "relational index system rewrite preserves document migration definitions and exact old row bytes" {
    const previous = "{\"version\":1,\"storage_mode\":\"document\",\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"string\"}},\"required\":[\"body\"],\"additionalProperties\":false}}}}";
    const active = "{\"version\":2,\"storage_mode\":\"document\",\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"string\"},\"new\":{\"type\":\"integer\"}},\"required\":[\"body\",\"new\"],\"additionalProperties\":false}}}}";
    const alloc = std.testing.allocator;
    var program = try ProgramSet.initDocumentPreservationWithRead(alloc, active, previous);
    defer program.deinit();
    const intent: contract.Intent = .{ .preserve_document = true, .source_schemas = &.{ previous, active }, .target_schema = active, .target_read_schema = previous, .program_digest = program.identity };
    var decoded = try ProgramSet.initIntent(alloc, intent);
    defer decoded.deinit();
    try decoded.requireSourceManifest(alloc, &.{ previous, active }, active);
    try std.testing.expectError(error.RestoreStagingScopeChanged, decoded.requireSourceManifest(alloc, &.{ previous, active }, previous));
    const original = "{ \"body\" : \"old\" }";
    const preserved = try decoded.preserveDocument(alloc, original);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings(original, preserved);
    const current = try decoded.preserveDocument(alloc, "{\"body\":\"new\",\"new\":4}");
    defer alloc.free(current);
    if (decoded.preserveDocument(alloc, "{\"body\":42}")) |invalid| {
        alloc.free(invalid);
        return error.TestUnexpectedResult;
    } else |_| {}
    var active_only = try ProgramSet.initDocumentPreservation(alloc, active);
    defer active_only.deinit();
    try std.testing.expect(!std.mem.eql(u8, &program.identity, &active_only.identity));
    var wrong = intent;
    wrong.target_read_schema = "";
    try std.testing.expectError(error.InvalidRestoreStagingCommand, wrong.validate());
}

test "relational index system rewrite certifies exact source semantics once before decoder publication" {
    const source =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const alloc = std.testing.allocator;
    var program = try ProgramSet.init(alloc, &.{source}, source, .{});
    defer program.deinit();
    try program.requireSourceManifest(alloc, &.{source}, source);
    const substituted = try std.mem.replaceOwned(u8, alloc, source, "\"x\"", "\"y\"");
    defer alloc.free(substituted);
    // Same version and one integer ordinal, different logical field identity.
    try std.testing.expectError(error.RestoreStagingScopeChanged, program.requireSourceManifest(alloc, &.{substituted}, substituted));
    const missing = try std.mem.replaceOwned(u8, alloc, source, "\"version\":1", "\"version\":2");
    defer alloc.free(missing);
    try std.testing.expectError(error.RestoreStagingScopeChanged, program.requireSourceManifest(alloc, &.{missing}, missing));
}
