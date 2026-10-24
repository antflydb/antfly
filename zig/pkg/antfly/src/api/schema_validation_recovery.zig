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

//! Stable public recovery identity. The checksum detects damage, not forgery;
//! possession never grants table access, admission, cancellation or adoption.
const std = @import("std");
const reads = @import("../metadata/schema_validation_read.zig");
const local = @import("antfly_local_sources");
const admission = @import("../metadata/schema_validation_admission.zig");
const wire = @import("antfly_metadata_openapi").types;

pub const Token = struct {
    const magic = "ASVT\x01\x00\x00\x00";
    const payload_len = 136;
    pub const encoded_len = std.base64.url_safe_no_pad.Encoder.calcSize(payload_len);

    fn checksum(bytes: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.schema-validation-recovery.v1");
        hash.update(bytes);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn encode(lookup: reads.Lookup) ![encoded_len]u8 {
        (reads.OwnerReadRequest{ .lookup = lookup, .ordinal = 0 }).validate() catch return error.InvalidSchemaValidationToken;
        var payload: [payload_len]u8 = undefined;
        @memcpy(payload[0..8], magic);
        std.mem.writeInt(u64, payload[8..16], lookup.group, .little);
        @memcpy(payload[16..48], &lookup.metadata_incarnation);
        std.mem.writeInt(u64, payload[48..56], lookup.table, .little);
        @memcpy(payload[56..72], &lookup.id);
        @memcpy(payload[72..104], &lookup.plan_digest);
        @memcpy(payload[104..136], &checksum(payload[0..104]));
        var output: [encoded_len]u8 = undefined;
        _ = std.base64.url_safe_no_pad.Encoder.encode(&output, &payload);
        return output;
    }

    pub fn decode(encoded: []const u8) !reads.Lookup {
        if (encoded.len != encoded_len) return error.InvalidSchemaValidationToken;
        var payload: [payload_len]u8 = undefined;
        std.base64.url_safe_no_pad.Decoder.decode(&payload, encoded) catch return error.InvalidSchemaValidationToken;
        if (!std.mem.eql(u8, payload[0..8], magic) or !std.mem.eql(u8, payload[104..136], &checksum(payload[0..104]))) return error.InvalidSchemaValidationToken;
        const lookup: reads.Lookup = .{ .group = std.mem.readInt(u64, payload[8..16], .little), .metadata_incarnation = payload[16..48].*, .table = std.mem.readInt(u64, payload[48..56], .little), .id = payload[56..72].*, .plan_digest = payload[72..104].* };
        const canonical = try encode(lookup);
        if (!std.mem.eql(u8, &canonical, encoded)) return error.InvalidSchemaValidationToken;
        return lookup;
    }
};

pub fn httpStatus(err: anyerror) u16 {
    return switch (err) {
        error.InvalidSchemaValidationToken => 400,
        error.SqlWrongObjectType, error.MetadataIncarnationMismatch => 409,
        error.MetadataReplicationPending, error.CatalogPublicationProofPending => 503,
        else => local.system_catalog_domain.httpStatus(err),
    };
}

pub fn read(source: anytype, alloc: std.mem.Allocator, context: local.api_operation.RequestContext, guard: local.system_catalog_domain.RelationMutationGuard, encoded: []const u8) ![]u8 {
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    const request: admission.StatusRequest = .{ .guard = guard, .lookup = try Token.decode(encoded) };
    try request.validate();
    const bytes = try source.systemCatalog(alloc, context, .{ .schema_validation_status = request });
    defer alloc.free(bytes);
    try context.ensureActive();
    if (bytes.len == 0 or bytes.len > admission.Status.max_wire_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(admission.Status, alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.requireFor(request);
    return project(alloc, encoded, parsed.value);
}

pub fn readTable(source: anytype, alloc: std.mem.Allocator, context: local.api_operation.RequestContext, target: local.system_catalog_domain.Target, encoded: []const u8) ![]u8 {
    if (!context.setting_admin or !context.schema_validation_authority) return error.Forbidden;
    try context.ensureActive();
    const lookup = try Token.decode(encoded);
    for (0..2) |attempt| {
        try context.ensureActive();
        const domain = local.system_catalog_domain;
        const targets = [_]domain.RelationTarget{.{ .database = target.database, .namespace = target.namespace, .name = target.table }};
        const request: domain.ResolveMany = .{ .relations = &targets };
        const bytes = try source.systemCatalog(alloc, context, .{ .resolve_many = request });
        defer alloc.free(bytes);
        try context.ensureActive();
        if (bytes.len == 0 or bytes.len > 16 * 1024) return error.InvalidCatalogRecord;
        var parsed = try std.json.parseFromSlice(domain.ResolvedMany, alloc, bytes, .{});
        defer parsed.deinit();
        try parsed.value.validateRelations(request);
        const owner = parsed.value.relations[0] orelse return error.TableNotFound;
        if (owner.owner.table_id != lookup.table) return error.CatalogGenerationChanged;
        if (owner.owner.kind != .table) return error.SqlWrongObjectType;
        if (!std.mem.eql(u8, owner.logical_table, target.table)) return error.InvalidCatalogRecord;
        return read(source, alloc, context, .{ .target = targets[0], .logical_table = owner.logical_table, .owner = owner.owner, .incarnation = parsed.value.relation_epoch.?.incarnation }, encoded) catch |err| {
            // Publication may advance this same table between resolution and the
            // status cut. Refresh once, preserving token/table authority and the
            // caller's deadline. Never rebind a replaced logical table.
            if (err == error.CatalogGenerationChanged and attempt == 0) continue;
            return err;
        };
    }
    unreachable;
}

fn project(a: std.mem.Allocator, encoded: []const u8, observed: admission.Status) ![]u8 {
    var status: wire.SchemaValidationStatus = .{ .schema_validation_token = encoded, .table_id = undefined, .active_schema_version = observed.active_schema_version, .state = .admission_unknown };
    var table: [20]u8 = undefined;
    status.table_id = try std.fmt.bufPrint(&table, "{d}", .{observed.lookup.table});
    if (observed.head) |head| {
        try observed.lookup.require(head);
        status.state = switch (head.phase) {
            .succeeded => .succeeded,
            .canceled => if (head.failure == .requested) .canceled else .failed,
            else => .pending,
        };
        status.phase = switch (head.phase) {
            inline else => |phase| @field(wire.SchemaValidationPhase, @tagName(phase)),
        };
        status.failure = switch (head.failure) {
            .none => null,
            inline else => |failure| @field(wire.SchemaValidationFailure, @tagName(failure)),
        };
        status.owner_count = head.owner_count;
        status.ready = head.ready;
        status.installed = head.installed;
        status.released = head.released;
        status.canceled = head.canceled;
    }
    return std.json.Stringify.valueAlloc(a, status, .{ .emit_null_optional_fields = false });
}

test "system catalog schema validation recovery token binds immutable metadata table and plan identity without granting authority" {
    const lookup: reads.Lookup = .{ .group = 21, .table = 7, .id = @splat(1), .metadata_incarnation = @splat('1'), .plan_digest = @splat(2) };
    const encoded = try Token.encode(lookup);
    try std.testing.expectEqual(@as(usize, 182), encoded.len);
    try std.testing.expectEqualDeep(lookup, try Token.decode(&encoded));
    for (0..encoded.len) |i| {
        var corrupt = encoded;
        corrupt[i] = if (corrupt[i] == 'A') 'B' else 'A';
        try std.testing.expectError(error.InvalidSchemaValidationToken, Token.decode(&corrupt));
    }
    try std.testing.expectError(error.InvalidSchemaValidationToken, Token.decode(encoded[0 .. encoded.len - 1]));
    var foreign = lookup;
    foreign.metadata_incarnation[0] = 'z';
    try std.testing.expectError(error.InvalidSchemaValidationToken, Token.encode(foreign));
    foreign = lookup;
    foreign.group = 0;
    try std.testing.expectError(error.InvalidSchemaValidationToken, Token.encode(foreign));
    foreign = lookup;
    foreign.table = 8;
    const other = try Token.encode(foreign);
    try std.testing.expect(!std.mem.eql(u8, &other, &encoded));
    try std.testing.expectEqualDeep(foreign, try Token.decode(&other));
}

test "system catalog schema validation recovery status refreshes only the same owner with bounded read retries" {
    const a = std.testing.allocator;
    const domain = local.system_catalog_domain;
    const context: local.api_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true };
    const Source = struct {
        failures: u8 = 1,
        version: u32 = 1,
        resolutions: usize = 0,
        observations: usize = 0,
        replace: bool = false,
        reincarnate: bool = false,
        failure: ?anyerror = null,
        fn systemCatalog(self: *@This(), alloc: std.mem.Allocator, _: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            switch (call) {
                .resolve_many => |request| {
                    self.resolutions += 1;
                    try std.testing.expectEqual(@as(usize, 1), request.relations.len);
                    try std.testing.expectEqualStrings("items", request.relations[0].name);
                    const table_id: u64 = if (self.replace and self.resolutions > 1) 8 else 7;
                    return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{
                        .revision = self.version,
                        .tables = &.{},
                        .relation_epoch = .{ .revision = self.version, .incarnation = if (self.reincarnate and self.resolutions > 1) @splat(0x22) else @splat(0x11) },
                        .relations = &.{.{ .owner = .{ .kind = .table, .table_id = table_id, .schema_version = self.version, .schema_digest = @splat(3) }, .logical_table = "items", .table = .{ .table_id = table_id, .name = "physical" } }},
                    }, .{});
                },
                .schema_validation_status => |request| {
                    self.observations += 1;
                    try request.validate();
                    if (self.failure) |err| return err;
                    if (self.observations <= self.failures) {
                        self.version += 1;
                        return error.CatalogGenerationChanged;
                    }
                    return std.json.Stringify.valueAlloc(alloc, admission.Status{ .lookup = request.lookup, .head = null, .active_schema_version = self.version }, .{});
                },
                else => return error.UnexpectedCall,
            }
        }
    };
    const token = try Token.encode(.{ .group = 21, .table = 7, .id = @splat(1), .metadata_incarnation = @splat('1'), .plan_digest = @splat(2) });
    var source: Source = .{};
    const bytes = try readTable(&source, a, context, .{ .table = "items" }, &token);
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(wire.SchemaValidationStatus, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.active_schema_version);
    try std.testing.expectEqual(wire.SchemaValidationState.admission_unknown, parsed.value.state);
    try std.testing.expectEqual(@as(usize, 2), source.resolutions);
    try std.testing.expectEqual(@as(usize, 2), source.observations);
    source = .{ .failures = 2 };
    try std.testing.expectError(error.CatalogGenerationChanged, readTable(&source, a, context, .{ .table = "items" }, &token));
    try std.testing.expectEqual(@as(usize, 2), source.resolutions);
    try std.testing.expectEqual(@as(usize, 2), source.observations);
    source = .{ .replace = true };
    try std.testing.expectError(error.CatalogGenerationChanged, readTable(&source, a, context, .{ .table = "items" }, &token));
    try std.testing.expectEqual(@as(usize, 2), source.resolutions);
    try std.testing.expectEqual(@as(usize, 1), source.observations);
    source = .{ .reincarnate = true };
    try std.testing.expectError(error.CatalogGenerationChanged, readTable(&source, a, context, .{ .table = "items" }, &token));
    try std.testing.expectEqual(@as(usize, 1), source.observations);
    source = .{ .failure = error.InvalidCatalogRecord };
    try std.testing.expectError(error.InvalidCatalogRecord, readTable(&source, a, context, .{ .table = "items" }, &token));
    try std.testing.expectEqual(@as(usize, 1), source.resolutions);
    try std.testing.expectEqual(@as(usize, 1), source.observations);
}
