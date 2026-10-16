// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Public read-only recovery identity and generated publication projection.
//! A checksum detects damaged handles; it is not a signature or authorization.
const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("../metadata/relational_index_publication.zig");
const wire = @import("antfly_indexes_openapi").types;
const Allocator = std.mem.Allocator;

pub const Token = struct {
    const magic = "AIPR\x01\x00\x00\x00";
    const payload_len = 128;
    pub const encoded_len = std.base64.url_safe_no_pad.Encoder.calcSize(payload_len);

    fn checksum(bytes: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.index-publication-recovery.v1");
        hash.update(bytes);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn encode(request: publication.AdmissionRequest) ![encoded_len]u8 {
        request.validate() catch return error.InvalidIndexPublicationToken;
        if (!local.metadata_incarnation.isValid(request.metadata_incarnation)) return error.InvalidIndexPublicationToken;
        var payload: [payload_len]u8 = undefined;
        @memcpy(payload[0..8], magic);
        @memcpy(payload[8..40], &request.metadata_incarnation);
        std.mem.writeInt(u64, payload[40..48], request.table_id, .little);
        @memcpy(payload[48..64], &request.id);
        @memcpy(payload[64..96], &request.plan_digest);
        @memcpy(payload[96..128], &checksum(payload[0..96]));
        var output: [encoded_len]u8 = undefined;
        _ = std.base64.url_safe_no_pad.Encoder.encode(&output, &payload);
        return output;
    }

    pub fn decode(encoded: []const u8) !publication.AdmissionRequest {
        if (encoded.len != encoded_len) return error.InvalidIndexPublicationToken;
        var payload: [payload_len]u8 = undefined;
        std.base64.url_safe_no_pad.Decoder.decode(&payload, encoded) catch return error.InvalidIndexPublicationToken;
        if (!std.mem.eql(u8, payload[0..8], magic) or
            !std.mem.eql(u8, payload[96..128], &checksum(payload[0..96]))) return error.InvalidIndexPublicationToken;
        const request: publication.AdmissionRequest = .{ .metadata_incarnation = payload[8..40].*, .table_id = std.mem.readInt(u64, payload[40..48], .little), .id = payload[48..64].*, .plan_digest = payload[64..96].* };
        const canonical = try encode(request);
        if (!std.mem.eql(u8, &canonical, encoded)) return error.InvalidIndexPublicationToken;
        return request;
    }
};

/// Trusted public ingress authorizes the current logical table before adding
/// these private capabilities. Retaining a token never manufactures that grant.
pub fn read(source: anytype, alloc: Allocator, context: local.api_operation.RequestContext, guard: local.system_catalog_domain.RelationMutationGuard, index_name: []const u8, encoded: []const u8) ![]u8 {
    if (!context.setting_admin or !context.index_publication_authority) return error.Forbidden;
    try context.ensureActive();
    const request = try Token.decode(encoded);
    if (guard.owner.table_id != request.table_id) return error.CatalogGenerationChanged;
    const scoped: publication.StatusRequest = .{ .guard = guard, .index_name = index_name, .admission = request };
    try scoped.validate();
    const bytes = try source.systemCatalog(alloc, context, .{ .relational_index_status = scoped });
    defer alloc.free(bytes);
    try context.ensureActive();
    if (bytes.len == 0 or bytes.len > publication.AdmissionStatus.max_wire_bytes) return error.InvalidCatalogRecord;
    var observed = try std.json.parseFromSlice(publication.AdmissionStatus, alloc, bytes, .{});
    defer observed.deinit();
    return project(alloc, index_name, encoded, request, observed.value);
}

fn encodeStatus(alloc: Allocator, index_name: []const u8, encoded: []const u8, observed: publication.AdmissionStatus) ![]u8 {
    return project(alloc, index_name, encoded, try Token.decode(encoded), observed);
}

fn project(alloc: Allocator, index_name: []const u8, encoded: []const u8, request: publication.AdmissionRequest, observed: publication.AdmissionStatus) ![]u8 {
    const confirmed = try observed.isAdmitted(request);
    const state: wire.IndexPublicationState = if (!confirmed) .admission_unknown else switch (observed.head.?.phase) {
        .published => .published,
        .canceled => if (observed.head.?.failure == .requested) .canceled else .failed,
        else => .pending,
    };
    const phase: ?wire.IndexPublicationPhase = if (observed.head) |head| switch (head.phase) {
        .fencing => .fencing,
        .reserving => .reserving,
        .building => .building,
        .installing => .installing,
        .releasing => .releasing,
        .published => .published,
        .canceling => .canceling,
        .cancel_releasing => .cancel_releasing,
        .canceled => .canceled,
    } else null;
    const failure: ?wire.IndexPublicationFailure = if (observed.head) |head| switch (head.failure) {
        .none => null,
        .duplicate_key => .duplicate_key,
        .invalid_candidate => .invalid_candidate,
        .requested => .requested,
    } else null;
    const id = std.fmt.bytesToHex(request.id, .lower);
    const digest = std.fmt.bytesToHex(request.plan_digest, .lower);
    var table: [20]u8 = undefined;
    const status: wire.IndexPublicationStatus = .{ .index_type = .relational, .index_name = index_name, .publication_token = encoded, .table_id = try std.fmt.bufPrint(&table, "{d}", .{request.table_id}), .publication_id = &id, .plan_digest = &digest, .metadata_incarnation = &request.metadata_incarnation, .state = state, .phase = phase, .failure = failure, .replication_pending = observed.replication_pending, .owner_count = if (observed.head) |head| head.owner_count else null };
    return std.json.Stringify.valueAlloc(alloc, status, .{ .emit_null_optional_fields = false });
}

test "relational index publication recovery tokens are canonical bounded readonly identities" {
    const request: publication.AdmissionRequest = .{ .metadata_incarnation = @splat('1'), .table_id = 101, .id = @splat(2), .plan_digest = @splat(3) };
    const encoded = try Token.encode(request);
    try std.testing.expectEqual(@as(usize, 171), encoded.len);
    try std.testing.expectEqualDeep(request, try Token.decode(&encoded));
    for (0..encoded.len) |i| {
        var damaged = encoded;
        damaged[i] = if (encoded[i] == 'A') 'B' else 'A';
        try std.testing.expectError(error.InvalidIndexPublicationToken, Token.decode(&damaged));
        try std.testing.expectError(error.InvalidIndexPublicationToken, Token.decode(encoded[0..i]));
    }
    var foreign = request;
    foreign.table_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &encoded, &try Token.encode(foreign)));
    foreign = request;
    foreign.metadata_incarnation = @splat('G');
    try std.testing.expectError(error.InvalidIndexPublicationToken, Token.encode(foreign));
}

test "relational index publication public projection withholds unmirrored completion and uses generated enums" {
    const a = std.testing.allocator;
    const request: publication.AdmissionRequest = .{ .metadata_incarnation = @splat('1'), .table_id = 101, .id = @splat(2), .plan_digest = @splat(3) };
    const encoded = try Token.encode(request);
    var observed: publication.AdmissionStatus = .{ .metadata_incarnation = request.metadata_incarnation };
    const Source = struct {
        observed: publication.AdmissionStatus,
        calls: usize = 0,
        pub fn systemCatalog(self: *@This(), alloc: Allocator, context: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try std.testing.expect(context.setting_admin and context.index_publication_authority);
            try std.testing.expect(call == .relational_index_status);
            try call.relational_index_status.validate();
            self.calls += 1;
            return std.json.Stringify.valueAlloc(alloc, self.observed, .{});
        }
    };
    var source: Source = .{ .observed = observed };
    const guard: local.system_catalog_domain.RelationMutationGuard = .{ .target = .{ .name = "items" }, .logical_table = "items", .owner = .{ .kind = .table, .table_id = 101, .schema_version = 1, .schema_digest = @splat(4) }, .incarnation = @splat(0x11) };
    try std.testing.expectError(error.Forbidden, read(&source, a, .{}, guard, "by_email", &encoded));
    try std.testing.expectEqual(@as(usize, 0), source.calls);
    const absent = try read(&source, a, .{ .setting_admin = true, .index_publication_authority = true }, guard, "by_email", &encoded);
    defer a.free(absent);
    var parsed_absent = try std.json.parseFromSlice(wire.IndexPublicationStatus, a, absent, .{});
    defer parsed_absent.deinit();
    try std.testing.expectEqual(wire.IndexPublicationState.admission_unknown, parsed_absent.value.state);
    try std.testing.expect(parsed_absent.value.phase == null);
    observed.head = .{ .plan_id = request.id, .plan_digest = request.plan_digest, .candidate_digest = @splat(4), .reservation_digest = @splat(5), .table_id = request.table_id, .owner_count = 1, .phase = .published, .counts = .{ 1, 1, 1, 1, 1, 0, 0 }, .revision = 10 };
    for ([_]bool{ false, true }) |pending| {
        observed.replication_pending = pending;
        const bytes = try encodeStatus(a, "by_email", &encoded, observed);
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(wire.IndexPublicationStatus, a, bytes, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(if (pending) wire.IndexPublicationState.admission_unknown else .published, parsed.value.state);
        try std.testing.expectEqual(wire.IndexPublicationPhase.published, parsed.value.phase.?);
    }
    observed.replication_pending = false;
    observed.head.?.phase = .canceled;
    observed.head.?.counts = .{ 1, 1, 0, 0, 0, 1, 1 };
    observed.head.?.cancellation_revision = 8;
    for ([_]local.storage_db_relational_index_publication_progress.Failure{ .duplicate_key, .invalid_candidate, .requested }) |reason| {
        observed.head.?.failure = reason;
        const bytes = try encodeStatus(a, "by_email", &encoded, observed);
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(wire.IndexPublicationStatus, a, bytes, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(if (reason == .requested) wire.IndexPublicationState.canceled else .failed, parsed.value.state);
        try std.testing.expect(parsed.value.failure != null);
    }
    var wrong_guard = guard;
    wrong_guard.owner.table_id += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, read(&source, a, .{ .setting_admin = true, .index_publication_authority = true }, wrong_guard, "by_email", &encoded));
    try std.testing.expectEqual(@as(usize, 1), source.calls);
}

test "relational index publication DDL recovery fields match the generated public receipt contract" {
    const a = std.testing.allocator;
    const encoded = try Token.encode(.{ .metadata_incarnation = @splat('1'), .table_id = 101, .id = @splat(2), .plan_digest = @splat(3) });
    const receipt: local.sql_catalog.DdlReceipt = .{ .database = "default", .namespace = "public", .table = "items", .table_id = "101", .schema_version = 2, .state = .admission_unknown, .fk_generation_publication_id = "22222222222222222222222222222222", .index_publication_token = &encoded };
    const bytes = try std.json.Stringify.valueAlloc(a, receipt, .{ .emit_null_optional_fields = false });
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDDLReceipt, a, bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(&encoded, parsed.value.index_publication_token.?);
    try std.testing.expectEqualStrings(receipt.fk_generation_publication_id.?, parsed.value.fk_generation_publication_id.?);
}
