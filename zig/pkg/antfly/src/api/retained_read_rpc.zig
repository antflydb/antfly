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

//! Private service-authenticated retained read protocol. Never expose this
//! protocol through public SQL routing: the coordinator owns user authorization
//! and supplies the already-authorized row policy on open. Owner credentials
//! are reauthenticated for every operation, including close and paging.
const std = @import("std");
const owner_mod = @import("retained_read_owner.zig");
const registry = @import("../storage/retained_read_registry.zig");
const types = @import("antfly_local_sources").storage_db_types;
const reads = @import("antfly_local_sources").api_table_read_source;
const time = @import("antfly_platform").time;
const RequestContext = @import("antfly_local_sources").api_operation.RequestContext;

pub const max_request_bytes = 4 << 20;
pub const max_response_bytes = 32 << 20;
pub const max_lease_ms: u32 = 30_000;
pub const Request = struct {
    operation: enum { admit, capture, validate, snapshot, open, open_snapshot, next, normalize, range_proofs, close, cancel },
    schema_version: u32,
    token: ?registry.Token = null,
    connection: u128 = 0,
    lease_ms: u32 = 5000,
    sequence: u64 = 0,
    limit: u32 = 128,
    from: []const u8 = "",
    to: []const u8 = "",
    query: ?types.RelationalRowQuery = null,
    filter_query_json: []const u8 = "",
    inclusive_from: bool = false,
    exclusive_to: bool = false,
    sql_document_preimage: bool = false,
    include_content_hashes: bool = false,
    include_range_proofs: bool = false,
    row_policy_principal_proof: []const u8 = "",
    row_policy_database: []const u8 = "",
    writes: []const types.BatchWrite = &.{},
};

// The legacy wire shape deliberately excludes native pointers/directories.
pub const WireRow = struct {
    id: []const u8,
    version: u64,
    schema_version: u32,
    value: std.json.Value,
    expected_content_digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    sql_nulls: ?[]const bool = null,
};
const native_page = @import("retained_read_page.zig");

/// Fixed-stack classification only; authentication and native scope checks are
/// still mandatory. Other read operations cannot claim protected transport.
pub fn isControlBody(body: []const u8) bool {
    if (body.len > 4096) return false;
    var bytes: [16384]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    const input = std.json.parseFromSliceLeaky(Request, fixed.allocator(), body, .{ .parse_numbers = false }) catch return false;
    if (input.operation != .close and input.operation != .cancel) return false;
    if (input.lease_ms == 0 or input.lease_ms > max_lease_ms) return false;
    if (input.query != null or input.writes.len != 0 or input.from.len != 0 or input.to.len != 0 or input.filter_query_json.len != 0 or input.row_policy_principal_proof.len != 0 or input.row_policy_database.len != 0) return false;
    if (input.operation == .cancel) return input.connection != 0;
    const token = input.token orelse return false;
    return token.incarnation != 0 and token.sequence != 0 and token.slot < 4096;
}

/// The route ACK authenticates routing, not resource destruction. Cleanup
/// evidence binds physical retirement to one process and exact issued token.
/// Connection revocation is never terminal evidence: it cannot fence a request
/// that has not yet inserted its pending capability.
pub const CleanupEvidence = struct {
    version: u8,
    incarnation: u128,
    state: enum { revoked, pending, retired },
    token: ?registry.Token = null,
    connection: u128 = 0,
};

pub const Response = struct {
    range_proofs: []const @import("antfly_local_sources").storage_range_protection.Proof = &.{},
    token: ?registry.Token = null,
    busy: bool = false,
    sequence: u64 = 0,
    rows: []const WireRow = &.{},
    after: ?[]const u8 = null,
    writes: []const types.BatchWrite = &.{},
    cleanup: ?CleanupEvidence = null,

    /// Preserve the existing read-response wire shape for older peers. Only
    /// cleanup replies add the new evidence; a route-only ACK is insufficient.
    pub fn jsonStringify(self: Response, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Response).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "cleanup")) {
                if (self.cleanup) |value| {
                    try stream.objectField(field);
                    try stream.write(value);
                }
            } else {
                try stream.objectField(field);
                try stream.write(@field(self, field));
            }
        }
        try stream.endObject();
    }
};

/// Scope is constructed by authenticated ingress, never deserialized from a
/// request. Body schema identity is only a requested epoch; native open checks
/// it against the pinned schema before a cursor can be created.
pub fn execute(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request) ![]u8 {
    return executeWithPageFormat(alloc, owner, scope, table, input, false);
}

pub fn executeWithPageFormat(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, native: bool) ![]u8 {
    return executeWithCodec(alloc, owner, scope, table, input, if (native) .v1 else .json);
}
pub fn executeWithCodec(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, format: native_page.Format) ![]u8 {
    return executeWithCodecContext(alloc, owner, scope, table, input, format, .{});
}

pub fn executeWithContext(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, request: RequestContext) ![]u8 {
    return executeWithCodecContext(alloc, owner, scope, table, input, .json, request);
}

pub fn executeWithCodecContext(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, format: native_page.Format, request: RequestContext) ![]u8 {
    const context = try request.platformDeadline();
    const fenced = switch (input.operation) {
        .capture, .validate, .snapshot, .open => true,
        else => false,
    };
    try checkCall(context, fenced);
    if (input.schema_version != scope.schema_version) return error.RetainedReadScopeChanged;
    if (input.lease_ms == 0 or input.lease_ms > max_lease_ms) return error.InvalidRetainedReadLease;
    const deadline = time.monotonicNs() +| @as(u64, input.lease_ms) * std.time.ns_per_ms;
    switch (input.operation) {
        .cancel => {
            if (input.connection == 0) return error.InvalidRetainedReadToken;
            owner.registry.disconnectScoped(scope, input.connection);
            return encodeFor(context, alloc, .{ .cleanup = .{ .version = 1, .incarnation = owner.registry.incarnation, .state = .revoked, .connection = input.connection } }, fenced);
        },
        .admit => {
            // A bounded point probe establishes the owner read-index barrier
            // without retaining any mutation fence or row data.
            if (try owner.source.lookupGroupLocal(alloc, scope.group_id, table, "", .{
                .execution_deadline_ns = @min(context.deadline_ns orelse deadline, deadline),
                .cancellation = context.cancellation,
            }, .read_index)) |value| {
                var found = value;
                found.deinit(alloc);
            }
            return encodeFor(context, alloc, .{}, fenced);
        },
        .capture => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            if (!owner.source.remote_statement_fences_safe) return error.SqlStatementSnapshotRequired;
            // Mutation freeze has a shorter safety ceiling than read snapshots.
            if (input.lease_ms > 5000) return error.InvalidRetainedReadLease;
            const route = owner.source.route_fence orelse return error.CatalogRouteFenceRequired;
            const token = try owner.captureWithAdmission(scope, input.connection, route, table, deadline, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            errdefer if (token) |value| owner.registry.close(value, scope) catch {};
            return encodeFor(context, alloc, .{ .token = token, .busy = token == null }, fenced);
        },
        .validate => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            try owner.validateCaptureWithAdmission(scope, input.token orelse return error.InvalidRetainedReadToken, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            return encodeFor(context, alloc, .{}, fenced);
        },
        .snapshot => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            const token = try owner.captureSnapshotWithAdmission(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, deadline, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            errdefer owner.registry.close(token, scope) catch {};
            return encodeFor(context, alloc, .{ .token = token }, fenced);
        },
        .open => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            const token = try owner.openWithAdmission(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, input.from, input.to, .{
                .relational_query = input.query orelse return error.InvalidRetainedReadQuery,
                .filter_query_json = input.filter_query_json,
                .inclusive_from = input.inclusive_from,
                .exclusive_to = input.exclusive_to,
                .sql_document_preimage = input.sql_document_preimage,
                .include_content_hashes = input.include_content_hashes,
                .include_range_proofs = input.include_range_proofs,
                .row_policy_principal_proof = input.row_policy_principal_proof,
                .row_policy_database = input.row_policy_database,
            }, deadline, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            errdefer owner.registry.close(token, scope) catch {};
            return encodeFor(context, alloc, .{ .token = token }, fenced);
        },
        .open_snapshot => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            const token = try owner.openSnapshotWithAdmission(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, input.from, input.to, .{
                .relational_query = input.query orelse return error.InvalidRetainedReadQuery,
                .filter_query_json = input.filter_query_json,
                .inclusive_from = input.inclusive_from,
                .exclusive_to = input.exclusive_to,
                .sql_document_preimage = input.sql_document_preimage,
                .include_content_hashes = input.include_content_hashes,
                .include_range_proofs = input.include_range_proofs,
                .row_policy_principal_proof = input.row_policy_principal_proof,
                .row_policy_database = input.row_policy_database,
            }, deadline, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            errdefer owner.registry.close(token, scope) catch {};
            return encodeFor(context, alloc, .{ .token = token }, fenced);
        },
        .next => {
            const token = input.token orelse return error.InvalidRetainedReadToken;
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            var page = try owner.nextWithAdmission(alloc, scope, token, input.sequence, input.limit, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            defer page.deinit();
            // Cursor has advanced. Encoding failure poisons it instead of
            // allowing a retry to omit a page without an explicit failure.
            errdefer owner.registry.close(token, scope) catch {};
            try checkCall(context, fenced);
            const encoded = if (format != .json)
                try native_page.encodeVersion(alloc, input.sequence, page.rows, page.after, if (format == .v2) .v2 else .v1)
            else
                try encodeLegacyPage(alloc, input.sequence, page.rows, page.after);
            errdefer alloc.free(encoded);
            try checkCall(context, fenced);
            return encoded;
        },
        .range_proofs => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            const proofs = try owner.rangeProofsWithAdmission(alloc, scope, input.token orelse return error.InvalidRetainedReadToken, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            defer alloc.free(proofs);
            return encodeFor(context, alloc, .{ .range_proofs = proofs }, fenced);
        },
        .normalize => {
            var budget = CallAdmission.init(context);
            const payload = budget.payload();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const writes = try owner.normalizeWithAdmission(arena.allocator(), scope, input.token orelse return error.InvalidRetainedReadToken, input.writes, @import("antfly_local_sources").common_read_admission.Port.init(&payload));
            return encodeFor(context, alloc, .{ .writes = writes }, fenced);
        },
        .close => {
            const token = input.token orelse return error.InvalidRetainedReadToken;
            const state = try owner.registry.closeAcknowledged(token, scope);
            return encodeFor(context, alloc, .{ .cleanup = .{ .version = 1, .incarnation = owner.registry.incarnation, .state = if (state == .retired) .retired else .pending, .token = token } }, fenced);
        },
    }
}

pub fn encodeLegacyPage(alloc: std.mem.Allocator, sequence: u64, input: []const reads.RelationalReadView.Row, after: ?[]const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = arena.allocator(), .limit = max_response_bytes * 4 };
    const a = budget.allocator();
    const rows = try a.alloc(WireRow, input.len);
    for (input, rows) |row, *out| {
        out.* = .{ .id = row.id, .version = row.version, .schema_version = row.schema_version, .value = row.value, .expected_content_digest = row.expected_content_digest, .document = row.document, .sql_nulls = row.sql_nulls };
        if (row.typed_cells) |cells| {
            var object: std.json.ObjectMap = .empty;
            const flags = try a.alloc(bool, cells.count());
            var count: usize = 0;
            const source: @import("antfly_local_sources").sql_catalog.Row = .{ .id = row.id, .version = row.version, .value = .null, .typed_cells = cells };
            for (cells.layout.names) |name| {
                if (!try source.hasField(name)) continue;
                const datum = try source.cell(name);
                const value = if (datum.array) |array| try @import("antfly_local_sources").sql_array_wire.toJsonLeaky(a, array.*, .{}) else if (datum.numeric) |number| value: {
                    var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = a };
                    break :value std.json.Value{ .string = try @import("antfly_local_sources").sql_numeric_value.format(&context, number.*) };
                } else if (datum.value == .float and !std.math.isFinite(datum.value.float))
                    std.json.Value{ .string = if (std.math.isNan(datum.value.float)) "NaN" else if (datum.value.float < 0) "-Infinity" else "Infinity" }
                else
                    datum.value;
                try object.put(a, name, value);
                flags[count] = datum.sql_null;
                count += 1;
            }
            out.value = .{ .object = object };
            out.sql_nulls = flags[0..count];
        }
    }
    return encode(alloc, .{ .sequence = sequence, .rows = rows, .after = after });
}

const CallAdmission = struct {
    context: RequestContext,
    wait: @import("runnable_fanout_owner.zig").RemoteWait,
    fn init(context: RequestContext) CallAdmission {
        return .{ .context = context, .wait = .{ .borrow = if (context.execution_team) |team| team.borrow else null } };
    }
    fn check(raw: *anyopaque) !void {
        const self: *CallAdmission = @ptrCast(@alignCast(raw));
        try checkpoint(self.context);
    }
    fn lifetime(raw: *anyopaque) !void {
        const self: *CallAdmission = @ptrCast(@alignCast(raw));
        try self.context.ensureExecutionActive();
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self: *CallAdmission = @ptrCast(@alignCast(raw));
        return self.context.deadline_ns;
    }
    fn progress(raw: *anyopaque) !void {
        const self: *CallAdmission = @ptrCast(@alignCast(raw));
        try checkpoint(self.context);
        const io_borrow = self.context.fanout_io orelse return error.ExecutionWaitUnsupported;
        var receiver = try io_borrow.receive();
        try @import("runnable_fanout_owner.zig").sleepForProgress(self.wait.borrow, receiver.io(), .fromMilliseconds(1));
        try checkpoint(self.context);
    }
    fn payload(self: *CallAdmission) @import("antfly_local_sources").common_read_admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .wait_for_progress = progress, .check_lifetime = lifetime, .deadline_ns = deadline }, .execution_wait = self.wait.boundary() };
    }
};

fn checkpoint(context: RequestContext) !void {
    try context.ensureExecutionActive();
    if (context.execution_team) |team| {
        const status = team.borrow.checkpoint(team.borrow.context);
        if (!status.isOk()) return @import("antfly_runtime_abi").error_abi.errorFromStatus(status);
    }
}
fn checkCall(context: RequestContext, fenced: bool) !void {
    if (!fenced) return checkpoint(context);
    try context.ensureExecutionActive();
}
fn encodeFor(context: RequestContext, alloc: std.mem.Allocator, response: Response, fenced: bool) ![]u8 {
    try checkCall(context, fenced);
    const bytes = try encode(alloc, response);
    errdefer alloc.free(bytes);
    try checkCall(context, fenced);
    return bytes;
}

pub fn encode(alloc: std.mem.Allocator, response: Response) ![]u8 {
    // Bound actual writer growth, including JSON string escaping, before it
    // allocates. Returned bytes have no allocator callbacks and may be freed
    // directly through the backing allocator after this budget goes away.
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = alloc, .limit = max_response_bytes };
    const bytes = std.json.Stringify.valueAlloc(budget.allocator(), response, .{}) catch |err| {
        if (budget.exhausted) return error.RetainedReadPageTooLarge;
        return err;
    };
    errdefer alloc.free(bytes);
    if (bytes.len > max_response_bytes) return error.RetainedReadPageTooLarge;
    return bytes;
}

/// A response owns all strings and exact number lexemes. SQL-null metadata is
/// mandatory and aligned; otherwise a JSON null could silently become SQL NULL.
pub fn decodePage(alloc: std.mem.Allocator, bytes: []const u8, sequence: u64, schema_version: u32, limit: u32) !reads.RelationalReadView.Page {
    if (native_page.isNative(bytes)) return native_page.decode(alloc, bytes, sequence, schema_version, limit);
    if (limit == 0 or limit > 4096 or bytes.len > max_response_bytes) return error.InvalidRetainedReadPage;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const response = try std.json.parseFromSliceLeaky(Response, arena.allocator(), bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
    if (response.cleanup != null or response.sequence != sequence or response.token != null or response.busy or response.rows.len > limit or response.writes.len != 0) return error.InvalidRetainedReadPage;
    for (response.rows) |row| {
        if (row.schema_version != schema_version or row.value != .object) return error.InvalidRetainedReadPage;
        const nulls = row.sql_nulls orelse return error.InvalidRetainedReadPage;
        if (nulls.len != row.value.object.count()) return error.InvalidRetainedReadPage;
        for (nulls, row.value.object.values()) |sql_null, value| if (sql_null and value != .null) return error.InvalidRetainedReadPage;
    }
    const rows = try arena.allocator().alloc(reads.RelationalReadView.Row, response.rows.len);
    for (response.rows, rows) |row, *out| out.* = .{ .id = row.id, .version = row.version, .schema_version = row.schema_version, .value = row.value, .expected_content_digest = row.expected_content_digest, .document = row.document, .sql_nulls = row.sql_nulls };
    return .{ .arena = arena, .rows = rows, .after = response.after };
}

pub const consumer_tests = consumerTests();
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const root = @import("antfly_source_root");
    if (@hasDecl(root, "implementation_tests_only") and root.implementation_tests_only) return struct {};
    return struct {
        test "retained read RPC native pages preserve typed payloads and legacy negotiation" {
            const Probe = struct {
                fn run(a: std.mem.Allocator, version: @import("antfly_local_sources").sql_spill.ColumnarVersion) !void {
                    const src = @import("antfly_local_sources");
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const owned = arena.allocator();
                    var array = try src.sql_array_text.decode(a, .int64, "[0:2]={9007199254740993,NULL,-7}", .{});
                    defer array.deinit();
                    var context: src.sql_numeric_value.Context = .{ .alloc = a };
                    var number = try src.sql_numeric_value.parse(&context, "9007199254740993.1200");
                    defer number.deinit();
                    const layout = try src.sql_catalog.Row.TypedLayout.init(owned, &.{ "a", "n", "j", "z", "missing", "f", "_id" });
                    const values = [_]src.sql_catalog.Row.Cell{ .typedArray(&array.value), .typedNumeric(&number.value), .json(.null), .{}, .{}, .json(.{ .float = std.math.nan(f64) }), .json(.{ .string = "payload-id" }) };
                    const rows = [_]reads.RelationalReadView.Row{.{ .id = "row", .version = std.math.maxInt(u64), .schema_version = 3, .value = .null, .typed_cells = .{ .layout = layout, .values = &values, .presence = &.{ true, true, true, true, false, true, true } }, .expected_content_digest = @splat(7) }};
                    const bytes = try native_page.encodeVersion(a, 4, &rows, "next", version);
                    defer a.free(bytes);
                    var page = try decodePage(a, bytes, 4, 3, 1);
                    defer page.deinit();
                    const row: src.sql_catalog.Row = .{ .id = page.rows[0].id, .version = page.rows[0].version, .value = page.rows[0].value, .typed_cells = page.rows[0].typed_cells };
                    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), row.version);
                    try std.testing.expectEqual(@as([32]u8, @splat(7)), page.rows[0].expected_content_digest.?);
                    try std.testing.expectEqual(@as(i64, 9007199254740993), (try row.cell("a")).array.?.flatCell(0).value.integer);
                    try std.testing.expectEqual(@as(i32, 0), (try row.cell("a")).array.?.dimensions[0].lower);
                    try std.testing.expect((try row.cell("a")).array.?.flatCell(1).sql_null);
                    try std.testing.expectEqual(version == .v2, (try row.cell("a")).array.?.stored != null);
                    const text = try src.sql_numeric_value.format(&context, (try row.cell("n")).numeric.?.*);
                    defer a.free(text);
                    try std.testing.expectEqualStrings("9007199254740993.1200", text);
                    try std.testing.expect(!(try row.cell("j")).sql_null);
                    try std.testing.expect((try row.cell("z")).sql_null);
                    try std.testing.expect(!try row.hasField("missing"));
                    try std.testing.expect(std.math.isNan((try row.cell("f")).value.float));
                    try std.testing.expectEqualStrings("row", (try page.rows[0].typed_cells.?.at(6)).value.string);
                    try std.testing.expectEqualStrings("next", page.after.?);
                    const legacy = try encodeLegacyPage(a, 4, &rows, "next");
                    defer a.free(legacy);
                    var legacy_page = try decodePage(a, legacy, 4, 3, 1);
                    defer legacy_page.deinit();
                    try std.testing.expect(legacy_page.rows[0].value.object.get("a").? == .object);
                    try std.testing.expect(!legacy_page.rows[0].sql_nulls.?[2]);
                    const legacy_float = try src.sql_document_row.declaredCell(a, .{ .name = "f", .path = "f", .type = .number, .element_type = .float64 }, .json(legacy_page.rows[0].value.object.get("f").?));
                    try std.testing.expect(std.math.isNan(legacy_float.value.float));
                    @memset(bytes, 0);
                    try std.testing.expectEqualStrings("row", row.id);
                    try std.testing.expectEqual(@as(i64, -7), (try row.cell("a")).array.?.flatCell(2).value.integer);
                }
            };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{version});
        }

        test "retained read RPC native pages reject truncation identity and invalid framing" {
            const a = std.testing.allocator;
            const row: reads.RelationalReadView.Row = .{ .id = "a", .version = 2, .schema_version = 3, .value = .{ .object = .empty }, .sql_nulls = &.{} };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                const bytes = try native_page.encodeVersion(a, 4, &.{row}, null, version);
                defer a.free(bytes);
                for (0..bytes.len) |length| {
                    if (native_page.decode(a, bytes[0..length], 4, 3, 1)) |value| {
                        var page = value;
                        page.deinit();
                        return error.AcceptedTruncatedNativePage;
                    } else |_| {}
                }
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 5, 3, 1));
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 4, 2, 1));
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 4, 3, 0));
            }
        }

        test "retained read RPC native pages tile both axes without losing presence" {
            const src = @import("antfly_local_sources");
            const a = std.testing.allocator;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const owned = arena.allocator();
            const names = try owned.alloc([]const u8, 1025);
            for (names, 0..) |*name, index| name.* = try std.fmt.allocPrint(owned, "c{d}", .{index});
            const layout = try src.sql_catalog.Row.TypedLayout.init(owned, names);
            const cells = try owned.alloc(src.sql_catalog.Row.Cell, names.len);
            for (cells, 0..) |*cell, index| cell.* = .json(.{ .integer = @intCast(index) });
            const rows = try owned.alloc(reads.RelationalReadView.Row, 257);
            for (rows, 0..) |*row, index| row.* = .{ .id = try std.fmt.allocPrint(owned, "r{d}", .{index}), .version = index, .schema_version = 3, .value = .null, .typed_cells = .{ .layout = layout, .values = cells } };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                const bytes = try native_page.encodeVersion(a, 4, rows, "next", version);
                defer a.free(bytes);
                var page = try decodePage(a, bytes, 4, 3, 257);
                defer page.deinit();
                try std.testing.expectEqual(@as(usize, 257), page.rows.len);
                try std.testing.expectEqualStrings("r256", page.rows[256].id);
                try std.testing.expect(page.rows[256].typed_cells.?.columnar != null);
                try std.testing.expectEqual(@as(usize, 0), page.rows[256].typed_cells.?.values.len);
                const value = try page.rows[256].typed_cells.?.at(1024);
                try std.testing.expectEqual(@as(i64, 1024), value.value.integer);
                try std.testing.expect(!value.sql_null);
                try std.testing.expectEqualStrings("next", page.after.?);
            }
        }

        test "retained read RPC cleanup evidence separates physical retirement from connection revocation" {
            const a = std.testing.allocator;
            const Fixture = struct {
                closes: usize = 0,
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = undefined, .release = release } };
                }
                fn validate(_: *anyopaque) !void {}
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.closes += 1;
                }
            };
            var state = try registry.Registry.init(a, std.testing.io, 7, 1, 1, 30 * std.time.ns_per_s);
            defer state.deinit();
            var fixture: Fixture = .{};
            const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
            const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
            const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .route_fence = route, .remote_statement_fences_safe = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
            const canceled = try execute(a, owner, scope, "rows", .{ .operation = .cancel, .schema_version = 5, .connection = 55 });
            defer a.free(canceled);
            var revoked = try std.json.parseFromSlice(Response, a, canceled, .{});
            defer revoked.deinit();
            try std.testing.expect(revoked.value.cleanup.?.state == .revoked);
            try std.testing.expectEqual(@as(u128, 7), revoked.value.cleanup.?.incarnation);
            try std.testing.expectEqual(@as(u128, 55), revoked.value.cleanup.?.connection);
            try std.testing.expect(revoked.value.cleanup.?.token == null);
            // An in-flight creation can arrive after connection revocation.
            // The response above must never authorize root/charge retirement.
            const captured = try execute(a, owner, scope, "rows", .{ .operation = .capture, .schema_version = 5, .connection = 55 });
            defer a.free(captured);
            var parsed = try std.json.parseFromSlice(Response, a, captured, .{});
            defer parsed.deinit();
            const token = parsed.value.token.?;
            try std.testing.expect(parsed.value.cleanup == null);
            var borrow = try state.borrow(token, scope, .capture, time.monotonicNs());
            defer borrow.deinit();
            const pending = try execute(a, owner, scope, "rows", .{ .operation = .close, .schema_version = 5, .token = token });
            defer a.free(pending);
            var pending_value = try std.json.parseFromSlice(Response, a, pending, .{});
            defer pending_value.deinit();
            try std.testing.expect(pending_value.value.cleanup.?.state == .pending);
            try std.testing.expectEqual(token, pending_value.value.cleanup.?.token.?);
            try std.testing.expectEqual(@as(usize, 0), fixture.closes);
            borrow.deinit();
            const retired = try execute(a, owner, scope, "rows", .{ .operation = .close, .schema_version = 5, .token = token });
            defer a.free(retired);
            var retired_value = try std.json.parseFromSlice(Response, a, retired, .{});
            defer retired_value.deinit();
            try std.testing.expect(retired_value.value.cleanup.?.state == .retired);
            try std.testing.expectEqual(token, retired_value.value.cleanup.?.token.?);
            try std.testing.expectEqual(@as(usize, 1), fixture.closes);
            var future = token;
            future.sequence += 1;
            try std.testing.expectError(error.InvalidRetainedReadToken, execute(a, owner, scope, "rows", .{ .operation = .close, .schema_version = 5, .token = future }));
            // Read replies keep their prior wire shape; the new field is
            // emitted only for cleanup, so existing read decoders still work.
            const legacy = try std.json.Stringify.valueAlloc(a, .{ .range_proofs = @as([]const @import("antfly_local_sources").storage_range_protection.Proof, &.{}), .token = @as(?registry.Token, null), .busy = false, .sequence = @as(u64, 0), .rows = @as([]const WireRow, &.{}), .after = @as(?[]const u8, null), .writes = @as([]const types.BatchWrite, &.{}) }, .{});
            defer a.free(legacy);
            const normal = try encode(a, .{});
            defer a.free(normal);
            try std.testing.expectEqualStrings(legacy, normal);
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(a, retired, 0, 5, 1));
        }

        test "retained read RPC validation checks original lifetime without lending under capture" {
            const a = std.testing.allocator;
            const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const Mode = enum { success, canceled, rejected_resume };
            const Fixture = struct {
                scheduler: *native.Scheduler,
                signal: std.atomic.Value(bool) = .init(false),
                mode: Mode,
                deadline: u64,
                calls: usize = 0,
                closes: usize = 0,
                scope_checks: usize = 0,
                fail_at: usize = 0,
                scope_cause: anyerror = error.NotFound,
                fn scopeCheck(raw: *const anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
                    self.scope_checks += 1;
                    if (self.scope_checks == self.fail_at) return self.scope_cause;
                    if (self.signal.load(.acquire)) return error.Canceled;
                }
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = legacy, .validate_with_admission = validate, .open = undefined, .release = release } };
                }
                fn unexpectedCpu(_: *anyopaque) callconv(.c) @import("antfly_runtime_abi").error_abi.Status {
                    return @import("antfly_runtime_abi").error_abi.statusFromError(error.TestRpcCpuHandoffWhileFenced);
                }
                fn legacy(_: *anyopaque) !void {
                    return error.TestUnadmittedValidation;
                }
                fn validate(raw: *anyopaque, control: reads.RelationalReadView.Admission) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.calls += 1;
                    try std.testing.expectEqual(@as(?u64, self.deadline), try control.deadlineNs());
                    try std.testing.expect(try control.executionWait() == null);
                    control.waitForProgress() catch |err| {
                        if (err != error.Backpressured) return err;
                    };
                    try control.checkLifetime();
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
                    try std.testing.expectEqual(@as(u64, 17), self.scheduler.stats().retained_bytes);
                    var health: native.Execution = .{};
                    try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                    switch (self.mode) {
                        .success => {},
                        .canceled => self.signal.store(true, .release),
                        .rejected_resume => self.scheduler.close(),
                    }
                    try control.checkLifetime();
                }
                fn release(raw: *anyopaque) void {
                    @as(*@This(), @ptrCast(@alignCast(raw))).closes += 1;
                }
            };
            for ([_]Mode{ .success, .canceled, .rejected_resume }) |mode| {
                var scheduling = try native.Scheduler.init(.{ .slots = 2 });
                var execution: native.Execution = .{};
                try execution.start(&scheduling, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
                defer execution.finish();
                var read: @import("runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
                var team = try read.team();
                team.borrow.checkpoint = Fixture.unexpectedCpu;
                var state = try registry.Registry.init(a, std.testing.io, 1, 2, 2, 10 * std.time.ns_per_s);
                defer state.deinit();
                const deadline = time.monotonicNs() + std.time.ns_per_s;
                var fixture: Fixture = .{ .scheduler = &scheduling, .mode = mode, .deadline = deadline };
                const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
                const capture = (try owner.capture(scope, 1, route, "rows", deadline)) orelse return error.TestMissingCapture;
                const result = executeWithContext(a, owner, scope, "rows", .{ .operation = .validate, .schema_version = 5, .token = capture }, .{ .deadline_ns = deadline, .execution_team = &team, .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.scopeCheck } });
                if (mode == .success) a.free(try result) else try std.testing.expectError(if (mode == .canceled) error.Canceled else error.SchedulerClosed, result);
                try std.testing.expectEqual(@as(usize, 1), fixture.calls);
                if (mode == .success) {
                    const checks = fixture.scope_checks;
                    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RowPolicyCatalogChanged, error.NotFound }) |cause| {
                        for (1..checks + 1) |fail_at| {
                            fixture.scope_checks = 0;
                            fixture.fail_at = fail_at;
                            fixture.scope_cause = cause;
                            try std.testing.expectError(cause, executeWithContext(a, owner, scope, "rows", .{ .operation = .validate, .schema_version = 5, .token = capture }, .{ .deadline_ns = deadline, .execution_team = &team, .cancellation = .{ .ptr = &fixture, .check_fn = Fixture.scopeCheck } }));
                            try std.testing.expectEqual(@as(usize, 0), fixture.closes);
                        }
                    }
                    fixture.fail_at = 0;
                }
                try std.testing.expectEqual(@as(usize, 0), fixture.closes);
                try state.close(capture, scope);
                try std.testing.expectEqual(@as(usize, 1), fixture.closes);
                execution.finish();
                try std.testing.expectEqual(@as(usize, 0), scheduling.stats().operations);
                try std.testing.expectEqual(@as(u64, 0), scheduling.stats().retained_bytes);
            }
        }

        test "retained read RPC capture and snapshot borrow original admission and retire rejected native outputs" {
            const a = std.testing.allocator;
            const Snapshot = @import("antfly_local_sources").storage_statement_read_fence.Snapshot;
            const Fixture = struct {
                signal: std.atomic.Value(bool) = .init(false),
                reject_capture: bool = true,
                reject_snapshot: bool = true,
                captures_closed: usize = 0,
                snapshots_closed: usize = 0,
                deadline: u64,
                fn capture(raw: *anyopaque, _: std.mem.Allocator, route: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, opts: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency, control: reads.RelationalReadView.Admission) !?reads.StatementReadFence {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try control.checkpoint();
                    try std.testing.expectError(error.Backpressured, control.waitForProgress());
                    try std.testing.expect(opts.execution_deadline_ns.? > self.deadline);
                    if (self.reject_capture) self.signal.store(true, .release);
                    // Original request cancellation cannot become the retained
                    // capture's independently owned cancellation capsule.
                    try std.testing.expect(!opts.cancellation.?.isCancelled());
                    try std.testing.expect(!route.admission_cancellation.isCancelled());
                    return .{ .ptr = self, .vtable = &.{ .validate = validate, .open = open, .capture_snapshot_with_admission = snapshot, .release = closeCapture } };
                }
                fn validate(_: *anyopaque) !void {}
                fn open(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !reads.RelationalReadView {
                    return error.TestUnexpectedOpen;
                }
                fn snapshot(raw: *anyopaque, _: std.mem.Allocator, control: reads.RelationalReadView.Admission) !Snapshot {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try control.checkpoint();
                    try std.testing.expectError(error.Backpressured, control.waitForProgress());
                    if (self.reject_snapshot) self.signal.store(true, .release);
                    return .{ .ptr = self, .vtable = &.{ .open = open, .release = closeSnapshot } };
                }
                fn closeCapture(raw: *anyopaque) void {
                    @as(*@This(), @ptrCast(@alignCast(raw))).captures_closed += 1;
                }
                fn closeSnapshot(raw: *anyopaque) void {
                    @as(*@This(), @ptrCast(@alignCast(raw))).snapshots_closed += 1;
                }
            };
            const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
            var execution: scheduling.Execution = .{};
            try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            defer execution.finish();
            var read: @import("runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
            var team = try read.team();
            var state = try registry.Registry.init(a, std.testing.io, 1, 2, 2, 10 * std.time.ns_per_s);
            defer state.deinit();
            const deadline = time.monotonicNs() + std.time.ns_per_s;
            var fixture: Fixture = .{ .deadline = deadline };
            const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
            const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
            const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .route_fence = route, .remote_statement_fences_safe = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_with_admission = Fixture.capture } } };
            const context: RequestContext = .{ .deadline_ns = deadline, .cancellation = .fromAtomic(&fixture.signal), .execution_team = &team, .fanout_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&std.testing.io) };
            const capture_input: Request = .{ .operation = .capture, .schema_version = 5, .connection = 1 };
            try std.testing.expectError(error.Canceled, executeWithContext(a, owner, scope, "rows", capture_input, context));
            try std.testing.expectEqual(@as(usize, 1), fixture.captures_closed);
            fixture.signal.store(false, .release);
            fixture.reject_capture = false;
            const encoded_capture = try executeWithContext(a, owner, scope, "rows", capture_input, context);
            defer a.free(encoded_capture);
            var parsed_capture = try std.json.parseFromSlice(Response, a, encoded_capture, .{});
            defer parsed_capture.deinit();
            const capture_token = parsed_capture.value.token.?;
            const snapshot_input: Request = .{ .operation = .snapshot, .schema_version = 5, .token = capture_token, .connection = 1 };
            try std.testing.expectError(error.Canceled, executeWithContext(a, owner, scope, "rows", snapshot_input, context));
            try std.testing.expectEqual(@as(usize, 1), fixture.snapshots_closed);
            fixture.signal.store(false, .release);
            fixture.reject_snapshot = false;
            const encoded_snapshot = try executeWithContext(a, owner, scope, "rows", snapshot_input, context);
            defer a.free(encoded_snapshot);
            var parsed_snapshot = try std.json.parseFromSlice(Response, a, encoded_snapshot, .{});
            defer parsed_snapshot.deinit();
            try state.close(capture_token, scope);
            try state.close(parsed_snapshot.value.token.?, scope);
            try std.testing.expectEqual(@as(usize, 2), fixture.captures_closed);
            try std.testing.expectEqual(@as(usize, 2), fixture.snapshots_closed);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        }
        test "retained read RPC page forwards original admission through checked callback dispatch" {
            const a = std.testing.allocator;
            const runtime = @import("antfly_runtime_abi");
            const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const admission = @import("antfly_local_sources").common_read_admission;
            const View = reads.RelationalReadView;
            const Call = runtime.callback_abi.Boundary(View.VTable);
            const Fixture = struct {
                scheduler: *scheduling.Scheduler,
                signal: std.atomic.Value(bool) = .init(false),
                reject: bool = false,
                normalizations: usize = 0,
                proof_reads: usize = 0,
                opens: usize = 0,
                calls: usize = 0,
                checks: usize = 0,
                fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
                    return Call.local_dispatch(contract, callback, args, output);
                }
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = open, .open_with_admission = openAdmitted, .release = release } };
                }
                fn validate(_: *anyopaque) !void {}
                fn release(_: *anyopaque) void {}
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !View {
                    return .{ .ptr = raw, .vtable = &.{ .next = unexpected, .next_with_admission = admitted, .normalize_with_admission = normalized, .range_proofs_with_admission = proofs, .close = release }, .boundary_dispatch = dispatch };
                }
                fn openAdmitted(raw: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, control: reads.RelationalReadView.Admission) !reads.RelationalReadView {
                    @as(*@This(), @ptrCast(@alignCast(raw))).opens += 1;
                    try control.checkpoint();
                    // Only the registry's independent lifetime controls may be
                    // retained; the opening caller's admission is separate.
                    try std.testing.expect(opts.cancellation != null);
                    return open(raw, alloc, from, to, opts);
                }
                fn normalized(raw: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: admission.Port) ![]types.BatchWrite {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try control.checkpoint();
                    try std.testing.expectEqual(@as(usize, 1), writes.len);
                    const result = try alloc.alloc(types.BatchWrite, 1);
                    errdefer alloc.free(result);
                    const key = try alloc.dupe(u8, writes[0].key);
                    errdefer alloc.free(key);
                    const value = try alloc.dupe(u8, writes[0].value);
                    result[0] = .{ .key = key, .value = value };
                    self.normalizations += 1;
                    if (self.reject) self.signal.store(true, .release);
                    return result;
                }
                fn proofs(raw: *anyopaque, alloc: std.mem.Allocator, control: admission.Port) ![]@import("antfly_local_sources").storage_range_protection.Proof {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try control.checkpoint();
                    const result = try alloc.alloc(@import("antfly_local_sources").storage_range_protection.Proof, 1);
                    result[0] = .{ .bucket = 1, .generation = 7 };
                    self.proof_reads += 1;
                    if (self.reject) self.signal.store(true, .release);
                    return result;
                }
                fn unexpected(_: *anyopaque, _: std.mem.Allocator, _: u32) !View.Page {
                    return error.TestLegacyPageEscape;
                }
                fn admitted(raw: *anyopaque, alloc: std.mem.Allocator, _: u32, control: admission.Port) !View.Page {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.calls += 1;
                    try control.checkpoint();
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
                    const wait = (try control.executionWait()) orelse return error.TestMissingExecutionWait;
                    try wait.beginWait();
                    try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                    var health: scheduling.Execution = .{};
                    try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                    try wait.endWait();
                    try control.checkpoint();
                    self.checks += 1;
                    return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
                }
            };
            var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
            var execution: scheduling.Execution = .{};
            try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
            defer execution.finish();
            var read: @import("runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
            var team = try read.team();
            var state = try registry.Registry.init(a, std.testing.io, 1, 4, 4, 10 * std.time.ns_per_s);
            defer state.deinit();
            var fixture: Fixture = .{ .scheduler = &scheduler };
            const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
            const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
            const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
            const deadline = time.monotonicNs() + std.time.ns_per_s;
            const capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
            const encoded_open = try executeWithContext(a, owner, scope, "rows", .{ .operation = .open, .schema_version = 5, .token = capture, .query = .{ .fields = &.{} } }, .{ .deadline_ns = deadline, .execution_team = &team });
            defer a.free(encoded_open);
            var parsed_open = try std.json.parseFromSlice(Response, a, encoded_open, .{});
            defer parsed_open.deinit();
            const cursor = parsed_open.value.token orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(@as(usize, 1), fixture.opens);
            try state.close(capture, scope);
            const context: RequestContext = .{ .deadline_ns = deadline, .execution_team = &team, .cancellation = .fromAtomic(&fixture.signal) };
            for ([_]bool{ false, true }) |normalize| {
                const input: Request = .{ .operation = if (normalize) .normalize else .range_proofs, .schema_version = 5, .token = cursor, .writes = if (normalize) &.{.{ .key = "row", .value = "{}" }} else &.{} };
                const encoded = try executeWithContext(a, owner, scope, "rows", input, context);
                defer a.free(encoded);
                var parsed = try std.json.parseFromSlice(Response, a, encoded, .{});
                defer parsed.deinit();
                if (normalize) try std.testing.expectEqualStrings("row", parsed.value.writes[0].key) else try std.testing.expectEqual(@as(?u64, 7), parsed.value.range_proofs[0].generation);
                fixture.reject = true;
                try std.testing.expectError(error.Canceled, executeWithContext(a, owner, scope, "rows", input, context));
                fixture.signal.store(false, .release);
                fixture.reject = false;
            }
            try std.testing.expectEqual(@as(usize, 2), fixture.normalizations);
            try std.testing.expectEqual(@as(usize, 2), fixture.proof_reads);
            // Pure normalization/proof failures cannot advance or poison paging.
            const result = try executeWithContext(a, owner, scope, "rows", .{ .operation = .next, .schema_version = 5, .token = cursor }, .{ .deadline_ns = deadline, .execution_team = &team });
            defer a.free(result);
            try std.testing.expectEqual(@as(usize, 1), fixture.calls);
            try std.testing.expectEqual(fixture.calls, fixture.checks);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try state.close(cursor, scope);
        }
        test "retained read RPC rejected creation closes physical output before returning its admission slot" {
            const a = std.testing.allocator;
            const Fixture = struct {
                signal: std.atomic.Value(bool) = .init(false),
                closed: usize = 0,
                reject: bool = true,
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = legacy, .open_with_admission = open, .capture_snapshot = snapshot, .release = release } };
                }
                fn validate(_: *anyopaque) !void {}
                fn release(_: *anyopaque) void {}
                fn snapshot(raw: *anyopaque, _: std.mem.Allocator) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
                    return .{ .ptr = raw, .vtable = &.{ .open = legacy, .open_with_admission = open, .release = release } };
                }
                fn legacy(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !reads.RelationalReadView {
                    return error.TestUnadmittedCreation;
                }
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions, control: reads.RelationalReadView.Admission) !reads.RelationalReadView {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try control.checkpoint();
                    try std.testing.expect(opts.cancellation != null);
                    if (self.reject) self.signal.store(true, .release);
                    return .{ .ptr = self, .vtable = &.{ .next = next, .close = close } };
                }
                fn next(_: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
                    return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
                }
                fn close(raw: *anyopaque) void {
                    @as(*@This(), @ptrCast(@alignCast(raw))).closed += 1;
                }
            };
            for ([_]bool{ false, true }) |snapshot| {
                var state = try registry.Registry.init(a, std.testing.io, 1, 2, 2, 10 * std.time.ns_per_s);
                defer state.deinit();
                var fixture: Fixture = .{};
                const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
                const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                const deadline = time.monotonicNs() + std.time.ns_per_s;
                const capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
                const parent = if (snapshot) try owner.captureSnapshot(scope, capture, 1, deadline) else capture;
                if (snapshot) try state.close(capture, scope);
                const input: Request = .{ .operation = if (snapshot) .open_snapshot else .open, .schema_version = 5, .token = parent, .query = .{ .fields = &.{} } };
                const context: RequestContext = .{ .deadline_ns = deadline, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fixture.signal) };
                try std.testing.expectError(error.Canceled, executeWithContext(a, owner, scope, "rows", input, context));
                try std.testing.expectEqual(@as(usize, 1), fixture.closed);
                fixture.signal.store(false, .release);
                fixture.reject = false;
                const encoded = try executeWithContext(a, owner, scope, "rows", input, context);
                defer a.free(encoded);
                var parsed = try std.json.parseFromSlice(Response, a, encoded, .{});
                defer parsed.deinit();
                try state.close(parsed.value.token.?, scope);
                try std.testing.expectEqual(@as(usize, 2), fixture.closed);
                try state.close(parent, scope);
            }
        }
        test "retained read RPC cancellation after page advancement poisons the cursor" {
            const a = std.testing.allocator;
            const Fixture = struct {
                signal: std.atomic.Value(bool) = .init(false),
                closed: usize = 0,
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: @import("../metadata/api.zig").CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = open, .release = release } };
                }
                fn validate(_: *anyopaque) !void {}
                fn release(_: *anyopaque) void {}
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !reads.RelationalReadView {
                    return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close } };
                }
                fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.signal.store(true, .release);
                    return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
                }
                fn close(raw: *anyopaque) void {
                    @as(*@This(), @ptrCast(@alignCast(raw))).closed += 1;
                }
            };
            for ([_]native_page.Format{ .json, .v1, .v2 }) |format| {
                var state = try registry.Registry.init(a, std.testing.io, 1, 4, 4, 10 * std.time.ns_per_s);
                defer state.deinit();
                var fixture: Fixture = .{};
                const owner: owner_mod.Owner = .{ .registry = &state, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
                const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route: @import("../metadata/api.zig").CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                const deadline = time.monotonicNs() + std.time.ns_per_s;
                const capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
                const cursor = try owner.open(scope, capture, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline);
                try state.close(capture, scope);
                try std.testing.expectError(error.Canceled, executeWithCodecContext(a, owner, scope, "rows", .{ .operation = .next, .schema_version = 5, .token = cursor }, format, .{ .deadline_ns = deadline, .cancellation = .fromAtomic(&fixture.signal) }));
                try std.testing.expectEqual(@as(usize, 1), fixture.closed);
                try std.testing.expectError(error.RetainedReadNotFound, owner.next(a, scope, cursor, 1, 1));
            }
        }
        test "retained read RPC validates exact typed page identity and null metadata" {
            const bytes = "{\"sequence\":4,\"rows\":[{\"id\":\"a\",\"version\":7,\"schema_version\":3,\"value\":{\"n\":9007199254740993,\"j\":null,\"missing\":null},\"sql_nulls\":[false,false,true]}],\"after\":\"a\"}";
            var page = try decodePage(std.testing.allocator, bytes, 4, 3, 1);
            defer page.deinit();
            try std.testing.expectEqualStrings("9007199254740993", page.rows[0].value.object.get("n").?.number_string);
            try std.testing.expectEqualSlices(bool, &.{ false, false, true }, page.rows[0].sql_nulls.?);
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, bytes, 5, 3, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, bytes, 4, 2, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, "{\"rows\":[{\"id\":\"a\",\"version\":1,\"schema_version\":3,\"value\":{\"j\":null}}]}", 0, 3, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, "{\"rows\":[{\"id\":\"a\",\"version\":1,\"schema_version\":3,\"value\":{\"j\":2},\"sql_nulls\":[true]}]}", 0, 3, 1));
        }
    };
}
