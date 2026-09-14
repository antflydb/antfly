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

//! Durable authority for an unpublished restore owner. Only logical primary
//! rows cross the source/target boundary; target identities and constraint
//! generations are allocated by the ordinary target schema/row pipeline.
const std = @import("std");
const identity = @import("doc_identity.zig");
const activation = @import("relational_integrity_activation.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const Allocator = std.mem.Allocator;
pub const key = "\x00\x00__metadata__:restore_staging_owner";
pub const bootstrap_key = "\x00\x00__metadata__:restore_staging_bootstrap";
/// Authenticated by the HA stream and pinned to the immutable reserved owner.
/// New hidden owners created after a seed can therefore be reconstructed before
/// applying their first lifecycle record, without consulting public placement.
pub const OwnerBootstrap = struct {
    scope: Scope,
    table_name: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8 = "",
    indexes_json: []const u8,
    byte_range: @import("types.zig").ByteRange,

    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try jw.beginObject();
        try jw.objectField("scope");
        try @import("relational_integrity_json.zig").write(self.scope, jw);
        try jw.objectField("table_name");
        try jw.write(self.table_name);
        try jw.objectField("schema_json");
        try jw.write(self.schema_json);
        try jw.objectField("read_schema_json");
        try jw.write(self.read_schema_json);
        try jw.objectField("indexes_json");
        try jw.write(self.indexes_json);
        try jw.objectField("byte_range");
        try @import("relational_integrity_json.zig").write(self.byte_range, jw);
        try jw.endObject();
    }
    pub fn validate(self: @This()) !void {
        try self.scope.validate();
        if (self.table_name.len == 0 or self.table_name.len > 255 or std.mem.indexOfAny(u8, self.table_name, "/\\\x00") != null or std.mem.eql(u8, self.table_name, ".") or std.mem.eql(u8, self.table_name, "..") or
            self.schema_json.len +| self.read_schema_json.len > 4 * 1024 * 1024 or self.indexes_json.len == 0 or self.indexes_json.len > 4 * 1024 * 1024 or
            !std.unicode.utf8ValidateSlice(self.table_name) or !std.unicode.utf8ValidateSlice(self.schema_json) or !std.unicode.utf8ValidateSlice(self.read_schema_json) or !std.unicode.utf8ValidateSlice(self.indexes_json) or
            self.byte_range.start.len > 1024 * 1024 or self.byte_range.end.len > 1024 * 1024 or (self.byte_range.end.len != 0 and std.mem.order(u8, self.byte_range.start, self.byte_range.end) != .lt)) return error.InvalidRestoreStagingCommand;
    }
    pub fn encode(self: @This(), alloc: Allocator) ![]u8 {
        try self.validate();
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const encoded = try alloc.alloc(u8, body.len + 36);
        @memcpy(encoded[0..4], "ARB1");
        @memcpy(encoded[4..][0..body.len], body);
        @memcpy(encoded[encoded.len - 32 ..], &digest(encoded[0 .. encoded.len - 32]));
        return encoded;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(@This()) {
        if (bytes.len < 36 or bytes.len > 64 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARB1") or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(@This(), alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};
pub const Digest = [32]u8;
pub const Phase = enum { reserved, importing, imported, validated, published, canceled };
pub const Timestamp = struct { key: []const u8, timestamp: u64 };
pub const ImportPage = struct { expected: Digest, next: []const u8, scope: Digest, timestamps: []const Timestamp };
pub const Control = union(enum) {
    begin: Scope,
    import_page: ImportPage,
    finish: struct { scope: Digest, phase: Phase },
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
pub const PreparedPage = struct {
    arena: std.heap.ArenaAllocator,
    phase: Phase,
    batch: ?@import("types.zig").BatchRequest,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Scope = struct {
    plan_id: [16]u8,
    plan_digest: Digest,
    source_artifact_digest: Digest,
    source_descriptor_digest: Digest = @splat(0),
    source_namespace: identity.Namespace,
    target_namespace: identity.Namespace,
    target_schema_digest: Digest,

    pub fn validateReservation(self: Scope) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            self.target_namespace.table_id == 0 or self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0)
            return error.InvalidRestoreStagingCommand;
    }

    pub fn validate(self: Scope) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.source_artifact_digest, 0) or self.target_namespace.table_id == 0 or
            self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0 or
            self.source_namespace.table_id == 0 or self.source_namespace.table_id == self.target_namespace.table_id)
            return error.InvalidRestoreStagingCommand;
    }
    pub fn digest(self: Scope) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-scope-v1");
        hash.update(&self.plan_id);
        hash.update(&self.plan_digest);
        hash.update(&self.source_artifact_digest);
        hash.update(&self.source_descriptor_digest);
        inline for (.{ self.source_namespace, self.target_namespace }) |namespace| {
            inline for (.{ namespace.table_id, namespace.shard_id, namespace.range_id }) |value| {
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, value, .little);
                hash.update(&bytes);
            }
        }
        hash.update(&self.target_schema_digest);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Progress = struct {
    scope: Scope,
    phase: Phase = .importing,
    rows: u64 = 0,
    cursor: []const u8 = "",
    logical_digest: Digest = @splat(0),
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.phase == .reserved or (self.phase == .canceled and self.scope.source_namespace.table_id == 0 and self.rows == 0 and self.cursor.len == 0)) try self.scope.validateReservation() else try self.scope.validate();
        if (self.cursor.len > 1024 * 1024) return error.InvalidRestoreStagingCommand;
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const out = try alloc.alloc(u8, body.len + 36);
        @memcpy(out[0..4], "ARS1");
        @memcpy(out[4..][0..body.len], body);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(Progress) {
        if (bytes.len < 36 or bytes.len > 8 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARS1") or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(Progress, alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        if (parsed.value.phase == .reserved or (parsed.value.phase == .canceled and parsed.value.scope.source_namespace.table_id == 0 and parsed.value.rows == 0 and parsed.value.cursor.len == 0)) {
            parsed.value.scope.validateReservation() catch return error.InvalidRestoreStagingRecord;
        } else parsed.value.scope.validate() catch return error.InvalidRestoreStagingRecord;
        if (parsed.value.cursor.len > 1024 * 1024) return error.InvalidRestoreStagingRecord;
        return parsed;
    }
    pub fn receipt(self: Progress) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-receipt-v1");
        hash.update(&self.scope.digest());
        hash.update(@tagName(self.phase));
        hash.update(&self.logical_digest);
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, self.rows, .little);
        hash.update(&count);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &out, .{});
    return out;
}
pub fn optional(txn: anytype) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn requireScope(alloc: Allocator, txn: anytype, expected: ?Digest, allow_importing: bool) !void {
    const raw = (try optional(txn)) orelse {
        if (expected != null) return error.RestoreStagingScopeChanged;
        return;
    };
    var progress = try Progress.decode(alloc, raw);
    defer progress.deinit();
    if (progress.value.phase == .published) {
        if (expected != null) return error.RestoreStagingScopeChanged;
        return;
    }
    if (progress.value.phase == .canceled) return error.RestoreStagingCanceled;
    if (progress.value.phase == .reserved) return error.RestoreStagingInProgress;
    if (!std.mem.eql(u8, &(expected orelse return error.RestoreStagingInProgress), &progress.value.scope.digest())) return error.RestoreStagingScopeChanged;
    if (!allow_importing and progress.value.phase == .importing) return error.RestoreStagingInProgress;
}

/// Once a validated receipt is durable, neither stale workers nor an already
/// captured private routing view may change its claims/checkpoint underneath it.
pub fn requireMutableScope(alloc: Allocator, txn: anytype, expected: ?Digest) !void {
    try requireScope(alloc, txn, expected, false);
    if (expected == null) return;
    var progress = try Progress.decode(alloc, (try optional(txn)) orelse return error.RestoreStagingScopeChanged);
    defer progress.deinit();
    if (progress.value.phase != .imported) return error.RestoreStagingScopeChanged;
}

/// Internal PreparedRow import admission, consumed under the DB apply fence.
pub const BatchAdmission = struct { expected: Digest, next: []const u8, scope: Digest };
pub fn validateImport(alloc: Allocator, txn: anytype, admission: BatchAdmission, row_count: usize) !void {
    const raw = (try optional(txn)) orelse return error.RestoreStagingScopeChanged;
    if (!std.mem.eql(u8, &digest(raw), &admission.expected)) return error.RestoreStagingProgressChanged;
    var before = try Progress.decode(alloc, raw);
    defer before.deinit();
    var after = Progress.decode(alloc, admission.next) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidRestoreStagingCommand;
    };
    defer after.deinit();
    if (before.value.phase != .importing or (after.value.phase != .importing and after.value.phase != .imported) or
        !std.mem.eql(u8, &before.value.scope.digest(), &admission.scope) or !std.mem.eql(u8, &after.value.scope.digest(), &admission.scope) or
        after.value.rows != std.math.add(u64, before.value.rows, row_count) catch return error.InvalidRestoreStagingCommand)
        return error.InvalidRestoreStagingCommand;
    if (after.value.phase == .importing and std.mem.order(u8, after.value.cursor, before.value.cursor) != .gt) return error.InvalidRestoreStagingCommand;
}

pub fn initialCoverage(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog) !?[]u8 {
    if (!activation.hasActive(catalog)) return null;
    var progress = try activation.status(txn, catalog);
    progress.state = .validating;
    progress.cursor = "";
    progress.rows_scanned = 0;
    progress.failure = "";
    progress.phase = for (catalog.bindings) |binding| {
        if (!binding.retired and binding.definition.kind == .unique) break .unique;
    } else .foreign_key;
    return try progress.encode(alloc);
}

test "restore staging owner scope and checksummed continuation exclude source identities" {
    const alloc = std.testing.allocator;
    const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) };
    const bytes = try (Progress{ .scope = scope }).encode(alloc);
    defer alloc.free(bytes);
    var decoded = try Progress.decode(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &scope.digest(), &decoded.value.scope.digest());
    bytes[5] ^= 1;
    try std.testing.expectError(error.InvalidRestoreStagingRecord, Progress.decode(alloc, bytes));
}

fn applyTestPage(alloc: Allocator, db: *@import("db.zig").DB, req: @import("types.zig").BatchRequest, index: u64, ha: bool) !void {
    if (!ha) return db.batchRaftReplicatedApply(req, .{ .term = 1, .index = index });
    const payload = try @import("../ha/effects.zig").encodeBatchMutationRequestAlloc(alloc, req);
    defer alloc.free(payload);
    try db.applyHAReplicationRecord(.{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = index, .previous_lsn = index - 1, .payload = payload });
    try std.testing.expectEqual(index, try db.haAppliedReplicationLsn());
}

test "relational integrity restore staging Raft controls retain HA append obligations and replay original import timestamps" {
    const db_mod = @import("db.zig");
    const primary_mod = @import("../ha/primary.zig");
    const effects = @import("../ha/effects.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-ha", .{tmp.sub_path});
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.setSchemaJson(alloc, "{}");
        try source.batch(.{ .timestamp_ns = 1234, .writes = &.{.{ .key = "row", .value = "{\"id\":1}" }} });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    defer source.close();
    for ([_]bool{ false, true }, 0..) |synchronous, trial| {
        const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/primary-{d}", .{ tmp.sub_path, trial });
        const replica_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/standby-{d}", .{ tmp.sub_path, trial });
        const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/log-{d}", .{ tmp.sub_path, trial }, 0);
        const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/slots-{d}", .{ tmp.sub_path, trial }, 0);
        var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 10, .shard_id = 11 }, .{});
        defer primary.close();
        try primary.createSlot("standby", 0);
        const Ack = struct {
            calls: usize = 0,
            fn wait(ptr: *anyopaque, stream: *primary_mod.Primary, lsn: u64, _: primary_mod.SyncPolicy) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                if (self.calls == 1) return error.InjectedRestoreMirrorWaitFailure;
                try stream.standbyStatusUpdate("standby", 1, lsn, lsn);
            }
        };
        var ack: Ack = .{};
        const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var target = try db_mod.DB.open(alloc, path, target_options);
        defer target.close();
        var replica = try db_mod.DB.open(alloc, replica_path, target_options);
        defer replica.close();
        try target.setSchemaJson(alloc, "{}");
        try replica.setSchemaJson(alloc, "{}");
        const schema = try @import("../schema.zig").serializeSchema(owned, target.core.schema orelse .{});
        const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = digest(schema) };
        try target.reserveRestoreStagingScoped(alloc, scope);
        try replica.reserveRestoreStagingScoped(alloc, scope);
        const bootstrap: OwnerBootstrap = .{ .scope = scope, .table_name = "docs", .schema_json = "{}", .indexes_json = "{}", .byte_range = .{ .start = "", .end = "" } };
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        var changed_bootstrap = bootstrap;
        changed_bootstrap.table_name = "another-table";
        try std.testing.expectError(error.RestoreStagingScopeChanged, target.installRestoreStagingBootstrap(alloc, changed_bootstrap));
        {
            var stored_bootstrap = (try target.readRestoreStagingBootstrap(alloc)) orelse return error.TestUnexpectedResult;
            defer stored_bootstrap.deinit();
            try std.testing.expectEqualStrings("docs", stored_bootstrap.value.table_name);
        }
        target.ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{ .mode = if (synchronous) .remote_write else .async, .standby_names = &.{"standby"}, .failure_policy = .block },
            .sync_wait_ctx = &ack,
            .sync_wait_fn = Ack.wait,
        };
        const begin: types.BatchRequest = .{ .restore_staging = .{ .begin = scope } };
        if (synchronous) {
            try std.testing.expectError(error.InjectedRestoreMirrorWaitFailure, target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 }));
            try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        }
        try target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 });
        try target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 });
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        var raft_index: u64 = 2;
        while (true) : (raft_index += 1) {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
            defer page.deinit();
            if (page.batch) |batch| {
                try target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = raft_index });
                try target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = raft_index });
            }
            if (page.phase == .imported) break;
        }
        for ([_]Phase{ .validated, .published }) |phase| {
            raft_index += 1;
            try target.batchRaftReplicatedApply(.{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = phase } } }, .{ .term = 1, .index = raft_index });
        }
        var saw_import = false;
        for (1..primary.lastLsn() + 1) |lsn| {
            var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestUnexpectedResult;
            defer entry.deinit(alloc);
            var decoded = try effects.decodeBatchMutationRequest(alloc, entry.record);
            defer decoded.deinit();
            if (decoded.value.request.restore_staging.? == .begin) {
                const restored_bootstrap = decoded.value.restore_staging_bootstrap orelse return error.TestUnexpectedResult;
                try std.testing.expectEqualStrings("docs", restored_bootstrap.table_name);
                try std.testing.expectEqualSlices(u8, &scope.digest(), &restored_bootstrap.scope.digest());
                try replica.installRestoreStagingBootstrap(alloc, restored_bootstrap);
            } else try std.testing.expect(decoded.value.restore_staging_bootstrap == null);
            if (decoded.value.request.restore_staging.? == .import_page and decoded.value.request.writes.len != 0) {
                saw_import = true;
                try std.testing.expectEqual(@as(u64, 1234), decoded.value.request.restore_staging.?.import_page.timestamps[0].timestamp);
            }
            try replica.applyHAReplicationRecord(entry.record);
        }
        try std.testing.expect(saw_import);
        var found = (try replica.lookup(alloc, "row", .{})) orelse return error.TestUnexpectedResult;
        defer found.deinit(alloc);
        try std.testing.expectEqualStrings("{\"id\":1}", found.json);
    }
}

test "relational integrity restore staging imports typed and document rows with restart and exact timestamps" {
    const db_mod = @import("db.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{
        "{}",
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"additionalProperties":false}}}}
    }, 0..) |schema_json, index| {
        const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source-{d}", .{ tmp.sub_path, index });
        defer alloc.free(source_path);
        const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target-{d}", .{ tmp.sub_path, index });
        defer alloc.free(target_path);
        var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .start_index_workers = false, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 20, .shard_id = 21, .range_id = 21 }, .start_index_workers = false, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        {
            var source = try db_mod.DB.open(alloc, source_path, source_options);
            defer source.close();
            try source.setSchemaJson(alloc, schema_json);
            try source.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"name\":\"alpha\"}" }}, .timestamp_ns = 123 });
            try source.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"id\":2,\"name\":\"beta\"}" }}, .timestamp_ns = 456 });
        }
        source_options.open_mode = .query_readonly;
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        var scope: Scope = undefined;
        var raft_index: u64 = 0;
        {
            var target = try db_mod.DB.open(alloc, target_path, target_options);
            defer target.close();
            try target.setSchemaJson(alloc, schema_json);
            const serialized = try @import("../schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
            defer alloc.free(serialized);
            scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = digest(serialized) };
            try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, scope.target_namespace);
            try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, scope.target_namespace);
            try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{ .restore_staging_scope = scope.digest() }));
            try std.testing.expectError(error.RestoreStagingInProgress, target.beginTransaction(1));
            var wrong_plan = scope;
            wrong_plan.plan_id[0] ^= 1;
            try std.testing.expectError(error.RestoreStagingScopeChanged, target.beginRestoreStaging(alloc, wrong_plan));
            raft_index += 1;
            try applyTestPage(alloc, &target, .{ .restore_staging = .{ .begin = scope } }, raft_index, index == 1);
            try target.beginRestoreStaging(alloc, scope);
            try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
            try std.testing.expectError(error.RestoreStagingInProgress, target.batch(.{ .writes = &.{.{ .key = "evil", .value = "{}" }} }));
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 1, .none);
            defer page.deinit();
            var corrupt = page.batch.?;
            const corrupted_progress = try alloc.dupe(u8, corrupt.restore_staging.?.import_page.next);
            defer alloc.free(corrupted_progress);
            corrupted_progress[4] ^= 1;
            corrupt.restore_staging.?.import_page.next = corrupted_progress;
            try std.testing.expectError(error.InvalidRestoreStagingCommand, target.batchReplicatedApply(corrupt));
            var unchanged = (try target.restoreStagingStatus(alloc)).?;
            defer unchanged.deinit();
            try std.testing.expectEqual(@as(u64, 0), unchanged.value.rows);
            raft_index += 1;
            try applyTestPage(alloc, &target, page.batch.?, raft_index, index == 1);
            try applyTestPage(alloc, &target, page.batch.?, raft_index, index == 1);
        }
        var target = try db_mod.DB.open(alloc, target_path, target_options);
        defer target.close();
        try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
        for (0..20) |_| {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 1, .none);
            defer page.deinit();
            if (page.batch) |req| {
                raft_index += 1;
                try applyTestPage(alloc, &target, req, raft_index, index == 1);
                try applyTestPage(alloc, &target, req, raft_index, index == 1);
            }
            if (page.phase == .imported) break;
        } else return error.TestUnexpectedResult;
        var status = (try target.restoreStagingStatus(alloc)).?;
        defer status.deinit();
        try std.testing.expectEqual(@as(u64, 2), status.value.rows);
        try std.testing.expectEqual(@as(u64, 123), try target.getTimestamp(alloc, "a"));
        try std.testing.expectEqual(@as(u64, 456), try target.getTimestamp(alloc, "b"));
        raft_index += 1;
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .validated } } }, raft_index, index == 1);
        const receipt = try target.finishRestoreStaging(alloc, scope.digest(), .validated);
        try std.testing.expectEqualSlices(u8, &receipt, &try target.finishRestoreStaging(alloc, scope.digest(), .validated));
        raft_index += 1;
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } }, raft_index, index == 1);
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } }, raft_index, index == 1);
        var row = (try target.lookup(alloc, "b", .{ .include_all_fields = true })).?;
        defer row.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, row.json, "beta") != null);
        try std.testing.expectError(error.RestoreStagingScopeChanged, target.lookup(alloc, "b", .{ .restore_staging_scope = scope.digest() }));
        _ = types;
    }
}
