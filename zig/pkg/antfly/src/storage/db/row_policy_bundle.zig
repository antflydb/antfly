// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable owner-local copy of one metadata-authenticated policy publication.
//! The bytes are installed only by a trusted metadata read-index callback and
//! committed with the table catalog through the data Raft state machine.
const std = @import("std");
const policies = @import("../../system_catalog/policies.zig");
const settings = @import("../../system_catalog/settings.zig");
const setting_catalog = @import("../../sql/setting_catalog.zig");
const sql_catalog = @import("../../sql/catalog.zig");
const schema_mod = @import("../schema.zig");
const gate = @import("row_policy_gate.zig");
const role_authority = @import("../../usermgr/row_policy_authority.zig");

pub const key = "\x00\x00__metadata__:row_policy_bundle";
/// An immutable next generation installed while the previous active bundle
/// continues serving. The serving barrier verifies these exact bytes before
/// replacing the durable primary bundle.
pub const candidate_key = "\x00\x00__metadata__:row_policy_candidate";
/// A committed publication whose owner-local read/schema leases have not yet
/// drained. Its exact receipt identity is durable, but it is not an ACK until
/// finalization writes the phase-specific receipt key.
pub const pending_key = "\x00\x00__metadata__:row_policy_pending";
pub const max_bytes: usize = policies.max_install_snapshot_bytes;
pub const receipt_encoded_len: usize = 112;

/// Exact owner-local durable evidence returned only after the bundle,
/// catalog phase and Raft applied marker have committed in one transaction.
pub const Receipt = struct {
    table_id: u64,
    generation: u64,
    catalog_epoch: u64,
    phase: policies.Publication.Phase,
    applied_term: u64,
    applied_index: u64,
    bundle_digest: [32]u8,
    descriptor_digest: [32]u8,

    pub fn key(self: @This(), buffer: *[128]u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:row_policy_receipt:{d}:{s}", .{ self.generation, @tagName(self.phase) });
    }

    pub fn encode(self: @This()) [receipt_encoded_len]u8 {
        var out: [receipt_encoded_len]u8 = @splat(0);
        @memcpy(out[0..4], "ARPR");
        std.mem.writeInt(u64, out[4..12], self.table_id, .little);
        std.mem.writeInt(u64, out[12..20], self.generation, .little);
        std.mem.writeInt(u64, out[20..28], self.catalog_epoch, .little);
        out[28] = @intFromEnum(self.phase);
        std.mem.writeInt(u64, out[32..40], self.applied_term, .little);
        std.mem.writeInt(u64, out[40..48], self.applied_index, .little);
        @memcpy(out[48..80], &self.bundle_digest);
        @memcpy(out[80..112], &self.descriptor_digest);
        return out;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != receipt_encoded_len or !std.mem.eql(u8, bytes[0..4], "ARPR") or
            !std.mem.allEqual(u8, bytes[29..32], 0)) return error.InvalidRowPolicyReceipt;
        const phase = std.enums.fromInt(policies.Publication.Phase, bytes[28]) orelse return error.InvalidRowPolicyReceipt;
        const result: @This() = .{
            .table_id = std.mem.readInt(u64, bytes[4..12], .little),
            .generation = std.mem.readInt(u64, bytes[12..20], .little),
            .catalog_epoch = std.mem.readInt(u64, bytes[20..28], .little),
            .phase = phase,
            .applied_term = std.mem.readInt(u64, bytes[32..40], .little),
            .applied_index = std.mem.readInt(u64, bytes[40..48], .little),
            .bundle_digest = bytes[48..80].*,
            .descriptor_digest = bytes[80..112].*,
        };
        if (result.table_id == 0 or result.generation == 0 or result.catalog_epoch == 0 or
            result.applied_term == 0 or result.applied_index == 0)
            return error.InvalidRowPolicyReceipt;
        return result;
    }
};

/// One immutable, cursor-owned evaluation state. The owner admission lease
/// outlives it, keeping the source bundle alive while these predicate pointers
/// are used. Row scratch is retained and reset rather than allocated per row.
pub const ReadEvaluation = struct {
    alloc: std.mem.Allocator,
    setting_view: setting_catalog.View,
    policy_view: policies.View,
    evaluator: policies.Evaluator,
    scratch: gate.EvaluationScratch,

    pub fn deinit(self: *@This()) void {
        self.scratch.deinit();
        self.evaluator.deinit();
        self.policy_view.deinit();
        self.setting_view.deinit();
        self.alloc.destroy(self);
    }

    pub fn permits(self: *@This(), row: @import("algebraic/relational_row_codec.zig").OrdinalRowView) !bool {
        return self.scratch.permits(row);
    }
};

pub const Installed = struct {
    alloc: std.mem.Allocator,
    parsed: std.json.Parsed(policies.InstallSnapshot),

    pub fn init(alloc: std.mem.Allocator, bytes: []const u8, table_id: u64, schema: schema_mod.TableSchema) !Installed {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidRowPolicyBundle;
        var parsed = std.json.parseFromSlice(policies.InstallSnapshot, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch return error.InvalidRowPolicyBundle;
        errdefer parsed.deinit();
        const bundle = parsed.value;
        try bundle.validateShape();
        if (schema.storage_mode != .relational or bundle.table_id != table_id or
            bundle.schema_version != schema.version) return error.RowPolicyCatalogChanged;
        const encoded = try schema_mod.serializeSchema(alloc, schema);
        defer alloc.free(encoded);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(encoded, &digest, .{});
        if (!std.mem.eql(u8, &digest, &bundle.schema_digest)) return error.RowPolicyCatalogChanged;

        // The SQL binder supports exactly these logical column kinds. Reject
        // an unsupported schema at install, not halfway through a scan.
        const columns = try alloc.alloc(sql_catalog.Column, schema.relational_columns.len);
        defer alloc.free(columns);
        for (schema.relational_columns, columns) |column, *out| out.* = .{
            .name = column.name,
            .path = column.path,
            .nullable = !column.required or column.allows_null,
            .type = switch (column.column_type) {
                .string => .string,
                .integer => .integer,
                .number => .number,
                .boolean => .boolean,
                .datetime => .datetime,
                .json => .json,
                else => return error.UnsupportedSqlExecution,
            },
        };
        const table: sql_catalog.Table = .{ .id = table_id, .physical_name = "", .schema_version = schema.version, .columns = columns };
        const SettingOwner = struct {
            fn load(ptr: *anyopaque, arena: std.mem.Allocator, scope: settings.Scope) !settings.Snapshot {
                const installed: *Installed = @ptrCast(@alignCast(ptr));
                const definitions = try arena.alloc(settings.Definition, installed.parsed.value.settings.len);
                for (installed.parsed.value.settings, definitions) |record, *definition| definition.* = record.effective(scope.principal, scope.database);
                return .{ .scope = scope, .epoch = installed.parsed.value.catalog_epoch, .definitions = definitions };
            }
        };
        var result: Installed = .{ .alloc = alloc, .parsed = parsed };
        // Validate typed DAGs and every referenced setting identity using an
        // arbitrary scope: definition IDs/generations and policy-sensitive
        // bits are invariant across principal-specific defaults.
        var setting_view = try setting_catalog.View.capture(alloc, .{ .ptr = &result, .load = SettingOwner.load }, .{ .principal = "__install__", .database = "__install__" }, &.{});
        defer setting_view.deinit();
        for (result.parsed.value.records) |record| try record.validate(table, digest, &setting_view);
        return result;
    }

    pub fn deinit(self: *Installed) void {
        self.parsed.deinit();
        self.* = undefined;
    }

    pub fn matchesCatalog(self: *const Installed, catalog: @import("table_catalog.zig").Catalog) bool {
        const value = self.parsed.value;
        const phase_matches = switch (catalog.row_policy_phase) {
            .disabled => value.phase == .disabled,
            .preparing => value.phase == .pending_install or value.phase == .pending_disable or value.phase == .serving_disable,
            .active => value.phase == .serving_install or value.phase == .active,
        };
        return value.policy_generation == catalog.row_policy_generation and
            value.catalog_epoch == catalog.row_policy_catalog_epoch and
            value.schema_version == catalog.active_schema_version and
            phase_matches;
    }

    pub fn captureEvaluation(self: *const Installed, alloc: std.mem.Allocator, schema: schema_mod.TableSchema, principal: *const role_authority.Payload, action: policies.Evaluator.Action) !*ReadEvaluation {
        const bundle = self.parsed.value;
        if ((bundle.phase != .serving_install and bundle.phase != .active) or bundle.table_id != principal.table_id or
            bundle.policy_generation != principal.policy_generation or bundle.catalog_epoch != principal.catalog_epoch or
            schema.version != bundle.schema_version) return error.RowPolicyCatalogChanged;
        const result = try alloc.create(ReadEvaluation);
        errdefer alloc.destroy(result);
        result.alloc = alloc;
        const SettingOwner = struct {
            fn load(ptr: *anyopaque, arena: std.mem.Allocator, scope: settings.Scope) !settings.Snapshot {
                const installed: *const Installed = @ptrCast(@alignCast(ptr));
                const definitions = try arena.alloc(settings.Definition, installed.parsed.value.settings.len);
                for (installed.parsed.value.settings, definitions) |record, *definition| definition.* = record.effective(scope.principal, scope.database);
                return .{ .scope = scope, .epoch = installed.parsed.value.catalog_epoch, .definitions = definitions };
            }
        };
        result.setting_view = try setting_catalog.View.capture(alloc, .{ .ptr = @constCast(self), .load = SettingOwner.load }, .{ .principal = principal.principal, .database = principal.database }, &.{});
        errdefer result.setting_view.deinit();
        const columns = try alloc.alloc(sql_catalog.Column, schema.relational_columns.len);
        defer alloc.free(columns);
        for (schema.relational_columns, columns) |column, *out| out.* = .{
            .name = column.name,
            .path = column.path,
            .nullable = !column.required or column.allows_null,
            .type = switch (column.column_type) {
                .string => .string,
                .integer => .integer,
                .number => .number,
                .boolean => .boolean,
                .datetime => .datetime,
                .json => .json,
                else => return error.UnsupportedSqlExecution,
            },
        };
        const PolicyOwner = struct {
            fn load(ptr: *anyopaque, _: std.mem.Allocator, table_id: u64, principal_name: []const u8, database: []const u8) !policies.Snapshot {
                const installed: *const Installed = @ptrCast(@alignCast(ptr));
                const value = installed.parsed.value;
                if (table_id != value.table_id) return error.RowPolicyCatalogChanged;
                return .{
                    .table_id = table_id,
                    .schema_version = value.schema_version,
                    .schema_digest = value.schema_digest,
                    .policy_generation = value.policy_generation,
                    .catalog_epoch = value.catalog_epoch,
                    .principal = principal_name,
                    .database = database,
                    .records = value.records,
                };
            }
        };
        const table: sql_catalog.Table = .{ .id = bundle.table_id, .physical_name = "", .schema_version = schema.version, .columns = columns };
        result.policy_view = try policies.View.capture(alloc, .{ .ptr = @constCast(self), .load = PolicyOwner.load }, table, bundle.schema_digest, &result.setting_view);
        errdefer result.policy_view.deinit();
        result.evaluator = try policies.Evaluator.initForAuthenticatedRoles(alloc, &result.policy_view, &result.setting_view, action, principal.roles);
        errdefer result.evaluator.deinit();
        result.scratch = try gate.EvaluationScratch.init(alloc, &result.evaluator);
        return result;
    }

    pub fn captureReadEvaluation(self: *const Installed, alloc: std.mem.Allocator, schema: schema_mod.TableSchema, principal: *const role_authority.Payload) !*ReadEvaluation {
        if (principal.access != .read) return error.RowPolicyAuthenticationRequired;
        return self.captureEvaluation(alloc, schema, principal, .select);
    }
};

pub fn load(alloc: std.mem.Allocator, store: anytype, table_id: u64, schema: schema_mod.TableSchema) !?Installed {
    const bytes = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(bytes);
    return try Installed.init(alloc, bytes, table_id, schema);
}

test "row-policy bundle validates exact schema identity and durable catalog phase" {
    const alloc = std.testing.allocator;
    const schema: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational };
    const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
    defer alloc.free(schema_bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema_bytes, &digest, .{});
    const bytes = try std.json.Stringify.valueAlloc(alloc, policies.InstallSnapshot{
        .table_id = 7,
        .schema_version = 2,
        .schema_digest = digest,
        .policy_generation = 3,
        .catalog_epoch = 5,
        .phase = .disabled,
        .records = &.{},
        .settings = &.{},
    }, .{});
    defer alloc.free(bytes);
    var installed = try Installed.init(alloc, bytes, 7, schema);
    defer installed.deinit();
    try std.testing.expect(installed.matchesCatalog(.{
        .storage_mode = .relational,
        .active_schema_version = 2,
        .row_policy_phase = .disabled,
        .row_policy_generation = 3,
        .row_policy_catalog_epoch = 5,
    }));
    try std.testing.expectError(error.RowPolicyCatalogChanged, Installed.init(alloc, bytes, 8, schema));
    try std.testing.expectError(error.RowPolicyCatalogChanged, Installed.init(alloc, bytes, 7, .{ .version = 3, .storage_mode = .relational }));
    try std.testing.expectError(error.InvalidRowPolicyBundle, Installed.init(alloc, "{}", 7, schema));
}

test "row-policy receipt preserves exact Raft and bundle identity" {
    const receipt: Receipt = .{
        .table_id = 7,
        .generation = 11,
        .catalog_epoch = 13,
        .phase = .pending_install,
        .applied_term = 17,
        .applied_index = 19,
        .bundle_digest = @splat(0xab),
        .descriptor_digest = @splat(0xcd),
    };
    const encoded = receipt.encode();
    try std.testing.expectEqualDeep(receipt, try Receipt.decode(&encoded));
    var key_buf: [128]u8 = undefined;
    try std.testing.expect(std.mem.indexOf(u8, try receipt.key(&key_buf), "11:pending_install") != null);
    var corrupt = encoded;
    corrupt[29] = 1;
    try std.testing.expectError(error.InvalidRowPolicyReceipt, Receipt.decode(&corrupt));
}

test "installed row policy applies USING to old images and WITH CHECK to new images" {
    const alloc = std.testing.allocator;
    const schema: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational };
    const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
    defer alloc.free(schema_bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema_bytes, &digest, .{});
    const allow: policies.Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
    }, .root = 0 };
    const deny: policies.Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = false } } },
    }, .root = 0 };
    const record: policies.Record = .{
        .id = 1,
        .generation = 1,
        .table_id = 7,
        .schema_version = 2,
        .schema_digest = digest,
        .name = "old_visible_new_denied",
        .commands = .{ .select = true, .insert = true, .update = true, .delete = true },
        .roles = &.{"PUBLIC"},
        .using = allow,
        .with_check = deny,
    };
    const bytes = try std.json.Stringify.valueAlloc(alloc, policies.InstallSnapshot{
        .table_id = 7,
        .schema_version = 2,
        .schema_digest = digest,
        .policy_generation = 3,
        .catalog_epoch = 5,
        .phase = .active,
        .records = &.{record},
        .settings = &.{},
    }, .{});
    defer alloc.free(bytes);
    var installed = try Installed.init(alloc, bytes, 7, schema);
    defer installed.deinit();
    const principal: role_authority.Payload = .{
        .principal = "alice",
        .roles = &.{},
        .auth_revision = 1,
        .table_id = 7,
        .table = "table:7",
        .database = "main",
        .policy_generation = 3,
        .catalog_epoch = 5,
        .access = .write,
        .expires = 100,
    };
    for ([_]struct { action: policies.Evaluator.Action, permitted: bool }{
        .{ .action = .select, .permitted = true },
        .{ .action = .insert, .permitted = false },
        .{ .action = .update_old, .permitted = true },
        .{ .action = .update_new, .permitted = false },
        .{ .action = .delete, .permitted = true },
    }) |case| {
        var evaluation = try installed.captureEvaluation(alloc, schema, &principal, case.action);
        defer evaluation.deinit();
        try std.testing.expectEqual(case.permitted, try evaluation.evaluator.permits(alloc, &.{}));
    }
}
