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

pub const AdapterNoopDdlReason = enum {
    schema_namespace,
    extension,
    transaction_control,
    session_setting,
};

pub const AdapterNoopDdlPlan = struct {
    reason: AdapterNoopDdlReason,
};

pub const EnumTypeCatalogPlan = union(enum) {
    create: CreateEnumTypePlan,
    add_value: AddEnumValuePlan,
    drop: DropEnumTypePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .add_value => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateEnumTypePlan = struct {
    type_name: []const u8,
    values: []const []const u8 = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        freeStringSlice(alloc, self.values);
        self.* = undefined;
    }
};

pub const AddEnumValuePlan = struct {
    type_name: []const u8,
    value: []const u8,
    if_not_exists: bool = false,
    position: EnumValuePosition = .none,
    neighbor_value: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        alloc.free(self.value);
        if (self.neighbor_value) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const EnumValuePosition = enum {
    none,
    before,
    after,
};

pub const DropEnumTypePlan = struct {
    type_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.type_name);
        self.* = undefined;
    }
};

pub const SchemaNamespaceCatalogPlan = union(enum) {
    create: CreateSchemaNamespacePlan,
    rename: RenameSchemaNamespacePlan,
    drop: DropSchemaNamespacePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .rename => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateSchemaNamespacePlan = struct {
    schema_name: []const u8,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        self.* = undefined;
    }
};

pub const RenameSchemaNamespacePlan = struct {
    schema_name: []const u8,
    new_schema_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        alloc.free(self.new_schema_name);
        self.* = undefined;
    }
};

pub const DropSchemaNamespacePlan = struct {
    schema_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_name);
        self.* = undefined;
    }
};

pub const ExtensionCatalogPlan = union(enum) {
    create: CreateExtensionPlan,
    update: UpdateExtensionPlan,
    drop: DropExtensionPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .update => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateExtensionPlan = struct {
    extension_name: []const u8,
    version: ?[]const u8 = null,
    if_not_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        if (self.version) |version| alloc.free(version);
        self.* = undefined;
    }
};

pub const UpdateExtensionPlan = struct {
    extension_name: []const u8,
    target_version: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        if (self.target_version) |version| alloc.free(version);
        self.* = undefined;
    }
};

pub const DropExtensionPlan = struct {
    extension_name: []const u8,
    if_exists: bool = false,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.extension_name);
        self.* = undefined;
    }
};

pub const FunctionCatalogPlan = union(enum) {
    create: CreateRoutinePlan,
    drop: DropRoutinePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*plan| plan.deinit(alloc),
            .drop => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const RoutineKind = enum {
    function,
    procedure,
};

pub const CreateRoutinePlan = struct {
    kind: RoutineKind,
    routine_name: []const u8,
    replace_existing: bool = false,
    argument_count: usize = 0,
    returns_type: ?[]const u8 = null,
    language: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.routine_name);
        if (self.returns_type) |returns_type| alloc.free(returns_type);
        if (self.language) |language| alloc.free(language);
        self.* = undefined;
    }
};

pub const DropRoutinePlan = struct {
    kind: RoutineKind,
    routine_name: []const u8,
    if_exists: bool = false,
    argument_count: usize = 0,
    cascade: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.routine_name);
        self.* = undefined;
    }
};

pub const AuthorizationCatalogPlan = union(enum) {
    create_role: CreateRolePlan,
    alter_role: AlterRolePlan,
    drop_role: DropRolePlan,
    grant_privilege: PrivilegeChangePlan,
    revoke_privilege: PrivilegeChangePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_role => |*plan| plan.deinit(alloc),
            .alter_role => |*plan| plan.deinit(alloc),
            .drop_role => |*plan| plan.deinit(alloc),
            .grant_privilege => |*plan| plan.deinit(alloc),
            .revoke_privilege => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateRolePlan = struct {
    role_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        self.* = undefined;
    }
};

pub const AlterRolePlan = struct {
    role_name: []const u8,
    setting_name: []const u8,
    setting_value: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        alloc.free(self.setting_name);
        alloc.free(self.setting_value);
        self.* = undefined;
    }
};

pub const DropRolePlan = struct {
    role_name: []const u8,
    if_exists: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.role_name);
        self.* = undefined;
    }
};

pub const PrivilegeChangePlan = struct {
    privileges: []const []const u8 = &.{},
    object_kind: []const u8,
    object_name: []const u8,
    principal_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.privileges);
        alloc.free(self.object_kind);
        alloc.free(self.object_name);
        alloc.free(self.principal_name);
        self.* = undefined;
    }
};

pub const PreparedStatementPlan = union(enum) {
    prepare: PrepareStatementPlan,
    execute: ExecutePreparedStatementPlan,
    deallocate: DeallocatePreparedStatementPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .prepare => |*plan| plan.deinit(alloc),
            .execute => |*plan| plan.deinit(alloc),
            .deallocate => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const PrepareStatementPlan = struct {
    statement_name: []const u8,
    parameter_count: usize = 0,
    statement_kind: PreparedStatementSubjectKind,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.statement_name);
        self.* = undefined;
    }
};

pub const PreparedStatementSubjectKind = enum {
    read,
    write,
    ddl,
};

pub const ExecutePreparedStatementPlan = struct {
    statement_name: []const u8,
    argument_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.statement_name);
        self.* = undefined;
    }
};

pub const DeallocatePreparedStatementPlan = struct {
    statement_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.statement_name) |name| alloc.free(name);
        self.* = undefined;
    }
};

pub const CursorPortalPlan = union(enum) {
    declare: DeclareCursorPortalPlan,
    fetch: FetchCursorPortalPlan,
    close: CloseCursorPortalPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .declare => |*plan| plan.deinit(alloc),
            .fetch => |*plan| plan.deinit(alloc),
            .close => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const DeclareCursorPortalPlan = struct {
    portal_name: []const u8,
    scroll: CursorScrollMode = .default,
    binary: bool = false,
    hold: bool = false,
    statement_kind: PreparedStatementSubjectKind,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.portal_name);
        self.* = undefined;
    }
};

pub const CursorScrollMode = enum {
    default,
    scroll,
    no_scroll,
};

pub const FetchCursorPortalPlan = struct {
    portal_name: []const u8,
    direction: CursorFetchDirection = .next,
    count: ?i64 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.portal_name);
        self.* = undefined;
    }
};

pub const CursorFetchDirection = enum {
    next,
    prior,
    first,
    last,
    absolute,
    relative,
    forward,
    backward,
    all,
};

pub const CloseCursorPortalPlan = struct {
    portal_name: ?[]const u8 = null,
    all: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.portal_name) |portal| alloc.free(portal);
        self.* = undefined;
    }
};

pub const SavepointTransactionPlan = union(enum) {
    savepoint: SavepointNamePlan,
    release: SavepointNamePlan,
    rollback_to: SavepointNamePlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .savepoint => |*plan| plan.deinit(alloc),
            .release => |*plan| plan.deinit(alloc),
            .rollback_to => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const SavepointNamePlan = struct {
    savepoint_name: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.savepoint_name);
        self.* = undefined;
    }
};

pub const CommentMetadataPlan = struct {
    target: CommentMetadataTarget,
    object_name: []const u8,
    parent_table_name: ?[]const u8 = null,
    comment_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.object_name);
        if (self.parent_table_name) |parent| alloc.free(parent);
        if (self.comment_json) |comment| alloc.free(comment);
        self.* = undefined;
    }
};

pub const CommentMetadataTarget = enum {
    table,
    column,
    index,
    constraint,
};

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

test "SQL adapter DDL enum plans own nested values" {
    const alloc = std.testing.allocator;

    var values = try alloc.alloc([]const u8, 2);
    values[0] = try alloc.dupe(u8, "queued");
    values[1] = try alloc.dupe(u8, "running");

    var create: EnumTypeCatalogPlan = .{ .create = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .values = values,
    } };
    create.deinit(alloc);

    var add: EnumTypeCatalogPlan = .{ .add_value = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .value = try alloc.dupe(u8, "done"),
        .position = .after,
        .neighbor_value = try alloc.dupe(u8, "running"),
    } };
    add.deinit(alloc);

    var drop: EnumTypeCatalogPlan = .{ .drop = .{
        .type_name = try alloc.dupe(u8, "job_state"),
        .if_exists = true,
        .cascade = true,
    } };
    drop.deinit(alloc);
}

test "SQL adapter DDL namespace and extension plans own strings" {
    const alloc = std.testing.allocator;

    var rename_schema: SchemaNamespaceCatalogPlan = .{ .rename = .{
        .schema_name = try alloc.dupe(u8, "public"),
        .new_schema_name = try alloc.dupe(u8, "app"),
    } };
    rename_schema.deinit(alloc);

    var create_extension: ExtensionCatalogPlan = .{ .create = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .version = try alloc.dupe(u8, "1.3"),
        .if_not_exists = true,
    } };
    create_extension.deinit(alloc);

    var update_extension: ExtensionCatalogPlan = .{ .update = .{
        .extension_name = try alloc.dupe(u8, "pgcrypto"),
        .target_version = try alloc.dupe(u8, "1.4"),
    } };
    update_extension.deinit(alloc);
}

test "SQL adapter DDL function and authorization plans own strings" {
    const alloc = std.testing.allocator;

    var create_routine: FunctionCatalogPlan = .{ .create = .{
        .kind = .function,
        .routine_name = try alloc.dupe(u8, "normalize_status"),
        .returns_type = try alloc.dupe(u8, "text"),
        .language = try alloc.dupe(u8, "sql"),
    } };
    create_routine.deinit(alloc);

    var alter_role: AuthorizationCatalogPlan = .{ .alter_role = .{
        .role_name = try alloc.dupe(u8, "app_user"),
        .setting_name = try alloc.dupe(u8, "app.tenant_id"),
        .setting_value = try alloc.dupe(u8, "tenant-a"),
    } };
    alter_role.deinit(alloc);

    var privileges = try alloc.alloc([]const u8, 2);
    privileges[0] = try alloc.dupe(u8, "select");
    privileges[1] = try alloc.dupe(u8, "insert");
    var grant: AuthorizationCatalogPlan = .{ .grant_privilege = .{
        .privileges = privileges,
        .object_kind = try alloc.dupe(u8, "table"),
        .object_name = try alloc.dupe(u8, "docs"),
        .principal_name = try alloc.dupe(u8, "app_user"),
    } };
    grant.deinit(alloc);
}

test "SQL adapter DDL prepared statement and cursor plans own strings" {
    const alloc = std.testing.allocator;

    var prepare: PreparedStatementPlan = .{ .prepare = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
        .parameter_count = 2,
        .statement_kind = .write,
    } };
    prepare.deinit(alloc);

    var execute: PreparedStatementPlan = .{ .execute = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
        .argument_count = 2,
    } };
    execute.deinit(alloc);

    var deallocate: PreparedStatementPlan = .{ .deallocate = .{
        .statement_name = try alloc.dupe(u8, "usage_plan"),
    } };
    deallocate.deinit(alloc);

    var declare: CursorPortalPlan = .{ .declare = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
        .scroll = .scroll,
        .binary = true,
        .hold = true,
        .statement_kind = .read,
    } };
    declare.deinit(alloc);

    var fetch: CursorPortalPlan = .{ .fetch = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
        .direction = .forward,
        .count = 10,
    } };
    fetch.deinit(alloc);

    var close: CursorPortalPlan = .{ .close = .{
        .portal_name = try alloc.dupe(u8, "usage_cursor"),
    } };
    close.deinit(alloc);
}

test "SQL adapter DDL savepoint and comment plans own strings" {
    const alloc = std.testing.allocator;

    var savepoint: SavepointTransactionPlan = .{ .savepoint = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    savepoint.deinit(alloc);

    var release: SavepointTransactionPlan = .{ .release = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    release.deinit(alloc);

    var rollback: SavepointTransactionPlan = .{ .rollback_to = .{
        .savepoint_name = try alloc.dupe(u8, "before_retry"),
    } };
    rollback.deinit(alloc);

    var column_comment = CommentMetadataPlan{
        .target = .column,
        .object_name = try alloc.dupe(u8, "status"),
        .parent_table_name = try alloc.dupe(u8, "usage_records"),
        .comment_json = try alloc.dupe(u8, "\"Current processing state\""),
    };
    column_comment.deinit(alloc);

    var table_comment = CommentMetadataPlan{
        .target = .table,
        .object_name = try alloc.dupe(u8, "usage_records"),
        .comment_json = try alloc.dupe(u8, "\"Usage event rows\""),
    };
    table_comment.deinit(alloc);
}
