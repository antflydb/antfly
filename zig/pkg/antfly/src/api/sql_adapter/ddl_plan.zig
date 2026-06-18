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
