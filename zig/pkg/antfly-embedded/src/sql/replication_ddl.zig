// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Compiler-owned logical replication syntax; never a provider capability.
const ast = @import("ast.zig");
const contract = @import("../system_catalog/logical_replication.zig");
pub const max_members = contract.max_members;
pub const max_options = contract.max_options;
pub const Option = contract.Option;
pub const Value = contract.Value;
pub const PublicationOptions = contract.PublicationOptions;
pub const SubscriptionOptions = contract.SubscriptionOptions;
pub const publicationOptions = contract.publicationOptions;
pub const subscriptionOptions = contract.subscriptionOptions;

pub const PublicationCreate = struct { all_tables: bool = false, tables: []const ast.Name = &.{}, options: []const Option = &.{} };
pub const PublicationAlter = union(enum) { add_tables: []const ast.Name, set_tables: []const ast.Name, drop_tables: []const ast.Name, options: []const Option };
pub const SubscriptionCreate = struct { connection: []const u8, publications: []const []const u8, options: []const Option = &.{} };
pub const PublicationChange = struct { mode: contract.MembershipMode, names: []const []const u8, options: contract.RefreshOptions };
pub const Syntax = union(enum) {
    create_publication: PublicationCreate,
    alter_publication: PublicationAlter,
    drop_publication,
    create_subscription: SubscriptionCreate,
    set_subscription_enabled: bool,
    alter_subscription_options: []const Option,
    refresh_subscription: contract.RefreshOptions,
    change_subscription_publications: PublicationChange,
    drop_subscription,

    pub fn validateClassification(self: Syntax, ddl: ast.CatalogDdl) !void {
        const kind: @FieldType(ast.CatalogDdl, "kind") = switch (self) {
            .create_publication, .alter_publication, .drop_publication => .publication,
            else => .subscription,
        };
        const action: @FieldType(ast.CatalogDdl, "action") = switch (self) {
            .create_publication, .create_subscription => .create,
            .drop_publication, .drop_subscription => .drop,
            else => .alter_schema,
        };
        if (ddl.kind != kind or ddl.action != action or ddl.schema_change != null or ddl.partition != null or ddl.trigger != null)
            return error.InvalidSqlSyntax;
    }

    pub fn commandTag(self: Syntax) []const u8 {
        return switch (self) {
            .create_publication => "CREATE PUBLICATION",
            .alter_publication => "ALTER PUBLICATION",
            .drop_publication => "DROP PUBLICATION",
            .create_subscription => "CREATE SUBSCRIPTION",
            .set_subscription_enabled, .alter_subscription_options, .refresh_subscription, .change_subscription_publications => "ALTER SUBSCRIPTION",
            .drop_subscription => "DROP SUBSCRIPTION",
        };
    }
};
