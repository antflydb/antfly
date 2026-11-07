// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A shared structural classifier for private controls that return no document
//! values. This is not admission authority: receivers still verify the real
//! command, connection, route, restore scope and receipt before any control.
const std = @import("std");
const Options = @import("../storage/db/types.zig").LookupOptions;
pub const Kind = enum { policy_receipt, sql_lock, handoff_install, handoff_identity };
pub const handoff_install = "{\"mode\":\"generation_handoff_install\"}";
pub const handoff_identity = "{\"mode\":\"generation_handoff_identity\"}";

fn allowed(comptime kind: Kind, comptime name: []const u8) bool {
    if (std.mem.eql(u8, name, "execution_deadline_ns") or std.mem.eql(u8, name, "execution_io") or std.mem.eql(u8, name, "cancellation")) return true;
    return switch (kind) {
        .policy_receipt => std.mem.eql(u8, name, "row_policy_receipt"),
        .sql_lock => std.mem.eql(u8, name, "sql_lock_query_json") or std.mem.eql(u8, name, "relation_owner_minter"),
        .handoff_install, .handoff_identity => std.mem.eql(u8, name, "relational_topology_json") or std.mem.eql(u8, name, "restore_staging_scope") or std.mem.eql(u8, name, "restore_staging_plan_id"),
    };
}

/// All unlisted fields, including privacy_original/version, row/digest/null
/// selectors, principal credentials and other control tags, stay default.
/// Reflection makes newly added effects fail closed rather than disappear
/// from a manually maintained negative list.
pub fn optionsMatch(comptime kind: Kind, opts: Options) bool {
    const defaults: Options = .{};
    inline for (@typeInfo(Options).@"struct".field_names, @typeInfo(Options).@"struct".field_types) |name, T| {
        if (comptime !allowed(kind, name)) {
            if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                if (@field(opts, name).len != 0) return false;
            } else if (!std.meta.eql(@field(opts, name), @field(defaults, name))) return false;
        }
    }
    return true;
}

pub fn classify(key: []const u8, opts: Options) ?Kind {
    if (key.len != 0) return null;
    if (opts.row_policy_receipt != null and optionsMatch(.policy_receipt, opts)) return .policy_receipt;
    if (opts.sql_lock_query_json.len != 0 and opts.sql_lock_query_json.len <= 32768 and optionsMatch(.sql_lock, opts)) return .sql_lock;
    if (opts.restore_staging_scope != null and opts.restore_staging_plan_id != null) {
        if (std.mem.eql(u8, opts.relational_topology_json, handoff_install) and optionsMatch(.handoff_install, opts)) return .handoff_install;
        if (std.mem.eql(u8, opts.relational_topology_json, handoff_identity) and optionsMatch(.handoff_identity, opts)) return .handoff_identity;
    }
    return null;
}

pub fn isHandoff(key: []const u8, opts: Options) bool {
    return if (classify(key, opts)) |kind| kind == .handoff_install or kind == .handoff_identity else false;
}

pub fn requireReceipt(key: []const u8, opts: Options) !void {
    if (classify(key, opts) != .policy_receipt) return error.InvalidRowPolicyReceipt;
}

/// Existing SQL-lock option guards may validate before checking whether this
/// is a native cleanup control. Preserve their separate key/body/MAC checks.
pub fn requireSqlLockOptions(opts: Options) !void {
    if (!optionsMatch(.sql_lock, opts)) return error.InvalidLockScope;
}
