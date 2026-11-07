// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const Options = @import("../storage/db/types.zig").LookupOptions;

// A private typed entrypoint, not a public lookup option or authorization
// bypass. Original reads use the existing authenticated internal transport.
pub fn requireOptions(a: std.mem.Allocator, opts: Options) !void {
    const defaults: Options = .{};
    inline for (@typeInfo(Options).@"struct".field_names, @typeInfo(Options).@"struct".field_types) |name, T| {
        if (comptime !std.mem.eql(u8, name, "include_all_fields") and !std.mem.eql(u8, name, "include_primary_digest") and !std.mem.eql(u8, name, "include_json_null_fields") and !std.mem.eql(u8, name, "relational_integrity_jobs_json") and !std.mem.eql(u8, name, "row_policy_principal_proof") and !std.mem.eql(u8, name, "row_policy_database") and !std.mem.eql(u8, name, "relation_owner_minter") and !std.mem.eql(u8, name, "privacy_policy_version") and !std.mem.eql(u8, name, "execution_deadline_ns") and !std.mem.eql(u8, name, "execution_io") and !std.mem.eql(u8, name, "cancellation")) {
            if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                if (@field(opts, name).len != 0) return error.InvalidArgument;
            } else if (!std.meta.eql(@field(opts, name), @field(defaults, name))) return error.InvalidArgument;
        }
    }
    if ((opts.row_policy_principal_proof.len == 0) != (opts.row_policy_database.len == 0)) return error.InvalidArgument;
    if (opts.relational_integrity_jobs_json.len == 0) {
        if (!opts.include_primary_digest or !opts.include_json_null_fields) return error.InvalidArgument;
        return;
    }
    if (opts.include_primary_digest or opts.include_json_null_fields or opts.relational_integrity_jobs_json.len > 4096) return error.InvalidArgument;
    // Only the existing bounded primary-observation command returns row
    // bodies. FK catalog, action, activation and arbitrary jobs stay excluded.
    const Query = struct { kind: []const u8, typed_nulls: bool, keys: []const []const u8, schema_version: u32, generation_set: [32]u8 };
    var parsed = std.json.parseFromSlice(Query, a, opts.relational_integrity_jobs_json, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidArgument;
    };
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.kind, "observations") or !parsed.value.typed_nulls or parsed.value.keys.len == 0 or parsed.value.keys.len > 8) return error.InvalidArgument;
}

test "mutation preimage selection rejects public projection and mixed private controls" {
    const a = std.testing.allocator;
    const primary: Options = .{ .include_primary_digest = true, .include_json_null_fields = true, .row_policy_principal_proof = "signed-principal", .row_policy_database = "main" };
    try requireOptions(a, primary);
    // Dynamic document layouts genuinely use schema0. Presence is required;
    // only the native receiver's exact current-version comparison is authority.
    for ([_]u32{ 0, 1 }) |version| {
        const query = try std.json.Stringify.valueAlloc(a, .{ .kind = "observations", .typed_nulls = true, .keys = &[_][]const u8{"a"}, .schema_version = version, .generation_set = @as([32]u8, @splat(1)) }, .{});
        defer a.free(query);
        try requireOptions(a, .{ .relational_integrity_jobs_json = query });
    }
    const missing_version = try std.json.Stringify.valueAlloc(a, .{ .kind = "observations", .typed_nulls = true, .keys = &[_][]const u8{"a"}, .generation_set = @as([32]u8, @splat(1)) }, .{});
    defer a.free(missing_version);
    try std.testing.expectError(error.InvalidArgument, requireOptions(a, .{ .relational_integrity_jobs_json = missing_version }));
    inline for (.{ "relational_integrity_catalog", "relational_integrity_action", "privacy_original" }) |field| {
        var mixed = primary;
        @field(mixed, field) = true;
        try std.testing.expectError(error.InvalidArgument, requireOptions(a, mixed));
    }
    var mixed = primary;
    mixed.fields = &.{"secret"};
    try std.testing.expectError(error.InvalidArgument, requireOptions(a, mixed));
    mixed = primary;
    mixed.sql_lock_query_json = "{}";
    try std.testing.expectError(error.InvalidArgument, requireOptions(a, mixed));
    try std.testing.expectError(error.InvalidArgument, requireOptions(a, .{}));
    try std.testing.expectError(error.InvalidArgument, requireOptions(a, .{ .relational_integrity_jobs_json = "{}" }));
}
