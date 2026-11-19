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

//! SQL package lifecycle on the admitted native catalog transaction lane.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const extensions = @import("../system_catalog/sql_extensions.zig");
const packages = @import("../extensions/mod.zig");
const recovery = @import("sql_extension_recovery.zig");
const common = @import("sql_catalog_recovery.zig");

pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.kind != .extension or ddl.name.database != null or ddl.name.namespace != null) return error.InvalidSqlSyntax;
    if (root.active_transaction != null) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    var region: std.heap.ArenaAllocator = .init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    try recovery.authorize(root.server, root.identity.*, a, root.database);
    const options = ddl.extension_options;
    const schema = if (options) |intent| intent.schema orelse root.namespace else root.namespace;
    var private = root.context;
    private.setting_admin = true;
    const query: extensions.SnapshotRequest = .{ .database = root.database, .name = ddl.name.table, .namespace = schema };
    const snapshot_bytes = try root.server.source.systemCatalog(a, private, .{ .sql_extension_snapshot = query });
    const snapshot = try std.json.parseFromSliceLeaky(extensions.Snapshot, a, snapshot_bytes, .{ .allocate = .alloc_always });
    try snapshot.validate(query);
    const expected: ?extensions.Identity = if (snapshot.installed) |installed| .{ .id = installed.record.id, .generation = installed.record.generation } else null;
    if (ddl.action != .create and expected == null and !(ddl.action == .drop and ddl.conditional)) return error.SqlUndefinedExtension;
    const registry: packages.sql_package.Registry = .{ .packages = snapshot.packages, .controls = snapshot.controls };
    var prepared_modules: std.StringHashMapUnmanaged(void) = .empty;
    var dependency_proofs: std.ArrayList(extensions.Dependency) = .empty;
    if (ddl.action == .create and expected == null) {
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        try preflight(root, a, registry, ddl.name.table, if (options) |intent| intent.version else null, ddl.cascade, snapshot.installed_extensions, &dependency_proofs, &prepared_modules, &seen, 0);
    } else if (ddl.action == .alter_schema) {
        const path = try registry.updatePath(a, ddl.name.table, snapshot.installed.?.record.package_version, if (options) |intent| intent.version else null);
        for (path) |package| try preparePackage(root, a, package, &prepared_modules);
    }
    var nonce: [16]u8 = undefined;
    const io = root.server.sharedApiIo() orelse return error.BackendRuntimeIoUnavailable;
    try io.randomSecure(&nonce);
    const request: extensions.Mutation = .{ .admission = .{ .nonce = nonce, .head = snapshot.head, .command_fence = if (root.context.sql_command_fence) |fence| fence.* else null }, .database = root.database, .database_id = snapshot.database_id, .incarnation = snapshot.incarnation, .registry_digest = try extensions.packageRegistryDigest(a, snapshot.packages, snapshot.controls), .name = ddl.name.table, .dependency_proofs = dependency_proofs.items, .operation = switch (ddl.action) {
        .create => .{ .install = .{ .namespace = schema, .schema_explicit = if (options) |intent| intent.schema != null else false, .version = if (options) |intent| intent.version else null, .if_not_exists = ddl.conditional, .cascade = ddl.cascade, .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled) } },
        .alter_schema => .{ .update = .{ .version = if (options) |intent| intent.version else null, .expected = expected.? } },
        .drop => .{ .drop = .{ .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } },
        .comment => .{ .comment = .{ .expected = expected.?, .description = ddl.comment_value } },
        else => return error.InvalidSqlSyntax,
    } };
    try request.validate();
    const observation = try extensions.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, snapshot.revision);
    errdefer receipt.deinit(allocators.result);
    if (root.notice_sink) |sink| if (sink.reserve) |reserve| try reserve(sink.ptr, 1, 1024);
    try root.context.ensureActive();
    const bytes = root.server.source.systemCatalog(a, private, .{ .sql_extension_mutate = request }) catch null;
    var known = if (bytes) |value| recovery.decodeOutcome(a, value, observation, snapshot.revision) else null;
    if (known == null) known = recovery.observe(root.server, root.identity.*, root.context, a, observation, snapshot.revision);
    if (known) |outcome| {
        const committed = try outcome.result();
        common.complete(&receipt, outcome);
        if (committed.no_op and root.notice_sink != null) {
            const message = if (ddl.action == .create) try std.fmt.allocPrint(a, "extension \"{s}\" already exists, skipping", .{ddl.name.table}) else if (ddl.action == .drop) try std.fmt.allocPrint(a, "extension \"{s}\" does not exist, skipping", .{ddl.name.table}) else try std.fmt.allocPrint(a, "version \"{s}\" of extension \"{s}\" is already installed", .{ snapshot.installed.?.record.package_version, ddl.name.table });
            const sink = root.notice_sink.?;
            sink.emit(sink.ptr, .{ .code = if (ddl.action == .create) "42710" else "00000", .message = message }) catch return .{ .mutation_outcome = .committed, .catalog_receipt = receipt };
        }
        return .{ .mutation_outcome = .committed, .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}

fn preflight(root: *execution.Adapter, a: std.mem.Allocator, registry: packages.sql_package.Registry, name: []const u8, version: ?[]const u8, cascade: bool, installed: []const extensions.Dependency, proofs: *std.ArrayList(extensions.Dependency), prepared_modules: *std.StringHashMapUnmanaged(void), seen: *std.StringHashMapUnmanaged(void), depth: usize) anyerror!void {
    if (depth >= extensions.max_installed) return error.SqlProgramLimitExceeded;
    if (seen.contains(name)) return;
    try seen.put(a, name, {});
    const package = try registry.select(name, version);
    try preparePackage(root, a, package, prepared_modules);
    if (cascade) for (package.dependencies) |dependency| {
        if (dependency.optional) continue;
        const existing: ?extensions.Dependency = for (installed) |candidate| {
            if (std.mem.eql(u8, candidate.name, dependency.name)) break candidate;
        } else null;
        if (existing) |proof| {
            const recorded = for (proofs.items) |prior| {
                if (prior.id == proof.id) break true;
            } else false;
            if (!recorded) try proofs.append(a, proof);
            continue;
        }
        try preflight(root, a, registry, dependency.name, null, true, installed, proofs, prepared_modules, seen, depth + 1);
    };
}
fn preparePackage(root: *execution.Adapter, a: std.mem.Allocator, package: packages.PackageManifest, prepared_modules: *std.StringHashMapUnmanaged(void)) !void {
    const control = package.sql orelse return error.SqlExtensionUnavailable;
    const io = root.server.sharedApiIo() orelse return error.BackendRuntimeIoUnavailable;
    for (control.exports) |declaration| {
        try root.context.ensureActive();
        const runtime = try packages.sql_package.wasmRuntime(package, declaration.runtime);
        const artifact = try packages.sql_package.wasmArtifact(package, runtime.artifact);
        const key = try std.json.Stringify.valueAlloc(a, .{ package.digest, artifact.digest, artifact.path, runtime.name }, .{});
        if (prepared_modules.contains(key)) continue;
        const runtime_mod = packages.wasmtime_runtime.PreparedSql;
        const manager = root.server.shared_resource_manager orelse &root.server.local_resource_manager;
        var reservation = try manager.reserve(.sql_execution_working_set, runtime_mod.sql_memory_bytes);
        defer reservation.release();
        var prepared = try packages.wasmtime_runtime.PreparedSql.prepare(a, .{ .package_name = package.name, .package_version = package.version, .package_digest = try packages.sql_package.digest(package.digest), .artifact = artifact.path, .artifact_digest = try packages.sql_package.digest(artifact.digest), .runtime_name = runtime.name, .entrypoint = "antfly_extension_call_tool", .export_name = declaration.export_name }, .{ .io = io, .package_store_root = root.server.cfg.extension_package_store_dir, .fuel = runtime_mod.sql_fuel, .max_memory_bytes = @intCast(runtime_mod.sql_memory_bytes) });
        defer prepared.deinit();
        const retained = try prepared.retainedBytes();
        if (retained > reservation.reservedBytes()) try reservation.growBoundedOversized(retained - reservation.reservedBytes(), 1);
        try prepared_modules.put(a, key, {});
    }
}
