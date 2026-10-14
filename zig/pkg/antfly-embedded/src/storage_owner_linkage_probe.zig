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

//! Executable fixture: builtin.is_test is false, so real archive calls are exercised.
const std = @import("std");
const client = @import("storage_client");
const db_mod = client.capi.db_mod;
const engine = client.storage_engine;
pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const root = args.next() orelse return error.MissingProbeDirectory;
    try std.Io.Dir.cwd().deleteTree(init.io, root);
    try std.Io.Dir.cwd().createDirPath(init.io, root);
    const physical = try std.fs.path.join(alloc, &.{ root, "physical" });
    defer alloc.free(physical);
    try std.Io.Dir.cwd().deleteTree(init.io, physical);
    if (!engine.enabled) return error.ProbeRequiresSharedStorage;
    const descriptor = engine.descriptorForValidation();
    engine.validateDescriptor(descriptor) catch |err| {
        inline for (.{ "table_layout", "db_layout", "core_layout", "handle_layout", "registry_layout" }) |field| {
            const actual = @field(descriptor, field);
            const expected = @field(engine, field);
            std.debug.print("storage ABI {s}: actual={any} expected={any}\n", .{ field, actual, expected });
        }
        for (descriptor.db_fields[0..descriptor.db_field_count], engine.db_fields) |actual, expected| {
            if (actual.offset != expected.offset or actual.size != expected.size or !std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                std.debug.print("DB field {s}: owner offset={d} size={d} type={s}; client offset={d} size={d} type={s}\n", .{ std.mem.span(actual.name), actual.offset, actual.size, std.mem.span(actual.type_name), expected.offset, expected.size, std.mem.span(expected.type_name) });
        }
        for (descriptor.core_fields[0..descriptor.core_field_count], engine.core_fields) |actual, expected| {
            if (actual.offset != expected.offset or actual.size != expected.size or !std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                std.debug.print("Core field {s}: owner offset={d} size={d} type={s}; client offset={d} size={d} type={s}\n", .{ std.mem.span(actual.name), actual.offset, actual.size, std.mem.span(actual.type_name), expected.offset, expected.size, std.mem.span(expected.type_name) });
        }
        return err;
    };
    var incompatible = engine.descriptorForValidation().*;
    incompatible.db_layout.size += 1;
    engine.validateDescriptor(&incompatible) catch |err| {
        if (err != error.UnsupportedVersion) return err;
        return exercise(alloc, root, physical);
    };
    return error.AcceptedIncompatibleStorageLayout;
}
fn exercise(alloc: std.mem.Allocator, root: []const u8, physical: []const u8) !void {
    const db = try db_mod.DB.openOwned(alloc, physical, .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.closeOwned();
    try db.batch(.{ .writes = &.{.{ .key = "shared-row", .value = "{\"n\":1}" }}, .sync_level = .write });
    const value = (try db.get(alloc, "shared-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(value);
    if (!std.mem.eql(u8, value, "{\"n\":1}")) return error.SharedStorageReadBackFailed;
    var looked_up = (try db.lookup(alloc, "shared-row", .{})) orelse return error.SharedStorageLookupFailed;
    defer looked_up.deinit(alloc);
    if (!std.mem.eql(u8, looked_up.json, value)) return error.SharedStorageLookupFailed;
    var scanned = try db.scan(alloc, "", "", .{ .include_documents = true });
    defer scanned.deinit(alloc);
    if (scanned.documents.len != 1 or try db.primaryDocCount(alloc) != 1) return error.SharedStorageScanFailed;
    var result = try db.search(alloc, .{});
    defer result.deinit();
    if (result.hits.len != 1) return error.SharedStorageQueryFailed;
    try db.sync(true);
    db.batch(.{ .row_policy_install_bundle = "reject-this-private-command" }) catch |err| {
        if (err != error.InvalidBatchRequest) return error.SharedStorageErrorIdentityLost;
        try exerciseServerHandle(alloc, root);
        return exerciseCapi(alloc, root);
    };
    return error.SharedStorageInvalidBatchAccepted;
}
fn exerciseCapi(alloc: std.mem.Allocator, root: []const u8) !void {
    const capi = client.capi;
    const path = try std.fs.path.join(alloc, &.{ root, "public.aflite" });
    defer alloc.free(path);
    const terminated = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(terminated);
    var handle: ?*anyopaque = null;
    if (capi.antfly_lite_create(terminated, &handle) != .ok) return error.SharedStorageCapiOpenFailed;
    defer capi.antfly_db_close(handle);
    if (capi.asHandle(handle) == null) return error.SharedStorageRegistryNotShared;
    var out: capi.ApiTypes.Buffer = .{};
    defer capi.antfly_buffer_free(&out);
    const bad = "invalid-json";
    const status = capi.antfly_db_batch_json(handle, .{ .ptr = bad.ptr, .len = bad.len }, &out);
    if (status == .ok) return error.SharedStorageInvalidJsonAccepted;
    if (capi.ApiTypes.lastErrorCode() != status or std.mem.span(capi.ApiTypes.lastErrorDetail()).len == 0)
        return error.SharedStorageErrorStateNotShared;
    std.debug.print("shared storage lifecycle, batch/query, named failure, registry/TLS and incompatible-layout checks passed\n", .{});
}

fn exerciseServerHandle(alloc: std.mem.Allocator, root: []const u8) !void {
    const capi = client.capi;
    const path = try std.fs.path.join(alloc, &.{ root, "server-created" });
    defer alloc.free(path);
    const handle = try alloc.create(capi.Handle);
    errdefer alloc.destroy(handle);
    handle.* = .{ .alloc = alloc, .db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false }) };
    errdefer handle.db.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "from-server", .value = "{\"origin\":\"server\"}" }}, .sync_level = .write });
    const id = try capi.handleRegistry().register(handle);
    defer capi.antfly_db_close(id);
    const key = "from-server";
    var out: capi.ApiTypes.Buffer = .{};
    defer capi.antfly_buffer_free(&out);
    if (capi.antfly_db_lookup_json(id, .{ .ptr = key.ptr, .len = key.len }, &out) != .ok)
        return error.SharedStorageServerHandleRejected;
    if (std.mem.indexOf(u8, out.ptr.?[0..out.len], "server") == null)
        return error.SharedStorageServerHandleReadFailed;
}
