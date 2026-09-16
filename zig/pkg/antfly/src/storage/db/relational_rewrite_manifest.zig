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

//! Bounded owner-private rewrite admission history. No primary rows are read.
//! Public definitions and their native immutable layouts must agree exactly.
const std = @import("std");
const public = @import("../../schema/mod.zig");
const native = @import("../schema.zig");
const contract = @import("relational_rewrite_contract.zig");
const Cancel = @import("types.zig").CancellationToken;
const public_active = "\x00\x00__metadata__:schema_json";
const runtime_prefix = "\x00\x00__metadata__:schema_v";
const Entry = struct { version: u32, json: []const u8 };

fn budget(total: *usize, size: usize) !void {
    total.* = std.math.add(usize, total.*, size) catch return error.RelationalRewriteBudgetExceeded;
    if (total.* > contract.max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
}

fn mappingVersion(alloc: std.mem.Allocator, txn: anytype, json: []const u8, runtime_bytes: *usize) !u32 {
    var parsed = try public.parseValidatedTableSchema(alloc, json);
    defer parsed.deinit(alloc);
    const derived = try public.deriveRuntimeTableSchema(alloc, parsed);
    defer native.freeSchema(alloc, derived);
    const key = try native.schemaVersionKeyAlloc(alloc, parsed.version);
    defer alloc.free(key);
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.UnknownSchemaVersion,
        else => return err,
    };
    try budget(runtime_bytes, bytes.len);
    const stored = try native.deserializeSchema(alloc, bytes);
    defer native.freeSchema(alloc, stored);
    if (!try native.schemasEqual(alloc, stored, derived)) return error.RestoreStagingScopeChanged;
    return parsed.version;
}

/// Uses caller-owned arena storage for the result and bounded temporary maps.
pub fn read(alloc: std.mem.Allocator, txn: anytype, cancel: Cancel) ![]const []const u8 {
    try cancel.check();
    const active = txn.get(public_active) catch |err| switch (err) {
        error.NotFound => return error.UnknownSchemaVersion,
        else => return err,
    };
    var total: usize = 0;
    var runtime_bytes: usize = 0;
    try budget(&total, active.len);
    const active_version = try mappingVersion(alloc, txn, active, &runtime_bytes);
    var entries: std.ArrayListUnmanaged(Entry) = .empty;
    defer entries.deinit(alloc);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(public.versioned_schema_key_prefix);
    while (row) |kv| : (row = try cursor.next()) {
        try cancel.check();
        if (!std.mem.startsWith(u8, kv.key, public.versioned_schema_key_prefix)) break;
        if (entries.items.len == contract.max_source_schemas) return error.RelationalRewriteBudgetExceeded;
        try budget(&total, kv.value.len);
        const version = try mappingVersion(alloc, txn, kv.value, &runtime_bytes);
        const key = try public.versionedSchemaKeyAlloc(alloc, version);
        defer alloc.free(key);
        if (!std.mem.eql(u8, key, kv.key)) return error.RestoreStagingScopeChanged;
        for (entries.items) |entry| if (entry.version == version) return error.RestoreStagingScopeChanged;
        try entries.append(alloc, .{ .version = version, .json = try alloc.dupe(u8, kv.value) });
    }
    const active_entry = for (entries.items) |entry| {
        if (entry.version == active_version) break entry;
    } else return error.UnknownSchemaVersion;
    if (!std.mem.eql(u8, active, active_entry.json)) return error.RestoreStagingScopeChanged;

    // A runtime-only epoch could still be referenced by old packed rows. It
    // must not disappear from the public manifest just because active/read
    // metadata no longer names it. Canonical keys also reject v01 aliases.
    var runtime_count: usize = 0;
    row = try cursor.seekAtOrAfter(runtime_prefix);
    while (row) |kv| : (row = try cursor.next()) {
        try cancel.check();
        if (!std.mem.startsWith(u8, kv.key, runtime_prefix)) break;
        runtime_count += 1;
        if (runtime_count > contract.max_source_schemas or kv.value.len > contract.max_schema_bytes) return error.RelationalRewriteBudgetExceeded;
        const version = std.fmt.parseInt(u32, kv.key[runtime_prefix.len..], 10) catch return error.RestoreStagingScopeChanged;
        const key = try native.schemaVersionKeyAlloc(alloc, version);
        defer alloc.free(key);
        if (!std.mem.eql(u8, key, kv.key)) return error.RestoreStagingScopeChanged;
        for (entries.items) |entry| {
            if (entry.version == version) break;
        } else return error.UnknownSchemaVersion;
    }
    if (runtime_count != entries.items.len) return error.UnknownSchemaVersion;
    std.mem.sort(Entry, entries.items, {}, struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return a.version < b.version;
        }
    }.less);
    const result = try alloc.alloc([]const u8, entries.items.len);
    for (result, entries.items) |*value, entry| value.* = entry.json;
    return result;
}
