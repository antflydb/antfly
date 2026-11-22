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

//! Operational overrides are durable, but are not publication authority.
//! Keeping them outside the frozen catalog preserves ordered receipts and
//! active source pins while future requests observe the new policy.
const std = @import("std");
const catalog = @import("enrichment_catalog.zig");
const semantics = @import("../artifact_semantics.zig");
pub const key = "\x00\x00__metadata__:enrichment_execution";
const Row = struct { name: []const u8, definition: [32]u8, execution_json: []const u8 };
const Overlay = struct { version: u32 = 1, rows: []const Row };

fn definition(a: std.mem.Allocator, cfg: catalog.EnrichmentConfig) ![32]u8 {
    const raw = try catalog.serializeCatalog(a, &.{cfg});
    defer a.free(raw);
    return semantics.digest(a, "", raw, "");
}

pub fn stage(a: std.mem.Allocator, txn: anytype, desired: []const u8) !void {
    const parsed = try std.json.parseFromSlice([]catalog.EnrichmentConfig, a, desired, .{});
    defer parsed.deinit();
    const rows = try a.alloc(Row, parsed.value.len);
    defer a.free(rows);
    for (parsed.value, rows) |cfg, *row| row.* = .{ .name = cfg.name, .definition = try definition(a, cfg), .execution_json = cfg.execution_json };
    const encoded = try std.json.Stringify.valueAlloc(a, Overlay{ .rows = rows }, .{});
    defer a.free(encoded);
    try txn.put(key, encoded);
}

pub fn apply(a: std.mem.Allocator, txn: anytype, entries: []catalog.EnrichmentConfig) !void {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const parsed = try std.json.parseFromSlice(Overlay, a, raw, .{});
    defer parsed.deinit();
    if (parsed.value.version != 1) return error.InvalidEnrichmentExecutionConfig;
    var names: std.StringHashMapUnmanaged(Row) = .empty;
    defer names.deinit(a);
    for (parsed.value.rows) |row| {
        const slot = try names.getOrPut(a, row.name);
        if (slot.found_existing) return error.InvalidEnrichmentExecutionConfig;
        slot.value_ptr.* = row;
    }
    for (entries) |*cfg| {
        const row = names.get(cfg.name) orelse continue;
        // An override cannot authorize a different producer after rename,
        // removal, restore or a genuine semantic recipe change.
        if (!std.meta.eql(row.definition, try definition(a, cfg.*))) continue;
        if (row.execution_json.len != 0) _ = try @import("../enrichment/execution_policy.zig").parseExecutionPolicyJson(a, row.execution_json);
        const owned = try a.dupe(u8, row.execution_json);
        if (cfg.execution_json.len != 0) a.free(cfg.execution_json);
        cfg.execution_json = owned;
    }
}
