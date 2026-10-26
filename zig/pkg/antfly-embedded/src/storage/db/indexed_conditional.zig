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

//! Final indexed-source delivery from the claimed immutable canonical tree.
//! Scalar identities and omissions require no value reads. Containers and open
//! projections retain the exact-source reference path instead of guessing.
const std = @import("std");
const protocol = @import("../../api/source_projection.zig");
const logical = @import("../document_commitment.zig");
const query = @import("document_query.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Result = struct { source: []u8, identities: []u8 };
pub fn load(output: A, scratch: A, session: anytype, id: []const u8, options: types.LookupOptions, prepared: protocol.Prepared) !?Result {
    return loadWithPlan(output, scratch, session, id, options, prepared, null);
}
pub fn loadWithPlan(output: A, scratch: A, session: anytype, id: []const u8, options: types.LookupOptions, prepared: protocol.Prepared, shared_plan: ?*const query.SpanProjectionPlan) !?Result {
    if (options.include_all_fields or prepared.request.fields.len == 0 or prepared.request.fields.len > 64 or !@import("../conditional_source.zig").exactSelectedValues(options.fields, prepared.request)) return null;
    _ = try session.originalBytes();
    const effect = session.record.plan.effects[0];
    const manifest = effect.manifest orelse return null;
    if (manifest.logical_record.len == 0) return null;
    const tree = (try logical.openTree(scratch, manifest.logical_record, manifest.logical_digest)) orelse return null;
    defer tree.deinit(scratch);
    if (std.mem.readInt(u64, tree.raw[4..12], .little) != manifest.logical_bytes or tree.raw[44] != effect.key[effect.key.len - 1]) return error.IndexSourceClaimsCorrupt;
    var omitted: [64]bool = @splat(false);
    var omissions: [64]query.Omission = undefined;
    var omission_count: usize = 0;
    var identities: std.json.ArrayHashMap(protocol.Identity) = .{};
    for (prepared.request.fields, prepared.conditions, 0..) |field, conditions, i| {
        var parts: std.ArrayListUnmanaged([]const u8) = .empty;
        var split = std.mem.splitScalar(u8, field.field, '.');
        while (split.next()) |part| {
            if (part.len == 0) return null;
            try parts.append(scratch, part);
        }
        const location = tree.directory.locate(parts.items);
        var state: protocol.State = .missing;
        var token: ?[]const u8 = null;
        if (location.value) |selected| {
            if (selected.covered != parts.items.len or selected.field.kind != .leaf) return null;
            token = try std.fmt.allocPrint(scratch, "sv1-{s}", .{std.fmt.bytesToHex(tree.canonical[selected.field.index], .lower)});
            state = if (field.mode == .value) .value else .reference;
            if (field.mode == .value) if (conditions.get(id)) |expected| if (std.mem.eql(u8, expected, token.?)) {
                state = .unchanged;
            };
        }
        try identities.map.put(scratch, field.field, .{ .state = state, .identity = token });
        omitted[i] = state != .value;
        if (omitted[i]) {
            omissions[omission_count] = .{ .parts = parts.items, .objects = location.objects };
            omission_count += 1;
        }
    }
    var selected_fields: std.ArrayListUnmanaged([]const u8) = .empty;
    for (options.fields) |field| {
        const omit = for (prepared.request.fields, omitted[0..prepared.request.fields.len]) |condition, absent| {
            if (absent and std.mem.eql(u8, condition.field, field)) break true;
        } else false;
        if (!omit) try selected_fields.append(scratch, field);
    }
    var partial: ?types.LookupResult = null;
    defer if (partial) |*value| value.deinit(scratch);
    if (selected_fields.items.len != 0) {
        var selection = (try query.SpanProjectionPlan.init(scratch, .{ .fields = selected_fields.items, .include_all_fields = false })) orelse return null;
        defer selection.deinit();
        partial = (try query.lookupSourceSessionWithPlanDetailed(scratch, scratch, session, &selection, options.output_exact)).lookup;
    }
    var owned_plan: ?query.SpanProjectionPlan = null;
    defer if (owned_plan) |*plan| plan.deinit();
    const full_plan = shared_plan orelse plan: {
        owned_plan = (try query.SpanProjectionPlan.init(scratch, options)) orelse return null;
        break :plan &owned_plan.?;
    };
    const source = (try query.lookupJsonWithPlanOmissions(scratch, output, if (partial) |value| value.json else "{}", full_plan, omissions[0..omission_count])).json;
    errdefer output.free(source);
    return .{ .source = source, .identities = try std.json.Stringify.valueAlloc(output, identities, .{ .emit_null_optional_fields = false }) };
}
