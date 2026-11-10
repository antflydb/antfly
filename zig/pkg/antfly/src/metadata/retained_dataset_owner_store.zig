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

//! Atomic retained producer journal and bounded per-owner work queue. The
//! surrounding metadata transaction verifies resource identity, authorization,
//! source topology and receipts; this module does not confer that authority.
const std = @import("std");
const model = @import("retained_dataset_owners.zig");
const A = std.mem.Allocator;
pub const Cursor = struct { resource: u64 = 0, generation: u64 = 0, owner_group: u64 = 0 };
pub const WorkKey = struct { cursor: Cursor, plan: model.Id };
pub const root_limit = 16 * 1024;
pub const owner_limit = 4096;
const roots = "\x00\x00__metadata__:retained_owner_plan_v1:";
const owners = "\x00\x00__metadata__:retained_owner_v1:";
// This journal index commits and snapshots with its owner rows. Keep it out
// of generic derived-name rebuilds, which do not own producer job history.
const work = "\x00\x00__metadata__:retained_owner_work_v1:";
pub fn planPrefix(buffer: []u8, metadata_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{d}:", .{ roots, metadata_group });
}
pub fn ownerPrefix(buffer: []u8, metadata_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{d}:", .{ owners, metadata_group });
}
fn planKey(buffer: []u8, metadata_group: u64, resource: u64, generation: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{d}:{x:0>16}:{x:0>16}", .{ roots, metadata_group, resource, generation });
}
fn ownerKey(buffer: []u8, metadata_group: u64, plan: model.Id, owner_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{d}:{s}:{x:0>16}", .{ owners, metadata_group, std.fmt.bytesToHex(plan, .lower), owner_group });
}
pub fn workPrefix(buffer: []u8, metadata_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{d}:", .{ work, metadata_group });
}
fn workKey(buffer: []u8, metadata_group: u64, cursor: Cursor) ![]const u8 {
    const prefix = try workPrefix(buffer, metadata_group);
    const offset = prefix.len;
    if (buffer.len - offset < 24) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[offset..][0..8], cursor.resource, .big);
    std.mem.writeInt(u64, buffer[offset + 8 ..][0..8], cursor.generation, .big);
    std.mem.writeInt(u64, buffer[offset + 16 ..][0..8], cursor.owner_group, .big);
    return buffer[0 .. offset + 24];
}
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
/// Plan has only inline values, so none of its bytes borrow the read scratch.
pub fn getPlan(a: A, txn: anytype, metadata_group: u64, resource: u64, generation: u64) !?model.Plan {
    var key: [192]u8 = undefined;
    const bytes = (try optional(txn, try planKey(&key, metadata_group, resource, generation))) orelse return null;
    if (bytes.len > root_limit) return error.InvalidRetainedOwnerPlan;
    var parsed = try std.json.parseFromSlice(model.Plan, a, bytes, .{});
    defer parsed.deinit();
    try parsed.value.validate();
    if (parsed.value.resource != resource or parsed.value.generation != generation) return error.InvalidRetainedOwnerPlan;
    return parsed.value;
}
pub fn getOwner(a: A, txn: anytype, metadata_group: u64, plan: model.Id, owner_group: u64) !?std.json.Parsed(model.Owner) {
    var key: [192]u8 = undefined;
    const bytes = (try optional(txn, try ownerKey(&key, metadata_group, plan, owner_group))) orelse return null;
    if (bytes.len > owner_limit) return error.InvalidRetainedOwnerPlan;
    var parsed = try std.json.parseFromSlice(model.Owner, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    if (!std.mem.eql(u8, &parsed.value.plan, &plan) or parsed.value.group != owner_group) return error.InvalidRetainedOwnerPlan;
    return parsed;
}
fn savePlan(a: A, txn: anytype, metadata_group: u64, plan: model.Plan) !void {
    try plan.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, plan, .{});
    defer a.free(bytes);
    if (bytes.len > root_limit) return error.InvalidRetainedOwnerPlan;
    var key: [192]u8 = undefined;
    try txn.put(try planKey(&key, metadata_group, plan.resource, plan.generation), bytes);
}
fn saveOwner(a: A, txn: anytype, metadata_group: u64, plan: model.Plan, owner: model.Owner) !void {
    try owner.validate();
    if (!std.mem.eql(u8, &owner.plan, &plan.id)) return error.InvalidRetainedOwnerPlan;
    const bytes = try std.json.Stringify.valueAlloc(a, owner, .{});
    defer a.free(bytes);
    if (bytes.len > owner_limit) return error.InvalidRetainedOwnerPlan;
    var key: [192]u8 = undefined;
    try txn.put(try ownerKey(&key, metadata_group, plan.id, owner.group), bytes);
    const index = try workKey(&key, metadata_group, .{ .resource = plan.resource, .generation = plan.generation, .owner_group = owner.group });
    if (owner.mode == .revoked) {
        try txn.delete(index);
    } else try txn.put(index, &plan.id);
}
fn expectedPlan(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, expected: u64) !model.Plan {
    const current = (try getPlan(a, txn, metadata_group, cursor.resource, cursor.generation)) orelse return error.RetainedOwnerPlanChanged;
    if (current.revision != expected) return error.RetainedOwnerPlanChanged;
    return current;
}
pub fn create(a: A, txn: anytype, metadata_group: u64, plan: model.Plan) !bool {
    try plan.validate();
    if (plan.phase != .activating or plan.revision != 1 or plan.registered != 0 or plan.registration_complete) return error.InvalidRetainedOwnerPlan;
    if (try getPlan(a, txn, metadata_group, plan.resource, plan.generation)) |existing| {
        if (!std.mem.eql(u8, &existing.id, &plan.id)) return error.RetainedOwnerPlanChanged;
        return false;
    }
    try savePlan(a, txn, metadata_group, plan);
    return true;
}
pub fn register(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, revision: u64, owner: model.Owner) !model.Plan {
    const current = try expectedPlan(a, txn, metadata_group, cursor, revision);
    const next = try current.register(owner);
    if (try getOwner(a, txn, metadata_group, current.id, owner.group)) |value| {
        var existing = value;
        existing.deinit();
        return error.InvalidRetainedOwnerPlan;
    }
    try saveOwner(a, txn, metadata_group, next, owner);
    try savePlan(a, txn, metadata_group, next);
    return next;
}
pub const Transition = enum { complete_registration, activate, close, retire };
pub fn transition(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, revision: u64, action: Transition) !model.Plan {
    const current = try expectedPlan(a, txn, metadata_group, cursor, revision);
    const next = switch (action) {
        .complete_registration => try current.completeRegistration(),
        .activate => try current.activate(),
        .close => try current.close(),
        .retire => try current.retire(),
    };
    try savePlan(a, txn, metadata_group, next);
    return next;
}
/// Verify the supplied native proof before invoking this function. Owner and
/// root revisions are checked against the same persisted transaction cut.
pub fn acknowledge(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, plan_revision: u64, owner_revision: u64, owner: model.Owner) !model.Plan {
    const current = try expectedPlan(a, txn, metadata_group, cursor, plan_revision);
    if (owner.group != cursor.owner_group) return error.RetainedOwnerPlanChanged;
    var previous = (try getOwner(a, txn, metadata_group, current.id, cursor.owner_group)) orelse return error.RetainedOwnerPlanChanged;
    defer previous.deinit();
    if (previous.value.revision != owner_revision) return error.RetainedOwnerPlanChanged;
    const next = try current.acknowledge(previous.value, owner);
    try saveOwner(a, txn, metadata_group, next, owner);
    try savePlan(a, txn, metadata_group, next);
    return next;
}
pub fn releaseSource(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, revision: u64, verified_receipt: model.Id) !model.Plan {
    const current = try expectedPlan(a, txn, metadata_group, cursor, revision);
    const next = try current.releaseSource(verified_receipt);
    try savePlan(a, txn, metadata_group, next);
    return next;
}
pub fn finishCollection(a: A, txn: anytype, metadata_group: u64, cursor: Cursor, revision: u64, verified_receipt: model.Id) !model.Plan {
    const current = try expectedPlan(a, txn, metadata_group, cursor, revision);
    const next = try current.finishCollection(verified_receipt);
    try savePlan(a, txn, metadata_group, next);
    return next;
}
/// One cyclic ordered seek. A cursor includes all three dimensions, so the
/// number of resources or owners cannot cause modulo-based starvation.
pub fn nextWork(txn: anytype, metadata_group: u64, after: Cursor) !?WorkKey {
    var prefix_buffer: [192]u8 = undefined;
    const prefix = try workPrefix(&prefix_buffer, metadata_group);
    var seek_buffer: [193]u8 = undefined;
    const exact = try workKey(&seek_buffer, metadata_group, after);
    seek_buffer[exact.len] = 0;
    var scan = try txn.openCursor();
    defer scan.close();
    var row = try scan.seekAtOrAfter(seek_buffer[0 .. exact.len + 1]);
    if (row == null or !std.mem.startsWith(u8, row.?.key, prefix)) row = try scan.seekAtOrAfter(prefix);
    const entry = row orelse return null;
    if (!std.mem.startsWith(u8, entry.key, prefix)) return null;
    if (entry.key.len != prefix.len + 24 or entry.value.len != 32) return error.InvalidRetainedOwnerPlan;
    const key = entry.key[prefix.len..];
    const result: WorkKey = .{ .cursor = .{ .resource = std.mem.readInt(u64, key[0..8], .big), .generation = std.mem.readInt(u64, key[8..16], .big), .owner_group = std.mem.readInt(u64, key[16..24], .big) }, .plan = entry.value[0..32].* };
    if (result.cursor.resource == 0 or result.cursor.generation == 0 or result.cursor.owner_group == 0) return error.InvalidRetainedOwnerPlan;
    return result;
}

test "retained owner journal keys order every resource generation and owner without collisions" {
    var one: [192]u8 = undefined;
    var two: [192]u8 = undefined;
    const a = try workKey(&one, 21, .{ .resource = 41, .generation = 1, .owner_group = 71 });
    const b = try workKey(&two, 21, .{ .resource = 41, .generation = 2, .owner_group = 1 });
    try std.testing.expect(std.mem.order(u8, a, b) == .lt);
    const c = try workKey(&two, 21, .{ .resource = 42, .generation = 1, .owner_group = 1 });
    try std.testing.expect(std.mem.order(u8, a, c) == .lt);
    const d = try workKey(&two, 21, .{ .resource = 41, .generation = 1, .owner_group = 72 });
    try std.testing.expect(std.mem.order(u8, a, d) == .lt);
    var root: [192]u8 = undefined;
    const root_key = try planKey(&root, 21, 41, 1);
    try std.testing.expect(!std.mem.eql(u8, a, root_key));
}
