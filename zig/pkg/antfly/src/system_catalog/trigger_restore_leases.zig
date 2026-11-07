// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Data-only indexed leases shared by every catalog dry-run and application.
//! Keeping this below metadata plan capture avoids a storage import cycle.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;
pub const Resource = struct { kind: domain.Kind, id: u64 };
fn prefix(out: []u8, group: u64, resource: Resource) ![]const u8 {
    if (group == 0 or resource.id == 0) return error.InvalidRestoreStaging;
    return std.fmt.bufPrint(out, "\x00\x00__metadata__:system_catalog:{d}:trigger_restore:{s}:{x:0>16}:", .{ group, @tagName(resource.kind), resource.id });
}
pub fn leaseKey(out: []u8, group: u64, resource: Resource, id: [16]u8) ![]const u8 {
    var buffer: [160]u8 = undefined;
    return std.fmt.bufPrint(out, "{s}{s}", .{ try prefix(&buffer, group, resource), std.fmt.bytesToHex(id, .lower) });
}
pub const Address = struct { resource: Resource, job: [16]u8 };
/// Reject malformed and noncanonical keys rather than permitting cold orphan
/// authority to evade the same indexed mutation guards used by live writers.
pub fn address(group: u64, key: []const u8) !?Address {
    var start_buffer: [128]u8 = undefined;
    const start = try std.fmt.bufPrint(&start_buffer, "\x00\x00__metadata__:system_catalog:{d}:trigger_restore:", .{group});
    if (!std.mem.startsWith(u8, key, start)) return null;
    const tail = key[start.len..];
    const split = std.mem.indexOfScalar(u8, tail, ':') orelse return error.InvalidRestoreStaging;
    const kind = std.meta.stringToEnum(domain.Kind, tail[0..split]) orelse return error.InvalidRestoreStaging;
    if (kind != .routine and kind != .namespace and kind != .database) return error.InvalidRestoreStaging;
    const identity = tail[split + 1 ..];
    if (identity.len != 49 or identity[16] != ':') return error.InvalidRestoreStaging;
    const id = std.fmt.parseInt(u64, identity[0..16], 16) catch return error.InvalidRestoreStaging;
    var job: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&job, identity[17..]) catch return error.InvalidRestoreStaging;
    if (std.mem.allEqual(u8, &job, 0)) return error.InvalidRestoreStaging;
    const result: Address = .{ .resource = .{ .kind = kind, .id = id }, .job = job };
    var canonical: [256]u8 = undefined;
    if (!std.mem.eql(u8, key, try leaseKey(&canonical, group, result.resource, job))) return error.InvalidRestoreStaging;
    return result;
}
pub fn requireResourceMutable(txn: anytype, group: u64, resource: Resource) !void {
    if (resource.kind != .routine and resource.kind != .namespace and resource.kind != .database) return;
    var buffer: [160]u8 = undefined;
    const start = try prefix(&buffer, group, resource);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(start)) orelse return;
    if (!std.mem.startsWith(u8, row.key, start)) return;
    if (row.key.len != start.len + 32 or !std.mem.eql(u8, row.value, &.{1})) return error.InvalidRestoreStaging;
    return if (resource.kind == .routine) error.SqlDependentCallable else error.TableTransitionActive;
}
/// All catalog mutation routes share this native transaction hook, including
/// routine SET SCHEMA/rename and enclosing namespace/database retirement.
/// Ordinary attached triggers still allow compatible function replacement;
/// only an unpublished restore's admitted authority is temporarily pinned.
pub fn requireUnleasedDelta(txn: anytype, group: u64, delta: domain.Delta) !void {
    for (delta.upserts) |resource| try requireResourceMutable(txn, group, .{ .kind = resource.kind, .id = resource.id });
    for (delta.removes) |resource| try requireResourceMutable(txn, group, .{ .kind = resource.kind, .id = resource.id });
}
