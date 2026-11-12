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

//! Bounded operator status; diagnostics never certify serving or reclamation.
const std = @import("std");
const local = @import("antfly_local_sources");
const model = @import("../metadata/retained_dataset_owners.zig");
const read = @import("retained_dataset_read.zig");
const archive_api = @import("../serverless/datasets/configured_archive.zig");
const operation = local.api_operation;
const A = std.mem.Allocator;
pub const Native = struct {
    ready: bool,
    draining: bool,
    closed: bool,
    archive_handoff: bool,
    backfill_cleanup_complete: bool,
    source_sequence: []const u8,
    replay_floor: []const u8,
    replay_latest: []const u8,
    retention_clock_ns: []const u8,
    last_attempt_ns: ?[]const u8 = null,
    last_error: ?[]const u8 = null,
};
pub const Archive = struct { pages: []const u8, sealed: bool, next_bucket_ns: []const u8 };
pub const Owner = struct {
    group: []const u8,
    mode: model.Mode,
    archive_authoritative: bool,
    native: ?Native = null,
    archive: ?Archive = null,
    status_error: ?[]const u8 = null,
};
pub const Details = struct {
    phase: model.Phase,
    registration_complete: bool,
    registered_owners: u32,
    ready_owners: u32,
    revoked_owners: u32,
    archived_owners: u32,
    owners: []const Owner,
    next_group: ?[]const u8 = null,
};
const Page = struct { plan: model.Plan, owners: []const model.Owner, next_group: ?u64 = null };
fn decimal(a: A, value: anytype) ![]const u8 {
    return std.fmt.allocPrint(a, "{d}", .{value});
}
fn project(a: A, value: local.storage_retained_series.Progress) !Native {
    return .{ .ready = value.ready, .draining = value.draining, .closed = value.closed, .archive_handoff = value.archive_handoff, .backfill_cleanup_complete = value.backfill_clean, .source_sequence = try decimal(a, value.source_sequence), .replay_floor = try decimal(a, value.feed_floor), .replay_latest = try decimal(a, value.feed_latest), .retention_clock_ns = try decimal(a, value.admitted_ns), .last_attempt_ns = if (value.last_attempt_ns) |time| try decimal(a, time) else null, .last_error = value.last_error };
}

pub fn inspect(server: anytype, a: A, resource: u64, descriptor: local.system_catalog_retained_datasets.Descriptor, after: u64, limit: u16, input: operation.RequestContext) !Details {
    if (limit == 0 or limit > 64) return error.InvalidRetainedOwnerPlan;
    var context = try input.platformDeadline();
    context.setting_admin = true;
    const raw = try server.source.systemCatalog(a, context, .{ .retained_owner_read = .{ .summary = .{ .resource = resource, .generation = descriptor.definition.generation, .after_group = after, .limit = limit } } });
    const page = try std.json.parseFromSliceLeaky(Page, a, raw, .{});
    if (page.owners.len > limit or page.plan.resource != resource or !std.mem.eql(u8, &page.plan.definition, &try descriptor.definition.fingerprint(a))) return error.RetainedOwnerPlanChanged;
    // Status is best effort: an unavailable archive must not hide the
    // authoritative lifecycle metadata or turn GET into an outage probe.
    context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
    const output = try a.alloc(Owner, page.owners.len);
    var archive: ?archive_api.Opened = null;
    defer if (archive) |*opened| opened.deinit();
    for (page.owners, output) |owner, *out| {
        out.* = .{ .group = try decimal(a, owner.group), .mode = owner.mode, .archive_authoritative = owner.independent_receipt != null };
        context.ensureActive() catch |err| {
            out.status_error = @errorName(err);
            continue;
        };
        if (descriptor.source) |source| if (owner.mode != .revoked) native_status: {
            var bounded = context;
            bounded.deadline_ns = @min(context.deadline_ns.?, @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
            const bytes = server.readRetainedOwnerRows(a, source.storage_name, owner.group, .{ .scope = .{ .resource = resource, .generation = page.plan.generation, .plan = page.plan.id, .group = owner.group, .revision = owner.revision }, .start_ns = "0", .end_ns = "1", .version = 3, .status_only = true, .limit = 1 }, bounded) catch |err| {
                out.status_error = @errorName(err);
                break :native_status;
            };
            const decoded = try std.json.parseFromSliceLeaky(read.NativeReply, a, bytes, .{});
            if (decoded.progress) |progress| out.native = try project(a, progress);
        };
        if (owner.cut) |cut| archived_status: {
            if (archive == null) {
                const config = server.cfg.node_config orelse break :archived_status;
                archive = archive_api.Opened.init(a, config, server.cfg.secret_store, descriptor, true) catch |err| {
                    out.status_error = @errorName(err);
                    break :archived_status;
                };
            }
            const io = server.sharedApiIo() orelse break :archived_status;
            const spec = try @import("retained_owner_binding.zig").specification(a, descriptor.definition, owner.partition, owner.incarnation);
            const manifest = archive.?.store(io, &context).getClosedExport(spec, .{ .closure = cut.closure, .sequence = cut.sequence, .complete_from_ns = try std.fmt.parseInt(i128, cut.complete_from_ns, 10), .admitted_ns = try std.fmt.parseInt(i128, cut.admitted_ns, 10) }) catch |err| {
                out.status_error = @errorName(err);
                break :archived_status;
            };
            if (manifest) |value| out.archive = .{ .pages = try decimal(a, value.pages), .sealed = value.sealed, .next_bucket_ns = try decimal(a, try local.retained_dataset_types.rowKeyTimestamp(value.next)) };
        }
    }
    try input.ensureActive();
    return .{ .phase = page.plan.phase, .registration_complete = page.plan.registration_complete, .registered_owners = page.plan.registered, .ready_owners = page.plan.ready, .revoked_owners = page.plan.revoked, .archived_owners = page.plan.independent, .owners = output, .next_group = if (page.next_group) |group| try decimal(a, group) else null };
}

test "retained owner native controls status projects exact clocks without source row keys" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const value = try project(arena.allocator(), .{ .definition = @splat(1), .source_incarnation = 1, .source_sequence = 9007199254740993, .admitted_ns = 123, .complete_from_ns = -1000, .backfill_cursor = "private-source-key", .last_error = "InvalidRetainedSeriesRow", .last_attempt_ns = 456 });
    const raw = try std.json.Stringify.valueAlloc(arena.allocator(), value, .{});
    try std.testing.expect(std.mem.indexOf(u8, raw, "private-source-key") == null);
    try std.testing.expectEqualStrings("9007199254740993", value.source_sequence);
    try std.testing.expectEqualStrings("InvalidRetainedSeriesRow", value.last_error.?);
}
