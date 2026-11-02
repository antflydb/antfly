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

//! Single-attempt admission and exact durable completion. Once a begin may
//! have crossed the proposal boundary, neither cancellation nor transport loss
//! permits replaying it. Only immutable job observations resolve uncertainty.
const std = @import("std");
const root = @import("../metadata/table_retirement_ledger.zig");
const operation = @import("antfly_local_sources").api_operation;
const Server = @import("http_server.zig").ApiHttpServer;

/// One durable metadata/owner effect per quantum. No volatile plan, name
/// recapture or accepted-receipt cache is needed after failover or restart.
pub fn step(server: *Server, after: u64, context: operation.RequestContext) !u64 {
    return stepInternal(server, after, context, false);
}

pub const TestDriver = if (@import("builtin").is_test) struct {
    /// Interrupt after real owner execution and signed observation, before its
    /// metadata ACK. The ordinary durable discovery feed must recover it cold.
    pub fn loseOwnerReply(server: *Server, after: u64, context: operation.RequestContext) !u64 {
        return stepInternal(server, after, context, true);
    }
} else struct {};

fn stepInternal(server: *Server, after: u64, context: operation.RequestContext, comptime lose_owner_reply: bool) !u64 {
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const page_bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_queue = after });
    const page = try std.json.parseFromSliceLeaky(@import("../metadata/table_retirement_work.zig").Page, a, page_bytes, .{});
    const lookup = page.lookup orelse return page.next;
    const work_bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_owner_work = lookup });
    const work = try std.json.parseFromSliceLeaky(root.Work, a, work_bytes, .{});
    try work.head.validate();
    if (!std.meta.eql(work.head.lookup, lookup)) return error.InvalidCatalogRecord;
    var command: root.Command = undefined;
    switch (work.head.phase) {
        .reserved, .physical_ready => {
            const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_effects = lookup });
            var effects = try @import("../metadata/table_retirement_manifest.zig").Owned.parse(a, bytes);
            defer effects.deinit();
            if (!std.meta.eql(try effects.value.reference(), work.head.effects) or !std.mem.eql(u8, &effects.value.metadata_incarnation, &work.head.incarnation)) return error.InvalidCatalogRecord;
            if (effects.value.root_ranges.len == 0) return error.GenerationAdmissionPending;
            // Elect a stable data-owner coordinator for preparation. Every
            // mutation still revalidates its metadata/data Raft leadership.
            if (server.cfg.relational_maintenance_leadership) |leadership| if (!leadership.isLocalLeader(effects.value.root_ranges[0].group_id)) return page.next;
            if (work.head.phase == .reserved) {
                command = .{ .seal = .{ .lookup = lookup, .expected_revision = work.head.revision, .plan = try @import("fk_generation_plan_builder.zig").buildTableRetirement(server, a, context, lookup.id, effects.value) } };
            } else if (effects.value.survivors.len == 0) {
                command = .{ .publish = .{ .lookup = lookup, .expected_revision = work.head.revision } };
            } else {
                var transport: @import("schema_validation_owner_transport.zig").Transport = .{ .server = server, .alloc = a };
                defer transport.deinit();
                const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
                var prepared = try @import("schema_validation_plan_builder.zig").buildTableRetirement(&transport, a, io, context, work.head, effects.value, if (server.cfg.deployment_mode == .standalone) .native else .raft);
                defer prepared.deinit();
                const accepted = try @import("schema_validation_admission.zig").beginWhenReady(server.source, a, io, context, prepared.command);
                switch (accepted) {
                    .reply => {},
                    .rejected => |err| return err,
                }
                return page.next;
            }
        },
        .retiring => {
            const owner = work.owner orelse return error.InvalidCatalogRecord;
            if (server.cfg.relational_maintenance_leadership) |leadership| if (!leadership.isLocalLeader(owner.group)) return page.next;
            const step_kind: root.physical.Step = @fromBackingInt(@backingInt(work.head.physical.?.phase));
            const proof: root.physical.Evidence = if (step_kind == .source_fenced)
                .{ .source = try server.executeFkGenerationSource(a, work.root_name, owner.group, .{ .plan_id = lookup.id, .child_table_id = lookup.table, .child_table_name = work.root_name, .child_group_id = owner.group, .action = .fence }, context) }
            else
                .{ .parent = try server.executeFkGenerationParent(a, work.owner_name, owner.group, .{ .plan_id = lookup.id, .child_table_id = lookup.table, .child_table_name = work.root_name, .parent_table_id = owner.table, .parent_group_id = owner.group, .action = switch (step_kind) {
                    .parent_staged => .stage,
                    .parent_activated => .activate,
                    .parent_acknowledged, .parent_coverage => .acknowledge,
                    else => unreachable,
                } }, context) };
            if (lose_owner_reply) return error.InjectedRetirementOwnerReplyLoss;
            command = .{ .ack = .{ .lookup = lookup, .expected_revision = work.head.revision, .ordinal = owner.ordinal, .step = step_kind, .evidence = proof } };
        },
        .validating_survivors, .published, .canceled => return page.next,
    }
    const bytes = try command.encodeAlloc(a);
    _ = try server.source.systemCatalog(a, context, .{ .table_retirement_submit = bytes });
    return page.next;
}
