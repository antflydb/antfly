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

//! Private native-root adapter for the metadata feed owner. This is not a public
//! authorization boundary: the coordinator must authorize the immutable feed,
//! verify the complete root set, filter/project images and certify distributed
//! outcomes before issuing any public delivery cursor. No snapshot or Raft
//! capability is advertised by this adapter.
const std = @import("std");
const local = @import("antfly_local_sources").api_local_table_writes;
const root_control = local.table_write_source.history_consumer;
const root_page = local.table_write_source.history_page;
const control = @import("changefeed_control.zig");
const A = std.mem.Allocator;
const Digest = control.Digest;
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub const Adapter = struct {
    source: local.TableWriteSource,
    namespace: control.Namespace,
    table_binding: Digest,
    /// The caller supplies the current authoritative metadata table record.
    /// A table name is a locator and cannot stand in for this incarnation proof.
    pub fn init(source: local.TableWriteSource, table: anytype) !Adapter {
        if (table.storage.engine != .local) return error.ChangefeedCapabilityUnsupported;
        if (source.vtable.native_source_admission_namespace == null or source.vtable.native_history_consumer_control == null or source.vtable.native_history_page == null) return error.ChangefeedCapabilityUnsupported;
        const namespace = try source.nativeSourceAdmissionNamespace();
        if (table.table_id == 0 or table.table_id != std.mem.readInt(u64, namespace[0..8], .big)) return error.ChangefeedConflict;
        return .{ .source = source, .namespace = namespace, .table_binding = control.tableBinding(table) };
    }
    pub fn validateScope(self: *Adapter, state: control.State) !control.Table {
        try state.validate();
        if (!same(self.namespace, try self.source.nativeSourceAdmissionNamespace())) return error.ChangefeedResynchronizationRequired;
        const definition = state.definition orelse return error.ChangefeedConflict;
        // Typed relational publications, lifecycle events and consistent
        // snapshots require their own implemented/qualified root capabilities.
        if (definition.start != .now or definition.events != 1) return error.ChangefeedCapabilityUnsupported;
        const table_id = std.mem.readInt(u64, self.namespace[0..8], .big);
        const table = for (definition.tables) |table| {
            if (table.table_id == table_id) break table;
        } else return error.ChangefeedConflict;
        if (table.contract != .native_document) return error.ChangefeedCapabilityUnsupported;
        if (!same(table.binding, self.table_binding)) return error.ChangefeedResynchronizationRequired;
        return table;
    }
    fn root(self: *Adapter, state: control.State) !control.Root {
        _ = try self.validateScope(state);
        const value = state.roots[try state.rootIndex(self.namespace)];
        if (value.released or value.ownership_epoch != 1 or value.snapshot != null) return error.ChangefeedConflict;
        return value;
    }
    fn reference(self: *Adapter, state: control.State) !root_control.Reference {
        const value = try self.root(state);
        if (value.consumer_epoch == 0) return error.ChangefeedConflict;
        return .{ .namespace = self.namespace, .authority = state.authority, .consumer = state.consumerId(), .epoch = value.consumer_epoch, .grant = value.grant_sequence, .definition = state.definition.?.identity() };
    }
    pub fn plan(self: *Adapter, state: control.State) !control.RootPlan {
        _ = try self.validateScope(state);
        if (state.phase != .enrolling or state.roots.len != 0) return error.ChangefeedConflict;
        return .{ .namespace = self.namespace, .ownership_epoch = 1 };
    }
    pub fn enroll(self: *Adapter, state: control.State) !control.Mutation {
        const table = try self.validateScope(state);
        const value = try self.root(state);
        if (state.phase != .enrolling) return error.ChangefeedConflict;
        const native = (try self.source.nativeHistoryConsumerControl(.{ .enroll = .{
            .namespace = self.namespace,
            .authority = state.authority,
            .sequence = value.grant_sequence,
            .consumer = state.consumerId(),
            .definition = state.definition.?.identity(),
            .expires_at_seconds = state.expires_at,
            .policy_revision = table.policy_revision,
        } })).consumer orelse return error.ChangefeedCorrupt;
        if (value.consumer_epoch != 0 and (value.consumer_epoch != native.epoch or value.start != native.start)) return error.ChangefeedResynchronizationRequired;
        // The original enrollment can have predated a metadata lease extension.
        // Recover its epoch first, then explicitly extend that actual lease.
        if (native.expires_at_seconds < state.expires_at) {
            _ = try self.source.nativeHistoryConsumerControl(.{ .renew = .{ .reference = .{
                .namespace = self.namespace,
                .authority = state.authority,
                .consumer = state.consumerId(),
                .epoch = native.epoch,
                .grant = value.grant_sequence,
                .definition = state.definition.?.identity(),
            }, .previous = native.expires_at_seconds, .next = state.expires_at } });
        }
        return .{ .enroll = .{ .namespace = self.namespace, .ownership_epoch = value.ownership_epoch, .grant_sequence = value.grant_sequence, .consumer_epoch = native.epoch, .start = native.start } };
    }
    pub fn renew(self: *Adapter, state: control.State) !void {
        if (state.phase != .enrolling and state.phase != .streaming) return error.ChangefeedResynchronizationRequired;
        const ref = try self.reference(state);
        const native = (try self.source.nativeHistoryConsumerControl(.{ .inspect = ref })).consumer orelse return error.ChangefeedCorrupt;
        if (native.expires_at_seconds > state.expires_at) return error.ChangefeedConflict;
        if (native.expires_at_seconds < state.expires_at) _ = try self.source.nativeHistoryConsumerControl(.{ .renew = .{ .reference = ref, .previous = native.expires_at_seconds, .next = state.expires_at } });
    }
    /// Native ACK may already have advanced after a lost root reply. Inspect
    /// that progress before applying a newer metadata-authorized target.
    pub fn flushAcknowledged(self: *Adapter, state: control.State) !control.Mutation {
        if (state.phase != .streaming) return error.ChangefeedResynchronizationRequired;
        const value = try self.root(state);
        const ref = try self.reference(state);
        const native = (try self.source.nativeHistoryConsumerControl(.{ .inspect = ref })).consumer orelse return error.ChangefeedCorrupt;
        if (native.acknowledged < value.reclaimed_through or native.acknowledged > value.acknowledged.sequence) return error.ChangefeedConflict;
        _ = try self.source.nativeHistoryConsumerControl(.{ .acknowledge = .{ .reference = ref, .previous = native.acknowledged, .next = value.acknowledged.sequence } });
        return .{ .reclaimed = .{ .namespace = self.namespace, .consumer_epoch = value.consumer_epoch, .previous = value.reclaimed_through, .next = value.acknowledged.sequence } };
    }
    pub fn read(self: *Adapter, a: A, state: control.State, cursor: Digest, now: u64, max_events: u32, max_bytes: u32) !root_page.Page {
        if (state.phase != .streaming) return error.ChangefeedResynchronizationRequired;
        if (now == 0 or now >= state.expires_at) return error.ChangefeedLeaseExpired;
        const ref = try self.reference(state);
        const roots = try state.rootsAt(a, cursor);
        defer a.free(roots);
        const position = roots[try state.rootIndex(self.namespace)].delivered;
        try self.renew(state);
        return self.source.nativeHistoryPage(a, .{ .reference = ref, .after = .{ .sequence = position.sequence, .event = position.event }, .now_seconds = now, .max_events = max_events, .max_bytes = max_bytes });
    }
    pub fn readWithContext(self: *Adapter, a: A, state: control.State, cursor: Digest, now: u64, max_events: u32, max_bytes: u32, request: anytype) !root_page.Page {
        try request.ensureActive();
        var page = try self.read(a, state, cursor, now, max_events, max_bytes);
        errdefer page.deinit();
        try request.ensureActive();
        return page;
    }
    /// Physical collection is bounded and follows the shared native ACK floor,
    /// including after the last consumer has been closed. It returns no rows.
    pub fn collect(self: *Adapter, state: control.State, max_frames: u8) !u8 {
        _ = try self.validateScope(state);
        return (try self.source.nativeHistoryConsumerControl(.{ .collect = .{ .namespace = self.namespace, .authority = state.authority, .max_frames = max_frames } })).reclaimed_frames;
    }
    pub fn retire(self: *Adapter, state: control.State) !control.Mutation {
        const value = try self.root(state);
        if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
        _ = try self.source.nativeHistoryConsumerControl(.{ .retire = .{
            .namespace = self.namespace,
            .authority = state.authority,
            .sequence = state.authority_sequence,
            .consumer = state.consumerId(),
            .definition = state.definition.?.identity(),
            .grant = value.grant_sequence,
        } });
        return .{ .release_root = .{ .namespace = self.namespace, .ownership_epoch = value.ownership_epoch, .proof = control.hash(.{ .domain = "antfly:native-feed-release:v1", .id = state.id, .epoch = state.epoch, .authority = state.authority, .sequence = state.authority_sequence, .namespace = self.namespace, .grant = value.grant_sequence }) } };
    }
};

// The caller captures this complete route vector after authorizing the live
// request. Routed reads never create or renew leases. Their native grant must
// already be durable, and the owner revalidates the route and grant.
pub const Routed = struct {
    const roots = @import("../api/history_roots.zig");
    const Context = @import("antfly_local_sources").api_operation.RequestContext;

    namespace: control.Namespace,
    source: roots.Source,
    captured: *const roots.Captured,

    pub fn init(source: roots.Source, captured: *const roots.Captured) !Routed {
        if (captured.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
        return .{ .namespace = captured.roots[0].namespace, .source = source, .captured = captured };
    }

    pub fn validateScope(self: *Routed, state: control.State) !control.Table {
        try state.validate();
        const definition = state.definition orelse return error.ChangefeedConflict;
        if ((definition.start == .snapshot_then_changes and self.source.bootstrap_pages == null) or definition.events != 1 or definition.tables.len != 1)
            return error.ChangefeedCapabilityUnsupported;
        const table = definition.tables[0];
        if (table.contract != .native_document) return error.ChangefeedCapabilityUnsupported;
        const selected = try self.captured.find(self.namespace);
        if (selected.fence.table_id != table.table_id or !same(selected.table_binding, table.binding))
            return error.ChangefeedResynchronizationRequired;
        if (state.roots.len != 1 or !same(state.roots[0].namespace, self.namespace) or
            state.roots[0].ownership_epoch != 1 or state.roots[0].released)
            return error.ChangefeedResynchronizationRequired;
        return table;
    }

    pub fn readBootstrap(self: *Routed, a: A, state: control.State, after_rows: u32, now: u64, max_rows: u32, max_bytes: u32, request: Context) !root_page.BootstrapPage {
        try request.ensureActive();
        _ = try self.validateScope(state);
        if (state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
        const root = state.roots[0];
        const pin = root.snapshot orelse return error.ChangefeedResynchronizationRequired;
        if (root.snapshot_released or root.consumer_epoch == 0) return error.ChangefeedResynchronizationRequired;
        if (now == 0 or now >= state.expires_at or now >= pin.expires_at) return error.ChangefeedLeaseExpired;
        const selected = try self.captured.find(self.namespace);
        const port = self.source.bootstrap_pages orelse return error.ChangefeedCapabilityUnsupported;
        var page = try port.read(a, selected.fence, selected.table_name, .{
            .version = 1,
            .request = .{
                .reference = .{
                    .namespace = self.namespace,
                    .authority = state.authority,
                    .consumer = state.consumerId(),
                    .epoch = root.consumer_epoch,
                    .grant = root.grant_sequence,
                    .definition = state.definition.?.identity(),
                },
                .digest = pin.digest,
                .now_seconds = now,
                .after_rows = after_rows,
                .max_rows = max_rows,
                .max_bytes = max_bytes,
            },
        }, request);
        errdefer page.deinit();
        try request.ensureActive();
        return page;
    }

    pub fn readWithContext(self: *Routed, a: A, state: control.State, cursor: Digest, now: u64, max_events: u32, max_bytes: u32, request: Context) !root_page.Page {
        try request.ensureActive();
        _ = try self.validateScope(state);
        if (state.phase != .streaming) return error.ChangefeedResynchronizationRequired;
        if (now == 0 or now >= state.expires_at) return error.ChangefeedLeaseExpired;
        const root = state.roots[0];
        if (root.consumer_epoch == 0) return error.ChangefeedResynchronizationRequired;
        const selected = try self.captured.find(self.namespace);
        const positions = try state.rootsAt(a, cursor);
        defer a.free(positions);
        const after = positions[0].delivered;
        var page = try self.source.pages.read(a, selected.fence, selected.table_name, .{
            .version = 1,
            .request = .{
                .reference = .{
                    .namespace = self.namespace,
                    .authority = state.authority,
                    .consumer = state.consumerId(),
                    .epoch = root.consumer_epoch,
                    .grant = root.grant_sequence,
                    .definition = state.definition.?.identity(),
                },
                .after = .{ .sequence = after.sequence, .event = after.event },
                .now_seconds = now,
                .max_events = max_events,
                .max_bytes = max_bytes,
            },
        }, request);
        errdefer page.deinit();
        try request.ensureActive();
        return page;
    }
};
