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

//! One streamed reverse pass plus exact point reads per head. Only the current
//! bounded manifest is retained, never all jobs or their catalog definitions.
//! Forward membership proves each required key; reverse counts reject extras.
//! This checks admission/cancellation custody, not physical deletion authority.
const std = @import("std");
const ledger = @import("scope_retirement_ledger.zig");
const wire = @import("scope_retirement_wire.zig");

pub fn verifyDirectory(backing: std.mem.Allocator, reader: anytype, group: u64, rows: anytype, context: anytype, canceled: ?*const std.atomic.Value(bool)) !void {
    var buffer: [320]u8 = undefined;
    var base_buffer: [128]u8 = undefined;
    const base = try ledger.prefix(&base_buffer, group);
    // The sibling member/queue/counter namespaces share this exact stem.
    const stem = base[0 .. base.len - 1];
    var expected: [13]u64 = @splat(0); // body, member, queue, cancellation outcome/link, view edges, children, drain outcome/link
    var received: [13]u64 = @splat(0);
    var heads: u64 = 0;
    var maximum_sequence: u64 = 0;
    var counter: ?u64 = null;
    var callable_members: usize = 0;
    var type_members: usize = 0;
    var removed_columns: RetiredColumnCache = .{ .arena = .init(backing) };
    defer removed_columns.arena.deinit();
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, stem)) continue;
        const suffix = row.key[stem.len..];
        if (std.mem.eql(u8, suffix, "-sequence")) {
            if (counter != null or row.value.len != 8) return error.InvalidScopeRetirement;
            counter = std.mem.readInt(u64, row.value[0..8], .little);
            if (counter.? == 0) return error.InvalidScopeRetirement;
        } else if (std.mem.startsWith(u8, suffix, "-member:")) {
            const lease = try ledger.LeaseRecord.decode(row.value);
            try lease.validate();
            if (lease.lookup.group != group) return error.InvalidScopeRetirement;
            const head = (try ledger.head(reader, lease.lookup)) orelse return error.InvalidScopeRetirement;
            if (!head.active() or !std.mem.eql(u8, &lease.metadata_incarnation, &head.metadata_incarnation)) return error.InvalidScopeRetirement;
            try increment(&received[1], 1);
        } else if (std.mem.startsWith(u8, suffix, "-queue:")) {
            const lookup = try ledger.QueueRecord.decode(row.value);
            try lookup.validate();
            if (lookup.group != group) return error.InvalidScopeRetirement;
            const head = (try ledger.head(reader, lookup)) orelse return error.InvalidScopeRetirement;
            if (!head.active() or !std.mem.eql(u8, row.key, try ledger.queueKey(&buffer, group, head.sequence))) return error.InvalidScopeRetirement;
            try increment(&received[2], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.indexOf(u8, suffix, ":child:") != null) {
            const child = try ledger.ChildRecord.decode(row.value);
            try child.validate();
            if (child.lookup.group != group or !std.mem.eql(u8, row.key, try ledger.childKey(&buffer, child.lookup, child.ordinal))) return error.InvalidScopeRetirement;
            const head = (try ledger.head(reader, child.lookup)) orelse return error.InvalidScopeRetirement;
            if (!head.active() and head.progress.phase != .published) return error.InvalidScopeRetirement;
            try increment(&received[6], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.indexOf(u8, suffix, ":child_outcome:") != null) {
            const query = try wire.QueryRecord.decode(row.value);
            try query.validate();
            const outcome = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeKey(&buffer, query)));
            try outcome.validate();
            if (outcome.result != .accepted or !std.meta.eql(outcome.query, query)) return error.InvalidScopeRetirement;
            const accepted = outcome.result.accepted;
            if (accepted.progress.completed_tables == 0 or
                !std.mem.eql(u8, row.key, try wire.childOutcomeKey(&buffer, accepted.lookup, accepted.progress.completed_tables - 1))) return error.InvalidScopeRetirement;
            try increment(&received[11], 1);
        } else if (std.mem.startsWith(u8, suffix, ":outcome:")) {
            const outcome = try wire.OutcomeRecord.decode(row.value);
            try outcome.validate();
            if (outcome.query.lookup.group != group or !std.mem.eql(u8, row.key, try wire.outcomeKey(&buffer, outcome.query))) return error.InvalidScopeRetirement;
            try (wire.Observation{ .outcome = outcome, .current = try ledger.head(reader, outcome.query.lookup) }).requireFor(outcome.query);
            if (outcome.result == .accepted and outcome.native_operation == .source_lost) {
                const child = (try ledger.child(reader, outcome.query.lookup, outcome.loss_ordinal.?)) orelse return error.InvalidScopeRetirement;
                const checkpoint = (try @import("temporary_relation_retirement.zig").read(reader, child)) orelse return error.InvalidScopeRetirement;
                if (outcome.result.accepted.native_losses == null or outcome.result.accepted.native_losses.?.count < checkpoint.sequence) return error.InvalidScopeRetirement;
                try @import("temporary_relation_retirement.zig").verifySummary(reader, outcome.query.lookup, outcome.result.accepted.native_losses);
            } else if (outcome.result == .accepted) switch (outcome.result.accepted.progress.phase) {
                .draining_connections => if (!std.mem.eql(u8, &outcome.query.nonce, &outcome.query.lookup.reference.id)) return error.InvalidScopeRetirement,
                .canceled, .published => {
                    if (!std.meta.eql(try wire.terminalQuery(reader, outcome.query.lookup), outcome.query)) return error.InvalidScopeRetirement;
                    try increment(&received[3], 1);
                },
                .retiring_tables, .publishing_catalog => {
                    const accepted = outcome.result.accepted;
                    if (accepted.progress.completed_tables == 0) {
                        if (accepted.progress.revision != 2 or !std.meta.eql(try wire.drainQuery(reader, accepted.lookup), outcome.query)) return error.InvalidScopeRetirement;
                        try increment(&received[7], 1);
                    } else {
                        if (!std.meta.eql(try wire.childQuery(reader, accepted.lookup, accepted.progress.completed_tables - 1), outcome.query)) return error.InvalidScopeRetirement;
                        try increment(&received[10], 1);
                    }
                },
            };
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":source_cursor")) {
            const cursor = try ledger.SourceCursorRecord.decode(row.value);
            const head = (try ledger.head(reader, cursor.lookup)) orelse return error.InvalidScopeRetirement;
            if (!std.mem.eql(u8, row.key, try ledger.key(&buffer, head.lookup, .source_cursor)) or !head.force or head.progress.phase != .draining_connections) return error.InvalidScopeRetirement;
            try context.verifySourceCursor(reader, head, cursor);
            try increment(&received[9], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":drain_outcome")) {
            const query = try wire.QueryRecord.decode(row.value);
            try query.validate();
            if (query.lookup.group != group or !std.mem.eql(u8, row.key, try ledger.key(&buffer, query.lookup, .drain_outcome))) return error.InvalidScopeRetirement;
            const head = (try ledger.head(reader, query.lookup)) orelse return error.InvalidScopeRetirement;
            if (head.progress.phase == .draining_connections or head.progress.phase == .canceled) return error.InvalidScopeRetirement;
            try increment(&received[8], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":terminal_outcome")) {
            const query = try wire.QueryRecord.decode(row.value);
            try query.validate();
            if (query.lookup.group != group or !std.mem.eql(u8, row.key, try ledger.key(&buffer, query.lookup, .terminal_outcome))) return error.InvalidScopeRetirement;
            const head = (try ledger.head(reader, query.lookup)) orelse return error.InvalidScopeRetirement;
            if (head.progress.phase != .canceled and head.progress.phase != .published) return error.InvalidScopeRetirement;
            try increment(&received[4], 1);
        } else if (try @import("abandoned_temporary_retirement.zig").verifyAuxiliary(backing, reader, row.key, row.value)) {
            // Individual old participant bodies and scoped completion records
            // remain owned by their original immutable abandonment checkpoint.
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":temporary_abandonment")) {
            var saved_key: [320]u8 = undefined;
            if (row.key.len > saved_key.len) return error.InvalidScopeRetirement;
            @memcpy(saved_key[0..row.key.len], row.key);
            const archive_key = saved_key[0..row.key.len];
            const archive = @import("abandoned_temporary_retirement.zig");
            const checkpoint = try archive.Record.decode(row.value);
            try checkpoint.validate();
            const owned = (try ledger.head(reader, checkpoint.lookup)) orelse return error.InvalidScopeRetirement;
            if (owned.temporary_abandonment == null or checkpoint.lookup.group != group or
                !std.mem.eql(u8, archive_key, try ledger.key(&buffer, checkpoint.lookup, .temporary_abandonment))) return error.InvalidScopeRetirement;
            try archive.verifyHead(backing, reader, owned);
            try increment(&received[12], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":manifest")) {
            // A corresponding head must read this exact canonical key below.
            try increment(&received[0], 1);
        } else if (std.mem.startsWith(u8, suffix, ":") and std.mem.endsWith(u8, suffix, ":head")) {
            const head = try ledger.HeadRecord.decode(row.value);
            try head.validate();
            if (head.lookup.group != group or !std.mem.eql(u8, row.key, try ledger.key(&buffer, head.lookup, .head))) return error.InvalidScopeRetirement;
            try context.verifyHead(reader, head);
            if (head.temporary_abandonment != null) {
                try @import("abandoned_temporary_retirement.zig").verifyHead(backing, reader, head);
                try increment(&expected[12], 1);
            }
            try @import("temporary_relation_retirement.zig").verifyHead(backing, reader, head);
            try increment(&heads, 1);
            maximum_sequence = @max(maximum_sequence, head.sequence);
            // Every admitted head has its original, atomically retained result.
            const admitted = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeNonceKey(&buffer, group, head.lookup.reference.id)));
            try admitted.validate();
            var initial = head;
            initial.progress = .{ .reference = head.lookup.reference };
            initial.native_losses = null;
            if (head.native_loss_requested != (admitted.native_operation == .reserve_lost_temporary)) return error.InvalidScopeRetirement;
            if ((head.temporary_abandonment != null) != (admitted.native_operation == .reserve_abandoned_temporary)) return error.InvalidScopeRetirement;
            if (admitted.result != .accepted or !std.meta.eql(admitted.query.lookup, head.lookup) or
                !std.meta.eql(admitted.result.accepted, initial)) return error.InvalidScopeRetirement;
            // Drainage has its own exact native outcome. No physical child
            // progress is admitted by that receipt or by a journal phase.
            switch (head.progress.phase) {
                .draining_connections => {
                    var expected_head = initial;
                    expected_head.native_losses = head.native_losses;
                    if (!std.meta.eql(head, expected_head)) return error.InvalidScopeRetirement;
                    if (try ledger.sourceCursor(reader, head.lookup)) |cursor| {
                        if (!head.force) return error.InvalidScopeRetirement;
                        try context.verifySourceCursor(reader, head, cursor);
                        try increment(&expected[9], 1);
                    }
                },
                .canceled => {
                    var terminal = initial;
                    terminal.progress = try initial.progress.cancelBeforeEffects();
                    if (!std.meta.eql(head, terminal)) return error.InvalidScopeRetirement;
                    const query = try wire.terminalQuery(reader, head.lookup);
                    const outcome = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeKey(&buffer, query)));
                    if (!std.meta.eql(outcome.query, query) or outcome.result != .accepted or !std.meta.eql(outcome.result.accepted, head)) return error.InvalidScopeRetirement;
                    try increment(&expected[3], 1);
                    try increment(&expected[4], 1);
                },
                .retiring_tables, .publishing_catalog, .published => {
                    var drained = initial;
                    drained.progress = try initial.progress.drained();
                    const query = try wire.drainQuery(reader, head.lookup);
                    const outcome = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeKey(&buffer, query)));
                    try outcome.validate();
                    if (!std.meta.eql(outcome.query, query) or outcome.result != .accepted) return error.InvalidScopeRetirement;
                    drained.native_losses = outcome.result.accepted.native_losses;
                    try @import("temporary_relation_retirement.zig").verifySummary(reader, head.lookup, drained.native_losses);
                    if (!std.meta.eql(outcome.result.accepted, drained) or
                        (head.native_loss_requested and (if (drained.native_losses) |losses| losses.count else 0) != head.lookup.reference.tables)) return error.InvalidScopeRetirement;
                    try context.verifyDrained(reader, head);
                    try increment(&expected[7], 1);
                    try increment(&expected[8], 1);
                    var completed = drained;
                    for (0..head.progress.completed_tables) |ordinal| {
                        completed.progress = try completed.progress.tablePublished(@intCast(ordinal));
                        const child_query = try wire.childQuery(reader, head.lookup, @intCast(ordinal));
                        const child_outcome = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeKey(&buffer, child_query)));
                        try child_outcome.validate();
                        if (!std.meta.eql(child_outcome.query, child_query) or child_outcome.result != .accepted) return error.InvalidScopeRetirement;
                        if (completed.native_losses) |prior| if (child_outcome.result.accepted.native_losses == null or child_outcome.result.accepted.native_losses.?.count < prior.count) return error.InvalidScopeRetirement;
                        completed.native_losses = child_outcome.result.accepted.native_losses;
                        try @import("temporary_relation_retirement.zig").verifySummary(reader, head.lookup, completed.native_losses);
                        if (!std.meta.eql(child_outcome.result.accepted, completed)) return error.InvalidScopeRetirement;
                        try context.verifyChildReady(reader, head, @intCast(ordinal));
                        try increment(&expected[10], 1);
                        try increment(&expected[11], 1);
                    }
                    if (head.progress.phase == .published) {
                        completed.progress = try completed.progress.published(head.progress.catalog_revision, 0);
                        const terminal_query = try wire.terminalQuery(reader, head.lookup);
                        const terminal_outcome = try wire.OutcomeRecord.decode(try reader.get(try wire.outcomeKey(&buffer, terminal_query)));
                        if (terminal_outcome.result != .accepted or !std.meta.eql(terminal_outcome.query, terminal_query) or
                            !std.meta.eql(terminal_outcome.result.accepted, head)) return error.InvalidScopeRetirement;
                        try increment(&expected[3], 1);
                        try increment(&expected[4], 1);
                    }
                    if (completed.native_losses) |prior| if (head.native_losses == null or head.native_losses.?.count < prior.count) return error.InvalidScopeRetirement;
                    completed.native_losses = head.native_losses;
                    if (!std.meta.eql(completed, head)) return error.InvalidScopeRetirement;
                },
            }
            const manifest_key = try ledger.key(&buffer, head.lookup, .manifest);
            const body = reader.get(manifest_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (head.active() or head.progress.phase == .published) {
                var prepared = try ledger.prepare(backing, group, head.lookup.reference.id, body orelse return error.InvalidScopeRetirement);
                defer prepared.deinit();
                if (!std.meta.eql(prepared.lookup, head.lookup) or
                    !std.mem.eql(u8, prepared.encoded, body.?) or
                    !std.mem.eql(u8, &prepared.manifest.metadata_incarnation, &head.metadata_incarnation) or
                    !prepared.manifest.creator.eql(head.creator) or !std.mem.eql(u8, &prepared.manifest.scope.labelsDigest(), &head.target_digest) or
                    !std.meta.eql(head.temporary_owner, if (prepared.manifest.scope.namespace) |namespace| namespace.sql_temporary_owner else null) or prepared.manifest.force != head.force) return error.InvalidScopeRetirement;
                if (head.progress.phase == .published) {
                    try context.verifyPublished(reader, head, prepared.manifest);
                    try increment(&expected[0], 1);
                    try increment(&expected[6], head.lookup.reference.tables);
                    if ((reader.get(try ledger.queueKey(&buffer, group, head.sequence)) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    }) != null) return error.InvalidScopeRetirement;
                    continue;
                }
                try ledger.requireMembers(reader, &prepared, head);
                try context.verifyManifest(reader, head, prepared.manifest);
                for (prepared.manifest.resources) |resource| if (resource.kind == .named_type) {
                    type_members += 1;
                };

                for (prepared.manifest.resources) |resource| if (resource.kind == .routine or resource.kind == .cast) {
                    callable_members += 1;
                };
                for (prepared.manifest.dependencies) |dependency| if (dependency.kind == .routine or dependency.kind == .cast) {
                    callable_members += 1;
                };
                for (prepared.manifest.view_edges) |edge| {
                    const selected = try @import("../system_catalog/views.zig").dependencyKey(&buffer, group, .{ .kind = if (edge.source_kind == .table) .table else .view, .id = edge.source, .column = edge.column }, edge.view);
                    if (!std.mem.eql(u8, try reader.get(selected), &.{1})) return error.InvalidScopeRetirement;
                }
                try increment(&expected[0], 1);
                try increment(&expected[1], 1 + prepared.manifest.resources.len + prepared.manifest.dependencies.len);
                try increment(&expected[2], 1);
                try increment(&expected[5], prepared.manifest.view_edges.len);
                try increment(&expected[6], head.lookup.reference.tables);
            } else {
                if (body != null) return error.InvalidScopeRetirement;
                if ((reader.get(try ledger.queueKey(&buffer, group, head.sequence)) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) != null) return error.InvalidScopeRetirement;
            }
        } else return error.InvalidScopeRetirement;
    }
    try @import("temporary_relation_retirement.zig").verifyDirectory(backing, reader, group, rows, canceled);
    // Inactive catalogs pay no lease lookup per unrelated FK/view edge. While
    // a scope is active, one reverse pass proves no dependent escaped its cut.
    if (expected[1] != 0) {
        var view_buffer: [128]u8 = undefined;
        const view_leases = @import("../system_catalog/view_leases.zig");
        const view_prefix = try view_leases.prefix(&view_buffer, group);
        var sequence_buffer: [128]u8 = undefined;
        const sequence_leases = @import("../system_catalog/sequence_leases.zig");
        const sequence_prefix = try sequence_leases.prefix(&sequence_buffer, group);
        var routine_buffer: [160]u8 = undefined;
        var cast_buffer: [160]u8 = undefined;
        const routine_prefix = try std.fmt.bufPrint(&routine_buffer, "{s}record:routine:", .{base[0 .. base.len - "scope-retirement:".len]});
        const cast_prefix = try std.fmt.bufPrint(&cast_buffer, "{s}record:cast:", .{base[0 .. base.len - "scope-retirement:".len]});
        try rows.reset();
        while (try rows.next()) |row| {
            if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
            // Callable indexes are derived. Prove reverse closure from native
            // resource rows, including external declarations not in a manifest.
            // Only one definition is decoded at a time; table-only scope jobs
            // do not add a parse of every unrelated callable during recovery.
            if (callable_members != 0 and (std.mem.startsWith(u8, row.key, routine_prefix) or std.mem.startsWith(u8, row.key, cast_prefix))) {
                var region = std.heap.ArenaAllocator.init(backing);
                defer region.deinit();
                const a = region.allocator();
                const local = @import("antfly_local_sources");
                const storage = @import("../system_catalog/storage.zig");
                const resource = try std.json.parseFromSliceLeaky(local.system_catalog_domain.Resource, a, row.value, .{});
                if ((resource.kind != .routine and resource.kind != .cast) or !std.mem.eql(u8, row.key, try storage.recordKeyAlloc(a, group, resource.kind, resource.id))) return error.InvalidScopeRetirement;
                var dependencies = try local.sql_catalog.callable_dependencies.Set.init(a, resource);
                defer dependencies.deinit(a);
                const owner = try ledger.member(reader, group, resource.kind, resource.id);
                for (dependencies.uses) |use| {
                    const source = try ledger.member(reader, group, .routine, use.reference.id);
                    if (source != null and source.?.intent == .retire and
                        (owner == null or owner.?.intent != .retire or !std.meta.eql(owner.?.lookup, source.?.lookup))) return error.InvalidScopeRetirement;
                    if (owner != null and owner.?.intent == .retire and
                        (source == null or !std.meta.eql(source.?.lookup, owner.?.lookup))) return error.InvalidScopeRetirement;
                }
            }
            if (type_members != 0) {
                const uses = @import("../system_catalog/named_type_dependencies.zig");
                if (try uses.tableFromKey(row.key, group)) |table| {
                    const ids = try uses.loadAlloc(backing, reader, group, table);
                    defer backing.free(ids);
                    const owner = try ledger.member(reader, group, .table, table);
                    for (ids) |id| if (try ledger.member(reader, group, .named_type, id)) |source| {
                        if (source.intent == .retire and (owner == null or owner.?.intent == .hold or !std.meta.eql(owner.?.lookup, source.lookup))) return error.InvalidScopeRetirement;
                    };
                }
            }
            if (try @import("schema_validation_dependencies.zig").Key.parse(row.key)) |dependency| {
                if (dependency.group == group and dependency.parent != null and
                    try @import("scope_retirement_fence.zig").occupied(reader, group, "table", dependency.parent.?)) return error.InvalidScopeRetirement;
            }
            if (std.mem.startsWith(u8, row.key, view_prefix)) {
                const selected = try view_leases.parse(group, row.key, row.value);
                if (try ledger.member(reader, group, .view, selected.view) != null) return error.InvalidScopeRetirement;
            }
            if (std.mem.startsWith(u8, row.key, sequence_prefix)) {
                const selected = try sequence_leases.parse(group, row.key, row.value);
                if (try ledger.member(reader, group, .sequence, selected.counter) != null) return error.InvalidScopeRetirement;
            }
            const edge = (try @import("../system_catalog/views.zig").dependencyFromKey(row.key, group)) orelse continue;
            const source = try ledger.member(reader, group, if (edge.dependency.kind == .table) .table else .view, edge.dependency.id);
            const owner = try ledger.member(reader, group, .view, edge.view);
            const removed = type_members != 0 and source != null and source.?.intent == .rewrite and edge.dependency.kind == .table and edge.dependency.column != 0 and
                try removed_columns.contains(reader, source.?.lookup, edge.dependency.id, edge.dependency.column);
            const retiring_source = if (source != null and (source.?.intent == .retire or removed)) source.?.lookup else null;
            if (retiring_source) |lookup| if (owner == null or owner.?.intent != .retire or !std.meta.eql(owner.?.lookup, lookup)) return error.InvalidScopeRetirement;
            if (owner != null and owner.?.intent == .retire) {
                if (source == null or !std.meta.eql(owner.?.lookup, source.?.lookup)) return error.InvalidScopeRetirement;
                try increment(&received[5], 1);
            }
        }
    }
    if (!std.meta.eql(expected, received)) return error.InvalidScopeRetirement;
    // Heads and outcomes are retained until a coordinated retention protocol is
    // implemented. A missing terminal head must not silently reset allocation.
    if (heads != (counter orelse 0) or maximum_sequence != (counter orelse 0)) return error.InvalidScopeRetirement;
}

fn increment(value: *u64, count: usize) !void {
    value.* = std.math.add(u64, value.*, count) catch return error.SqlProgramLimitExceeded;
}

/// One immutable root at a time. Ordered view edges reuse its removed-column
/// set; recovery memory never grows with all active scope manifests.
const RetiredColumnCache = struct {
    const Column = struct { table: u64, column: u64 };
    arena: std.heap.ArenaAllocator,
    root: ?ledger.Lookup = null,
    columns: std.AutoHashMapUnmanaged(Column, void) = .empty,
    fn contains(self: *@This(), reader: anytype, lookup: ledger.Lookup, table: u64, column: u64) !bool {
        if (self.root == null or !std.meta.eql(self.root.?, lookup)) {
            _ = self.arena.reset(.retain_capacity);
            self.columns = .empty;
            self.root = null;
            const a = self.arena.allocator();
            const local = @import("antfly_local_sources");
            const Image = struct { dependencies: []const struct { kind: local.system_catalog_domain.Kind, id: u64, action: enum { retire, rewrite, hold }, schema_after: ?[]const u8 = null } = &.{} };
            var buffer: [320]u8 = undefined;
            const image = try std.json.parseFromSliceLeaky(Image, a, try reader.get(try ledger.key(&buffer, lookup, .manifest)), .{ .ignore_unknown_fields = true });
            for (image.dependencies) |dependency| if (dependency.action == .rewrite) {
                if (dependency.kind != .table or dependency.schema_after == null) return error.InvalidScopeRetirement;
                var scratch = std.heap.ArenaAllocator.init(self.arena.child_allocator);
                defer scratch.deinit();
                const region = scratch.allocator();
                const after = try std.json.parseFromSliceLeaky(std.json.Value, region, dependency.schema_after.?, .{});
                const mode = after.object.get("storage_mode") orelse continue;
                if (mode != .string or !std.mem.eql(u8, mode.string, "relational")) continue;
                const properties = try local.sql_catalog.schema_columns.properties(after);
                const attributes: local.system_catalog_column_identities.Store(@TypeOf(reader.*)) = .{ .txn = reader, .group_id = lookup.group };
                const before = (try attributes.load(region, dependency.id)) orelse return error.InvalidScopeRetirement;
                for (before.value.columns) |removed| if (!properties.object.contains(removed.name)) {
                    if (self.columns.count() == 16384) return error.InvalidScopeRetirement;
                    try self.columns.put(a, .{ .table = dependency.id, .column = removed.id }, {});
                };
            };
            self.root = lookup;
        }
        return self.columns.contains(.{ .table = table, .column = column });
    }
};
