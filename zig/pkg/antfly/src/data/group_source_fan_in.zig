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

//! Bounded scatter of source covering-index reads. Every task owns its charged
//! arena; only joined immutable results are copied into the caller's arena.
//! Eight in-flight owners bound response memory and avoid serial network RTTs
//! across a table. Publication still checks the final topology after joining.
const std = @import("std");
const native = @import("antfly_local_sources");
const catalog = @import("../api/table_catalog.zig");
const resources = native.storage_resource_manager;
const contract = native.storage_db_group_source_contract;
const reads = native.storage_db_group_source_reads;
const work = native.storage_db_group_source_worker;
const Member = native.storage_db_group_sources.Member;
const A = std.mem.Allocator;
pub fn collect(job: anytype, a: A, definition: work.Definition, partition: native.storage_db_group_source_index.Partition, ordinal: i64, routes: catalog.RoutedSpanSnapshot, io: std.Io, comptime check: anytype) !work.FanIn {
    const Job = @TypeOf(job);
    const Task = struct {
        job: Job,
        definition: work.Definition,
        partition: native.storage_db_group_source_index.Partition,
        ordinal: i64,
        earlier_pending: bool = false,
        routes: catalog.RoutedSpanSnapshot,
        route_index: usize,
        budget: resources.BudgetedAllocator,
        arena: std.heap.ArenaAllocator,
        members: []const Member = &.{},
        cut: contract.SourceCut = undefined,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.read() catch |err| {
                self.failure = if (err == error.OutOfMemory and self.budget.denied()) error.ResourceBudgetExceeded else err;
            };
        }
        fn read(self: *@This()) !void {
            const owned = self.arena.allocator();
            const route = self.routes.routes[self.route_index];
            var source = self.job.server.read_source.source();
            source.route_fence = self.routes.fenceAt(self.route_index);
            var query = contract.Query{ .action = .partition, .artifact = self.definition.artifact, .definition = self.definition.definition, .group = self.partition.group, .segment = self.partition.segment, .cell = self.partition.cell, .ordinal = self.ordinal, .max_bytes = contract.max_partition_bytes };
            var revision: ?u64 = null;
            var members: std.ArrayList(Member) = .empty;
            var bytes: usize = 0;
            while (true) {
                try check(self.job);
                const request = try std.json.Stringify.valueAlloc(owned, query, .{});
                var response = (try source.lookupGroupLocal(owned, route.group_id, self.job.table, "", .{ .group_source_query_json = request, .execution_deadline_ns = self.job.deadline_ns, .cancellation = .fromAtomic(&self.job.server.background_jobs_shutdown) }, .read_index)) orelse return error.NotFound;
                defer response.deinit(owned);
                const page = try std.json.parseFromSliceLeaky(reads.SourcePage, owned, response.json, .{ .allocate = .alloc_always });
                var namespace: [24]u8 = undefined;
                std.mem.writeInt(u64, namespace[0..8], route.identity_namespace.table_id, .big);
                std.mem.writeInt(u64, namespace[8..16], route.identity_namespace.shard_id, .big);
                std.mem.writeInt(u64, namespace[16..24], route.identity_namespace.range_id, .big);
                if (!std.mem.eql(u8, &namespace, &page.owner.namespace)) return error.TopologyChanged;
                self.earlier_pending = self.earlier_pending or page.earlier_pending;
                if (revision) |expected| {
                    if (expected != page.revision) return error.EnrichmentSourceChanged;
                } else revision = page.revision;
                self.cut = .{ .owner = page.owner, .revision = page.revision };
                for (page.members) |member| {
                    bytes += member.document.len + member.text.len;
                    if (bytes > contract.max_partition_bytes or members.items.len == 65536) return error.GroupedSourcePartitionTooLarge;
                    try members.append(owned, member);
                }
                if (page.done) break;
                if (page.cursor.len == 0 or std.mem.eql(u8, query.cursor, page.cursor)) return error.GroupedSourceCorrupt;
                query.cursor = page.cursor;
            }
            self.members = try members.toOwnedSlice(owned);
        }
    };
    var members: std.ArrayList(Member) = .empty;
    const cuts = try a.alloc(contract.SourceCut, routes.routes.len);
    var bytes: usize = 0;
    var offset: usize = 0;
    var earlier_pending = false;
    while (offset < routes.routes.len) {
        try check(job);
        const count = @min(8, routes.routes.len - offset);
        var tasks: [8]Task = undefined;
        var futures: [8]std.Io.Future(void) = undefined;
        var started: usize = 0;
        var joined: usize = 0;
        defer {
            for (futures[joined..started]) |*future| future.await(io);
            for (tasks[0..started]) |*task| {
                task.arena.deinit();
                task.budget.deinit();
            }
        }
        for (0..count) |i| {
            tasks[i] = .{ .job = job, .definition = definition, .partition = partition, .ordinal = ordinal, .routes = routes, .route_index = offset + i, .budget = resources.BudgetedAllocator.init(&job.server.provisioned_storage.resource_manager, .shard_transition_working_set, job.server.alloc, 1), .arena = undefined };
            tasks[i].arena = std.heap.ArenaAllocator.init(tasks[i].budget.allocator());
            futures[i] = io.async(Task.run, .{&tasks[i]});
            started += 1;
        }
        for (0..count) |i| {
            futures[i].await(io);
            joined += 1;
            if (tasks[i].failure) |err| return err;
            cuts[offset + i] = tasks[i].cut;
            earlier_pending = earlier_pending or tasks[i].earlier_pending;
            for (tasks[i].members) |member| {
                bytes += member.document.len + member.text.len;
                if (bytes > contract.max_partition_bytes or members.items.len == 65536) return error.GroupedSourcePartitionTooLarge;
                var copied = member;
                copied.document = try a.dupe(u8, member.document);
                copied.text = try a.dupe(u8, member.text);
                copied.segment = try a.dupe(u8, member.segment);
                try members.append(a, copied);
            }
        }
        offset += count;
    }
    return .{ .earlier_pending = earlier_pending, .members = try members.toOwnedSlice(a), .sources = cuts, .topology_epoch = routes.topology_epoch };
}
