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

//! One borrowed route cut for admission's complete owner cohort. Routing is a
//! delivery hint, never authority; each owner independently signs its real cut.
const std = @import("std");
const local = @import("antfly_local_sources");
const router = @import("table_router.zig");
const preparation = @import("schema_validation_preparation.zig");
pub const Transport = struct {
    server: *@import("http_server.zig").ApiHttpServer,
    alloc: std.mem.Allocator,
    groups: []u64 = &.{},
    routes: ?[]router.GroupRoute = null,
    next: usize = 0,
    deadline_ns: ?u64 = null,

    pub fn deinit(self: *@This()) void {
        if (self.routes) |routes| router.freeGroupRoutes(self.alloc, routes);
        self.alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn captureForeignKeys(self: *@This(), a: std.mem.Allocator, before: local.common_topology_records.TableRecord, names: []const []const u8, context: local.api_operation.RequestContext) !local.api_relational_integrity_commit.ForeignKeyValidationSource {
        try context.ensureActive();
        const reader = self.server.table_reads orelse return error.ForeignKeyValidationRequired;
        var snapshot = (try self.server.source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer self.server.source.freeAdminSnapshot(&snapshot);
        const actual = for (snapshot.tables) |table| {
            if (table.table_id == before.table_id) break table;
        } else return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, actual.name, before.name) or !std.mem.eql(u8, actual.schema_json, before.schema_json)) return error.CatalogGenerationChanged;
        return local.api_relational_integrity_commit.captureForeignKeyValidationSource(a, reader, snapshot.tables, before.name, names, context);
    }

    pub fn prepareCohort(self: *@This(), ranges: []const local.common_topology_records.RangeRecord, context: local.api_operation.RequestContext) !void {
        try context.ensureActive();
        if (self.groups.len != 0) return error.InvalidGenerationPublication;
        self.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| preparation.max_budget_ms * std.time.ns_per_ms);
        self.groups = try self.alloc.alloc(u64, ranges.len);
        for (ranges, self.groups) |range, *group| group.* = range.group_id;
        if (self.server.cfg.deployment_mode == .standalone) return;
        if (self.server.cfg.deployment_mode != .distributed) return error.UnsupportedSqlExecution;
    }

    fn refreshRoutes(self: *@This(), table: []const u8, context: local.api_operation.RequestContext) !void {
        var fallback = router.CatalogBackedGroupRouter.init(self.server.catalogSource(), self.server.localSessionNodeId());
        const io = self.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
        const platform = @import("antfly_platform");
        const deadline = self.deadline_ns orelse return error.InvalidGenerationPublication;
        const hosted = (self.server.cfg.session_router orelse fallback.router()).withBudget(.{ .clock = .{ .deadline_ns = deadline }, .cancellation = context.cancellation });
        var retry_ns: u64 = 20 * std.time.ns_per_ms;
        // This phase is read-only: no owner fence or user command exists yet.
        // A newly published owner cohort may briefly precede its route report.
        // Refresh the complete route cut within the existing request budget;
        // never retry submission or retain a partially resolved cohort.
        while (true) {
            try context.ensureActive();
            if (try router.resolveTableGroupRoutes(self.alloc, self.server.catalogSource(), hosted, table, self.groups, .prefer_leader)) |routes| {
                if (self.routes) |previous| router.freeGroupRoutes(self.alloc, previous);
                self.routes = routes;
                return;
            }
            const remaining = deadline -| platform.time.monotonicNs();
            if (remaining == 0) return error.CatalogPublicationProofPending;
            io.sleep(.fromNanoseconds(@min(remaining, retry_ns)), .awake) catch return error.Canceled;
            retry_ns = @min(retry_ns * 2, 250 * std.time.ns_per_ms);
        }
    }

    pub fn prepare(self: *@This(), a: std.mem.Allocator, group: u64, table: []const u8, request: preparation.Request, context: local.api_operation.RequestContext) ![]u8 {
        try context.ensureActive();
        if (self.next >= self.groups.len or self.groups[self.next] != group) return error.InvalidGenerationPublication;
        if (self.routes == null and self.server.cfg.deployment_mode == .distributed) try self.refreshRoutes(table, context);
        const index = self.next;
        self.next += 1;
        const io = self.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
        const deadline = self.deadline_ns orelse return error.InvalidGenerationPublication;
        var bounded = context;
        bounded.deadline_ns = deadline;
        var retry_ns: u64 = 20 * std.time.ns_per_ms;
        while (true) {
            try context.ensureActive();
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.CatalogPublicationProofPending;
            return self.prepareOnce(a, index, group, table, request, bounded) catch |err| switch (@as(anyerror, err)) {
                // Owner preparation is also read-only. In particular, a remote
                // 503 is readiness, never uncertain DDL admission. Preserve the
                // request identity and refresh delivery hints after backoff.
                error.GenerationAdmissionPending, error.SchemaValidationRuntimeRecoveryRequired, error.IndexOwnerReceiptChanged, error.StorageReadTemporarilyUnavailable, error.CatalogRoutingUnavailable, error.NotLeader, error.ReadUnavailable, error.ReadIndexTimeout, error.UnknownGroup => {
                    const remaining = deadline -| @import("antfly_platform").time.monotonicNs();
                    if (remaining == 0) return error.CatalogPublicationProofPending;
                    io.sleep(.fromNanoseconds(@min(remaining, retry_ns)), .awake) catch return error.Canceled;
                    retry_ns = @min(retry_ns * 2, 250 * std.time.ns_per_ms);
                    if (self.server.cfg.deployment_mode == .distributed) try self.refreshRoutes(table, bounded);
                    continue;
                },
                else => return err,
            };
        }
    }

    fn prepareOnce(self: *@This(), a: std.mem.Allocator, index: usize, group: u64, table: []const u8, request: preparation.Request, context: local.api_operation.RequestContext) ![]u8 {
        if (self.routes) |routes| switch (routes[index]) {
            .local => {},
            .remote => |remote| {
                var client = @import("http_client.zig").ApiHttpClient.init(a, self.server.cfg.session_executor orelse return error.HttpExecutorUnavailable);
                _ = client.withInternalServiceAuth(self.server.cfg.internal_service_secret, self.server.cfg.internal_service_issuer);
                var response = try client.fetchSchemaValidationPreparation(remote.base_uri, group, table, request, context);
                defer response.deinit();
                return a.dupe(u8, response.transport.body);
            },
        };
        return (self.server.cfg.schema_validation_preparation orelse return error.GenerationAdmissionPending).prepare(a, group, table, request, context);
    }
};
