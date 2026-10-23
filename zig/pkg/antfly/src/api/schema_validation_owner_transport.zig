// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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

    pub fn deinit(self: *@This()) void {
        if (self.routes) |routes| router.freeGroupRoutes(self.alloc, routes);
        self.alloc.free(self.groups);
        self.* = undefined;
    }

    pub fn prepareCohort(self: *@This(), ranges: []const local.common_topology_records.RangeRecord, context: local.api_operation.RequestContext) !void {
        try context.ensureActive();
        if (self.groups.len != 0) return error.InvalidGenerationPublication;
        self.groups = try self.alloc.alloc(u64, ranges.len);
        for (ranges, self.groups) |range, *group| group.* = range.group_id;
        if (self.server.cfg.deployment_mode == .standalone) return;
        if (self.server.cfg.deployment_mode != .distributed) return error.UnsupportedSqlExecution;
        var fallback = router.CatalogBackedGroupRouter.init(self.server.catalogSource(), self.server.localSessionNodeId());
        const hosted = (self.server.cfg.session_router orelse fallback.router()).withBudget(.{ .clock = .{ .deadline_ns = context.deadline_ns }, .cancellation = context.cancellation });
        self.routes = (try router.resolveGroupRoutes(self.alloc, self.server.catalogSource(), hosted, self.groups, .prefer_leader)) orelse return error.GenerationAdmissionPending;
    }

    pub fn prepare(self: *@This(), a: std.mem.Allocator, group: u64, table: []const u8, request: preparation.Request, context: local.api_operation.RequestContext) ![]u8 {
        try context.ensureActive();
        if (self.next >= self.groups.len or self.groups[self.next] != group) return error.InvalidGenerationPublication;
        const index = self.next;
        self.next += 1;
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
