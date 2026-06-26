// Copyright 2026 Antfly, Inc.
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

const binder = @import("binder.zig");
const classifier = @import("classifier.zig");
const ddl_plan = @import("ddl_plan.zig");

pub const DurableSqlPlan = union(enum) {
    ddl: ddl_plan.LoweredDdlPlan,
    session: ddl_plan.SessionCatalogPlan,
    transaction: binder.TransactionLogicalPlan,
    prepared_statement: ddl_plan.PreparedStatementPlan,
    cursor: ddl_plan.CursorPortalPlan,
    notification: ddl_plan.NotificationChannelPlan,
    routine: binder.RoutineLogicalPlan,
    auth: binder.AuthorizationLogicalPlan,
    extension: ddl_plan.ExtensionCatalogPlan,
    maintenance: ddl_plan.MaintenanceJobPlan,
    bulk_io: ddl_plan.BulkIoPlan,

    pub fn deinit(self: *@This(), alloc: @import("std").mem.Allocator) void {
        switch (self.*) {
            .ddl => |*plan| plan.deinit(alloc),
            .session => |*plan| plan.deinit(alloc),
            .transaction => |*plan| plan.deinit(alloc),
            .prepared_statement => |*plan| plan.deinit(alloc),
            .cursor => |*plan| plan.deinit(alloc),
            .notification => |*plan| plan.deinit(alloc),
            .routine => |*plan| plan.deinit(alloc),
            .auth => |*plan| plan.deinit(alloc),
            .extension => |*plan| plan.deinit(alloc),
            .maintenance => |*plan| plan.deinit(alloc),
            .bulk_io => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }

    pub fn fromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlan {
        return switch (logical.*) {
            .ddl => |plan| moveLogical(logical, .{ .ddl = plan }),
            .session => |plan| moveLogical(logical, .{ .session = plan }),
            .transaction => |plan| moveLogical(logical, .{ .transaction = plan }),
            .prepared_statement => |plan| moveLogical(logical, .{ .prepared_statement = plan }),
            .cursor => |plan| moveLogical(logical, .{ .cursor = plan }),
            .notification => |plan| moveLogical(logical, .{ .notification = plan }),
            .routine => |plan| moveLogical(logical, .{ .routine = plan }),
            .auth => |plan| moveLogical(logical, .{ .auth = plan }),
            .extension => |plan| moveLogical(logical, .{ .extension = plan }),
            .maintenance => |plan| moveLogical(logical, .{ .maintenance = plan }),
            .bulk_io => |plan| moveLogical(logical, .{ .bulk_io = plan }),
            else => error.UnsupportedSqlShape,
        };
    }

    pub fn fromDdlPayload(plan: *ddl_plan.LoweredDdlPlan) DurableSqlPlan {
        const durable: DurableSqlPlan = .{ .ddl = plan.* };
        plan.* = .{ .adapter_noop = .{ .reason = .transaction_control } };
        return durable;
    }

    fn moveLogical(logical: *binder.LogicalSqlPlan, durable: DurableSqlPlan) DurableSqlPlan {
        logical.* = .{ .read = classifier.SqlReadStatementKind.query };
        return durable;
    }
};
