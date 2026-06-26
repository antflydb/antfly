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
const ddl_plan = @import("ddl_plan.zig");

pub const DurableSqlPlan = union(enum) {
    ddl: *ddl_plan.LoweredDdlPlan,
    session: *ddl_plan.SessionCatalogPlan,
    transaction: *binder.TransactionLogicalPlan,
    prepared_statement: *ddl_plan.PreparedStatementPlan,
    cursor: *ddl_plan.CursorPortalPlan,
    notification: *ddl_plan.NotificationChannelPlan,
    routine: *binder.RoutineLogicalPlan,
    auth: *binder.AuthorizationLogicalPlan,
    extension: *ddl_plan.ExtensionCatalogPlan,
    maintenance: *ddl_plan.MaintenanceJobPlan,
    bulk_io: *ddl_plan.BulkIoPlan,

    pub fn fromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlan {
        return switch (logical.*) {
            .ddl => |*plan| .{ .ddl = plan },
            .session => |*plan| .{ .session = plan },
            .transaction => |*plan| .{ .transaction = plan },
            .prepared_statement => |*plan| .{ .prepared_statement = plan },
            .cursor => |*plan| .{ .cursor = plan },
            .notification => |*plan| .{ .notification = plan },
            .routine => |*plan| .{ .routine = plan },
            .auth => |*plan| .{ .auth = plan },
            .extension => |*plan| .{ .extension = plan },
            .maintenance => |*plan| .{ .maintenance = plan },
            .bulk_io => |*plan| .{ .bulk_io = plan },
            else => error.UnsupportedSqlShape,
        };
    }

    pub fn fromDdlPayload(plan: *ddl_plan.LoweredDdlPlan) DurableSqlPlan {
        return .{ .ddl = plan };
    }
};
