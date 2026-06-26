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
    table_ddl: binder.TableDdlLogicalPlan,
    catalog_ddl: binder.CatalogDdlLogicalPlan,
    extension: ddl_plan.ExtensionCatalogPlan,

    pub fn deinit(self: *@This(), alloc: @import("std").mem.Allocator) void {
        switch (self.*) {
            .table_ddl => |*plan| plan.deinit(alloc),
            .catalog_ddl => |*plan| plan.deinit(alloc),
            .extension => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }

    pub fn fromLogical(logical: *binder.LogicalSqlPlan) !DurableSqlPlan {
        return switch (logical.*) {
            .table_ddl => |plan| moveLogical(logical, .{ .table_ddl = plan }),
            .catalog_ddl => |plan| moveLogical(logical, .{ .catalog_ddl = plan }),
            .extension => |plan| moveLogical(logical, .{ .extension = plan }),
            else => error.UnsupportedSqlShape,
        };
    }

    fn moveLogical(logical: *binder.LogicalSqlPlan, durable: DurableSqlPlan) DurableSqlPlan {
        logical.* = .{ .other_ddl = .{ .moved = {} } };
        return durable;
    }
};
