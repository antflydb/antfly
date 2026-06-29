// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const metadata_api = @import("snapshot.zig");
const metadata_table_manager = @import("../table_manager.zig");

pub const CatalogSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
        begin_secondary_index_rebuild_range: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest,
        ) anyerror!void = null,
        finish_secondary_index_rebuild_range: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest,
        ) anyerror!void = null,
        invalidate_secondary_index_rebuild_range: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest,
        ) anyerror!void = null,
        begin_schema_rewrite_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SchemaRewriteJobBeginRequest,
        ) anyerror!void = null,
        finish_schema_rewrite_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SchemaRewriteJobFinishRequest,
        ) anyerror!void = null,
        invalidate_schema_rewrite_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.SchemaRewriteJobInvalidateRequest,
        ) anyerror!void = null,
        upsert_table_emptying_job: ?*const fn (
            ptr: *anyopaque,
            record: metadata_table_manager.TableEmptyingJobRecord,
        ) anyerror!void = null,
        upsert_table: ?*const fn (
            ptr: *anyopaque,
            record: metadata_table_manager.TableRecord,
        ) anyerror!void = null,
        apply_table_catalog_update_with_schema_rewrite_jobs: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
        ) anyerror!void = null,
        apply_table_catalog_batch_update_with_schema_rewrite_jobs: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
        ) anyerror!void = null,
        apply_table_catalog_drop_with_schema_rewrite_jobs: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
        ) anyerror!void = null,
        remove_table_emptying_job: ?*const fn (
            ptr: *anyopaque,
            job_id: u64,
        ) anyerror!void = null,
        begin_table_emptying_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingJobBeginRequest,
        ) anyerror!void = null,
        finish_table_emptying_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingJobFinishRequest,
        ) anyerror!void = null,
        invalidate_table_emptying_job: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingJobInvalidateRequest,
        ) anyerror!void = null,
        promote_table_emptying_barrier: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
        ) anyerror!void = null,
        reset_identity_allocators_for_table_emptying_barrier: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest,
        ) anyerror!void = null,
        supports_identity_allocator_reset_for_table_emptying_barrier: ?*const fn (ptr: *anyopaque) bool = null,
        promote_secondary_index_ready: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            expected_generation: u64,
        ) anyerror!bool = null,
        compare_and_swap_table_schema: ?*const fn (
            ptr: *anyopaque,
            request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
        ) anyerror!void = null,
    };

    pub fn adminSnapshot(self: CatalogSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    pub fn beginSecondaryIndexRebuildRange(
        self: CatalogSource,
        request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest,
    ) !void {
        const fn_ptr = self.vtable.begin_secondary_index_rebuild_range orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn finishSecondaryIndexRebuildRange(
        self: CatalogSource,
        request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest,
    ) !void {
        const fn_ptr = self.vtable.finish_secondary_index_rebuild_range orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn invalidateSecondaryIndexRebuildRange(
        self: CatalogSource,
        request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest,
    ) !void {
        const fn_ptr = self.vtable.invalidate_secondary_index_rebuild_range orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn beginSchemaRewriteJob(
        self: CatalogSource,
        request: metadata_table_manager.SchemaRewriteJobBeginRequest,
    ) !void {
        const fn_ptr = self.vtable.begin_schema_rewrite_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn finishSchemaRewriteJob(
        self: CatalogSource,
        request: metadata_table_manager.SchemaRewriteJobFinishRequest,
    ) !void {
        const fn_ptr = self.vtable.finish_schema_rewrite_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn invalidateSchemaRewriteJob(
        self: CatalogSource,
        request: metadata_table_manager.SchemaRewriteJobInvalidateRequest,
    ) !void {
        const fn_ptr = self.vtable.invalidate_schema_rewrite_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn upsertTableEmptyingJob(
        self: CatalogSource,
        record: metadata_table_manager.TableEmptyingJobRecord,
    ) !void {
        const fn_ptr = self.vtable.upsert_table_emptying_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, record);
    }

    pub fn upsertTable(
        self: CatalogSource,
        record: metadata_table_manager.TableRecord,
    ) !void {
        const fn_ptr = self.vtable.upsert_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, record);
    }

    pub fn applyTableCatalogUpdateWithSchemaRewriteJobs(
        self: CatalogSource,
        request: metadata_table_manager.TableCatalogUpdateWithSchemaRewriteJobsRequest,
    ) !void {
        const fn_ptr = self.vtable.apply_table_catalog_update_with_schema_rewrite_jobs orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn applyTableCatalogBatchUpdateWithSchemaRewriteJobs(
        self: CatalogSource,
        request: metadata_table_manager.TableCatalogBatchUpdateWithSchemaRewriteJobsRequest,
    ) !void {
        const fn_ptr = self.vtable.apply_table_catalog_batch_update_with_schema_rewrite_jobs orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn applyTableCatalogDropWithSchemaRewriteJobs(
        self: CatalogSource,
        request: metadata_table_manager.TableCatalogDropWithSchemaRewriteJobsRequest,
    ) !void {
        const fn_ptr = self.vtable.apply_table_catalog_drop_with_schema_rewrite_jobs orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn removeTableEmptyingJob(
        self: CatalogSource,
        job_id: u64,
    ) !void {
        const fn_ptr = self.vtable.remove_table_emptying_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, job_id);
    }

    pub fn beginTableEmptyingJob(
        self: CatalogSource,
        request: metadata_table_manager.TableEmptyingJobBeginRequest,
    ) !void {
        const fn_ptr = self.vtable.begin_table_emptying_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn finishTableEmptyingJob(
        self: CatalogSource,
        request: metadata_table_manager.TableEmptyingJobFinishRequest,
    ) !void {
        const fn_ptr = self.vtable.finish_table_emptying_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn invalidateTableEmptyingJob(
        self: CatalogSource,
        request: metadata_table_manager.TableEmptyingJobInvalidateRequest,
    ) !void {
        const fn_ptr = self.vtable.invalidate_table_emptying_job orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn promoteTableEmptyingBarrier(
        self: CatalogSource,
        request: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
    ) !void {
        const fn_ptr = self.vtable.promote_table_emptying_barrier orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn resetIdentityAllocatorsForTableEmptyingBarrier(
        self: CatalogSource,
        request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest,
    ) !void {
        const fn_ptr = self.vtable.reset_identity_allocators_for_table_emptying_barrier orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn supportsIdentityAllocatorResetForTableEmptyingBarrier(self: CatalogSource) bool {
        if (self.vtable.supports_identity_allocator_reset_for_table_emptying_barrier) |fn_ptr| return fn_ptr(self.ptr);
        return self.vtable.reset_identity_allocators_for_table_emptying_barrier != null;
    }

    pub fn promoteSecondaryIndexReady(
        self: CatalogSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        expected_generation: u64,
    ) !bool {
        const fn_ptr = self.vtable.promote_secondary_index_ready orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, expected_generation);
    }

    pub fn compareAndSwapTableSchema(
        self: CatalogSource,
        request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
    ) !void {
        const fn_ptr = self.vtable.compare_and_swap_table_schema orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }
};

pub fn emptyCatalogSource() CatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = emptyAdminSnapshot,
            .free_admin_snapshot = emptyFreeAdminSnapshot,
        },
    };
}

pub fn unavailableCatalogSource() CatalogSource {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .admin_snapshot = unavailableAdminSnapshot,
            .free_admin_snapshot = emptyFreeAdminSnapshot,
        },
    };
}

fn emptyAdminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
    return .{
        .status = .{
            .metadata_group_id = 0,
            .metrics = .{},
        },
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
}

fn unavailableAdminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
    return error.UnsupportedOperation;
}

fn emptyFreeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
