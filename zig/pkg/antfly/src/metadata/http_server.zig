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

const std = @import("std");
const common_secrets = @import("../common/secrets.zig");
const group_ids = @import("../common/group_ids.zig");
const metadata_api = @import("api.zig");
const metadata_admin = @import("admin.zig");
const extension_domain = @import("../extensions/mod.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_table_workflow = @import("table_workflow.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_transition_state = @import("transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const http_common = @import("../raft/transport/http_common.zig");
const backups_api = @import("../api/backups.zig");
const http_route_helpers = @import("../api/http_route_helpers.zig");
const indexes_api = @import("../api/indexes.zig");
const tables_api = @import("../api/tables.zig");
const foreign_mod = @import("../foreign/mod.zig");
const platform_time = @import("../platform/time.zig");
const routes = @import("http_routes.zig");
const service = @import("service.zig");

pub const MetadataHttpServerConfig = struct {};

pub const SplitRequest = struct {
    split_key: []const u8,
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    transition_id: ?u64 = null,
};

pub const MergeRequest = struct {
    donor_group_id: u64,
    receiver_group_id: u64,
    transition_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
};

pub const NodeShutdownRequest = struct {
    type: []const u8 = "remove",
    reason: []const u8 = "",
};

pub const NodeShutdownStoreStatus = struct {
    store_id: u64,
    placement_intent_count: usize = 0,
    group_status_count: usize = 0,
    runtime_group_count: usize = 0,
    local_voter_count: usize = 0,
    local_leader_count: usize = 0,
};

pub const NodeShutdownStatus = struct {
    node_id: u64,
    type: []const u8 = "remove",
    phase: []const u8,
    safe_to_terminate: bool,
    blocked: bool = false,
    blocked_reason: ?[]const u8 = null,
    message: ?[]const u8 = null,
    stores: []const NodeShutdownStoreStatus,
    pending_groups: []const u64,
};

const RestoreExtensionsRequest = struct {
    installed_extensions: []const extension_domain.InstalledExtension = &.{},
    extension_members: []const extension_domain.ExtensionMember = &.{},
    extension_dependencies: []const extension_domain.ExtensionDependency = &.{},
};

pub const ReplaceTableDefinitionRequest = struct {
    expected: metadata_table_manager.TableRecord,
    definition: metadata_table_manager.TableRecord,
};

pub const ReseedExactCutoverResult = struct {
    slot_name: []u8,
    publication_name: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.slot_name);
        alloc.free(self.publication_name);
        self.* = undefined;
    }
};

pub const AdminSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        head: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataHead = null,
        status: *const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataStatus,
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        validate_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) anyerror!bool = null,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
        create_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) anyerror!void = null,
        replace_table_definition: ?*const fn (ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) anyerror!void = null,
        restore_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) anyerror!void = null,
        drop_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) anyerror!void = null,
        update_schema: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) anyerror!void = null,
        create_index: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) anyerror!void = null,
        drop_index: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) anyerror!void = null,
        put_artifact_enrichment: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) anyerror!void = null,
        delete_artifact_enrichment: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) anyerror!void = null,
        upsert_node: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) anyerror!void = null,
        request_node_shutdown: ?*const fn (ptr: *anyopaque, node_id: u64) anyerror!void = null,
        cancel_node_shutdown: ?*const fn (ptr: *anyopaque, node_id: u64) anyerror!void = null,
        finalize_node_shutdown: ?*const fn (ptr: *anyopaque, node_id: u64) anyerror!void = null,
        upsert_store: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) anyerror!void = null,
        report_store_status: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) anyerror!void = null,
        upsert_schema_progress: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.SchemaProgressRecord) anyerror!void = null,
        trigger_reallocate: ?*const fn (ptr: *anyopaque) anyerror!void = null,
        request_split: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: SplitRequest) anyerror!void = null,
        request_merge: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: MergeRequest) anyerror!void = null,
        reseed_replication_source_exact_cutover: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) anyerror!ReseedExactCutoverResult = null,
        install_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.InstallExtensionRequest) anyerror!extension_domain.InstalledExtension = null,
        update_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) anyerror!extension_domain.InstalledExtension = null,
        drop_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) anyerror!void = null,
        enable_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) anyerror!extension_domain.InstalledExtension = null,
        disable_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) anyerror!extension_domain.InstalledExtension = null,
        configure_extension: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) anyerror!extension_domain.InstalledExtension = null,
        restore_extensions: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, installed: []const extension_domain.InstalledExtension, members: []const extension_domain.ExtensionMember, dependencies: []const extension_domain.ExtensionDependency) anyerror!void = null,
        record_json_response_allocation: ?*const fn (ptr: *anyopaque, bytes: usize) void = null,
    };

    pub fn head(self: AdminSource) !metadata_api.MetadataHead {
        if (self.vtable.head) |head_fn| return try head_fn(self.ptr);
        const current_status = try self.status();
        return .{
            .metadata_group_id = current_status.metadata_group_id,
            .metadata_epoch = current_status.metadata_epoch,
        };
    }

    pub fn status(self: AdminSource) !metadata_api.MetadataStatus {
        return try self.vtable.status(self.ptr);
    }

    pub fn adminSnapshot(self: AdminSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn validatePublication(self: AdminSource, contract: metadata_api.CatalogPublicationContract) !bool {
        const validate = self.vtable.validate_publication orelse return error.UnsupportedOperation;
        return try validate(self.ptr, contract);
    }

    pub fn freeAdminSnapshot(self: AdminSource, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    pub fn createTable(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
        const fn_ptr = self.vtable.create_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn replaceTableDefinition(self: AdminSource, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !void {
        const fn_ptr = self.vtable.replace_table_definition orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, expected, replacement);
    }

    pub fn restoreTable(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
        const fn_ptr = self.vtable.restore_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, location_uri, backup_id);
    }

    pub fn dropTable(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const fn_ptr = self.vtable.drop_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn updateSchema(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const fn_ptr = self.vtable.update_schema orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, schema_json);
    }

    pub fn createIndex(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const fn_ptr = self.vtable.create_index orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, index_json);
    }

    pub fn dropIndex(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const fn_ptr = self.vtable.drop_index orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, index_name);
    }

    pub fn putArtifactEnrichment(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
        const fn_ptr = self.vtable.put_artifact_enrichment orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, enrichment_name, enrichment_json);
    }

    pub fn deleteArtifactEnrichment(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) !void {
        const fn_ptr = self.vtable.delete_artifact_enrichment orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, enrichment_name);
    }

    pub fn upsertNode(self: AdminSource, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
        const fn_ptr = self.vtable.upsert_node orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, record);
    }

    pub fn requestNodeShutdown(self: AdminSource, node_id: u64) !void {
        const fn_ptr = self.vtable.request_node_shutdown orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, node_id);
    }

    pub fn cancelNodeShutdown(self: AdminSource, node_id: u64) !void {
        const fn_ptr = self.vtable.cancel_node_shutdown orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, node_id);
    }

    pub fn finalizeNodeShutdown(self: AdminSource, node_id: u64) !void {
        const fn_ptr = self.vtable.finalize_node_shutdown orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, node_id);
    }

    pub fn upsertStore(self: AdminSource, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
        const fn_ptr = self.vtable.upsert_store orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, record);
    }

    pub fn reportStoreStatus(self: AdminSource, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
        const fn_ptr = self.vtable.report_store_status orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, report);
    }

    pub fn upsertSchemaProgress(self: AdminSource, alloc: std.mem.Allocator, record: metadata_table_manager.SchemaProgressRecord) !void {
        const fn_ptr = self.vtable.upsert_schema_progress orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, record);
    }

    pub fn triggerReallocate(self: AdminSource) !void {
        const fn_ptr = self.vtable.trigger_reallocate orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr);
    }

    pub fn requestSplit(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, req: SplitRequest) !void {
        const fn_ptr = self.vtable.request_split orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn requestMerge(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, req: MergeRequest) !void {
        const fn_ptr = self.vtable.request_merge orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn reseedReplicationSourceExactCutover(self: AdminSource, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) !ReseedExactCutoverResult {
        const fn_ptr = self.vtable.reseed_replication_source_exact_cutover orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, source_ordinal);
    }

    pub fn installExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
        const fn_ptr = self.vtable.install_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name, req);
    }

    pub fn updateExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const fn_ptr = self.vtable.update_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name, req);
    }

    pub fn dropExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) !void {
        const fn_ptr = self.vtable.drop_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name, req);
    }

    pub fn enableExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const fn_ptr = self.vtable.enable_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name);
    }

    pub fn disableExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const fn_ptr = self.vtable.disable_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name);
    }

    pub fn configureExtension(self: AdminSource, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const fn_ptr = self.vtable.configure_extension orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, name, req);
    }

    pub fn restoreExtensions(
        self: AdminSource,
        alloc: std.mem.Allocator,
        installed: []const extension_domain.InstalledExtension,
        members: []const extension_domain.ExtensionMember,
        dependencies: []const extension_domain.ExtensionDependency,
    ) !void {
        const fn_ptr = self.vtable.restore_extensions orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, installed, members, dependencies);
    }

    pub fn recordJsonResponseAllocation(self: AdminSource, bytes: usize) void {
        const fn_ptr = self.vtable.record_json_response_allocation orelse return;
        fn_ptr(self.ptr, bytes);
    }

    pub fn fromMetadataService(svc: *service.MetadataService) AdminSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .head = metadataServiceHead,
                .status = metadataServiceStatus,
                .admin_snapshot = metadataServiceAdminSnapshot,
                .validate_publication = metadataServiceValidatePublication,
                .free_admin_snapshot = metadataServiceFreeAdminSnapshot,
                .create_table = metadataServiceCreateTable,
                .replace_table_definition = metadataServiceReplaceTableDefinition,
                .restore_table = metadataServiceRestoreTable,
                .drop_table = metadataServiceDropTable,
                .update_schema = metadataServiceUpdateSchema,
                .create_index = metadataServiceCreateIndex,
                .drop_index = metadataServiceDropIndex,
                .put_artifact_enrichment = metadataServicePutArtifactEnrichment,
                .delete_artifact_enrichment = metadataServiceDeleteArtifactEnrichment,
                .upsert_node = metadataServiceUpsertNode,
                .request_node_shutdown = metadataServiceRequestNodeShutdown,
                .cancel_node_shutdown = metadataServiceCancelNodeShutdown,
                .finalize_node_shutdown = metadataServiceFinalizeNodeShutdown,
                .upsert_store = metadataServiceUpsertStore,
                .report_store_status = metadataServiceReportStoreStatus,
                .upsert_schema_progress = metadataServiceUpsertSchemaProgress,
                .trigger_reallocate = metadataServiceTriggerReallocate,
                .request_split = metadataServiceRequestSplit,
                .request_merge = metadataServiceRequestMerge,
                .reseed_replication_source_exact_cutover = metadataServiceReseedReplicationSourceExactCutover,
                .install_extension = metadataServiceInstallExtension,
                .update_extension = metadataServiceUpdateExtension,
                .drop_extension = metadataServiceDropExtension,
                .enable_extension = metadataServiceEnableExtension,
                .disable_extension = metadataServiceDisableExtension,
                .configure_extension = metadataServiceConfigureExtension,
                .restore_extensions = metadataServiceRestoreExtensions,
                .record_json_response_allocation = metadataServiceRecordJsonResponseAllocation,
            },
        };
    }

    pub fn fromMetadataHttpService(svc: *service.MetadataHttpService) AdminSource {
        return .{
            .ptr = svc,
            .vtable = &.{
                .head = metadataHttpServiceHead,
                .status = metadataHttpServiceStatus,
                .admin_snapshot = metadataHttpServiceAdminSnapshot,
                .validate_publication = metadataHttpServiceValidatePublication,
                .free_admin_snapshot = metadataHttpServiceFreeAdminSnapshot,
                .create_table = metadataHttpServiceCreateTable,
                .replace_table_definition = metadataHttpServiceReplaceTableDefinition,
                .restore_table = metadataHttpServiceRestoreTable,
                .drop_table = metadataHttpServiceDropTable,
                .update_schema = metadataHttpServiceUpdateSchema,
                .create_index = metadataHttpServiceCreateIndex,
                .drop_index = metadataHttpServiceDropIndex,
                .put_artifact_enrichment = metadataHttpServicePutArtifactEnrichment,
                .delete_artifact_enrichment = metadataHttpServiceDeleteArtifactEnrichment,
                .upsert_node = metadataHttpServiceUpsertNode,
                .request_node_shutdown = metadataHttpServiceRequestNodeShutdown,
                .cancel_node_shutdown = metadataHttpServiceCancelNodeShutdown,
                .finalize_node_shutdown = metadataHttpServiceFinalizeNodeShutdown,
                .upsert_store = metadataHttpServiceUpsertStore,
                .report_store_status = metadataHttpServiceReportStoreStatus,
                .upsert_schema_progress = metadataHttpServiceUpsertSchemaProgress,
                .trigger_reallocate = metadataHttpServiceTriggerReallocate,
                .request_split = metadataHttpServiceRequestSplit,
                .request_merge = metadataHttpServiceRequestMerge,
                .reseed_replication_source_exact_cutover = metadataHttpServiceReseedReplicationSourceExactCutover,
                .install_extension = metadataHttpServiceInstallExtension,
                .update_extension = metadataHttpServiceUpdateExtension,
                .drop_extension = metadataHttpServiceDropExtension,
                .enable_extension = metadataHttpServiceEnableExtension,
                .disable_extension = metadataHttpServiceDisableExtension,
                .configure_extension = metadataHttpServiceConfigureExtension,
                .restore_extensions = metadataHttpServiceRestoreExtensions,
                .record_json_response_allocation = metadataHttpServiceRecordJsonResponseAllocation,
            },
        };
    }

    fn metadataServiceHead(ptr: *anyopaque) !metadata_api.MetadataHead {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return svc.head();
    }

    fn metadataServiceStatus(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.status();
    }

    fn metadataServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.adminSnapshot();
    }

    fn metadataServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.ensureLinearizableRead();
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        return contract.matches(&snapshot);
    }

    fn metadataServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        svc.freeAdminSnapshot(snapshot);
    }

    fn flushMetadataServiceMutation(svc: *service.MetadataService) !void {
        _ = svc;
    }

    fn flushMetadataHttpServiceMutation(svc: *service.MetadataHttpService) !void {
        _ = svc;
    }

    fn replaceTableDefinitionOnService(
        svc: anytype,
        expected: metadata_table_manager.TableRecord,
        replacement: metadata_table_manager.TableRecord,
    ) !void {
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const current = findTableByName(&snapshot, replacement.name) orelse return error.TableNotFound;
        if (!metadata_table_manager.tableDefinitionsEqual(current.*, expected) or replacement.table_id != expected.table_id) return error.TableGenerationChanged;
        if (extensionOwnsTableShape(&snapshot, replacement.name)) return error.ExtensionOwnedObject;
        try svc.upsertTable(replacement);
    }

    fn metadataServiceCreateTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        const table = tables_api.deriveTableRecord(table_name, req);
        const ranges = try tables_api.deriveInitialRanges(alloc, table);
        defer {
            for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }
        _ = try workflow.createTableWithRanges(svc, table, ranges);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceReplaceTableDefinition(ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try replaceTableDefinitionOnService(svc, expected, replacement);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceRestoreTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try persistRestoreTableIntent(svc, alloc, table_name, location_uri, backup_id);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceDropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsTableScopedObject(&snapshot, table_name)) return error.ExtensionOwnedObject;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.dropTable(svc, table.table_id);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceUpdateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsTableShape(&snapshot, table_name)) return error.ExtensionOwnedObject;

        const updated = try tables_api.applySchemaUpdateRecord(alloc, table, schema_json);
        defer metadata_table_manager.freeTable(alloc, updated);
        try svc.upsertTable(updated);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceCreateIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsIndex(&snapshot, table_name, index_name)) return error.ExtensionOwnedObject;

        var updated = table.*;
        updated.indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        try svc.upsertTable(updated);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceDropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsIndex(&snapshot, table_name, index_name)) return error.ExtensionOwnedObject;

        const indexes_json = (try indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        try svc.upsertTable(updated);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServicePutArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsEnrichment(&snapshot, table_name, enrichment_name)) return error.ExtensionOwnedObject;

        var updated = table.*;
        updated.indexes_json = try indexes_api.addEnrichmentToTableIndexesJson(alloc, table.indexes_json, enrichment_name, enrichment_json);
        defer alloc.free(updated.indexes_json);
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, updated.indexes_json);
        try svc.upsertTable(updated);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceDeleteArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsEnrichment(&snapshot, table_name, enrichment_name)) return error.ExtensionOwnedObject;

        const indexes_json = (try indexes_api.removeEnrichmentFromTableIndexesJson(alloc, table.indexes_json, enrichment_name)) orelse return error.EnrichmentNotFound;
        defer alloc.free(indexes_json);
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        try svc.upsertTable(updated);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceUpsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeStore(alloc, record);
        try svc.registerStore(record);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceUpsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeNode(alloc, record);
        try svc.registerNode(record);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceRequestNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.requestNodeShutdown(node_id);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceCancelNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.cancelNodeShutdown(node_id);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceFinalizeNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.finalizeNodeShutdown(node_id);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceReportStoreStatus(ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        defer freeStoreStatusReport(alloc, report);
        try svc.reportStoreStatus(report);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceUpsertSchemaProgress(ptr: *anyopaque, _: std.mem.Allocator, record: metadata_table_manager.SchemaProgressRecord) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.upsertSchemaProgress(record);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceTriggerReallocate(ptr: *anyopaque) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.requestReallocation(@intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)));
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceRequestSplit(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: SplitRequest) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const source_group_id = req.source_group_id orelse findRangeForKey(snapshot.ranges, table.table_id, req.split_key) orelse return error.RangeNotFound;
        try group_ids.requireDataGroupId(source_group_id);
        try validateSplitDocIdentityCompatibility(&snapshot, source_group_id);
        const destination_group_id = req.destination_group_id orelse deriveGroupId(table_name, req.split_key, 0x53504c47, source_group_id);
        try group_ids.requireDataGroupId(destination_group_id);
        const transition_id = req.transition_id orelse deriveTransitionId(table_name, req.split_key, 0x53504c54);
        if (findActiveSplitForSource(snapshot.split_transitions, source_group_id)) |active| {
            if (splitRequestMatches(active, transition_id, source_group_id, destination_group_id, req.split_key)) return;
            return error.SplitInProgress;
        }

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.requestSplit(svc, .{
            .transition_id = transition_id,
            .table_id = table.table_id,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .split_key = req.split_key,
        });
        try flushMetadataServiceMutation(svc);
        var committed = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&committed);
        const accepted = findSplitById(committed.split_transitions, transition_id) orelse return error.SplitInProgress;
        if (!splitRequestMatches(accepted, transition_id, source_group_id, destination_group_id, req.split_key)) {
            return error.SplitInProgress;
        }
    }

    fn metadataServiceRequestMerge(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: MergeRequest) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        try group_ids.requireDataGroupId(req.donor_group_id);
        try group_ids.requireDataGroupId(req.receiver_group_id);
        try validateMergeDocIdentityCompatibility(&snapshot, req.donor_group_id, req.receiver_group_id, req.allow_doc_identity_reassignment);

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(svc);
        _ = try workflow.requestMerge(svc, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, table_name, 0x4d524754),
            .table_id = table.table_id,
            .donor_group_id = req.donor_group_id,
            .receiver_group_id = req.receiver_group_id,
            .allow_doc_identity_reassignment = req.allow_doc_identity_reassignment,
        });
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceReseedReplicationSourceExactCutover(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) !ReseedExactCutoverResult {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        svc.cdc_runtime_mutex.lockUncancelable(std.Options.debug_io);
        defer svc.cdc_runtime_mutex.unlock(std.Options.debug_io);
        return try reseedReplicationSourceExactCutoverForService(service.MetadataService, svc, alloc, table_name, source_ordinal, flushMetadataServiceMutation);
    }

    fn metadataServiceInstallExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.installOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceUpdateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.updateOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceDropExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try extension_domain.lifecycle.dropOnService(svc, alloc, name, req);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceEnableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.enableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceDisableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.disableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceConfigureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.configureOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceRestoreExtensions(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        installed: []const extension_domain.InstalledExtension,
        members: []const extension_domain.ExtensionMember,
        dependencies: []const extension_domain.ExtensionDependency,
    ) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try extension_domain.lifecycle.restoreOnService(svc, installed, members, dependencies);
        try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceRecordJsonResponseAllocation(ptr: *anyopaque, bytes: usize) void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        svc.recordJsonResponseAllocation(bytes);
    }

    fn metadataHttpServiceStatus(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.status();
    }

    fn metadataHttpServiceHead(ptr: *anyopaque) !metadata_api.MetadataHead {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return svc.head();
    }

    fn metadataHttpServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.adminSnapshot();
    }

    fn metadataHttpServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        // Followers use Raft's built-in ReadIndex forwarding and wait until
        // the returned committed index is applied locally.
        try svc.ensureLinearizableRead();
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        return contract.matches(&snapshot);
    }

    fn metadataHttpServiceFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        svc.freeAdminSnapshot(snapshot);
    }

    fn metadataHttpServiceCreateTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        const table = tables_api.deriveTableRecord(table_name, req);
        const ranges = try tables_api.deriveInitialRanges(alloc, table);
        defer {
            for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(ranges);
        }
        std.log.info("metadata create table begin table={s} ranges={d}", .{ table_name, ranges.len });
        _ = try workflow.createTableWithRanges(svc, table, ranges);
        std.log.info("metadata create table reconciled table={s}", .{table_name});
        try flushMetadataHttpServiceMutation(svc);
        std.log.info("metadata create table round complete table={s}", .{table_name});
    }

    fn metadataHttpServiceReplaceTableDefinition(ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try replaceTableDefinitionOnService(svc, expected, replacement);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceRestoreTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try persistRestoreTableIntent(svc, alloc, table_name, location_uri, backup_id);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceDropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsTableScopedObject(&snapshot, table_name)) return error.ExtensionOwnedObject;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.dropTable(svc, table.table_id);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceUpdateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsTableShape(&snapshot, table_name)) return error.ExtensionOwnedObject;

        const updated = try tables_api.applySchemaUpdateRecord(alloc, table, schema_json);
        defer metadata_table_manager.freeTable(alloc, updated);
        try svc.upsertTable(updated);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceCreateIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsIndex(&snapshot, table_name, index_name)) return error.ExtensionOwnedObject;

        var updated = table.*;
        updated.indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        try svc.upsertTable(updated);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceDropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsIndex(&snapshot, table_name, index_name)) return error.ExtensionOwnedObject;

        const indexes_json = (try indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        try svc.upsertTable(updated);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServicePutArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsEnrichment(&snapshot, table_name, enrichment_name)) return error.ExtensionOwnedObject;

        var updated = table.*;
        updated.indexes_json = try indexes_api.addEnrichmentToTableIndexesJson(alloc, table.indexes_json, enrichment_name, enrichment_json);
        defer alloc.free(updated.indexes_json);
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, updated.indexes_json);
        try svc.upsertTable(updated);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceDeleteArtifactEnrichment(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (extensionOwnsEnrichment(&snapshot, table_name, enrichment_name)) return error.ExtensionOwnedObject;

        const indexes_json = (try indexes_api.removeEnrichmentFromTableIndexesJson(alloc, table.indexes_json, enrichment_name)) orelse return error.EnrichmentNotFound;
        defer alloc.free(indexes_json);
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, indexes_json);
        var updated = table.*;
        updated.indexes_json = indexes_json;
        try svc.upsertTable(updated);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceUpsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeStore(alloc, record);
        try svc.registerStore(record);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceUpsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeNode(alloc, record);
        try svc.registerNode(record);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceRequestNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.requestNodeShutdown(node_id);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceCancelNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.cancelNodeShutdown(node_id);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceFinalizeNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.finalizeNodeShutdown(node_id);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceReportStoreStatus(ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        defer freeStoreStatusReport(alloc, report);
        try svc.reportStoreStatus(report);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceUpsertSchemaProgress(ptr: *anyopaque, _: std.mem.Allocator, record: metadata_table_manager.SchemaProgressRecord) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.upsertSchemaProgress(record);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceTriggerReallocate(ptr: *anyopaque) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.requestReallocation(@intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)));
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceRequestSplit(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: SplitRequest) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const source_group_id = req.source_group_id orelse findRangeForKey(snapshot.ranges, table.table_id, req.split_key) orelse return error.RangeNotFound;
        try group_ids.requireDataGroupId(source_group_id);
        try validateSplitDocIdentityCompatibility(&snapshot, source_group_id);
        const destination_group_id = req.destination_group_id orelse deriveGroupId(table_name, req.split_key, 0x53504c47, source_group_id);
        try group_ids.requireDataGroupId(destination_group_id);
        const transition_id = req.transition_id orelse deriveTransitionId(table_name, req.split_key, 0x53504c54);
        if (findActiveSplitForSource(snapshot.split_transitions, source_group_id)) |active| {
            if (splitRequestMatches(active, transition_id, source_group_id, destination_group_id, req.split_key)) return;
            return error.SplitInProgress;
        }

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.requestSplit(svc, .{
            .transition_id = transition_id,
            .table_id = table.table_id,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .split_key = req.split_key,
        });
        try flushMetadataHttpServiceMutation(svc);
        var committed = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&committed);
        const accepted = findSplitById(committed.split_transitions, transition_id) orelse return error.SplitInProgress;
        if (!splitRequestMatches(accepted, transition_id, source_group_id, destination_group_id, req.split_key)) {
            return error.SplitInProgress;
        }
    }

    fn metadataHttpServiceRequestMerge(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: MergeRequest) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var snapshot = try svc.adminSnapshot();
        defer svc.freeAdminSnapshot(&snapshot);
        const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        try group_ids.requireDataGroupId(req.donor_group_id);
        try group_ids.requireDataGroupId(req.receiver_group_id);
        try validateMergeDocIdentityCompatibility(&snapshot, req.donor_group_id, req.receiver_group_id, req.allow_doc_identity_reassignment);

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(svc);
        _ = try workflow.requestMerge(svc, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, table_name, 0x4d524754),
            .table_id = table.table_id,
            .donor_group_id = req.donor_group_id,
            .receiver_group_id = req.receiver_group_id,
            .allow_doc_identity_reassignment = req.allow_doc_identity_reassignment,
        });
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceReseedReplicationSourceExactCutover(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) !ReseedExactCutoverResult {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        svc.cdc_runtime_mutex.lockUncancelable(std.Options.debug_io);
        defer svc.cdc_runtime_mutex.unlock(std.Options.debug_io);
        return try reseedReplicationSourceExactCutoverForService(service.MetadataHttpService, svc, alloc, table_name, source_ordinal, flushMetadataHttpServiceMutation);
    }

    fn metadataHttpServiceInstallExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.installOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceUpdateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.updateOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceDropExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try extension_domain.lifecycle.dropOnService(svc, alloc, name, req);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceEnableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.enableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceDisableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.disableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceConfigureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_domain.lifecycle.configureOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceRestoreExtensions(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        installed: []const extension_domain.InstalledExtension,
        members: []const extension_domain.ExtensionMember,
        dependencies: []const extension_domain.ExtensionDependency,
    ) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try extension_domain.lifecycle.restoreOnService(svc, installed, members, dependencies);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceRecordJsonResponseAllocation(ptr: *anyopaque, bytes: usize) void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        svc.recordJsonResponseAllocation(bytes);
    }
};

pub const MetadataHttpServer = struct {
    alloc: std.mem.Allocator,
    cfg: MetadataHttpServerConfig,
    source: AdminSource,

    pub fn init(alloc: std.mem.Allocator, cfg: MetadataHttpServerConfig, source: AdminSource) MetadataHttpServer {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .source = source,
        };
    }

    pub fn executor(self: *MetadataHttpServer) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn deinit(self: *MetadataHttpServer) void {
        self.* = undefined;
    }

    pub fn handle(self: *MetadataHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        return self.handleWithAllocator(self.alloc, req);
    }

    fn handleWithAllocator(self: *MetadataHttpServer, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        _ = self.cfg;
        switch (req.method) {
            .GET => {
                if (routes.Routes.matchInternalNodeShutdown(req.uri)) |node_id| {
                    var snapshot = try self.source.adminSnapshot();
                    defer self.source.freeAdminSnapshot(&snapshot);
                    var status = try buildNodeShutdownStatus(alloc, &snapshot, node_id);
                    defer freeNodeShutdownStatus(alloc, &status);
                    return try jsonResponse(alloc, self.source, status);
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.health)) {
                    return try textResponse(alloc, 200, "ok");
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.head)) {
                    return try jsonResponse(alloc, self.source, try self.source.head());
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.status)) {
                    return try jsonResponse(alloc, self.source, try self.source.status());
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.admin_snapshot)) {
                    var snapshot = try self.source.adminSnapshot();
                    defer self.source.freeAdminSnapshot(&snapshot);
                    return try jsonResponse(alloc, self.source, snapshot);
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.active_transitions)) {
                    var snapshot = try self.source.adminSnapshot();
                    defer self.source.freeAdminSnapshot(&snapshot);
                    var active = try metadata_admin.listActiveTransitions(alloc, &snapshot);
                    defer metadata_admin.freeActiveTransitions(alloc, &active);

                    const Response = struct {
                        split: []const @TypeOf(snapshot.split_transitions[0]),
                        merge: []const @TypeOf(snapshot.merge_transitions[0]),
                    };

                    const split = try cloneValues(alloc, @TypeOf(snapshot.split_transitions[0]), active.split);
                    defer alloc.free(split);
                    const merge = try cloneValues(alloc, @TypeOf(snapshot.merge_transitions[0]), active.merge);
                    defer alloc.free(merge);
                    return try jsonResponse(alloc, self.source, Response{
                        .split = split,
                        .merge = merge,
                    });
                }
                if (routes.Routes.matchTableRanges(req.uri)) |table_id| {
                    var snapshot = try self.source.adminSnapshot();
                    defer self.source.freeAdminSnapshot(&snapshot);
                    const refs = try metadata_admin.listTableRanges(alloc, &snapshot, table_id);
                    defer metadata_admin.freeRangeRefs(alloc, refs);
                    const records = try cloneValues(alloc, @TypeOf(snapshot.ranges[0]), refs);
                    defer alloc.free(records);
                    return try jsonResponse(alloc, self.source, records);
                }
                if (routes.Routes.matchGroupPlacement(req.uri)) |group_id| {
                    var snapshot = try self.source.adminSnapshot();
                    defer self.source.freeAdminSnapshot(&snapshot);
                    const refs = try metadata_admin.listGroupPlacement(alloc, &snapshot, group_id);
                    defer metadata_admin.freePlacementRefs(alloc, refs);
                    const records = try cloneValues(alloc, @TypeOf(snapshot.placement_intents[0]), refs);
                    defer alloc.free(records);
                    return try jsonResponse(alloc, self.source, records);
                }
            },
            .POST => {
                if (std.mem.eql(u8, req.uri, routes.Routes.internal_catalog_publication_check)) {
                    var parsed = std.json.parseFromSlice(metadata_api.CatalogPublicationContract, alloc, req.body, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    }) catch return try textResponse(alloc, 400, "invalid catalog publication contract");
                    defer parsed.deinit();
                    const valid = self.source.validatePublication(parsed.value) catch |err| switch (err) {
                        error.NotLeader, error.ProposalDropped, error.LeaderTransferInProgress, error.MetadataLinearizableReadTimeout => return try notLeaderResponse(alloc),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, if (valid) 204 else 409, "");
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.internal_reallocate)) {
                    self.source.triggerReallocate() catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.internal_extension_restore)) {
                    var parsed = std.json.parseFromSlice(RestoreExtensionsRequest, alloc, req.body, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    }) catch return try textResponse(alloc, 400, "invalid extension restore request");
                    defer parsed.deinit();
                    self.source.restoreExtensions(
                        alloc,
                        parsed.value.installed_extensions,
                        parsed.value.extension_members,
                        parsed.value.extension_dependencies,
                    ) catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.UnsupportedManifestApiVersion,
                        error.UnsupportedPackageKind,
                        error.UnsupportedExtensionScope,
                        error.UnsupportedObjectKindForV1,
                        error.InvalidJsonObject,
                        error.EmptyName,
                        error.InvalidIdentifier,
                        error.MemberTableOutsideScope,
                        => return try textResponse(alloc, 400, "invalid extension restore request"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalExtensionUpdate(req.uri)) |extension_route| {
                    var parsed = std.json.parseFromSlice(extension_domain.UpdateExtensionRequest, alloc, jsonBodyOrEmptyObject(req.body), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch {
                        return try textResponse(alloc, 400, "invalid extension update request");
                    };
                    defer parsed.deinit();
                    var installed = self.source.updateExtension(alloc, extension_route.name, parsed.value) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    defer installed.deinitOwned(alloc);
                    return try jsonResponse(alloc, self.source, installed);
                }
                if (routes.Routes.matchInternalExtensionDrop(req.uri)) |extension_route| {
                    var parsed = std.json.parseFromSlice(extension_domain.DropExtensionRequest, alloc, jsonBodyOrEmptyObject(req.body), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch {
                        return try textResponse(alloc, 400, "invalid extension drop request");
                    };
                    defer parsed.deinit();
                    self.source.dropExtension(alloc, extension_route.name, parsed.value) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalExtensionEnable(req.uri)) |extension_route| {
                    var installed = self.source.enableExtension(alloc, extension_route.name) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    defer installed.deinitOwned(alloc);
                    return try jsonResponse(alloc, self.source, installed);
                }
                if (routes.Routes.matchInternalExtensionDisable(req.uri)) |extension_route| {
                    var installed = self.source.disableExtension(alloc, extension_route.name) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    defer installed.deinitOwned(alloc);
                    return try jsonResponse(alloc, self.source, installed);
                }
                if (routes.Routes.matchInternalExtension(req.uri)) |extension_route| {
                    var parsed = std.json.parseFromSlice(extension_domain.InstallExtensionRequest, alloc, jsonBodyOrEmptyObject(req.body), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch {
                        return try textResponse(alloc, 400, "invalid extension install request");
                    };
                    defer parsed.deinit();
                    var installed = self.source.installExtension(alloc, extension_route.name, parsed.value) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    defer installed.deinitOwned(alloc);
                    return try jsonResponse(alloc, self.source, installed);
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.internal_nodes)) {
                    var node = parseNodeRecord(alloc, req.body) catch return try textResponse(alloc, 400, "invalid node registration request");
                    var node_owned = true;
                    defer if (node_owned) metadata_table_manager.freeNode(alloc, node);
                    var store: ?metadata_table_manager.StoreRecord = null;
                    var store_owned = false;
                    defer if (store_owned) {
                        if (store) |record| metadata_table_manager.freeStore(alloc, record);
                    };
                    if (parseNodeRegistrationIncludesStore(alloc, req.body) catch return try textResponse(alloc, 400, "invalid node registration request")) {
                        store = parseStoreRecord(alloc, req.body) catch return try textResponse(alloc, 400, "invalid node registration request");
                        store_owned = true;
                        if (store.?.node_id != node.node_id or store.?.store_id != node.node_id) return try textResponse(alloc, 400, "store identity must match node identity");
                        try self.preserveExistingStoreDrainIntent(&store.?);
                        if (self.source.vtable.upsert_store == null) return try textResponse(alloc, 405, "unsupported operation");
                    }
                    try self.preserveExistingNodeLifecycle(alloc, &node);
                    if (self.source.vtable.upsert_node == null) return try textResponse(alloc, 405, "unsupported operation");
                    node_owned = false;
                    self.source.upsertNode(alloc, node) catch |err| switch (err) {
                        else => return err,
                    };
                    if (store) |record| {
                        store_owned = false;
                        self.source.upsertStore(alloc, record) catch |err| switch (err) {
                            else => return err,
                        };
                    }
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalNodeStatus(req.uri)) |node_id| {
                    const report = parseNodeStatusReport(alloc, req.body, node_id) catch return try textResponse(alloc, 400, "invalid node status request");
                    self.source.reportStoreStatus(alloc, report) catch |err| switch (err) {
                        error.UnknownStore => return try textResponse(alloc, 404, "node not found"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (std.mem.eql(u8, req.uri, routes.Routes.internal_schema_progress)) {
                    const record = parseSchemaProgressRecord(req.body) catch return try textResponse(alloc, 400, "invalid schema progress request");
                    self.source.upsertSchemaProgress(alloc, record) catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTable(req.uri)) |table| {
                    var create_req = parseCreateTableRequest(alloc, req.body) catch return try textResponse(alloc, 400, "invalid create table request");
                    defer create_req.deinit(alloc);
                    self.source.createTable(alloc, table.table_name, create_req) catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return try textResponse(alloc, 400, "invalid create table request"),
                        else => return err,
                    };
                    return try textResponse(alloc, 201, "created");
                }
                if (routes.Routes.matchInternalTableRestore(req.uri)) |table| {
                    var restore_req = parseInternalTableRestoreRequest(alloc, req.body) catch return try textResponse(alloc, 400, "invalid restore request");
                    defer restore_req.deinit();
                    self.source.restoreTable(alloc, table.table_name, restore_req.value.location, restore_req.value.backup_id) catch |err| {
                        if (backups_api.backupLocationErrorMessage(err)) |msg| {
                            return try textResponse(alloc, 400, msg);
                        }
                        switch (err) {
                            error.TableAlreadyExists => return try textResponse(alloc, 409, "table already exists"),
                            error.InvalidBackupRequest, error.UnsupportedBackupFormat, error.UnsupportedBackupMigrationState => {
                                return try textResponse(alloc, 400, "invalid restore request");
                            },
                            error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                            else => return err,
                        }
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableSplit(req.uri)) |table| {
                    const split_req = parseSplitRequest(alloc, req.body) catch return try textResponse(alloc, 400, "invalid split request");
                    defer alloc.free(split_req.split_key);
                    validateSplitRequestDocIdentity(self.source, table.table_name, split_req) catch |err| switch (err) {
                        error.TableNotFound, error.RangeNotFound => return try textResponse(alloc, 404, "not found"),
                        error.DocIdentityNamespaceMismatch => return try textResponse(alloc, 409, "doc identity namespace mismatch"),
                        else => return try textResponse(alloc, 400, "invalid split request"),
                    };
                    self.source.requestSplit(alloc, table.table_name, split_req) catch |err| switch (err) {
                        error.TableNotFound, error.RangeNotFound => return try textResponse(alloc, 404, "not found"),
                        error.DocIdentityNamespaceMismatch => return try textResponse(alloc, 409, "doc identity namespace mismatch"),
                        error.SplitInProgress, error.ConflictingSplitTransition => return try textResponse(alloc, 409, "split already in progress"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return try textResponse(alloc, 400, "invalid split request"),
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableMerge(req.uri)) |table| {
                    const merge_req = parseMergeRequest(alloc, req.body) catch return try textResponse(alloc, 400, "invalid merge request");
                    validateMergeRequestDocIdentity(self.source, table.table_name, merge_req) catch |err| switch (err) {
                        error.TableNotFound, error.RangeNotFound => return try textResponse(alloc, 404, "not found"),
                        error.DocIdentityNamespaceMismatch => return try textResponse(alloc, 409, "doc identity namespace mismatch"),
                        else => return try textResponse(alloc, 400, "invalid merge request"),
                    };
                    self.source.requestMerge(alloc, table.table_name, merge_req) catch |err| switch (err) {
                        error.TableNotFound, error.RangeNotFound => return try textResponse(alloc, 404, "not found"),
                        error.DocIdentityNamespaceMismatch => return try textResponse(alloc, 409, "doc identity namespace mismatch"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return try textResponse(alloc, 400, "invalid merge request"),
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableReplicationSourceReseedExactCutover(req.uri)) |source_path| {
                    var result = self.source.reseedReplicationSourceExactCutover(alloc, source_path.table_name, source_path.source_ordinal) catch |err| switch (err) {
                        error.TableNotFound, error.UnknownReplicationSource => return try textResponse(alloc, 404, "not found"),
                        error.InvalidReplicationSourceConfig, error.UnsupportedReplicationSource => return try textResponse(alloc, 400, "invalid replication source"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    defer result.deinit(alloc);
                    return .{
                        .status = 202,
                        .content_type = try alloc.dupe(u8, "application/json"),
                        .body = try std.fmt.allocPrint(alloc, "{{\"slot_name\":\"{s}\",\"publication_name\":\"{s}\"}}", .{ result.slot_name, result.publication_name }),
                    };
                }
            },
            .PUT => {
                if (routes.Routes.matchInternalExtensionConfig(req.uri)) |extension_route| {
                    var parsed = std.json.parseFromSlice(extension_domain.ConfigureExtensionRequest, alloc, jsonBodyOrEmptyObject(req.body), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch {
                        return try textResponse(alloc, 400, "invalid extension config request");
                    };
                    defer parsed.deinit();
                    var installed = self.source.configureExtension(alloc, extension_route.name, parsed.value) catch |err| {
                        return try extensionLifecycleErrorResponse(alloc, err);
                    };
                    defer installed.deinitOwned(alloc);
                    return try jsonResponse(alloc, self.source, installed);
                }
                if (routes.Routes.matchInternalNodeShutdown(req.uri)) |node_id| {
                    parseNodeShutdownRequest(alloc, req.body) catch return try textResponse(alloc, 400, "invalid node shutdown request");
                    self.requestNodeShutdown(alloc, node_id) catch |err| switch (err) {
                        error.NodeNotFound => return try textResponse(alloc, 404, "node not found"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableDefinition(req.uri)) |table| {
                    var parsed = std.json.parseFromSlice(ReplaceTableDefinitionRequest, alloc, req.body, .{ .allocate = .alloc_always }) catch {
                        return try textResponse(alloc, 400, "invalid table definition replacement");
                    };
                    defer parsed.deinit();
                    if (!std.mem.eql(u8, parsed.value.definition.name, table.table_name)) {
                        return try textResponse(alloc, 400, "table definition name mismatch");
                    }
                    if (!std.mem.eql(u8, parsed.value.expected.name, table.table_name)) {
                        return try textResponse(alloc, 400, "expected table definition name mismatch");
                    }
                    self.source.replaceTableDefinition(parsed.value.expected, parsed.value.definition) catch |err| switch (err) {
                        error.TableNotFound => return try textResponse(alloc, 404, "table not found"),
                        error.TableGenerationChanged => return try textResponse(alloc, 409, "table generation changed"),
                        error.ExtensionOwnedObject, error.UnsupportedOperation => return try textResponse(alloc, 405, "method not allowed"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableSchema(req.uri)) |table| {
                    self.source.updateSchema(alloc, table.table_name, req.body) catch |err| switch (err) {
                        error.TableNotFound => return try textResponse(alloc, 404, "table not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => return try textResponse(alloc, 400, "invalid schema update request"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableIndex(req.uri)) |table_index| {
                    self.source.createIndex(alloc, table_index.table_name, table_index.index_name, req.body) catch |err| switch (err) {
                        error.TableNotFound => return try textResponse(alloc, 404, "table not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.InvalidTableIndexMetadata, error.InvalidCreateIndexRequest, error.UnsupportedCreateTableRequest => return try textResponse(alloc, 400, "unsupported index configuration"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTableEnrichment(req.uri)) |table_enrichment| {
                    const table_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, table_enrichment.table_name) catch |err| switch (err) {
                        error.InvalidArgument => return try textResponse(alloc, 400, "invalid table name"),
                        else => return err,
                    };
                    defer alloc.free(table_name);
                    const enrichment_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, table_enrichment.enrichment_name) catch |err| switch (err) {
                        error.InvalidArgument => return try textResponse(alloc, 400, "invalid artifact enrichment name"),
                        else => return err,
                    };
                    defer alloc.free(enrichment_name);
                    self.source.putArtifactEnrichment(alloc, table_name, enrichment_name, req.body) catch |err| switch (err) {
                        error.TableNotFound => return try textResponse(alloc, 404, "table not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.InvalidTableIndexMetadata, error.InvalidExtensionEnrichment, error.InvalidEnrichmentConfig, error.ConflictingEnrichmentConfig => return try textResponse(alloc, 400, "unsupported artifact enrichment configuration"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
            },
            .DELETE => {
                if (routes.Routes.matchInternalNodeShutdown(req.uri)) |node_id| {
                    self.cancelNodeShutdown(alloc, node_id) catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalNode(req.uri)) |node_id| {
                    self.finalizeNodeShutdown(alloc, node_id) catch |err| switch (err) {
                        error.ActiveNodeFinalizeRejected => return try textResponse(alloc, 409, "active node cannot be finalized"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 202, "accepted");
                }
                if (routes.Routes.matchInternalTable(req.uri)) |table| {
                    self.source.dropTable(alloc, table.table_name) catch |err| switch (err) {
                        error.TableNotFound => return try textResponse(alloc, 404, "table not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 204, "");
                }
                if (routes.Routes.matchInternalTableIndex(req.uri)) |table_index| {
                    self.source.dropIndex(alloc, table_index.table_name, table_index.index_name) catch |err| switch (err) {
                        error.TableNotFound, error.IndexNotFound => return try textResponse(alloc, 404, "index not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        else => return err,
                    };
                    return try textResponse(alloc, 204, "");
                }
                if (routes.Routes.matchInternalTableEnrichment(req.uri)) |table_enrichment| {
                    const table_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, table_enrichment.table_name) catch |err| switch (err) {
                        error.InvalidArgument => return try textResponse(alloc, 400, "invalid table name"),
                        else => return err,
                    };
                    defer alloc.free(table_name);
                    const enrichment_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, table_enrichment.enrichment_name) catch |err| switch (err) {
                        error.InvalidArgument => return try textResponse(alloc, 400, "invalid artifact enrichment name"),
                        else => return err,
                    };
                    defer alloc.free(enrichment_name);
                    self.source.deleteArtifactEnrichment(alloc, table_name, enrichment_name) catch |err| switch (err) {
                        error.TableNotFound, error.EnrichmentNotFound => return try textResponse(alloc, 404, "artifact enrichment not found"),
                        error.ExtensionOwnedObject => return try textResponse(alloc, 405, "method not allowed"),
                        error.UnsupportedOperation => return try textResponse(alloc, 405, "unsupported operation"),
                        error.InvalidTableIndexMetadata, error.InvalidExtensionEnrichment, error.InvalidEnrichmentConfig, error.ConflictingEnrichmentConfig => return try textResponse(alloc, 400, "unsupported artifact enrichment configuration"),
                        else => return err,
                    };
                    return try textResponse(alloc, 204, "");
                }
            },
        }
        return try textResponse(alloc, 404, "not found");
    }

    fn preserveExistingStoreDrainIntent(self: *MetadataHttpServer, record: *metadata_table_manager.StoreRecord) !void {
        if (record.drain_requested) return;
        var snapshot = try self.source.adminSnapshot();
        defer self.source.freeAdminSnapshot(&snapshot);
        for (snapshot.nodes) |node| {
            if (node.node_id != record.node_id) continue;
            if (!metadata_table_manager.nodeLifecycleActive(node.lifecycle)) {
                record.drain_requested = true;
                return;
            }
            break;
        }
        for (snapshot.stores) |existing| {
            if (existing.store_id != record.store_id) continue;
            record.drain_requested = existing.drain_requested;
            return;
        }
    }

    fn preserveExistingNodeLifecycle(self: *MetadataHttpServer, alloc: std.mem.Allocator, record: *metadata_table_manager.NodeRecord) !void {
        if (!metadata_table_manager.nodeLifecycleActive(record.lifecycle)) return;
        var snapshot = try self.source.adminSnapshot();
        defer self.source.freeAdminSnapshot(&snapshot);
        for (snapshot.nodes) |existing| {
            if (existing.node_id != record.node_id) continue;
            if (metadata_table_manager.nodeLifecycleActive(existing.lifecycle)) return;
            alloc.free(record.lifecycle);
            record.lifecycle = try alloc.dupe(u8, existing.lifecycle);
            return;
        }
    }

    fn shutdownRequestWouldChange(snapshot: *const metadata_api.AdminSnapshot, node_id: u64) bool {
        var node_found = false;
        for (snapshot.nodes) |node| {
            if (node.node_id != node_id) continue;
            node_found = true;
            if (metadata_table_manager.nodeLifecycleActive(node.lifecycle)) return true;
            break;
        }
        if (!node_found) return true;
        for (snapshot.stores) |store| {
            if (store.node_id == node_id and !store.drain_requested) return true;
        }
        return false;
    }

    fn shutdownCancelWouldChange(snapshot: *const metadata_api.AdminSnapshot, node_id: u64) bool {
        for (snapshot.nodes) |node| {
            if (node.node_id == node_id and !metadata_table_manager.nodeLifecycleActive(node.lifecycle)) return true;
        }
        for (snapshot.stores) |store| {
            if (store.node_id == node_id and store.drain_requested) return true;
        }
        return false;
    }

    fn requestNodeShutdown(self: *MetadataHttpServer, alloc: std.mem.Allocator, node_id: u64) !void {
        if (self.source.vtable.request_node_shutdown) |_| {
            try self.source.requestNodeShutdown(node_id);
            self.source.triggerReallocate() catch |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            };
            return;
        }

        if (self.source.vtable.upsert_node == null or self.source.vtable.upsert_store == null) return error.UnsupportedOperation;
        var snapshot = try self.source.adminSnapshot();
        defer self.source.freeAdminSnapshot(&snapshot);

        var changed = false;
        var node_found = false;
        for (snapshot.nodes) |node| {
            if (node.node_id != node_id) continue;
            node_found = true;
            if (std.mem.eql(u8, node.lifecycle, metadata_table_manager.node_lifecycle_draining)) break;

            var updated_node: metadata_table_manager.NodeRecord = undefined;
            {
                var cloned_node = try metadata_table_manager.cloneNode(alloc, node);
                errdefer metadata_table_manager.freeNode(alloc, cloned_node);
                const draining_lifecycle = try alloc.dupe(u8, metadata_table_manager.node_lifecycle_draining);
                alloc.free(cloned_node.lifecycle);
                cloned_node.lifecycle = draining_lifecycle;
                updated_node = cloned_node;
            }
            try self.source.upsertNode(alloc, updated_node);
            changed = true;
            break;
        }
        if (!node_found) {
            var draining_node: metadata_table_manager.NodeRecord = undefined;
            {
                const role = try alloc.dupe(u8, "data");
                errdefer alloc.free(role);
                const lifecycle = try alloc.dupe(u8, metadata_table_manager.node_lifecycle_draining);
                draining_node = .{
                    .node_id = node_id,
                    .role = role,
                    .lifecycle = lifecycle,
                };
            }
            try self.source.upsertNode(alloc, draining_node);
            changed = true;
        }

        for (snapshot.stores) |store| {
            if (store.node_id != node_id) continue;
            if (store.drain_requested) continue;

            var updated = try metadata_table_manager.cloneStore(alloc, store);
            updated.drain_requested = true;
            try self.source.upsertStore(alloc, updated);
            changed = true;
        }

        if (changed) {
            self.source.triggerReallocate() catch |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            };
        }
    }

    fn cancelNodeShutdown(self: *MetadataHttpServer, alloc: std.mem.Allocator, node_id: u64) !void {
        if (self.source.vtable.cancel_node_shutdown) |_| {
            try self.source.cancelNodeShutdown(node_id);
            self.source.triggerReallocate() catch |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            };
            return;
        }

        if (self.source.vtable.upsert_node == null or self.source.vtable.upsert_store == null) return error.UnsupportedOperation;
        var snapshot = try self.source.adminSnapshot();
        defer self.source.freeAdminSnapshot(&snapshot);

        var changed = false;
        for (snapshot.nodes) |node| {
            if (node.node_id != node_id) continue;
            if (metadata_table_manager.nodeLifecycleActive(node.lifecycle)) break;

            var updated_node = try metadata_table_manager.cloneNode(alloc, node);
            var updated_node_owned = true;
            errdefer if (updated_node_owned) metadata_table_manager.freeNode(alloc, updated_node);
            const active_lifecycle = try alloc.dupe(u8, metadata_table_manager.node_lifecycle_active);
            alloc.free(updated_node.lifecycle);
            updated_node.lifecycle = active_lifecycle;
            updated_node_owned = false;
            try self.source.upsertNode(alloc, updated_node);
            changed = true;
            break;
        }

        for (snapshot.stores) |store| {
            if (store.node_id != node_id) continue;
            if (!store.drain_requested) continue;

            var updated = try metadata_table_manager.cloneStore(alloc, store);
            var updated_owned = true;
            errdefer if (updated_owned) metadata_table_manager.freeStore(alloc, updated);
            updated.drain_requested = false;
            updated_owned = false;
            try self.source.upsertStore(alloc, updated);
            changed = true;
        }

        if (changed) {
            self.source.triggerReallocate() catch |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            };
        }
    }

    fn finalizeNodeShutdown(self: *MetadataHttpServer, alloc: std.mem.Allocator, node_id: u64) !void {
        _ = alloc;
        if (self.source.vtable.finalize_node_shutdown) |_| {
            var snapshot = try self.source.adminSnapshot();
            defer self.source.freeAdminSnapshot(&snapshot);
            if (finalizeWouldDeleteActiveNodeOrStore(&snapshot, node_id)) return error.ActiveNodeFinalizeRejected;
            try self.source.finalizeNodeShutdown(node_id);
            self.source.triggerReallocate() catch |err| switch (err) {
                error.UnsupportedOperation => {},
                else => return err,
            };
            return;
        }
        return error.UnsupportedOperation;
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.handleWithAllocator(alloc, req) catch |err| switch (err) {
            error.NotLeader, error.ProposalDropped, error.LeaderTransferInProgress => try notLeaderResponse(alloc),
            else => return err,
        };
    }
};

fn cloneValues(
    alloc: std.mem.Allocator,
    comptime T: type,
    refs: anytype,
) ![]T {
    const out = try alloc.alloc(T, refs.len);
    for (refs, 0..) |record, i| {
        out[i] = record.*;
    }
    return out;
}

const InternalTableRestoreRequest = struct {
    backup_id: []const u8,
    location: []const u8,
};

/// Metadata-to-metadata restore dispatch is an internal control-plane request.
/// It deliberately does not carry a public named connection: the data node
/// resolves the location using its own configured storage authority.
fn parseInternalTableRestoreRequest(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(InternalTableRestoreRequest) {
    const parsed = try std.json.parseFromSlice(InternalTableRestoreRequest, alloc, body, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try backups_api.validateBackupId(parsed.value.backup_id);
    if (parsed.value.location.len == 0 or parsed.value.location.len > 4096) return error.InvalidBackupRequest;
    return parsed;
}

fn buildNodeShutdownStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    node_id: u64,
) !NodeShutdownStatus {
    var stores = std.ArrayListUnmanaged(NodeShutdownStoreStatus).empty;
    errdefer stores.deinit(alloc);
    var pending_groups = std.ArrayListUnmanaged(u64).empty;
    errdefer pending_groups.deinit(alloc);

    var node_known = false;
    var node_draining = false;
    for (snapshot.nodes) |node| {
        if (node.node_id != node_id) continue;
        node_known = true;
        node_draining = !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
        break;
    }

    var placement_total: usize = 0;
    for (snapshot.placement_intents) |intent| {
        if (intent.record.local_node_id != node_id) continue;
        placement_total += 1;
        try appendUniqueU64(alloc, &pending_groups, intent.record.group_id);
    }

    var group_status_total: usize = 0;
    var runtime_group_total: usize = 0;
    var local_voter_total: usize = 0;
    var local_leader_total: usize = 0;
    var store_drain_total: usize = 0;
    var insufficient_shard_voters = false;

    for (snapshot.stores) |store| {
        if (store.node_id != node_id) continue;
        if (store.drain_requested) store_drain_total += 1;
        var store_status = NodeShutdownStoreStatus{ .store_id = store.store_id };

        for (snapshot.placement_intents) |intent| {
            if (intent.record.local_node_id != node_id) continue;
            if (intent.store_id != 0 and intent.store_id != store.store_id) continue;
            store_status.placement_intent_count += 1;
        }

        for (store.group_statuses) |group_status| {
            store_status.group_status_count += 1;
            group_status_total += 1;
            try appendUniqueU64(alloc, &pending_groups, group_status.group_id);
            if (group_status.local_voter) {
                store_status.local_voter_count += 1;
                local_voter_total += 1;
                if (group_status.voter_count == 1) insufficient_shard_voters = true;
            }
            if (group_status.local_leader) {
                store_status.local_leader_count += 1;
                local_leader_total += 1;
            }
        }

        for (store.runtime_statuses) |runtime_status| {
            if (runtime_status.node_id != 0 and runtime_status.node_id != node_id) continue;
            if (runtime_status.store_id != 0 and runtime_status.store_id != store.store_id) continue;
            store_status.runtime_group_count += 1;
            runtime_group_total += 1;
            try appendUniqueU64(alloc, &pending_groups, runtime_status.group_id);
        }

        try stores.append(alloc, store_status);
    }

    const no_termination_debt = placement_total == 0 and
        group_status_total == 0 and
        runtime_group_total == 0 and
        local_voter_total == 0 and
        local_leader_total == 0;
    const administratively_draining = node_draining or store_drain_total > 0;
    const node_not_found = !node_known and stores.items.len == 0 and placement_total == 0 and group_status_total == 0 and runtime_group_total == 0;
    const blocked_reason: ?[]const u8 = if (administratively_draining and insufficient_shard_voters)
        "InsufficientShardVoters"
    else
        null;
    const blocked = blocked_reason != null;
    const message: ?[]const u8 = if (blocked)
        "Node hosts a shard with no other voters; add or restore another voter before scale-down can complete"
    else
        null;
    const safe_to_terminate = node_not_found or (administratively_draining and no_termination_debt);
    const phase: []const u8 = if (node_not_found)
        "not_found"
    else if (!administratively_draining)
        "active"
    else if (blocked)
        "blocked"
    else if (safe_to_terminate)
        "complete"
    else
        "draining";

    return .{
        .node_id = node_id,
        .phase = phase,
        .safe_to_terminate = safe_to_terminate,
        .blocked = blocked,
        .blocked_reason = blocked_reason,
        .message = message,
        .stores = try stores.toOwnedSlice(alloc),
        .pending_groups = try pending_groups.toOwnedSlice(alloc),
    };
}

fn finalizeWouldDeleteActiveNodeOrStore(snapshot: *const metadata_api.AdminSnapshot, node_id: u64) bool {
    var draining_node = false;
    for (snapshot.nodes) |node| {
        if (node.node_id != node_id) continue;
        if (metadata_table_manager.nodeLifecycleActive(node.lifecycle)) return true;
        draining_node = true;
        break;
    }
    if (draining_node) return false;
    for (snapshot.stores) |store| {
        if (store.node_id == node_id and !store.drain_requested) return true;
    }
    return false;
}

fn freeNodeShutdownStatus(alloc: std.mem.Allocator, status: *NodeShutdownStatus) void {
    alloc.free(status.stores);
    alloc.free(status.pending_groups);
    status.* = undefined;
}

fn appendUniqueU64(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(u64), value: u64) !void {
    if (value == 0) return;
    for (list.items) |existing| {
        if (existing == value) return;
    }
    try list.append(alloc, value);
}

fn parseSplitRequest(alloc: std.mem.Allocator, body: []const u8) !SplitRequest {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSplitRequest,
    };
    const split_key = root.get("split_key") orelse return error.InvalidSplitRequest;
    if (split_key != .string) return error.InvalidSplitRequest;

    return .{
        .split_key = try alloc.dupe(u8, split_key.string),
        .source_group_id = if (root.get("source_group_id")) |value| try parseU64Field(value) else null,
        .destination_group_id = if (root.get("destination_group_id")) |value| try parseU64Field(value) else null,
        .transition_id = if (root.get("transition_id")) |value| try parseU64Field(value) else null,
    };
}

fn parseMergeRequest(alloc: std.mem.Allocator, body: []const u8) !MergeRequest {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidMergeRequest,
    };
    return .{
        .donor_group_id = try parseU64Field(root.get("donor_group_id") orelse return error.InvalidMergeRequest),
        .receiver_group_id = try parseU64Field(root.get("receiver_group_id") orelse return error.InvalidMergeRequest),
        .transition_id = if (root.get("transition_id")) |value| try parseU64Field(value) else null,
        .allow_doc_identity_reassignment = if (root.get("allow_doc_identity_reassignment")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidMergeRequest,
        } else false,
    };
}

fn parseCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !tables_api.CreateTableRequest {
    return try tables_api.parseStoredCreateTableRequest(alloc, body);
}

const RestoreMetadataSpec = struct {
    table: metadata_table_manager.TableRecord,
    ranges: []metadata_table_manager.RangeRecord,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        metadata_table_manager.freeTable(alloc, self.table);
        for (self.ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(self.ranges);
        self.* = undefined;
    }
};

fn loadRestoreMetadataSpec(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    location_uri: []const u8,
    backup_id: []const u8,
    secret_store: ?*common_secrets.FileStore,
) !RestoreMetadataSpec {
    var location = try backups_api.openBackupLocationWithSecrets(alloc, location_uri, secret_store);
    defer location.deinit(alloc);
    var manifest = backups_api.readManifestFromLocation(alloc, &location, backup_id) catch return error.InvalidBackupRequest;
    defer manifest.deinit(alloc);

    const table = backups_api.deriveRestoreTableRecord(alloc, table_name, location_uri, &manifest) catch {
        return error.InvalidBackupRequest;
    };
    errdefer metadata_table_manager.freeTable(alloc, table);
    const ranges = try backups_api.deriveRestoreRanges(alloc, table.table_id, location_uri, &manifest);
    errdefer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    return .{
        .table = table,
        .ranges = ranges,
    };
}

fn serviceSecretStore(service_impl: anytype) ?*common_secrets.FileStore {
    const Ptr = @TypeOf(service_impl);
    const Service = std.meta.Child(Ptr);
    if (comptime @hasField(Service, "secret_store")) {
        return service_impl.secret_store;
    }
    return null;
}

fn persistRestoreTableIntent(service_impl: anytype, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
    var spec = try loadRestoreMetadataSpec(alloc, table_name, location_uri, backup_id, serviceSecretStore(service_impl));
    defer spec.deinit(alloc);

    var snapshot = try service_impl.adminSnapshot();
    defer service_impl.freeAdminSnapshot(&snapshot);
    if (findTableByName(&snapshot, table_name)) |existing| {
        if (!try metadata_table_manager.restoreIntentTopologyCompatible(alloc, existing.*, snapshot.ranges, spec.table, spec.ranges))
            return error.TableAlreadyExists;
    }

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    _ = try workflow.createTableWithRanges(service_impl, spec.table, spec.ranges);
}

const ParsedGroupStatus = struct {
    group_id: u64,
    doc_count: ?u64 = null,
    disk_bytes: ?u64 = null,
    empty: ?bool = null,
    created_at_millis: ?u64 = null,
    updated_at_millis: ?u64 = null,
    local_leader: ?bool = null,
    local_voter: ?bool = null,
    voter_count: ?u16 = null,
    voter_set_known: ?bool = null,
    voter_set_fingerprint: ?metadata_table_manager.VoterSetFingerprint = null,
    joint_consensus: ?bool = null,
    transition_pending: ?bool = null,
    replay_required: ?bool = null,
    replay_caught_up: ?bool = null,
    cutover_ready: ?bool = null,
    reads_ready_after_cutover: ?bool = null,
};

const ParsedRuntimeIndexStatus = struct {
    name: ?[]const u8 = null,
    kind: ?[]const u8 = null,
    doc_count: ?u64 = null,
    term_count: ?u64 = null,
    edge_count: ?u64 = null,
    node_count: ?u64 = null,
    root_node: ?u64 = null,
    coverage_produced_count: ?u64 = null,
    coverage_skipped_count: ?u64 = null,
    coverage_terminal_failed_count: ?u64 = null,
    coverage_generation: ?u64 = null,
    coverage_config_hash: ?u64 = null,
    coverage_identity_ready: ?bool = null,
    coverage_summary_ready: ?bool = null,
    backfill_active: ?bool = null,
    backfill_progress_millis: ?u16 = null,
    replay_applied_sequence: ?u64 = null,
    replay_target_sequence: ?u64 = null,
    replay_catch_up_required: ?bool = null,
};

const ParsedRuntimeGroupStatus = struct {
    table_id: ?u64 = null,
    table_name: ?[]const u8 = null,
    group_id: ?u64 = null,
    store_id: ?u64 = null,
    node_id: ?u64 = null,
    updated_at_ns: ?u64 = null,
    source: ?[]const u8 = null,
    freshness: ?[]const u8 = null,
    topology_generation: ?u64 = null,
    lsm_root_generation: ?u64 = null,
    status_generation: ?u64 = null,
    doc_count: ?u64 = null,
    disk_bytes: ?u64 = null,
    created_at_millis: ?u64 = null,
    index_count: ?u32 = null,
    enrichment_enabled: ?bool = null,
    enrichment_target_sequence: ?u64 = null,
    enrichment_applied_sequence: ?u64 = null,
    enrichment_retrying: ?bool = null,
    enrichment_worker_failed: ?bool = null,
    async_indexing_active: ?bool = null,
    async_startup_active: ?bool = null,
    async_dense_catch_up_active: ?bool = null,
    async_bulk_coalescing_active: ?bool = null,
    doc_identity: ?metadata_table_manager.RuntimeDocIdentityStatusReport = null,
    doc_set_planning: ?metadata_table_manager.RuntimeDocSetPlanningStatusReport = null,
    indexes: ?[]ParsedRuntimeIndexStatus = null,
};

fn parseStoreRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_table_manager.StoreRecord {
    const Parsed = struct {
        store_id: u64,
        node_id: u64,
        api_url: ?[]const u8 = null,
        raft_url: ?[]const u8 = null,
        role: ?[]const u8 = null,
        health_class: ?[]const u8 = null,
        failure_domain: ?[]const u8 = null,
        live: ?bool = null,
        drain_requested: ?bool = null,
        capacity_bytes: ?u64 = null,
        available_bytes: ?u64 = null,
        lease_pressure: ?u32 = null,
        read_load: ?u32 = null,
        write_load: ?u32 = null,
        active_backfills: ?u32 = null,
        backfill_progress_millis: ?u16 = null,
        group_statuses: ?[]ParsedGroupStatus = null,
        runtime_statuses: ?[]ParsedRuntimeGroupStatus = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.store_id == 0 or parsed.value.node_id == 0) return error.InvalidNodeID;
    const group_statuses = try cloneParsedGroupStatuses(alloc, parsed.value.group_statuses orelse &.{});
    errdefer metadata_table_manager.freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneParsedRuntimeGroupStatuses(alloc, parsed.value.runtime_statuses orelse &.{});
    errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    return .{
        .store_id = parsed.value.store_id,
        .node_id = parsed.value.node_id,
        .api_url = try alloc.dupe(u8, parsed.value.api_url orelse ""),
        .raft_url = try alloc.dupe(u8, parsed.value.raft_url orelse ""),
        .role = try alloc.dupe(u8, parsed.value.role orelse "data"),
        .health_class = try alloc.dupe(u8, parsed.value.health_class orelse "healthy"),
        .failure_domain = try alloc.dupe(u8, parsed.value.failure_domain orelse ""),
        .live = parsed.value.live orelse true,
        .drain_requested = parsed.value.drain_requested orelse false,
        .capacity_bytes = parsed.value.capacity_bytes orelse 0,
        .available_bytes = parsed.value.available_bytes orelse 0,
        .lease_pressure = parsed.value.lease_pressure orelse 0,
        .read_load = parsed.value.read_load orelse 0,
        .write_load = parsed.value.write_load orelse 0,
        .active_backfills = parsed.value.active_backfills orelse 0,
        .backfill_progress_millis = parsed.value.backfill_progress_millis orelse 1000,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

fn parseNodeRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_table_manager.NodeRecord {
    const Parsed = struct {
        node_id: u64,
        role: ?[]const u8 = null,
        lifecycle: ?[]const u8 = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.node_id == 0) return error.InvalidNodeID;
    if (parsed.value.lifecycle) |lifecycle| {
        if (!metadata_table_manager.nodeLifecycleActive(lifecycle)) return error.InvalidNodeLifecycle;
    }
    const role = try alloc.dupe(u8, parsed.value.role orelse "data");
    errdefer alloc.free(role);
    return .{
        .node_id = parsed.value.node_id,
        .role = role,
        .lifecycle = try alloc.dupe(u8, parsed.value.lifecycle orelse metadata_table_manager.node_lifecycle_active),
    };
}

fn parseNodeRegistrationIncludesStore(alloc: std.mem.Allocator, body: []const u8) !bool {
    const Parsed = struct {
        store_id: ?u64 = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    return parsed.value.store_id != null;
}

fn parseNodeShutdownRequest(alloc: std.mem.Allocator, body: []const u8) !void {
    if (body.len == 0) return;
    const Parsed = struct {
        type: ?[]const u8 = null,
        reason: ?[]const u8 = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.type) |shutdown_type| {
        if (!std.mem.eql(u8, shutdown_type, "remove")) return error.UnsupportedNodeShutdownType;
    }
}

fn parseNodeStatusReport(alloc: std.mem.Allocator, body: []const u8, node_id: u64) !metadata_table_manager.StoreStatusReport {
    const Parsed = struct {
        store_id: ?u64 = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.store_id) |store_id| {
        if (store_id != node_id) return error.NodeStatusStoreMismatch;
        return try parseStoreStatusReport(alloc, body);
    }

    const report = try parseStoreStatusReportWithDefaultStoreID(alloc, body, node_id);
    errdefer freeStoreStatusReport(alloc, report);
    return report;
}

fn parseStoreStatusReport(alloc: std.mem.Allocator, body: []const u8) !metadata_table_manager.StoreStatusReport {
    return try parseStoreStatusReportWithDefaultStoreID(alloc, body, null);
}

fn parseStoreStatusReportWithDefaultStoreID(alloc: std.mem.Allocator, body: []const u8, default_store_id: ?u64) !metadata_table_manager.StoreStatusReport {
    const Parsed = struct {
        store_id: ?u64 = null,
        live: ?bool = null,
        health_class: ?[]const u8 = null,
        capacity_bytes: ?u64 = null,
        available_bytes: ?u64 = null,
        lease_pressure: ?u32 = null,
        read_load: ?u32 = null,
        write_load: ?u32 = null,
        active_backfills: ?u32 = null,
        backfill_progress_millis: ?u16 = null,
        group_statuses: ?[]ParsedGroupStatus = null,
        runtime_statuses: ?[]ParsedRuntimeGroupStatus = null,
    };

    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    const group_statuses = try cloneParsedGroupStatuses(alloc, parsed.value.group_statuses orelse &.{});
    errdefer metadata_table_manager.freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneParsedRuntimeGroupStatuses(alloc, parsed.value.runtime_statuses orelse &.{});
    errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    const store_id = parsed.value.store_id orelse default_store_id orelse return error.MissingStoreID;
    if (store_id == 0) return error.InvalidNodeID;
    return .{
        .store_id = store_id,
        .live = parsed.value.live orelse true,
        .health_class = try alloc.dupe(u8, parsed.value.health_class orelse "healthy"),
        .capacity_bytes = parsed.value.capacity_bytes orelse 0,
        .available_bytes = parsed.value.available_bytes orelse 0,
        .lease_pressure = parsed.value.lease_pressure orelse 0,
        .read_load = parsed.value.read_load orelse 0,
        .write_load = parsed.value.write_load orelse 0,
        .active_backfills = parsed.value.active_backfills orelse 0,
        .backfill_progress_millis = parsed.value.backfill_progress_millis orelse 1000,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

fn cloneParsedGroupStatuses(
    alloc: std.mem.Allocator,
    parsed_group_statuses: anytype,
) ![]metadata_table_manager.GroupStatusReport {
    const out = try alloc.alloc(metadata_table_manager.GroupStatusReport, parsed_group_statuses.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (parsed_group_statuses, 0..) |parsed, i| {
        out[i] = .{
            .group_id = parsed.group_id,
            .doc_count = parsed.doc_count orelse 0,
            .disk_bytes = parsed.disk_bytes orelse 0,
            .empty = parsed.empty orelse true,
            .created_at_millis = parsed.created_at_millis orelse 0,
            .updated_at_millis = parsed.updated_at_millis orelse 0,
            .local_leader = parsed.local_leader orelse false,
            .local_voter = parsed.local_voter orelse false,
            .voter_count = parsed.voter_count orelse 0,
            .voter_set_known = parsed.voter_set_known orelse false,
            .voter_set_fingerprint = parsed.voter_set_fingerprint orelse [_]u8{0} ** metadata_table_manager.voter_set_fingerprint_len,
            .joint_consensus = parsed.joint_consensus orelse false,
            .transition_pending = parsed.transition_pending orelse false,
            .replay_required = parsed.replay_required orelse false,
            .replay_caught_up = parsed.replay_caught_up orelse false,
            .cutover_ready = parsed.cutover_ready orelse false,
            .reads_ready_after_cutover = parsed.reads_ready_after_cutover orelse false,
        };
        initialized += 1;
    }
    return out;
}

fn cloneParsedRuntimeGroupStatuses(
    alloc: std.mem.Allocator,
    parsed_runtime_statuses: []const ParsedRuntimeGroupStatus,
) ![]metadata_table_manager.RuntimeGroupStatusReport {
    const out = try alloc.alloc(metadata_table_manager.RuntimeGroupStatusReport, parsed_runtime_statuses.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (parsed_runtime_statuses, 0..) |parsed, i| {
        out[i] = try cloneParsedRuntimeGroupStatus(alloc, parsed);
        initialized += 1;
    }
    return out;
}

fn cloneParsedRuntimeGroupStatus(
    alloc: std.mem.Allocator,
    parsed: ParsedRuntimeGroupStatus,
) !metadata_table_manager.RuntimeGroupStatusReport {
    const indexes = try cloneParsedRuntimeIndexStatuses(alloc, parsed.indexes orelse &.{});
    errdefer metadata_table_manager.freeRuntimeIndexStatusReports(alloc, indexes);
    const table_name = try alloc.dupe(u8, parsed.table_name orelse "");
    errdefer alloc.free(table_name);
    const source = try alloc.dupe(u8, parsed.source orelse "unknown");
    errdefer alloc.free(source);
    const freshness = try alloc.dupe(u8, parsed.freshness orelse "unknown");
    errdefer alloc.free(freshness);
    return .{
        .table_id = parsed.table_id orelse 0,
        .table_name = table_name,
        .group_id = parsed.group_id orelse 0,
        .store_id = parsed.store_id orelse 0,
        .node_id = parsed.node_id orelse 0,
        .updated_at_ns = parsed.updated_at_ns orelse 0,
        .source = source,
        .freshness = freshness,
        .topology_generation = parsed.topology_generation orelse 0,
        .lsm_root_generation = parsed.lsm_root_generation orelse 0,
        .status_generation = parsed.status_generation orelse 0,
        .doc_count = parsed.doc_count orelse 0,
        .disk_bytes = parsed.disk_bytes orelse 0,
        .created_at_millis = parsed.created_at_millis orelse 0,
        .index_count = parsed.index_count orelse @intCast(indexes.len),
        .enrichment_enabled = parsed.enrichment_enabled orelse false,
        .enrichment_target_sequence = parsed.enrichment_target_sequence orelse 0,
        .enrichment_applied_sequence = parsed.enrichment_applied_sequence orelse 0,
        .enrichment_retrying = parsed.enrichment_retrying orelse false,
        .enrichment_worker_failed = parsed.enrichment_worker_failed orelse false,
        .async_indexing_active = parsed.async_indexing_active orelse false,
        .async_startup_active = parsed.async_startup_active orelse (parsed.async_indexing_active orelse false),
        .async_dense_catch_up_active = parsed.async_dense_catch_up_active orelse (parsed.async_indexing_active orelse false),
        .async_bulk_coalescing_active = parsed.async_bulk_coalescing_active orelse (parsed.async_indexing_active orelse false),
        .doc_identity = parsed.doc_identity orelse .{},
        .doc_set_planning = parsed.doc_set_planning orelse .{},
        .indexes = indexes,
    };
}

fn cloneParsedRuntimeIndexStatuses(
    alloc: std.mem.Allocator,
    parsed_indexes: []const ParsedRuntimeIndexStatus,
) ![]metadata_table_manager.RuntimeIndexStatusReport {
    const out = try alloc.alloc(metadata_table_manager.RuntimeIndexStatusReport, parsed_indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| metadata_table_manager.freeRuntimeIndexStatusReport(alloc, record);
        if (out.len > 0) alloc.free(out);
    }
    for (parsed_indexes, 0..) |parsed, i| {
        out[i] = try cloneParsedRuntimeIndexStatus(alloc, parsed);
        initialized += 1;
    }
    return out;
}

fn cloneParsedRuntimeIndexStatus(
    alloc: std.mem.Allocator,
    parsed: ParsedRuntimeIndexStatus,
) !metadata_table_manager.RuntimeIndexStatusReport {
    const name = try alloc.dupe(u8, parsed.name orelse "");
    errdefer alloc.free(name);
    const kind = try alloc.dupe(u8, parsed.kind orelse "");
    errdefer alloc.free(kind);
    return .{
        .name = name,
        .kind = kind,
        .doc_count = parsed.doc_count orelse 0,
        .term_count = parsed.term_count orelse 0,
        .edge_count = parsed.edge_count orelse 0,
        .node_count = parsed.node_count orelse 0,
        .root_node = parsed.root_node orelse 0,
        .coverage_produced_count = parsed.coverage_produced_count orelse 0,
        .coverage_skipped_count = parsed.coverage_skipped_count orelse 0,
        .coverage_terminal_failed_count = parsed.coverage_terminal_failed_count orelse 0,
        .coverage_generation = parsed.coverage_generation orelse 0,
        .coverage_config_hash = parsed.coverage_config_hash orelse 0,
        .coverage_identity_ready = parsed.coverage_identity_ready orelse false,
        .coverage_summary_ready = parsed.coverage_summary_ready orelse false,
        .backfill_active = parsed.backfill_active orelse false,
        .backfill_progress_millis = parsed.backfill_progress_millis orelse 0,
        .replay_applied_sequence = parsed.replay_applied_sequence orelse 0,
        .replay_target_sequence = parsed.replay_target_sequence orelse 0,
        .replay_catch_up_required = parsed.replay_catch_up_required orelse false,
    };
}

fn parseSchemaProgressRecord(body: []const u8) !metadata_table_manager.SchemaProgressRecord {
    const parsed = try std.json.parseFromSlice(metadata_table_manager.SchemaProgressRecord, std.heap.page_allocator, body, .{});
    defer parsed.deinit();
    return parsed.value;
}

fn parseU64Field(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |int_value| std.math.cast(u64, int_value) orelse error.InvalidIntegerField,
        else => error.InvalidIntegerField,
    };
}

fn reseedReplicationSourceExactCutoverForService(
    comptime ServiceType: type,
    svc: *ServiceType,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    source_ordinal: u32,
    comptime flushFn: fn (*ServiceType) anyerror!void,
) !ReseedExactCutoverResult {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    var existing = try parseReplicationSourceCleanupAlloc(alloc, table.name, table.replication_sources_json, source_ordinal);
    defer existing.deinit(alloc);
    const updated = try cloneTableWithReseededExactCutoverSource(alloc, table.*, source_ordinal);
    errdefer {
        alloc.free(updated.table.replication_sources_json);
        alloc.free(updated.slot_name);
        alloc.free(updated.publication_name);
    }
    try cleanupReplicationSourceArtifactsForService(ServiceType, svc, alloc, existing);
    try svc.upsertTable(updated.table);
    alloc.free(updated.table.replication_sources_json);
    try svc.removeReplicationSourceStatus(table.table_id, source_ordinal);
    try flushFn(svc);
    return .{
        .slot_name = updated.slot_name,
        .publication_name = updated.publication_name,
    };
}

const ReseededTable = struct {
    table: metadata_table_manager.TableRecord,
    slot_name: []u8,
    publication_name: []u8,
};

const ReplicationSourceCleanup = struct {
    dsn: []u8,
    slot_name: []u8,
    publication_name: []u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.dsn);
        alloc.free(self.slot_name);
        alloc.free(self.publication_name);
        self.* = undefined;
    }
};

fn cloneTableWithReseededExactCutoverSource(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
    source_ordinal: u32,
) !ReseededTable {
    const seed: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    const reseeded = try reseedReplicationSourcesExactCutoverAlloc(alloc, table.name, table.replication_sources_json, source_ordinal, seed);
    errdefer {
        alloc.free(reseeded.replication_sources_json);
        alloc.free(reseeded.slot_name);
        alloc.free(reseeded.publication_name);
    }
    var updated = table;
    updated.replication_sources_json = reseeded.replication_sources_json;
    return .{
        .table = updated,
        .slot_name = reseeded.slot_name,
        .publication_name = reseeded.publication_name,
    };
}

const ReseededReplicationSources = struct {
    replication_sources_json: []u8,
    slot_name: []u8,
    publication_name: []u8,
};

fn reseedReplicationSourcesExactCutoverAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    replication_sources_json: []const u8,
    source_ordinal: u32,
    seed: u64,
) !ReseededReplicationSources {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, arena_alloc, replication_sources_json, .{});
    const items = switch (parsed.value) {
        .array => |array| array.items,
        else => return error.InvalidReplicationSourceConfig,
    };
    if (source_ordinal >= items.len) return error.UnknownReplicationSource;
    if (items[source_ordinal] != .object) return error.InvalidReplicationSourceConfig;

    const source = &items[source_ordinal].object;
    const type_value = source.get("type") orelse return error.InvalidReplicationSourceConfig;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "postgres")) return error.UnsupportedReplicationSource;
    const postgres_table_value = source.get("postgres_table") orelse return error.InvalidReplicationSourceConfig;
    if (postgres_table_value != .string) return error.InvalidReplicationSourceConfig;

    const slot_name = try deriveFreshPostgresIdentifierAlloc(alloc, "antfly", table_name, postgres_table_value.string, source_ordinal, seed);
    errdefer alloc.free(slot_name);
    const publication_name = try deriveFreshPostgresIdentifierAlloc(alloc, "antfly_pub", table_name, postgres_table_value.string, source_ordinal, seed);
    errdefer alloc.free(publication_name);

    try source.put(arena_alloc, "slot_name", .{ .string = try arena_alloc.dupe(u8, slot_name) });
    try source.put(arena_alloc, "publication_name", .{ .string = try arena_alloc.dupe(u8, publication_name) });
    try source.put(arena_alloc, "require_exact_cutover", .{ .bool = true });

    return .{
        .replication_sources_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{}),
        .slot_name = slot_name,
        .publication_name = publication_name,
    };
}

fn cleanupReplicationSourceArtifactsForService(
    comptime ServiceType: type,
    svc: *ServiceType,
    alloc: std.mem.Allocator,
    cleanup: ReplicationSourceCleanup,
) !void {
    if (!@hasField(ServiceType, "cdc_backfill_registry")) return error.UnsupportedOperation;
    const config = foreign_mod.Config{
        .kind = .postgres,
        .dsn = try alloc.dupe(u8, cleanup.dsn),
    };
    var source = try svc.cdc_backfill_registry.create(alloc, config);
    defer source.deinit(alloc);
    var params = foreign_mod.ReplicationCleanupParams{
        .slot_name = try alloc.dupe(u8, cleanup.slot_name),
        .publication_name = try alloc.dupe(u8, cleanup.publication_name),
    };
    defer params.deinit(alloc);
    try source.cleanupReplication(alloc, params);
}

fn parseReplicationSourceCleanupAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    replication_sources_json: []const u8,
    source_ordinal: u32,
) !ReplicationSourceCleanup {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidReplicationSourceConfig;
    if (source_ordinal >= parsed.value.array.items.len) return error.UnknownReplicationSource;
    const source = parsed.value.array.items[source_ordinal];
    if (source != .object) return error.InvalidReplicationSourceConfig;

    const type_value = source.object.get("type") orelse return error.InvalidReplicationSourceConfig;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "postgres")) return error.UnsupportedReplicationSource;
    const dsn_value = source.object.get("dsn") orelse return error.InvalidReplicationSourceConfig;
    if (dsn_value != .string) return error.InvalidReplicationSourceConfig;
    const postgres_table_value = source.object.get("postgres_table") orelse return error.InvalidReplicationSourceConfig;
    if (postgres_table_value != .string) return error.InvalidReplicationSourceConfig;

    return .{
        .dsn = try alloc.dupe(u8, dsn_value.string),
        .slot_name = if (source.object.get("slot_name")) |value|
            switch (value) {
                .string => try alloc.dupe(u8, value.string),
                .null => try deriveDefaultPostgresSlotNameAlloc(alloc, table_name, postgres_table_value.string),
                else => return error.InvalidReplicationSourceConfig,
            }
        else
            try deriveDefaultPostgresSlotNameAlloc(alloc, table_name, postgres_table_value.string),
        .publication_name = if (source.object.get("publication_name")) |value|
            switch (value) {
                .string => try alloc.dupe(u8, value.string),
                .null => try deriveDefaultPostgresPublicationNameAlloc(alloc, table_name, postgres_table_value.string),
                else => return error.InvalidReplicationSourceConfig,
            }
        else
            try deriveDefaultPostgresPublicationNameAlloc(alloc, table_name, postgres_table_value.string),
    };
}

fn deriveFreshPostgresIdentifierAlloc(
    alloc: std.mem.Allocator,
    prefix: []const u8,
    table_name: []const u8,
    postgres_table: []const u8,
    source_ordinal: u32,
    seed: u64,
) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "{s}_{s}_{s}_{d}_{d}", .{ prefix, table_name, postgres_table, source_ordinal, seed });
    defer alloc.free(raw);
    return try sanitizePostgresIdentifierAlloc(alloc, raw, 63);
}

fn deriveDefaultPostgresSlotNameAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    postgres_table: []const u8,
) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "antfly_{s}_{s}", .{ table_name, postgres_table });
    defer alloc.free(raw);
    return try sanitizePostgresIdentifierAlloc(alloc, raw, 63);
}

fn deriveDefaultPostgresPublicationNameAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    postgres_table: []const u8,
) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "antfly_pub_{s}_{s}", .{ table_name, postgres_table });
    defer alloc.free(raw);
    return try sanitizePostgresIdentifierAlloc(alloc, raw, 63);
}

fn sanitizePostgresIdentifierAlloc(
    alloc: std.mem.Allocator,
    raw: []const u8,
    max_len: usize,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (raw) |byte| {
        if (out.items.len >= max_len) break;
        if (std.ascii.isAlphanumeric(byte) or byte == '_') {
            try out.append(alloc, std.ascii.toLower(byte));
        } else {
            try out.append(alloc, '_');
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn findTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn extensionMemberTableName(member: extension_domain.ExtensionMember) ?[]const u8 {
    if (member.table_name.len != 0) return member.table_name;
    if (member.scope.kind == .table) return member.scope.table_name;
    return null;
}

fn extensionOwnsIndex(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8, index_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .index) continue;
        const member_table = extensionMemberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name) and std.mem.eql(u8, member.object_name, index_name)) return true;
    }
    return false;
}

fn extensionOwnsEnrichment(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8, enrichment_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .enrichment) continue;
        const member_table = extensionMemberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name) and std.mem.eql(u8, member.object_name, enrichment_name)) return true;
    }
    return false;
}

fn extensionOwnsTableSchema(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .table_schema) continue;
        const member_table = extensionMemberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name)) return true;
    }
    return false;
}

fn extensionOwnsTableDataShape(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        if (member.object_kind != .data_shape) continue;
        const shape_kind = member.shape_kind orelse continue;
        if (shape_kind != .document and shape_kind != .row) continue;
        const member_table = extensionMemberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name)) return true;
    }
    return false;
}

fn extensionOwnsTableShape(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
    return extensionOwnsTableSchema(snapshot, table_name) or extensionOwnsTableDataShape(snapshot, table_name);
}

fn extensionOwnsTableScopedObject(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
    for (snapshot.extension_members) |member| {
        const member_table = extensionMemberTableName(member) orelse continue;
        if (std.mem.eql(u8, member_table, table_name)) return true;
    }
    return false;
}

test "metadata http extension ownership helpers protect internal table mutations" {
    var tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "memories",
        .placement_role = "data",
    }};
    var members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .object_kind = .table_schema,
            .object_name = "memory_record",
            .table_name = "memories",
        },
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .object_kind = .index,
            .object_name = "memory_text",
            .table_name = "memories",
        },
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memories" },
            .object_kind = .enrichment,
            .object_name = "memory_embed",
        },
        .{
            .extension_name = "memoryaf",
            .scope = .{ .kind = .table, .table_name = "memory_events" },
            .object_kind = .data_shape,
            .object_name = "memory_event",
            .shape_kind = .row,
        },
    };
    var snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .extension_members = members[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    try std.testing.expect(extensionOwnsTableScopedObject(&snapshot, "memories"));
    try std.testing.expect(extensionOwnsTableSchema(&snapshot, "memories"));
    try std.testing.expect(extensionOwnsIndex(&snapshot, "memories", "memory_text"));
    try std.testing.expect(!extensionOwnsTableSchema(&snapshot, "memory_events"));
    try std.testing.expect(extensionOwnsTableDataShape(&snapshot, "memory_events"));
    try std.testing.expect(extensionOwnsTableShape(&snapshot, "memory_events"));
    try std.testing.expect(!extensionOwnsIndex(&snapshot, "memories", "manual_text"));
    try std.testing.expect(!extensionOwnsTableScopedObject(&snapshot, "sessions"));
}

fn findRangeForKey(ranges: []const metadata_table_manager.RangeRecord, table_id: u64, key: []const u8) ?u64 {
    for (ranges) |record| {
        if (record.table_id != table_id) continue;
        if (key.len > 0 and record.start_key.len > 0 and std.mem.order(u8, key, record.start_key) == .lt) continue;
        if (record.end_key) |end_key| {
            if (std.mem.order(u8, key, end_key) != .lt) continue;
        }
        return record.group_id;
    }
    return null;
}

fn findSplitById(
    records: []const metadata_transition_state.SplitTransitionRecord,
    transition_id: u64,
) ?metadata_transition_state.SplitTransitionRecord {
    for (records) |record| {
        if (record.transition_id == transition_id) return record;
    }
    return null;
}

fn findActiveSplitForSource(
    records: []const metadata_transition_state.SplitTransitionRecord,
    source_group_id: u64,
) ?metadata_transition_state.SplitTransitionRecord {
    for (records) |record| {
        if (record.source_group_id == source_group_id and
            record.phase != .finalized and
            record.phase != .rolled_back)
        {
            return record;
        }
    }
    return null;
}

fn splitRequestMatches(
    record: metadata_transition_state.SplitTransitionRecord,
    transition_id: u64,
    source_group_id: u64,
    destination_group_id: u64,
    split_key: []const u8,
) bool {
    return record.transition_id == transition_id and
        record.source_group_id == source_group_id and
        record.destination_group_id == destination_group_id and
        record.split_key != null and
        std.mem.eql(u8, record.split_key.?, split_key);
}

fn validateMergeDocIdentityCompatibility(
    snapshot: *const metadata_api.AdminSnapshot,
    donor_group_id: u64,
    receiver_group_id: u64,
    allow_doc_identity_reassignment: bool,
) !void {
    const donor = findMergedGroupStatus(snapshot.merged_group_statuses, donor_group_id) orelse return error.DocIdentityNamespaceMismatch;
    const receiver = findMergedGroupStatus(snapshot.merged_group_statuses, receiver_group_id) orelse return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity_reassignment_active or receiver.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity_namespace_conflict or receiver.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity.rebuild_required or receiver.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
    if (donor.doc_identity.ordinal_capacity_exhausted or receiver.doc_identity.ordinal_capacity_exhausted) return error.DocIdentityNamespaceMismatch;
    if (!runtimeDocIdentityHasOrdinalRows(donor.doc_identity) or !runtimeDocIdentityHasOrdinalRows(receiver.doc_identity)) return;
    if (allow_doc_identity_reassignment) return;
    if (!runtimeDocIdentitySameNamespace(donor.doc_identity, receiver.doc_identity)) return error.DocIdentityNamespaceMismatch;
}

fn validateMergeRequestDocIdentity(source: AdminSource, table_name: []const u8, req: MergeRequest) !void {
    var snapshot = try source.adminSnapshot();
    defer source.freeAdminSnapshot(&snapshot);
    _ = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    try validateMergeDocIdentityCompatibility(&snapshot, req.donor_group_id, req.receiver_group_id, req.allow_doc_identity_reassignment);
}

fn validateSplitDocIdentityCompatibility(
    snapshot: *const metadata_api.AdminSnapshot,
    source_group_id: u64,
) !void {
    const source = findMergedGroupStatus(snapshot.merged_group_statuses, source_group_id) orelse return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity_reassignment_active) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity_namespace_conflict) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity.rebuild_required) return error.DocIdentityNamespaceMismatch;
    if (source.doc_identity.ordinal_capacity_exhausted) return error.DocIdentityNamespaceMismatch;
}

fn validateSplitRequestDocIdentity(source: AdminSource, table_name: []const u8, req: SplitRequest) !void {
    var snapshot = try source.adminSnapshot();
    defer source.freeAdminSnapshot(&snapshot);
    const table = findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    const source_group_id = req.source_group_id orelse findRangeForKey(snapshot.ranges, table.table_id, req.split_key) orelse return error.RangeNotFound;
    try validateSplitDocIdentityCompatibility(&snapshot, source_group_id);
}

fn findMergedGroupStatus(statuses: []const metadata_reconciler.MergedGroupStatus, group_id: u64) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn runtimeDocIdentityHasOrdinalRows(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0;
}

fn runtimeDocIdentitySameNamespace(
    left: metadata_table_manager.RuntimeDocIdentityStatusReport,
    right: metadata_table_manager.RuntimeDocIdentityStatusReport,
) bool {
    return left.namespace_table_id == right.namespace_table_id and
        left.namespace_shard_id == right.namespace_shard_id and
        left.namespace_range_id == right.namespace_range_id;
}

fn deriveTransitionId(table_name: []const u8, key: []const u8, seed: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

fn deriveGroupId(table_name: []const u8, key: []const u8, seed: u64, reserved: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    var id = group_ids.dataGroupIdFromHash(hasher.final());
    if (id == 0 or id == reserved) id +%= 1;
    if (id == 0 or group_ids.isSystemGroupId(id)) return group_ids.dataGroupIdFromHash(reserved +% 1);
    return id;
}

fn jsonResponse(alloc: std.mem.Allocator, source: AdminSource, value: anytype) !http_common.HttpResponse {
    const body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    errdefer alloc.free(body);
    source.recordJsonResponseAllocation(body.len);
    return .{
        .status = 200,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = body,
    };
}

fn jsonBodyOrEmptyObject(body: []const u8) []const u8 {
    return if (body.len == 0) "{}" else body;
}

fn extensionLifecycleErrorResponse(alloc: std.mem.Allocator, err: anyerror) !http_common.HttpResponse {
    return switch (err) {
        error.UnsupportedOperation => try textResponse(alloc, 405, "unsupported operation"),
        error.PackageNotFound, error.ExtensionNotInstalled, error.TableNotFound => try textResponse(alloc, 404, "not found"),
        error.ExtensionAlreadyInstalled => try textResponse(alloc, 409, "extension already installed"),
        error.DependentExtensionExists => try textResponse(alloc, 409, "dependent extension exists"),
        error.RequiredExtensionNotInstalled => try textResponse(alloc, 409, "required extension not installed"),
        error.UnsupportedManifestApiVersion,
        error.UnsupportedPackageKind,
        error.UnsupportedExtensionScope,
        error.UnsupportedObjectKindForV1,
        error.PackageVersionMismatch,
        error.UpdatePathNotFound,
        error.ExtensionDisabled,
        error.ExtensionLifecycleBusy,
        error.InvalidCreateTableRequest,
        error.InvalidCreateIndexRequest,
        error.InvalidTableIndexMetadata,
        error.InvalidExtensionEnrichment,
        error.InvalidEnrichmentConfig,
        error.ConflictingEnrichmentConfig,
        error.UnrequestedCapabilityGrant,
        error.InvalidJsonObject,
        error.EmptyName,
        error.InvalidIdentifier,
        error.MemberTableOutsideScope,
        => try textResponse(alloc, 400, "invalid extension lifecycle request"),
        else => err,
    };
}

fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

fn notLeaderResponse(alloc: std.mem.Allocator) !http_common.HttpResponse {
    const headers = try alloc.alloc(http_common.Header, 2);
    var initialized_headers: usize = 0;
    errdefer {
        for (headers[0..initialized_headers]) |*header| header.deinit(alloc);
        alloc.free(headers);
    }

    var retry_after_name: ?[]u8 = try alloc.dupe(u8, "Retry-After");
    errdefer if (retry_after_name) |value| alloc.free(value);
    var retry_after_value: ?[]u8 = try alloc.dupe(u8, "1");
    errdefer if (retry_after_value) |value| alloc.free(value);
    headers[0] = .{ .name = retry_after_name.?, .value = retry_after_value.? };
    retry_after_name = null;
    retry_after_value = null;
    initialized_headers += 1;

    var not_leader_name: ?[]u8 = try alloc.dupe(u8, http_common.metadata_not_leader_header);
    errdefer if (not_leader_name) |value| alloc.free(value);
    var not_leader_value: ?[]u8 = try alloc.dupe(u8, http_common.metadata_not_leader_value);
    errdefer if (not_leader_value) |value| alloc.free(value);
    headers[1] = .{ .name = not_leader_name.?, .value = not_leader_value.? };
    not_leader_name = null;
    not_leader_value = null;
    initialized_headers += 1;

    const content_type = try alloc.dupe(u8, "application/json");
    errdefer alloc.free(content_type);
    const body = try alloc.dupe(u8, "{\"error\":\"metadata leader unavailable\"}");
    errdefer alloc.free(body);
    return .{
        .status = 503,
        .content_type = content_type,
        .headers = headers,
        .body = body,
    };
}

fn freeStoreStatusReport(alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) void {
    alloc.free(report.health_class);
    metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
    metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
}

test "metadata http server serves status and filtered admin routes" {
    const FakeSource = struct {
        fn iface(_: *@This()) AdminSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .validate_publication = validatePublication,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metadata_epoch = 5,
                .metrics = .{},
                .projected_tables = 1,
                .projected_tables_with_replication_sources = 1,
                .projected_replication_sources = 2,
                .projected_replication_source_statuses_reseed_recommended = 1,
                .projected_replication_source_lag_millis_max = 34,
                .projected_replication_source_observed_lag_millis_max = 56,
                .projected_replication_source_statuses_with_source_commit_timestamp = 1,
                .projected_ranges = 2,
                .projected_stores = 1,
                .backfill_stores = 1,
                .active_backfills = 2,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 1, .name = "docs", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
                    .{ .group_id = 11, .table_id = 1, .doc_identity_shard_id = 10, .doc_identity_range_id = 10, .start_key = "doc:m", .end_key = "doc:z" },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{
                    .{ .store_id = 7, .node_id = 1, .role = "data", .health_class = "healthy", .failure_domain = "rack-a", .active_backfills = 2, .backfill_progress_millis = 350 },
                })[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{
                    .{ .record = .{ .group_id = 10, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = @constCast((&[_]u64{2})[0..]) },
                })[0..]),
                .local_bootstrap_statuses = @constCast((&[_]@import("../raft/host.zig").BootstrapStatus{
                    .{
                        .group_id = 10,
                        .kind = .backup_db_snapshot_restore,
                        .phase = .failed,
                        .attempts = 2,
                        .last_updated_at_millis = 1234,
                        .last_error = "InvalidBackupLocation",
                        .backup_id = "snap1",
                        .snapshot_path = "snap1/groups/10",
                    },
                })[0..]),
                .replication_source_statuses = @constCast((&[_]metadata_table_manager.ReplicationSourceStatusRecord{
                    .{
                        .table_id = 1,
                        .source_ordinal = 0,
                        .source_kind = "postgres",
                        .external_table = "users",
                        .cutover_mode = "exported_snapshot",
                        .slot_name = "antfly_postgres_users_docs",
                        .publication_name = "antfly_pub_postgres_users_docs",
                        .phase = "snapshot",
                        .checkpoint = "lsn:0/16B6A50",
                        .snapshot_offset = 2,
                        .prepared_checkpoint = "lsn:0/16B6A50",
                        .stream_checkpoint = "lsn:0/16B6A50",
                        .lag_records = 12,
                        .lag_millis = 34,
                        .last_source_commit_at_ms = 1200,
                        .updated_at_ms = 555,
                    },
                })[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{
                    .{ .transition_id = 9001, .attempt_epoch = 1, .source_group_id = 10, .destination_group_id = 12, .phase = .bootstrap_peer },
                })[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{
                    .{ .transition_id = 9010, .donor_group_id = 11, .receiver_group_id = 10, .phase = .prepare },
                })[0..]),
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{
                    .{
                        .group_id = 10,
                        .doc_identity_reassignment_active = true,
                        .doc_identity = .{
                            .namespace_table_id = 1,
                            .namespace_shard_id = 10,
                            .namespace_range_id = 10,
                            .next_ordinal = 6,
                            .allocated_ordinals = 5,
                            .live_ordinals = 5,
                        },
                    },
                })[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn validatePublication(_: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            var snapshot = try adminSnapshot(undefined);
            return contract.matches(&snapshot);
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var status_resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.status });
    defer status_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), status_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"metadata_group_id\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_tables_with_replication_sources\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_replication_sources\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_replication_source_lag_millis_max\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_replication_source_observed_lag_millis_max\":56") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_replication_source_statuses_with_source_commit_timestamp\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"projected_replication_source_statuses_reseed_recommended\":1") != null);

    var ranges_resp = try server.handle(.{ .method = .GET, .uri = "/metadata/v1/tables/1/ranges" });
    defer ranges_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body, "\"group_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body, "\"group_id\":11") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body, "\"doc_identity_shard_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body, "\"doc_identity_range_id\":10") != null);

    var placement_resp = try server.handle(.{ .method = .GET, .uri = "/metadata/v1/groups/10/placement" });
    defer placement_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, placement_resp.body, "\"group_id\":10") != null);

    var shutdown_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/1/shutdown" });
    defer shutdown_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body, "\"phase\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body, "\"safe_to_terminate\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body, "\"pending_groups\":[10]") != null);

    var snapshot_resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.admin_snapshot });
    defer snapshot_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"local_bootstrap_statuses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"phase\":\"failed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"backup_id\":\"snap1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"snapshot_path\":\"snap1/groups/10\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"replication_source_statuses\"") != null);

    const publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 77,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(publication_body);
    var publication_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_catalog_publication_check,
        .body = publication_body,
    });
    defer publication_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), publication_resp.status);
    const foreign_group_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 78,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(foreign_group_publication_body);
    var foreign_group_publication_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_catalog_publication_check,
        .body = foreign_group_publication_body,
    });
    defer foreign_group_publication_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), foreign_group_publication_resp.status);
    const stale_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 77,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:b", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(stale_publication_body);
    var stale_publication_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_catalog_publication_check,
        .body = stale_publication_body,
    });
    defer stale_publication_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), stale_publication_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"replication_source_action_hints\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"source_kind\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"external_table\":\"users\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"cutover_mode\":\"exported_snapshot\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"slot_name\":\"antfly_postgres_users_docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"publication_name\":\"antfly_pub_postgres_users_docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"snapshot_offset\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"prepared_checkpoint\":\"lsn:0/16B6A50\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"stream_checkpoint\":\"lsn:0/16B6A50\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"lag_records\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"lag_millis\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"last_source_commit_at_ms\":1200") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"doc_identity_shard_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"doc_identity_range_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"merged_group_statuses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"doc_identity_reassignment_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body, "\"namespace_range_id\":10") != null);

    var active_resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.active_transitions });
    defer active_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, active_resp.body, "\"transition_id\":9001") != null);
    try std.testing.expect(std.mem.indexOf(u8, active_resp.body, "\"transition_id\":9010") != null);
}

test "metadata http server maps extension-owned object mutations to method not allowed" {
    const FakeSource = struct {
        fn iface(_: *@This()) AdminSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .drop_table = dropTable,
                    .update_schema = updateSchema,
                    .create_index = createIndex,
                    .drop_index = dropIndex,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 77, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn dropTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !void {
            return error.ExtensionOwnedObject;
        }

        fn updateSchema(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            return error.ExtensionOwnedObject;
        }

        fn createIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !void {
            return error.ExtensionOwnedObject;
        }

        fn dropIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) !void {
            return error.ExtensionOwnedObject;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var schema_resp = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/tables/memories/schema", .body = "{}" });
    defer schema_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 405), schema_resp.status);

    var create_index_resp = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/tables/memories/indexes/memory_text", .body = "{\"type\":\"full_text\"}" });
    defer create_index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 405), create_index_resp.status);

    var drop_index_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/tables/memories/indexes/memory_text" });
    defer drop_index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 405), drop_index_resp.status);

    var drop_table_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/tables/memories" });
    defer drop_table_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 405), drop_table_resp.status);
}

test "metadata http server replaces a table definition through compare-and-swap" {
    const FakeSource = struct {
        replaced: bool = false,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .replace_table_definition = replaceTableDefinition,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn replaceTableDefinition(ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (expected.table_id != 42 or !std.mem.eql(u8, expected.description, "original")) return error.TableGenerationChanged;
            try std.testing.expectEqual(@as(u64, 42), replacement.table_id);
            try std.testing.expectEqualStrings("docs", replacement.name);
            try std.testing.expectEqualStrings("restored", replacement.description);
            self.replaced = true;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var response = try server.handle(.{
        .method = .PUT,
        .uri = "/internal/v1/tables/docs/definition",
        .body =
        \\{"expected":{"table_id":42,"name":"docs","description":"original"},"definition":{"table_id":42,"name":"docs","description":"restored"}}
        ,
    });
    defer response.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 202), response.status);
    try std.testing.expect(source.replaced);
}

test "metadata http server registers nodes and marks node stores draining for shutdown" {
    const FakeSource = struct {
        nodes: [2]metadata_table_manager.NodeRecord = .{
            .{ .node_id = 9, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
            .{ .node_id = 99, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
        },
        stores: [2]metadata_table_manager.StoreRecord = .{
            .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
            .{ .store_id = 99, .node_id = 99, .role = "data", .health_class = "healthy", .live = true },
        },
        node_count: usize = 0,
        store_count: usize = 1,
        reallocate_triggered: bool = false,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .upsert_node = upsertNode,
                    .upsert_store = upsertStore,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = self.nodes[0..self.node_count],
                .stores = self.stores[0..self.store_count],
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            defer metadata_table_manager.freeNode(alloc, record);
            const index: usize = if (record.node_id == 9) 0 else if (record.node_id == 99) 1 else return;
            if (index >= self.node_count) self.node_count = index + 1;
            self.nodes[index].lifecycle = if (metadata_table_manager.nodeLifecycleActive(record.lifecycle))
                metadata_table_manager.node_lifecycle_active
            else
                metadata_table_manager.node_lifecycle_draining;
        }

        fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            defer metadata_table_manager.freeStore(alloc, record);
            const index: usize = if (record.store_id == 9) 0 else if (record.store_id == 99) 1 else return;
            if (index >= self.store_count) self.store_count = index + 1;
            self.stores[index].drain_requested = record.drain_requested;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reallocate_triggered = true;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var node_resp = try server.handle(.{ .method = .POST, .uri = routes.Routes.internal_nodes, .body = "{\"node_id\":9,\"role\":\"data\"}" });
    defer node_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), node_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.node_count);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(source.nodes[0].lifecycle));

    var draining_register_resp = try server.handle(.{ .method = .POST, .uri = routes.Routes.internal_nodes, .body = "{\"node_id\":99,\"role\":\"data\",\"lifecycle\":\"draining\"}" });
    defer draining_register_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), draining_register_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.node_count);
    try std.testing.expect(!source.stores[1].drain_requested);

    var shutdown_resp = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/nodes/9/shutdown", .body = "{\"type\":\"remove\",\"reason\":\"test\"}" });
    defer shutdown_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), shutdown_resp.status);
    try std.testing.expect(std.mem.eql(u8, source.nodes[0].lifecycle, metadata_table_manager.node_lifecycle_draining));
    try std.testing.expect(source.stores[0].drain_requested);
    try std.testing.expect(source.reallocate_triggered);

    var register_node_resp = try server.handle(.{ .method = .POST, .uri = routes.Routes.internal_nodes, .body = "{\"node_id\":9,\"role\":\"data\"}" });
    defer register_node_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), register_node_resp.status);
    try std.testing.expect(std.mem.eql(u8, source.nodes[0].lifecycle, metadata_table_manager.node_lifecycle_draining));

    var register_node_store_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}",
    });
    defer register_node_store_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), register_node_store_resp.status);
    try std.testing.expect(source.stores[0].drain_requested);

    var status_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/9/shutdown" });
    defer status_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"phase\":\"complete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"safe_to_terminate\":true") != null);

    source.reallocate_triggered = false;
    var cancel_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9/shutdown" });
    defer cancel_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(source.nodes[0].lifecycle));
    try std.testing.expect(!source.stores[0].drain_requested);
    try std.testing.expect(source.reallocate_triggered);

    var cancelled_status_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/9/shutdown" });
    defer cancelled_status_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, cancelled_status_resp.body, "\"phase\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cancelled_status_resp.body, "\"safe_to_terminate\":false") != null);

    var post_cancel_register_node_store_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}",
    });
    defer post_cancel_register_node_store_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), post_cancel_register_node_store_resp.status);
    try std.testing.expect(!source.stores[0].drain_requested);

    source.reallocate_triggered = false;
    var retry_cancel_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9/shutdown" });
    defer retry_cancel_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), retry_cancel_resp.status);
    try std.testing.expect(!source.reallocate_triggered);

    var retry_resp = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/nodes/99/shutdown", .body = "{\"type\":\"remove\"}" });
    defer retry_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), retry_resp.status);
    try std.testing.expectEqual(@as(usize, 2), source.node_count);
    try std.testing.expect(std.mem.eql(u8, source.nodes[1].lifecycle, metadata_table_manager.node_lifecycle_draining));

    var register_unknown_store_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":99,\"node_id\":99,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}",
    });
    defer register_unknown_store_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), register_unknown_store_resp.status);
    try std.testing.expect(source.stores[1].drain_requested);

    var unknown_status_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/99/shutdown" });
    defer unknown_status_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, unknown_status_resp.body, "\"phase\":\"complete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unknown_status_resp.body, "\"safe_to_terminate\":true") != null);
}

test "metadata http server reports unknown shutdown node safe to terminate" {
    const FakeSource = struct {
        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .nodes = @constCast((&[_]metadata_table_manager.NodeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var status_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/42/shutdown" });
    defer status_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), status_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"phase\":\"not_found\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"safe_to_terminate\":true") != null);
}

test "metadata http server appends explicit shutdown commands even when snapshot appears unchanged" {
    const FakeSource = struct {
        nodes: [1]metadata_table_manager.NodeRecord = .{
            .{ .node_id = 9, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
        },
        request_count: usize = 0,
        cancel_count: usize = 0,
        reallocate_count: usize = 0,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .request_node_shutdown = requestNodeShutdown,
                    .cancel_node_shutdown = cancelNodeShutdown,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = self.nodes[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn requestNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9), node_id);
            self.request_count += 1;
        }

        fn cancelNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9), node_id);
            self.cancel_count += 1;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reallocate_count += 1;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var cancel_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9/shutdown" });
    defer cancel_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.cancel_count);
    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);

    source.nodes[0].lifecycle = metadata_table_manager.node_lifecycle_draining;
    var request_resp = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/nodes/9/shutdown", .body = "{\"type\":\"remove\"}" });
    defer request_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), request_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.request_count);
    try std.testing.expectEqual(@as(usize, 2), source.reallocate_count);
}

test "metadata http server finalizes node shutdown through explicit command" {
    const FakeSource = struct {
        finalize_count: usize = 0,
        reallocate_count: usize = 0,
        nodes: [1]metadata_table_manager.NodeRecord = .{
            .{ .node_id = 9, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
        },

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .finalize_node_shutdown = finalizeNodeShutdown,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = self.nodes[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn finalizeNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9), node_id);
            self.finalize_count += 1;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reallocate_count += 1;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var active_finalize_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9" });
    defer active_finalize_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), active_finalize_resp.status);
    try std.testing.expectEqual(@as(usize, 0), source.finalize_count);
    try std.testing.expectEqual(@as(usize, 0), source.reallocate_count);

    source.nodes[0].lifecycle = metadata_table_manager.node_lifecycle_draining;
    var finalize_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9" });
    defer finalize_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), finalize_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.finalize_count);
    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);
}

test "metadata http server rejects finalizing active store-only node" {
    const FakeSource = struct {
        finalize_count: usize = 0,
        stores: [1]metadata_table_manager.StoreRecord = .{
            .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
        },

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .finalize_node_shutdown = finalizeNodeShutdown,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = &.{},
                .stores = self.stores[0..],
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn finalizeNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9), node_id);
            self.finalize_count += 1;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var active_finalize_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9" });
    defer active_finalize_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), active_finalize_resp.status);
    try std.testing.expectEqual(@as(usize, 0), source.finalize_count);

    source.stores[0].drain_requested = true;
    var draining_finalize_resp = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9" });
    defer draining_finalize_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), draining_finalize_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.finalize_count);
}

test "metadata http server stale registration from another admin instance cannot redrain cancelled node" {
    const FakeSource = struct {
        nodes: [1]metadata_table_manager.NodeRecord = .{
            .{ .node_id = 9, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
        },
        stores: [1]metadata_table_manager.StoreRecord = .{
            .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
        },
        stage_next_store: bool = false,
        pending_store: ?metadata_table_manager.StoreRecord = null,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .upsert_node = upsertNode,
                    .upsert_store = upsertStore,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.pending_store) |record| metadata_table_manager.freeStore(alloc, record);
            self.pending_store = null;
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .nodes = self.nodes[0..],
                .stores = self.stores[0..],
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            defer metadata_table_manager.freeNode(alloc, record);
            self.nodes[0].lifecycle = if (metadata_table_manager.nodeLifecycleActive(record.lifecycle))
                metadata_table_manager.node_lifecycle_active
            else
                metadata_table_manager.node_lifecycle_draining;
        }

        fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            defer metadata_table_manager.freeStore(alloc, record);
            if (self.stage_next_store) {
                self.stage_next_store = false;
                self.pending_store = try metadata_table_manager.cloneStore(alloc, record);
                return;
            }
            self.applyStore(record);
        }

        fn triggerReallocate(_: *anyopaque) !void {}

        fn applyPendingStore(self: *@This(), alloc: std.mem.Allocator) !void {
            const record = self.pending_store orelse return error.MissingPendingStore;
            self.pending_store = null;
            defer metadata_table_manager.freeStore(alloc, record);
            self.applyStore(record);
        }

        fn applyStore(self: *@This(), record: metadata_table_manager.StoreRecord) void {
            if (!metadata_table_manager.nodeLifecycleActive(self.nodes[0].lifecycle)) {
                self.stores[0].drain_requested = true;
                return;
            }
            self.stores[0].drain_requested = record.drain_requested and self.stores[0].drain_requested;
        }
    };

    var source = FakeSource{};
    defer source.deinit(std.testing.allocator);
    var server_a = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var server_b = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var shutdown_resp = try server_a.handle(.{ .method = .PUT, .uri = "/internal/v1/nodes/9/shutdown", .body = "{\"type\":\"remove\"}" });
    defer shutdown_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), shutdown_resp.status);
    try std.testing.expect(source.stores[0].drain_requested);

    source.stage_next_store = true;
    var stale_register_resp = try server_a.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}",
    });
    defer stale_register_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), stale_register_resp.status);
    try std.testing.expect(source.pending_store.?.drain_requested);

    var cancel_resp = try server_b.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/9/shutdown" });
    defer cancel_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(source.nodes[0].lifecycle));
    try std.testing.expect(!source.stores[0].drain_requested);

    try source.applyPendingStore(std.testing.allocator);
    try std.testing.expect(!source.stores[0].drain_requested);
}

test "metadata http server keeps shutdown unsafe while local group statuses remain" {
    const FakeSource = struct {
        const group_statuses = [_]metadata_table_manager.GroupStatusReport{.{
            .group_id = 44,
            .updated_at_millis = 10,
            .local_leader = false,
            .local_voter = true,
            .voter_count = 1,
        }};
        stores: [1]metadata_table_manager.StoreRecord = .{.{
            .store_id = 4,
            .node_id = 4,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .drain_requested = true,
            .group_statuses = @constCast(group_statuses[0..]),
        }},

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 77, .metadata_epoch = 5, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = self.stores[0..],
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var status_resp = try server.handle(.{ .method = .GET, .uri = "/internal/v1/nodes/4/shutdown" });
    defer status_resp.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"phase\":\"blocked\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"blocked\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"blocked_reason\":\"InsufficientShardVoters\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"message\":\"Node hosts a shard with no other voters") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"safe_to_terminate\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"group_status_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body, "\"pending_groups\":[44]") != null);
}

test "metadata http server round-trips over std http listener" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");

    const FakeSource = struct {
        fn iface(_: *@This()) AdminSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1900, .metadata_epoch = 8 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 1900,
                .metadata_epoch = 8,
                .metrics = .{},
                .projected_tables = 1,
                .projected_ranges = 1,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1900, .metadata_epoch = 8, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 1, .name = "docs", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:z" },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const status_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, routes.Routes.status });
    defer std.testing.allocator.free(status_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var resp = try executor.executor().execute(std.heap.page_allocator, .{
        .method = .GET,
        .uri = status_uri,
    });
    defer resp.deinit(std.heap.page_allocator);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"metadata_group_id\":1900") != null);
}

test "metadata http server accepts internal reallocate and split merge routes" {
    const FakeSource = struct {
        reallocate_count: usize = 0,
        restore_count: usize = 0,
        split_count: usize = 0,
        merge_count: usize = 0,
        node_count: usize = 0,
        store_count: usize = 0,
        store_status_count: usize = 0,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .upsert_node = upsertNode,
                    .upsert_store = upsertStore,
                    .report_store_status = reportStoreStatus,
                    .trigger_reallocate = triggerReallocate,
                    .restore_table = restoreTable,
                    .request_split = requestSplit,
                    .request_merge = requestMerge,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{
                        .group_id = 9,
                        .range_id = 9,
                        .table_id = 1,
                        .start_key = "doc:a",
                        .end_key = "doc:m",
                    },
                    .{
                        .group_id = 10,
                        .range_id = 10,
                        .table_id = 1,
                        .start_key = "doc:m",
                        .end_key = "doc:z",
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{
                    .{ .group_id = 9 },
                    .{ .group_id = 10 },
                })[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reallocate_count += 1;
        }

        fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
            defer metadata_table_manager.freeNode(alloc, record);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            self.node_count += 1;
        }

        fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
            defer metadata_table_manager.freeStore(alloc, record);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), record.store_id);
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            self.store_count += 1;
        }

        fn reportStoreStatus(ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
            defer freeStoreStatusReport(alloc, report);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), report.store_id);
            try std.testing.expectEqualStrings("healthy", report.health_class);
            self.store_status_count += 1;
        }

        fn restoreTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("file:///tmp/out", location_uri);
            try std.testing.expectEqualStrings("snap1", backup_id);
            self.restore_count += 1;
        }

        fn requestSplit(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: SplitRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:m", req.split_key);
            self.split_count += 1;
        }

        fn requestMerge(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: MergeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 10), req.donor_group_id);
            try std.testing.expectEqual(@as(u64, 9), req.receiver_group_id);
            try std.testing.expect(req.allow_doc_identity_reassignment);
            self.merge_count += 1;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var reallocate = try server.handle(.{ .method = .POST, .uri = routes.Routes.internal_reallocate, .body = "" });
    defer reallocate.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), reallocate.status);

    var zero_node = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"node_id\":0}",
        .content_type = "application/json",
    });
    defer zero_node.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), zero_node.status);

    var zero_store = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":0,\"node_id\":0}",
        .content_type = "application/json",
    });
    defer zero_store.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), zero_store.status);

    var mismatched_store = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":7,\"node_id\":1}",
        .content_type = "application/json",
    });
    defer mismatched_store.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), mismatched_store.status);

    var zero_node_status = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/nodes/0/status",
        .body = "{\"health_class\":\"healthy\"}",
        .content_type = "application/json",
    });
    defer zero_node_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), zero_node_status.status);

    var zero_node_shutdown = try server.handle(.{ .method = .PUT, .uri = "/internal/v1/nodes/0/shutdown", .body = "{\"type\":\"remove\"}" });
    defer zero_node_shutdown.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), zero_node_shutdown.status);

    var zero_node_finalize = try server.handle(.{ .method = .DELETE, .uri = "/internal/v1/nodes/0" });
    defer zero_node_finalize.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), zero_node_finalize.status);

    var store = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.internal_nodes,
        .body = "{\"store_id\":7,\"node_id\":7}",
        .content_type = "application/json",
    });
    defer store.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), store.status);

    var store_status = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/nodes/7/status",
        .body = "{\"store_id\":7,\"health_class\":\"healthy\"}",
        .content_type = "application/json",
    });
    defer store_status.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), store_status.status);

    var restore = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/restore",
        .body = "{\"backup_id\":\"snap1\",\"location\":\"file:///tmp/out\"}",
        .content_type = "application/json",
    });
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), restore.status);

    var split = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/split",
        .body = "{\"split_key\":\"doc:m\"}",
        .content_type = "application/json",
    });
    defer split.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), split.status);

    var merge = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/merge",
        .body = "{\"donor_group_id\":10,\"receiver_group_id\":9,\"allow_doc_identity_reassignment\":true}",
        .content_type = "application/json",
    });
    defer merge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), merge.status);

    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);
    try std.testing.expectEqual(@as(usize, 1), source.node_count);
    try std.testing.expectEqual(@as(usize, 1), source.store_count);
    try std.testing.expectEqual(@as(usize, 1), source.store_status_count);
    try std.testing.expectEqual(@as(usize, 1), source.restore_count);
    try std.testing.expectEqual(@as(usize, 1), source.split_count);
    try std.testing.expectEqual(@as(usize, 1), source.merge_count);
}

test "metadata http server rejects split and merge during active doc identity reassignment before source mutation" {
    const FakeSource = struct {
        split_count: usize = 0,
        merge_count: usize = 0,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .request_split = requestSplit,
                    .request_merge = requestMerge,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{
                        .group_id = 9,
                        .range_id = 9,
                        .table_id = 1,
                        .start_key = "doc:a",
                        .end_key = "doc:m",
                    },
                    .{
                        .group_id = 10,
                        .range_id = 10,
                        .table_id = 1,
                        .start_key = "doc:m",
                        .end_key = "doc:z",
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{
                    .{ .group_id = 9, .doc_identity_reassignment_active = true },
                    .{ .group_id = 10, .doc_identity_reassignment_active = true },
                })[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn requestSplit(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: SplitRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.split_count += 1;
        }

        fn requestMerge(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: MergeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.merge_count += 1;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var split = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/split",
        .body = "{\"split_key\":\"doc:m\"}",
        .content_type = "application/json",
    });
    defer split.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), split.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split.body);

    var merge = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/merge",
        .body = "{\"donor_group_id\":10,\"receiver_group_id\":9,\"allow_doc_identity_reassignment\":true}",
        .content_type = "application/json",
    });
    defer merge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), merge.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge.body);

    try std.testing.expectEqual(@as(usize, 0), source.split_count);
    try std.testing.expectEqual(@as(usize, 0), source.merge_count);
}

test "metadata http server maps source split merge doc identity conflicts" {
    const FakeSource = struct {
        split_count: usize = 0,
        merge_count: usize = 0,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .request_split = requestSplit,
                    .request_merge = requestMerge,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{
                        .group_id = 9,
                        .range_id = 9,
                        .table_id = 1,
                        .start_key = "doc:a",
                        .end_key = "doc:m",
                    },
                    .{
                        .group_id = 10,
                        .range_id = 10,
                        .table_id = 1,
                        .start_key = "doc:m",
                        .end_key = "doc:z",
                    },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]metadata_reconciler.MergedGroupStatus{
                    .{ .group_id = 9 },
                    .{ .group_id = 10 },
                })[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn requestSplit(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: SplitRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:m", req.split_key);
            self.split_count += 1;
            return error.DocIdentityNamespaceMismatch;
        }

        fn requestMerge(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: MergeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 10), req.donor_group_id);
            try std.testing.expectEqual(@as(u64, 9), req.receiver_group_id);
            self.merge_count += 1;
            return error.DocIdentityNamespaceMismatch;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var split = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/split",
        .body = "{\"split_key\":\"doc:m\"}",
        .content_type = "application/json",
    });
    defer split.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), split.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split.body);

    var merge = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/merge",
        .body = "{\"donor_group_id\":10,\"receiver_group_id\":9}",
        .content_type = "application/json",
    });
    defer merge.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), merge.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge.body);

    try std.testing.expectEqual(@as(usize, 1), source.split_count);
    try std.testing.expectEqual(@as(usize, 1), source.merge_count);
}

test "metadata merge request validation rejects incompatible doc identity namespaces" {
    var statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{
            .group_id = 91,
            .doc_identity = .{
                .namespace_table_id = 9,
                .namespace_shard_id = 91,
                .namespace_range_id = 9001,
                .next_ordinal = 12,
                .allocated_ordinals = 11,
            },
        },
        .{
            .group_id = 92,
            .doc_identity = .{
                .namespace_table_id = 9,
                .namespace_shard_id = 92,
                .namespace_range_id = 9002,
                .next_ordinal = 7,
                .allocated_ordinals = 6,
            },
        },
    };
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        .merged_group_statuses = @constCast(statuses[0..]),
    };

    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 92, false),
    );
    try validateMergeDocIdentityCompatibility(&snapshot, 91, 92, true);

    statuses[0].doc_identity.rebuild_required = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 92, true),
    );
    statuses[0].doc_identity.rebuild_required = false;
    statuses[1].doc_identity_namespace_conflict = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 92, true),
    );
    statuses[1].doc_identity_namespace_conflict = false;
    statuses[0].doc_identity_reassignment_active = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 92, true),
    );
    statuses[0].doc_identity_reassignment_active = false;
    statuses[0].doc_identity.ordinal_capacity_exhausted = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 92, true),
    );
    statuses[0].doc_identity.ordinal_capacity_exhausted = false;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 93, false),
    );
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 91, 93, true),
    );
}

test "metadata merge validation handles rolling mixed-version doc identity status fixtures" {
    var statuses = [_]metadata_reconciler.MergedGroupStatus{
        .{
            .group_id = 101,
            // Old binaries report no ordinal rows. Merge validation must not
            // require a namespace until both sides advertise DOCID metadata.
            .doc_identity = .{},
        },
        .{
            .group_id = 102,
            .doc_identity = .{
                .namespace_table_id = 10,
                .namespace_shard_id = 102,
                .namespace_range_id = 1002,
                .next_ordinal = 12,
                .allocated_ordinals = 11,
            },
        },
    };
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        .merged_group_statuses = @constCast(statuses[0..]),
    };

    try validateMergeDocIdentityCompatibility(&snapshot, 101, 102, false);

    statuses[0].doc_identity_reassignment_active = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 101, 102, true),
    );
    statuses[0].doc_identity_reassignment_active = false;

    statuses[0].doc_identity = .{
        .namespace_table_id = 10,
        .namespace_shard_id = 101,
        .namespace_range_id = 1001,
        .next_ordinal = 3,
        .allocated_ordinals = 2,
    };
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateMergeDocIdentityCompatibility(&snapshot, 101, 102, false),
    );
    try validateMergeDocIdentityCompatibility(&snapshot, 101, 102, true);
}

test "metadata split request validation rejects stale doc identity namespace" {
    var statuses = [_]metadata_reconciler.MergedGroupStatus{.{
        .group_id = 91,
        .doc_identity = .{
            .namespace_table_id = 9,
            .namespace_shard_id = 91,
            .namespace_range_id = 9001,
            .next_ordinal = 12,
            .allocated_ordinals = 11,
        },
    }};
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        .merged_group_statuses = @constCast(statuses[0..]),
    };

    try validateSplitDocIdentityCompatibility(&snapshot, 91);
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateSplitDocIdentityCompatibility(&snapshot, 92),
    );

    statuses[0].doc_identity.rebuild_required = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateSplitDocIdentityCompatibility(&snapshot, 91),
    );
    statuses[0].doc_identity.rebuild_required = false;
    statuses[0].doc_identity_namespace_conflict = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateSplitDocIdentityCompatibility(&snapshot, 91),
    );
    statuses[0].doc_identity_namespace_conflict = false;
    statuses[0].doc_identity_reassignment_active = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateSplitDocIdentityCompatibility(&snapshot, 91),
    );
    statuses[0].doc_identity_reassignment_active = false;
    statuses[0].doc_identity.ordinal_capacity_exhausted = true;
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        validateSplitDocIdentityCompatibility(&snapshot, 91),
    );
}

test "metadata http server returns 400 for invalid internal restore backup locations" {
    const FakeSource = struct {
        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .restore_table = restoreTable,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 2, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn restoreTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !void {
            return error.MissingEndpoint;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var restore = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/restore",
        .body = "{\"backup_id\":\"snap1\",\"location\":\"s3://bucket/out\"}",
        .content_type = "application/json",
    });
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore.status);
    try std.testing.expectEqualStrings("text/plain", restore.content_type.?);
    try std.testing.expectEqualStrings(
        "missing S3-compatible endpoint; set AWS_ENDPOINT_URL for s3:// backups",
        restore.body,
    );
}

test "metadata http server accepts reseed exact cutover route" {
    const FakeSource = struct {
        reseed_table_name: ?[]const u8 = null,
        reseed_source_ordinal: ?u32 = null,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .head = head,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .reseed_replication_source_exact_cutover = reseedReplicationSourceExactCutover,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 1, .metadata_epoch = 3 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 3, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 3, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn reseedReplicationSourceExactCutover(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) !ReseedExactCutoverResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reseed_table_name = table_name;
            self.reseed_source_ordinal = source_ordinal;
            return .{
                .slot_name = try alloc.dupe(u8, "fresh_slot"),
                .publication_name = try alloc.dupe(u8, "fresh_pub"),
            };
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/tables/docs/replication-sources/1/reseed-exact-cutover",
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqualStrings("docs", source.reseed_table_name.?);
    try std.testing.expectEqual(@as(u32, 1), source.reseed_source_ordinal.?);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"slot_name\":\"fresh_slot\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"publication_name\":\"fresh_pub\"") != null);
}
