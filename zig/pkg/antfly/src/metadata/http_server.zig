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
const ant_json = @import("antfly-json");
const httpx = @import("httpx");
const group_ids = @import("../common/group_ids.zig");
const metadata_api = @import("api.zig");
const metadata_authority = @import("authority.zig");
const metadata_admin = @import("admin.zig");
const admin_read_operations = @import("admin_read_operations.zig");
const admin_mutation_operations = @import("admin_mutation_operations.zig");
const extension_operations = @import("extension_operations.zig");
const node_operations = @import("node_operations.zig");
const table_operations = @import("table_operations.zig");
const operation = @import("../api/operation.zig");
const extension_domain = @import("../extensions/mod.zig");
const extension_lifecycle = @import("../extensions/lifecycle.zig");
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
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;
const routes = @import("http_routes.zig");
const service = @import("service.zig");

pub const MetadataHttpServerConfig = struct {};

pub const SplitRequest = table_operations.SplitRequest;
pub const MergeRequest = table_operations.MergeRequest;

pub const NodeShutdownRequest = struct {
    type: []const u8 = "remove",
    reason: []const u8 = "",
};

pub const NodeShutdownStoreStatus = admin_read_operations.NodeShutdownStoreStatus;
pub const NodeShutdownStatus = admin_read_operations.NodeShutdownStatus;

const RestoreExtensionsRequest = struct {
    installed_extensions: []const extension_domain.InstalledExtension = &.{},
    extension_members: []const extension_domain.ExtensionMember = &.{},
    extension_dependencies: []const extension_domain.ExtensionDependency = &.{},
};

pub const ReplaceTableDefinitionRequest = struct {
    expected: metadata_table_manager.TableRecord,
    definition: metadata_table_manager.TableRecord,
};

pub const ReseedExactCutoverResult = table_operations.ReseedExactCutoverResult;

pub const AdminSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        head: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataHead = null,
        linearizable_head: ?*const fn (ptr: *anyopaque, request: operation.RequestContext) anyerror!metadata_api.MetadataHead = null,
        linearizable_snapshot: ?*const fn (ptr: *anyopaque, request: operation.RequestContext) anyerror!metadata_api.AdminSnapshot = null,
        runtime_topology: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataRuntimeTopology = null,
        status: *const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataStatus,
        admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
        validate_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) anyerror!bool = null,
        validate_table_publication: ?*const fn (ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) anyerror!bool = null,
        free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
        create_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) anyerror!void = null,
        replace_table_definition: ?*const fn (ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) anyerror!void = null,
        restore_table: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            location_uri: []const u8,
            connection: []const u8,
            artifact_backup_id: []const u8,
            manifest: *const backups_api.TableBackupManifest,
        ) anyerror!void = null,
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
            .metadata_incarnation = current_status.metadata_incarnation,
            .metadata_epoch = current_status.metadata_epoch,
        };
    }

    pub fn linearizableHead(self: AdminSource, request: operation.RequestContext) !metadata_api.MetadataHead {
        const fn_ptr = self.vtable.linearizable_head orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn linearizableSnapshot(self: AdminSource, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        const fn_ptr = self.vtable.linearizable_snapshot orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, request);
    }

    pub fn status(self: AdminSource) !metadata_api.MetadataStatus {
        return try self.vtable.status(self.ptr);
    }

    pub fn runtimeTopology(self: AdminSource) !metadata_api.MetadataRuntimeTopology {
        const topology_fn = self.vtable.runtime_topology orelse return error.UnsupportedOperation;
        return try topology_fn(self.ptr);
    }

    pub fn adminSnapshot(self: AdminSource) !metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn validatePublication(self: AdminSource, contract: metadata_api.CatalogPublicationContract) !bool {
        const validate = self.vtable.validate_publication orelse return error.UnsupportedOperation;
        return try validate(self.ptr, contract);
    }

    pub fn validateTablePublication(self: AdminSource, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const validate = self.vtable.validate_table_publication orelse return error.UnsupportedOperation;
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

    pub fn restoreTable(
        self: AdminSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        artifact_backup_id: []const u8,
        manifest: *const backups_api.TableBackupManifest,
    ) !void {
        const fn_ptr = self.vtable.restore_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, location_uri, connection, artifact_backup_id, manifest);
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
                .linearizable_head = metadataServiceLinearizableHead,
                .linearizable_snapshot = metadataServiceLinearizableSnapshot,
                .runtime_topology = metadataServiceRuntimeTopology,
                .status = metadataServiceStatus,
                .admin_snapshot = metadataServiceAdminSnapshot,
                .validate_publication = metadataServiceValidatePublication,
                .validate_table_publication = metadataServiceValidateTablePublication,
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
                .linearizable_head = metadataHttpServiceLinearizableHead,
                .linearizable_snapshot = metadataHttpServiceLinearizableSnapshot,
                .runtime_topology = metadataHttpServiceRuntimeTopology,
                .status = metadataHttpServiceStatus,
                .admin_snapshot = metadataHttpServiceAdminSnapshot,
                .validate_publication = metadataHttpServiceValidatePublication,
                .validate_table_publication = metadataHttpServiceValidateTablePublication,
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

    fn sameSnapshotFence(before: service.AdminSnapshotFence, after: service.AdminSnapshotFence) bool {
        return service.sameAdminSnapshotFence(before, after);
    }

    /// The read-index barrier establishes authority, while the before/after
    /// status token makes the multi-collection AdminSnapshot capture atomic.
    /// Optimistic retries avoid holding the Raft runtime lock while cloning a
    /// potentially large response.
    fn coherentLinearizableSnapshot(
        comptime Service: type,
        svc: *Service,
        request: operation.RequestContext,
    ) !metadata_api.AdminSnapshot {
        return service.coherentLinearizableAdminSnapshot(Service, svc, request);
    }

    fn metadataServiceLinearizableHead(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.MetadataHead {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try svc.ensureLinearizableReadWithContext(request);
        try request.ensureActive();
        return svc.head();
    }

    fn metadataServiceLinearizableSnapshot(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try coherentLinearizableSnapshot(service.MetadataService, svc, request);
    }

    fn metadataServiceStatus(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.status();
    }

    fn metadataServiceRuntimeTopology(ptr: *anyopaque) !metadata_api.MetadataRuntimeTopology {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.runtimeTopology();
    }

    fn metadataServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.adminSnapshot();
    }

    fn metadataServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.validatePublication(contract);
    }

    fn metadataServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        return try svc.validateTablePublication(contract);
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
        try svc.replaceTableDefinition(expected, replacement);
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

    fn metadataServiceRestoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        artifact_backup_id: []const u8,
        manifest: *const backups_api.TableBackupManifest,
    ) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try persistRestoreTableIntent(svc, alloc, table_name, location_uri, connection, artifact_backup_id, manifest);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.requestReallocation(platform_clock.Clock.real().nowRealtimeMs());
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
        // This route returns 202: a successful durable proposal is the
        // acceptance boundary. Projection is asynchronous, so requiring the
        // transition to be visible here spuriously rejects a valid admission.
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
        var installed = try extension_lifecycle.installOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceUpdateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.updateOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceDropExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) !void {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        try extension_lifecycle.dropOnService(svc, alloc, name, req);
        if (!req.dry_run) try flushMetadataServiceMutation(svc);
    }

    fn metadataServiceEnableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.enableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceDisableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.disableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataServiceMutation(svc);
        return installed;
    }

    fn metadataServiceConfigureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.configureOnService(svc, alloc, name, req);
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
        try extension_lifecycle.restoreOnService(svc, installed, members, dependencies);
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

    fn metadataHttpServiceLinearizableHead(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.MetadataHead {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try svc.ensureLinearizableReadWithContext(request);
        try request.ensureActive();
        return svc.head();
    }

    fn metadataHttpServiceLinearizableSnapshot(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try coherentLinearizableSnapshot(service.MetadataHttpService, svc, request);
    }

    fn metadataHttpServiceRuntimeTopology(ptr: *anyopaque) !metadata_api.MetadataRuntimeTopology {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.runtimeTopology();
    }

    fn metadataHttpServiceAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.adminSnapshot();
    }

    fn metadataHttpServiceValidatePublication(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        // Followers use Raft's built-in ReadIndex forwarding and wait until
        // the returned committed index is applied locally.
        return try svc.validatePublication(contract);
    }

    fn metadataHttpServiceValidateTablePublication(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        return try svc.validateTablePublication(contract);
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

    fn metadataHttpServiceRestoreTable(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        artifact_backup_id: []const u8,
        manifest: *const backups_api.TableBackupManifest,
    ) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try persistRestoreTableIntent(svc, alloc, table_name, location_uri, connection, artifact_backup_id, manifest);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.replaceTableDefinition(table.*, updated);
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
        try svc.requestReallocation(platform_clock.Clock.real().nowRealtimeMs());
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
        // A 202 acknowledges the durable proposal. The Raft apply/projection
        // boundary is asynchronous and must be observed through status APIs.
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
        var installed = try extension_lifecycle.installOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceUpdateExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.updateOnService(svc, alloc, name, req);
        errdefer installed.deinitOwned(alloc);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceDropExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.DropExtensionRequest) !void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        try extension_lifecycle.dropOnService(svc, alloc, name, req);
        if (!req.dry_run) try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceEnableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.enableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceDisableExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.disableOnService(svc, alloc, name);
        errdefer installed.deinitOwned(alloc);
        try flushMetadataHttpServiceMutation(svc);
        return installed;
    }

    fn metadataHttpServiceConfigureExtension(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, req: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        var installed = try extension_lifecycle.configureOnService(svc, alloc, name, req);
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
        try extension_lifecycle.restoreOnService(svc, installed, members, dependencies);
        try flushMetadataHttpServiceMutation(svc);
    }

    fn metadataHttpServiceRecordJsonResponseAllocation(ptr: *anyopaque, bytes: usize) void {
        const svc: *service.MetadataHttpService = @ptrCast(@alignCast(ptr));
        svc.recordJsonResponseAllocation(bytes);
    }
};

pub const MetadataHttpServer = struct {
    source: AdminSource,

    pub fn init(alloc: std.mem.Allocator, cfg: MetadataHttpServerConfig, source: AdminSource) MetadataHttpServer {
        _ = alloc;
        _ = cfg;
        return .{ .source = source };
    }

    pub fn deinit(self: *MetadataHttpServer) void {
        self.* = undefined;
    }

    /// Registers the metadata administration contract as concrete contextual
    /// routes. Unknown paths are rejected by httpx and never enter the
    /// metadata operation layer.
    pub fn registerRoutes(self: *MetadataHttpServer, server: *httpx.Server) !void {
        try server.get(routes.Routes.health, httpx.Handler.bind(self, metadataHealth));
        try server.get(routes.Routes.head, httpx.Handler.bind(self, metadataHead));
        // POST prevents intermediary GET caches from bypassing the read-index
        // barrier that gives this endpoint its meaning.
        try server.post(routes.Routes.internal_linearizable_head, httpx.Handler.bind(self, metadataLinearizableHead));
        try server.post(routes.Routes.internal_linearizable_snapshot, httpx.Handler.bind(self, metadataLinearizableSnapshot));
        try server.get(routes.Routes.runtime_topology, httpx.Handler.bind(self, metadataRuntimeTopology));
        try server.get(routes.Routes.status, httpx.Handler.bind(self, metadataStatus));
        try server.get(routes.Routes.admin_snapshot, httpx.Handler.bind(self, metadataSnapshot));
        try server.get(routes.Routes.active_transitions, httpx.Handler.bind(self, metadataActiveTransitions));
        try server.get(
            routes.Routes.table_ranges_prefix ++ ":table_id" ++ routes.Routes.table_ranges_suffix,
            httpx.Handler.bind(self, metadataTableRanges),
        );
        try server.get(
            routes.Routes.group_placement_prefix ++ ":group_id" ++ routes.Routes.group_placement_suffix,
            httpx.Handler.bind(self, metadataGroupPlacement),
        );
        const node_shutdown_path = routes.Routes.internal_nodes_prefix ++ ":node_id" ++ routes.Routes.internal_node_shutdown_suffix;
        try server.get(node_shutdown_path, httpx.Handler.bind(self, metadataNodeShutdownStatus));
        try server.put(node_shutdown_path, httpx.Handler.bind(self, metadataRequestNodeShutdown));
        try server.delete(node_shutdown_path, httpx.Handler.bind(self, metadataCancelNodeShutdown));
        try server.post(routes.Routes.internal_nodes, httpx.Handler.bind(self, metadataRegisterNode));
        const node_path = routes.Routes.internal_nodes_prefix ++ ":node_id";
        try server.delete(node_path, httpx.Handler.bind(self, metadataFinalizeNodeShutdown));
        try server.post(node_path ++ routes.Routes.internal_node_status_suffix, httpx.Handler.bind(self, metadataReportNodeStatus));
        try server.post(routes.Routes.internal_catalog_publication_check, httpx.Handler.bind(self, metadataCatalogPublicationCheck));
        try server.post(routes.Routes.internal_catalog_table_publication_check, httpx.Handler.bind(self, metadataCatalogTablePublicationCheck));
        try server.post(routes.Routes.internal_reallocate, httpx.Handler.bind(self, metadataTriggerReallocate));
        try server.post(routes.Routes.internal_schema_progress, httpx.Handler.bind(self, metadataUpsertSchemaProgress));
        try server.post(routes.Routes.internal_extension_restore, httpx.Handler.bind(self, metadataRestoreExtensions));
        const extension_path = routes.Routes.internal_extensions_prefix ++ ":extension_name";
        try server.post(extension_path, httpx.Handler.bind(self, metadataInstallExtension));
        try server.post(extension_path ++ routes.Routes.internal_extension_update_suffix, httpx.Handler.bind(self, metadataUpdateExtension));
        try server.post(extension_path ++ routes.Routes.internal_extension_drop_suffix, httpx.Handler.bind(self, metadataDropExtension));
        try server.post(extension_path ++ routes.Routes.internal_extension_enable_suffix, httpx.Handler.bind(self, metadataEnableExtension));
        try server.post(extension_path ++ routes.Routes.internal_extension_disable_suffix, httpx.Handler.bind(self, metadataDisableExtension));
        try server.put(extension_path ++ routes.Routes.internal_extension_config_suffix, httpx.Handler.bind(self, metadataConfigureExtension));

        const table_path = routes.Routes.internal_tables_prefix ++ ":table_name";
        try server.post(table_path, httpx.Handler.bind(self, metadataCreateTable));
        try server.delete(table_path, httpx.Handler.bind(self, metadataDropTable));
        try server.put(table_path ++ routes.Routes.internal_table_definition_suffix, httpx.Handler.bind(self, metadataReplaceTableDefinition));
        try server.put(table_path ++ routes.Routes.internal_table_schema_suffix, httpx.Handler.bind(self, metadataUpdateTableSchema));
        const index_path = table_path ++ routes.Routes.internal_table_indexes_infix ++ ":index_name";
        try server.put(index_path, httpx.Handler.bind(self, metadataCreateTableIndex));
        try server.delete(index_path, httpx.Handler.bind(self, metadataDropTableIndex));
        const enrichment_path = table_path ++ routes.Routes.internal_table_enrichments_infix ++ ":enrichment_name";
        try server.put(enrichment_path, httpx.Handler.bind(self, metadataPutTableEnrichment));
        try server.delete(enrichment_path, httpx.Handler.bind(self, metadataDeleteTableEnrichment));
        try server.post(table_path ++ routes.Routes.internal_table_restore_suffix, httpx.Handler.bind(self, metadataRestoreTable));
        try server.post(table_path ++ routes.Routes.internal_table_replication_sources_infix ++ ":source_ordinal" ++ routes.Routes.internal_table_reseed_exact_cutover_suffix, httpx.Handler.bind(self, metadataReseedReplicationSourceExactCutover));
        try server.post(table_path ++ routes.Routes.internal_split_suffix, httpx.Handler.bind(self, metadataRequestTableSplit));
        try server.post(table_path ++ routes.Routes.internal_merge_suffix, httpx.Handler.bind(self, metadataRequestTableMerge));
    }

    fn requestContext(ctx: *httpx.Context) operation.RequestContext {
        return .{
            .cancellation = if (ctx.cancellation != null or ctx.cancellation_probe != null) .{
                .ptr = ctx,
                .is_cancelled_fn = struct {
                    fn call(raw: *const anyopaque) bool {
                        const context: *const httpx.Context = @ptrCast(@alignCast(raw));
                        return context.isCancellationRequested();
                    }
                }.call,
            } else .none,
            .request_id = ctx.header("x-request-id") orelse "",
        };
    }

    fn readOperations(self: *MetadataHttpServer) admin_read_operations.Operations {
        return .{ .source = .{
            .ptr = self,
            .vtable = &.{
                .head = readHead,
                .linearizable_head = readLinearizableHead,
                .linearizable_snapshot = readLinearizableSnapshot,
                .status = readStatus,
                .admin_snapshot = readSnapshot,
                .free_admin_snapshot = freeReadSnapshot,
            },
        } };
    }

    fn readHead(ptr: *anyopaque) !metadata_api.MetadataHead {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.head();
    }

    fn readLinearizableHead(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.MetadataHead {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.linearizableHead(request);
    }

    fn readLinearizableSnapshot(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.AdminSnapshot {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.linearizableSnapshot(request);
    }

    fn readStatus(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.status();
    }

    fn readSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.adminSnapshot();
    }

    fn freeReadSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        self.source.freeAdminSnapshot(snapshot);
    }

    fn trackedJson(self: *MetadataHttpServer, ctx: *httpx.Context, value: anytype) !httpx.Response {
        const response = try ctx.json(value);
        self.source.recordJsonResponseAllocation(if (response.body) |body| body.len else 0);
        return response;
    }

    fn executeTypedHandlerForTest(
        self: *MetadataHttpServer,
        method: httpx.Method,
        uri: []const u8,
        params: []const httpx.RouteParam,
        comptime handler: anytype,
    ) !httpx.Response {
        var request = try httpx.Request.init(std.testing.allocator, method, uri);
        defer request.deinit();
        var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
        defer ctx.deinit();
        ctx.params = params;
        return handler(self, &ctx);
    }

    fn executeTypedHandlerWithBodyForTest(
        self: *MetadataHttpServer,
        method: httpx.Method,
        uri: []const u8,
        params: []const httpx.RouteParam,
        body: []const u8,
        comptime handler: anytype,
    ) !httpx.Response {
        var request = try httpx.Request.init(std.testing.allocator, method, uri);
        defer request.deinit();
        try request.setBody(body);
        var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
        defer ctx.deinit();
        ctx.params = params;
        return handler(self, &ctx);
    }

    fn metadataReadError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        if (metadata_authority.isRetryableError(err)) {
            try ctx.setHeader("Retry-After", "1");
            try ctx.setHeader(http_common.metadata_not_leader_header, http_common.metadata_not_leader_value);
            return ctx.status(503).text("metadata authority unavailable");
        }
        return switch (err) {
            error.InvalidArgument => ctx.status(400).text("invalid path parameter"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            error.Canceled => ctx.status(408).text("request canceled"),
            error.DeadlineExceeded => ctx.status(504).text("request deadline exceeded"),
            else => err,
        };
    }

    fn metadataHealth(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        self.readOperations().health(requestContext(ctx)) catch |err| return metadataReadError(ctx, err);
        return ctx.text("ok");
    }

    fn metadataHead(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const result = self.readOperations().head(requestContext(ctx)) catch |err| return metadataReadError(ctx, err);
        return self.trackedJson(ctx, result);
    }

    fn metadataLinearizableHead(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const request = requestContext(ctx);
        const result = self.readOperations().linearizableHead(request) catch |err| return metadataReadError(ctx, err);
        request.ensureActive() catch |err| return metadataReadError(ctx, err);
        return self.trackedJson(ctx, result);
    }

    fn metadataLinearizableSnapshot(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const operations = self.readOperations();
        const request = requestContext(ctx);
        var result = operations.linearizableSnapshot(request) catch |err| return metadataReadError(ctx, err);
        defer operations.freeSnapshot(&result);
        request.ensureActive() catch |err| return metadataReadError(ctx, err);
        return self.trackedJson(ctx, result);
    }

    fn metadataRuntimeTopology(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const result = self.source.runtimeTopology() catch |err| return metadataReadError(ctx, err);
        return self.trackedJson(ctx, result);
    }

    fn metadataStatus(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const result = self.readOperations().status(requestContext(ctx)) catch |err| return metadataReadError(ctx, err);
        return self.trackedJson(ctx, result);
    }

    fn metadataSnapshot(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const operations = self.readOperations();
        var result = operations.snapshot(requestContext(ctx)) catch |err| return metadataReadError(ctx, err);
        defer operations.freeSnapshot(&result);
        return self.trackedJson(ctx, result);
    }

    fn metadataActiveTransitions(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        var result = self.readOperations().activeTransitions(ctx.allocator, requestContext(ctx)) catch |err| return metadataReadError(ctx, err);
        defer result.deinit(ctx.allocator);
        return self.trackedJson(ctx, result);
    }

    fn numericParam(ctx: *httpx.Context, name: []const u8, allow_zero: bool) !u64 {
        const value = ctx.param(name) orelse return error.InvalidArgument;
        const parsed = std.fmt.parseUnsigned(u64, value, 10) catch return error.InvalidArgument;
        if (!allow_zero and parsed == 0) return error.InvalidArgument;
        return parsed;
    }

    fn metadataTableRanges(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_id = numericParam(ctx, "table_id", true) catch |err| return metadataReadError(ctx, err);
        const records = self.readOperations().tableRanges(ctx.allocator, requestContext(ctx), table_id) catch |err| return metadataReadError(ctx, err);
        defer ctx.allocator.free(records);
        return self.trackedJson(ctx, records);
    }

    fn metadataGroupPlacement(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const group_id = numericParam(ctx, "group_id", true) catch |err| return metadataReadError(ctx, err);
        const records = self.readOperations().groupPlacement(ctx.allocator, requestContext(ctx), group_id) catch |err| return metadataReadError(ctx, err);
        defer ctx.allocator.free(records);
        return self.trackedJson(ctx, records);
    }

    fn metadataNodeShutdownStatus(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const node_id = numericParam(ctx, "node_id", false) catch return ctx.status(404).text("not found");
        var result = self.readOperations().nodeShutdownStatus(ctx.allocator, requestContext(ctx), node_id) catch |err| return metadataReadError(ctx, err);
        defer result.deinit(ctx.allocator);
        return self.trackedJson(ctx, result);
    }

    fn mutationOperations(self: *MetadataHttpServer) admin_mutation_operations.Operations {
        return .{ .source = .{
            .ptr = self,
            .vtable = &.{
                .validate_publication = validatePublicationOperation,
                .validate_table_publication = validateTablePublicationOperation,
                .trigger_reallocate = triggerReallocateOperation,
                .upsert_schema_progress = upsertSchemaProgressOperation,
            },
        } };
    }

    fn validatePublicationOperation(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.validatePublication(contract);
    }

    fn validateTablePublicationOperation(ptr: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.validateTablePublication(contract);
    }

    fn triggerReallocateOperation(ptr: *anyopaque) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.triggerReallocate();
    }

    fn upsertSchemaProgressOperation(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.SchemaProgressRecord) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.upsertSchemaProgress(alloc, record);
    }

    fn metadataMutationError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        if (err == error.UnsupportedOperation) return ctx.status(405).text("unsupported operation");
        if (err == error.ReallocationProtocolUpgradeRequired)
            return ctx.status(503).text("metadata voter upgrade required");
        if (metadata_authority.isMutationNotAdmittedError(err)) {
            // Raft rejected this command before assigning a log index. Only
            // this narrower proof authorizes an at-most-once client to route
            // the same mutation to another metadata replica.
            try ctx.setHeader(
                http_common.metadata_mutation_not_admitted_header,
                http_common.metadata_mutation_not_admitted_value,
            );
        }
        return metadataReadError(ctx, err);
    }

    fn metadataCatalogPublicationCheck(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse "";
        var parsed = std.json.parseFromSlice(metadata_api.CatalogPublicationContract, ctx.allocator, body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return ctx.status(400).text("invalid catalog publication contract");
        defer parsed.deinit();
        const valid = self.mutationOperations().validatePublication(requestContext(ctx), parsed.value) catch |err|
            return metadataMutationError(ctx, err);
        return ctx.status(if (valid) 204 else 409).text("");
    }

    fn metadataCatalogTablePublicationCheck(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse "";
        var parsed = std.json.parseFromSlice(metadata_api.CatalogTablePublicationContract, ctx.allocator, body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return ctx.status(400).text("invalid catalog table publication contract");
        defer parsed.deinit();
        const valid = self.mutationOperations().validateTablePublication(requestContext(ctx), parsed.value) catch |err|
            return metadataMutationError(ctx, err);
        return ctx.status(if (valid) 204 else 409).text("");
    }

    fn metadataTriggerReallocate(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        self.mutationOperations().triggerReallocate(requestContext(ctx)) catch |err|
            return metadataMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn metadataUpsertSchemaProgress(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse "";
        var parsed = std.json.parseFromSlice(metadata_table_manager.SchemaProgressRecord, ctx.allocator, body, .{}) catch
            return ctx.status(400).text("invalid schema progress request");
        defer parsed.deinit();
        self.mutationOperations().upsertSchemaProgress(ctx.allocator, requestContext(ctx), parsed.value) catch |err|
            return metadataMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn extensionOperations(self: *MetadataHttpServer) extension_operations.Operations {
        return .{ .source = .{ .ptr = self, .vtable = &.{
            .install = installExtensionOperation,
            .update = updateExtensionOperation,
            .drop = dropExtensionOperation,
            .enable = enableExtensionOperation,
            .disable = disableExtensionOperation,
            .configure = configureExtensionOperation,
            .restore = restoreExtensionsOperation,
        } } };
    }

    fn installExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, request: extension_domain.InstallExtensionRequest) !extension_domain.InstalledExtension {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.installExtension(alloc, name, request);
    }

    fn updateExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, request: extension_domain.UpdateExtensionRequest) !extension_domain.InstalledExtension {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.updateExtension(alloc, name, request);
    }

    fn dropExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, request: extension_domain.DropExtensionRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.dropExtension(alloc, name, request);
    }

    fn enableExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.enableExtension(alloc, name);
    }

    fn disableExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) !extension_domain.InstalledExtension {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.disableExtension(alloc, name);
    }

    fn configureExtensionOperation(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, request: extension_domain.ConfigureExtensionRequest) !extension_domain.InstalledExtension {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.configureExtension(alloc, name, request);
    }

    fn restoreExtensionsOperation(ptr: *anyopaque, alloc: std.mem.Allocator, installed: []const extension_domain.InstalledExtension, members: []const extension_domain.ExtensionMember, dependencies: []const extension_domain.ExtensionDependency) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.restoreExtensions(alloc, installed, members, dependencies);
    }

    fn extensionName(ctx: *httpx.Context) ![]const u8 {
        const name = ctx.param("extension_name") orelse return error.InvalidArgument;
        if (name.len == 0) return error.InvalidArgument;
        return name;
    }

    fn extensionError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        return switch (err) {
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            error.PackageNotFound, error.ExtensionNotInstalled, error.TableNotFound => ctx.status(404).text("not found"),
            error.ExtensionAlreadyInstalled => ctx.status(409).text("extension already installed"),
            error.DependentExtensionExists => ctx.status(409).text("dependent extension exists"),
            error.RequiredExtensionNotInstalled => ctx.status(409).text("required extension not installed"),
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
            => ctx.status(400).text("invalid extension lifecycle request"),
            else => metadataReadError(ctx, err),
        };
    }

    fn metadataInstallExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var parsed = std.json.parseFromSlice(extension_domain.InstallExtensionRequest, ctx.allocator, jsonBodyOrEmptyObject((try ctx.body()) orelse ""), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch
            return ctx.status(400).text("invalid extension install request");
        defer parsed.deinit();
        var installed = self.extensionOperations().install(ctx.allocator, requestContext(ctx), name, parsed.value) catch |err| return extensionError(ctx, err);
        defer installed.deinitOwned(ctx.allocator);
        return self.trackedJson(ctx, installed);
    }

    fn metadataUpdateExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var parsed = std.json.parseFromSlice(extension_domain.UpdateExtensionRequest, ctx.allocator, jsonBodyOrEmptyObject((try ctx.body()) orelse ""), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch
            return ctx.status(400).text("invalid extension update request");
        defer parsed.deinit();
        var installed = self.extensionOperations().update(ctx.allocator, requestContext(ctx), name, parsed.value) catch |err| return extensionError(ctx, err);
        defer installed.deinitOwned(ctx.allocator);
        return self.trackedJson(ctx, installed);
    }

    fn metadataDropExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var parsed = std.json.parseFromSlice(extension_domain.DropExtensionRequest, ctx.allocator, jsonBodyOrEmptyObject((try ctx.body()) orelse ""), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch
            return ctx.status(400).text("invalid extension drop request");
        defer parsed.deinit();
        self.extensionOperations().drop(ctx.allocator, requestContext(ctx), name, parsed.value) catch |err| return extensionError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn metadataEnableExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var installed = self.extensionOperations().enable(ctx.allocator, requestContext(ctx), name) catch |err| return extensionError(ctx, err);
        defer installed.deinitOwned(ctx.allocator);
        return self.trackedJson(ctx, installed);
    }

    fn metadataDisableExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var installed = self.extensionOperations().disable(ctx.allocator, requestContext(ctx), name) catch |err| return extensionError(ctx, err);
        defer installed.deinitOwned(ctx.allocator);
        return self.trackedJson(ctx, installed);
    }

    fn metadataConfigureExtension(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const name = extensionName(ctx) catch |err| return extensionError(ctx, err);
        var parsed = std.json.parseFromSlice(extension_domain.ConfigureExtensionRequest, ctx.allocator, jsonBodyOrEmptyObject((try ctx.body()) orelse ""), .{ .allocate = .alloc_always, .ignore_unknown_fields = true }) catch
            return ctx.status(400).text("invalid extension config request");
        defer parsed.deinit();
        var installed = self.extensionOperations().configure(ctx.allocator, requestContext(ctx), name, parsed.value) catch |err| return extensionError(ctx, err);
        defer installed.deinitOwned(ctx.allocator);
        return self.trackedJson(ctx, installed);
    }

    fn metadataRestoreExtensions(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        var parsed = std.json.parseFromSlice(RestoreExtensionsRequest, ctx.allocator, (try ctx.body()) orelse "", .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return ctx.status(400).text("invalid extension restore request");
        defer parsed.deinit();
        self.extensionOperations().restore(ctx.allocator, requestContext(ctx), parsed.value.installed_extensions, parsed.value.extension_members, parsed.value.extension_dependencies) catch |err| switch (err) {
            error.UnsupportedManifestApiVersion,
            error.UnsupportedPackageKind,
            error.UnsupportedExtensionScope,
            error.UnsupportedObjectKindForV1,
            error.InvalidJsonObject,
            error.EmptyName,
            error.InvalidIdentifier,
            error.MemberTableOutsideScope,
            => return ctx.status(400).text("invalid extension restore request"),
            else => return extensionError(ctx, err),
        };
        return ctx.status(202).text("accepted");
    }

    fn nodeOperations(self: *MetadataHttpServer) node_operations.Operations {
        return .{ .source = .{
            .ptr = self,
            .vtable = &.{
                .snapshot = nodeSnapshotOperation,
                .free_snapshot = freeNodeSnapshotOperation,
                .upsert_node = upsertNodeOperation,
                .upsert_store = upsertStoreOperation,
                .report_store_status = reportStoreStatusOperation,
                .request_shutdown = requestNodeShutdownOperation,
                .cancel_shutdown = cancelNodeShutdownOperation,
                .finalize_shutdown = finalizeNodeShutdownOperation,
                .trigger_reallocate = triggerReallocateOperation,
            },
            .supports_upsert_node = self.source.vtable.upsert_node != null,
            .supports_upsert_store = self.source.vtable.upsert_store != null,
            .supports_report_store_status = self.source.vtable.report_store_status != null,
            .supports_request_shutdown = self.source.vtable.request_node_shutdown != null,
            .supports_cancel_shutdown = self.source.vtable.cancel_node_shutdown != null,
            .supports_finalize_shutdown = self.source.vtable.finalize_node_shutdown != null,
        } };
    }

    fn nodeSnapshotOperation(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.adminSnapshot();
    }

    fn freeNodeSnapshotOperation(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        self.source.freeAdminSnapshot(snapshot);
    }

    fn upsertNodeOperation(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.upsertNode(alloc, record);
    }

    fn upsertStoreOperation(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.upsertStore(alloc, record);
    }

    fn reportStoreStatusOperation(ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.reportStoreStatus(alloc, report);
    }

    fn requestNodeShutdownOperation(ptr: *anyopaque, node_id: u64) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.requestNodeShutdown(node_id);
    }

    fn cancelNodeShutdownOperation(ptr: *anyopaque, node_id: u64) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.cancelNodeShutdown(node_id);
    }

    fn finalizeNodeShutdownOperation(ptr: *anyopaque, node_id: u64) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.finalizeNodeShutdown(node_id);
    }

    fn nodeMutationError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        return switch (err) {
            error.InvalidArgument, error.StoreIdentityMismatch => ctx.status(400).text("invalid node request"),
            error.NodeNotFound, error.UnknownStore => ctx.status(404).text("node not found"),
            error.ActiveNodeFinalizeRejected => ctx.status(409).text("node is not ready to finalize"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            else => metadataReadError(ctx, err),
        };
    }

    fn nodeLifecycleMutationError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        if (err == error.MetadataMutationOutcomeUnknown) {
            // An earlier lifecycle command may already be durable even though
            // a follow-up reallocation step lost authority. Expose only the
            // broad observation hint; replay is not proven safe.
            try ctx.setHeader("Retry-After", "1");
            try ctx.setHeader(http_common.metadata_not_leader_header, http_common.metadata_not_leader_value);
            return ctx.status(503).text("metadata mutation outcome unknown");
        }
        if (metadata_authority.isMutationNotAdmittedError(err)) {
            try ctx.setHeader(
                http_common.metadata_mutation_not_admitted_header,
                http_common.metadata_mutation_not_admitted_value,
            );
        }
        return nodeMutationError(ctx, err);
    }

    fn metadataRegisterNode(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const body = (try ctx.body()) orelse "";
        const node = parseNodeRecord(ctx.allocator, body) catch return ctx.status(400).text("invalid node registration request");
        var registration = node_operations.Registration{ .node = node };
        defer registration.deinit(ctx.allocator);
        if (parseNodeRegistrationIncludesStore(ctx.allocator, body) catch return ctx.status(400).text("invalid node registration request")) {
            registration.store = parseStoreRecord(ctx.allocator, body) catch return ctx.status(400).text("invalid node registration request");
        }
        self.nodeOperations().register(ctx.allocator, requestContext(ctx), &registration) catch |err| switch (err) {
            error.StoreIdentityMismatch => return ctx.status(400).text("store identity must match node identity"),
            else => return nodeMutationError(ctx, err),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataReportNodeStatus(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const node_id = numericParam(ctx, "node_id", false) catch return ctx.status(404).text("not found");
        const report = parseNodeStatusReport(ctx.allocator, (try ctx.body()) orelse "", node_id) catch
            return ctx.status(400).text("invalid node status request");
        var owned = node_operations.StatusReport{ .value = report };
        defer owned.deinit(ctx.allocator);
        self.nodeOperations().reportStatus(ctx.allocator, requestContext(ctx), &owned) catch |err| return nodeMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn metadataRequestNodeShutdown(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const node_id = numericParam(ctx, "node_id", false) catch return ctx.status(404).text("not found");
        parseNodeShutdownRequest(ctx.allocator, (try ctx.body()) orelse "") catch
            return ctx.status(400).text("invalid node shutdown request");
        self.nodeOperations().requestShutdown(ctx.allocator, requestContext(ctx), node_id) catch |err| return nodeLifecycleMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn metadataCancelNodeShutdown(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const node_id = numericParam(ctx, "node_id", false) catch return ctx.status(404).text("not found");
        self.nodeOperations().cancelShutdown(ctx.allocator, requestContext(ctx), node_id) catch |err| return nodeLifecycleMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn metadataFinalizeNodeShutdown(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const node_id = numericParam(ctx, "node_id", false) catch return ctx.status(404).text("not found");
        self.nodeOperations().finalizeShutdown(requestContext(ctx), node_id) catch |err| return nodeLifecycleMutationError(ctx, err);
        return ctx.status(202).text("accepted");
    }

    fn tableOperations(self: *MetadataHttpServer) table_operations.Operations {
        return .{ .source = .{ .ptr = self, .vtable = &.{
            .create_table = createTableOperation,
            .replace_definition = replaceTableDefinitionOperation,
            .restore_table = restoreTableOperation,
            .drop_table = dropTableOperation,
            .update_schema = updateTableSchemaOperation,
            .create_index = createTableIndexOperation,
            .drop_index = dropTableIndexOperation,
            .put_enrichment = putTableEnrichmentOperation,
            .delete_enrichment = deleteTableEnrichmentOperation,
            .validate_split = validateTableSplitOperation,
            .request_split = requestTableSplitOperation,
            .validate_merge = validateTableMergeOperation,
            .request_merge = requestTableMergeOperation,
            .reseed_exact_cutover = reseedTableExactCutoverOperation,
        } } };
    }

    fn createTableOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, request: tables_api.CreateTableRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.createTable(alloc, table_name, request);
    }

    fn replaceTableDefinitionOperation(ptr: *anyopaque, expected: metadata_table_manager.TableRecord, replacement: metadata_table_manager.TableRecord) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.replaceTableDefinition(expected, replacement);
    }

    fn restoreTableOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, request: table_operations.RestoreRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.restoreTable(alloc, table_name, request.location, request.connection, request.artifact_backup_id, &request.manifest);
    }

    fn dropTableOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.dropTable(alloc, table_name);
    }

    fn updateTableSchemaOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.updateSchema(alloc, table_name, schema_json);
    }

    fn createTableIndexOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.createIndex(alloc, table_name, index_name, index_json);
    }

    fn dropTableIndexOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.dropIndex(alloc, table_name, index_name);
    }

    fn putTableEnrichmentOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.putArtifactEnrichment(alloc, table_name, enrichment_name, enrichment_json);
    }

    fn deleteTableEnrichmentOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.deleteArtifactEnrichment(alloc, table_name, enrichment_name);
    }

    fn validateTableSplitOperation(ptr: *anyopaque, table_name: []const u8, request: SplitRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return validateSplitRequestDocIdentity(self.source, table_name, request);
    }

    fn requestTableSplitOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, request: SplitRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.requestSplit(alloc, table_name, request);
    }

    fn validateTableMergeOperation(ptr: *anyopaque, table_name: []const u8, request: MergeRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return validateMergeRequestDocIdentity(self.source, table_name, request);
    }

    fn requestTableMergeOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, request: MergeRequest) !void {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.requestMerge(alloc, table_name, request);
    }

    fn reseedTableExactCutoverOperation(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, source_ordinal: u32) !ReseedExactCutoverResult {
        const self: *MetadataHttpServer = @ptrCast(@alignCast(ptr));
        return self.source.reseedReplicationSourceExactCutover(alloc, table_name, source_ordinal);
    }

    fn requiredParam(ctx: *httpx.Context, name: []const u8) ![]const u8 {
        const value = ctx.param(name) orelse return error.InvalidArgument;
        if (value.len == 0) return error.InvalidArgument;
        return value;
    }

    fn metadataCreateTable(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        var request = parseCreateTableRequest(ctx.allocator, (try ctx.body()) orelse "") catch
            return ctx.status(400).text("invalid create table request");
        defer request.deinit(ctx.allocator);
        self.tableOperations().create(ctx.allocator, requestContext(ctx), table_name, request) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest, error.InvalidArgument => return ctx.status(400).text("invalid create table request"),
            error.UnsupportedOperation => return ctx.status(405).text("unsupported operation"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(201).text("created");
    }

    fn metadataReplaceTableDefinition(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        var parsed = std.json.parseFromSlice(ReplaceTableDefinitionRequest, ctx.allocator, (try ctx.body()) orelse "", .{ .allocate = .alloc_always }) catch
            return ctx.status(400).text("invalid table definition replacement");
        defer parsed.deinit();
        self.tableOperations().replaceDefinition(requestContext(ctx), table_name, parsed.value.expected, parsed.value.definition) catch |err| switch (err) {
            error.TableNameMismatch => return ctx.status(400).text("table definition name mismatch"),
            error.ExpectedTableNameMismatch => return ctx.status(400).text("expected table definition name mismatch"),
            error.TableNotFound => return ctx.status(404).text("table not found"),
            error.TableGenerationChanged => return ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => return ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject, error.UnsupportedOperation => return ctx.status(405).text("method not allowed"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataDropTable(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        self.tableOperations().drop(ctx.allocator, requestContext(ctx), table_name) catch |err| switch (err) {
            error.TableNotFound => return ctx.status(404).text("table not found"),
            error.TableTransitionActive => return ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject => return ctx.status(405).text("method not allowed"),
            error.UnsupportedOperation => return ctx.status(405).text("unsupported operation"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(204).text("");
    }

    fn metadataUpdateTableSchema(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        self.tableOperations().updateSchema(ctx.allocator, requestContext(ctx), table_name, (try ctx.body()) orelse "") catch |err| switch (err) {
            error.TableNotFound => return ctx.status(404).text("table not found"),
            error.TableGenerationChanged => return ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => return ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject => return ctx.status(405).text("method not allowed"),
            error.UnsupportedOperation => return ctx.status(405).text("unsupported operation"),
            error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => return ctx.status(400).text("invalid schema update request"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataCreateTableIndex(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        const index_name = requiredParam(ctx, "index_name") catch return ctx.status(400).text("invalid index name");
        self.tableOperations().createIndex(ctx.allocator, requestContext(ctx), table_name, index_name, (try ctx.body()) orelse "") catch |err| switch (err) {
            error.TableNotFound => return ctx.status(404).text("table not found"),
            error.TableGenerationChanged => return ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => return ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject => return ctx.status(405).text("method not allowed"),
            error.UnsupportedOperation => return ctx.status(405).text("unsupported operation"),
            error.InvalidTableIndexMetadata, error.InvalidCreateIndexRequest, error.UnsupportedCreateTableRequest => return ctx.status(400).text("unsupported index configuration"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataDropTableIndex(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        const index_name = requiredParam(ctx, "index_name") catch return ctx.status(400).text("invalid index name");
        self.tableOperations().dropIndex(ctx.allocator, requestContext(ctx), table_name, index_name) catch |err| switch (err) {
            error.TableNotFound, error.IndexNotFound => return ctx.status(404).text("index not found"),
            error.TableGenerationChanged => return ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => return ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject => return ctx.status(405).text("method not allowed"),
            error.UnsupportedOperation => return ctx.status(405).text("unsupported operation"),
            else => return metadataReadError(ctx, err),
        };
        return ctx.status(204).text("");
    }

    fn decodedEnrichmentParams(ctx: *httpx.Context) !struct { table_name: []u8, enrichment_name: []u8 } {
        const raw_table = requiredParam(ctx, "table_name") catch return error.InvalidTableName;
        const table_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.allocator, raw_table) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidTableName,
            else => return err,
        };
        errdefer ctx.allocator.free(table_name);
        const raw_enrichment = requiredParam(ctx, "enrichment_name") catch return error.InvalidEnrichmentName;
        return .{
            .table_name = table_name,
            .enrichment_name = http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.allocator, raw_enrichment) catch |err| switch (err) {
                error.InvalidArgument => return error.InvalidEnrichmentName,
                else => return err,
            },
        };
    }

    fn metadataPutTableEnrichment(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const params = decodedEnrichmentParams(ctx) catch |err| switch (err) {
            error.InvalidTableName => return ctx.status(400).text("invalid table name"),
            error.InvalidEnrichmentName => return ctx.status(400).text("invalid artifact enrichment name"),
            else => return err,
        };
        defer ctx.allocator.free(params.table_name);
        defer ctx.allocator.free(params.enrichment_name);
        self.tableOperations().putEnrichment(ctx.allocator, requestContext(ctx), params.table_name, params.enrichment_name, (try ctx.body()) orelse "") catch |err|
            return tableEnrichmentError(ctx, err, false);
        return ctx.status(202).text("accepted");
    }

    fn metadataDeleteTableEnrichment(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const params = decodedEnrichmentParams(ctx) catch |err| switch (err) {
            error.InvalidTableName => return ctx.status(400).text("invalid table name"),
            error.InvalidEnrichmentName => return ctx.status(400).text("invalid artifact enrichment name"),
            else => return err,
        };
        defer ctx.allocator.free(params.table_name);
        defer ctx.allocator.free(params.enrichment_name);
        self.tableOperations().deleteEnrichment(ctx.allocator, requestContext(ctx), params.table_name, params.enrichment_name) catch |err|
            return tableEnrichmentError(ctx, err, true);
        return ctx.status(204).text("");
    }

    fn tableEnrichmentError(ctx: *httpx.Context, err: anyerror, deleting: bool) !httpx.Response {
        return switch (err) {
            error.TableNotFound => ctx.status(404).text(if (deleting) "artifact enrichment not found" else "table not found"),
            error.EnrichmentNotFound => ctx.status(404).text(if (deleting) "artifact enrichment not found" else "table not found"),
            error.TableGenerationChanged => ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => ctx.status(409).text("table transition active"),
            error.ExtensionOwnedObject => ctx.status(405).text("method not allowed"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            error.InvalidTableIndexMetadata, error.InvalidExtensionEnrichment, error.InvalidEnrichmentConfig, error.ConflictingEnrichmentConfig => ctx.status(400).text("unsupported artifact enrichment configuration"),
            else => metadataReadError(ctx, err),
        };
    }

    fn metadataRestoreTable(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        var parsed = std.json.parseFromSlice(table_operations.RestoreRequest, ctx.allocator, (try ctx.body()) orelse "", .{ .allocate = .alloc_always }) catch
            return ctx.status(400).text("invalid restore request");
        defer parsed.deinit();
        self.tableOperations().restore(ctx.allocator, requestContext(ctx), table_name, parsed.value) catch |err| {
            if (backups_api.backupLocationErrorMessage(err)) |msg| return ctx.status(400).text(msg);
            return switch (err) {
                error.TableAlreadyExists => ctx.status(409).text("table already exists"),
                error.InvalidBackupRequest, error.UnsupportedBackupFormat, error.UnsupportedBackupMigrationState => ctx.status(400).text("invalid restore request"),
                error.BackupIntegrityMissing,
                error.BackupArtifactIntegrityMismatch,
                error.BackupArtifactMissing,
                error.BackupArtifactFormatMismatch,
                => ctx.status(400).text(backups_api.integrity_failure_message),
                error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
                else => metadataReadError(ctx, err),
            };
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataRequestTableSplit(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        const split_request = parseSplitRequest(ctx.allocator, (try ctx.body()) orelse "") catch
            return ctx.status(400).text("invalid split request");
        defer ctx.allocator.free(split_request.split_key);
        self.tableOperations().requestSplit(ctx.allocator, requestContext(ctx), table_name, split_request) catch |err| return switch (err) {
            error.TableNotFound, error.RangeNotFound => ctx.status(404).text("not found"),
            error.DocIdentityNamespaceMismatch => ctx.status(409).text("doc identity namespace mismatch"),
            error.SplitInProgress, error.ConflictingSplitTransition => ctx.status(409).text("split already in progress"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            error.Canceled, error.DeadlineExceeded => metadataReadError(ctx, err),
            else => ctx.status(400).text("invalid split request"),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataRequestTableMerge(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        const merge_request = parseMergeRequest(ctx.allocator, (try ctx.body()) orelse "") catch
            return ctx.status(400).text("invalid merge request");
        self.tableOperations().requestMerge(ctx.allocator, requestContext(ctx), table_name, merge_request) catch |err| return switch (err) {
            error.TableNotFound, error.RangeNotFound => ctx.status(404).text("not found"),
            error.DocIdentityNamespaceMismatch => ctx.status(409).text("doc identity namespace mismatch"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            error.Canceled, error.DeadlineExceeded => metadataReadError(ctx, err),
            else => ctx.status(400).text("invalid merge request"),
        };
        return ctx.status(202).text("accepted");
    }

    fn metadataReseedReplicationSourceExactCutover(self: *MetadataHttpServer, ctx: *httpx.Context) !httpx.Response {
        const table_name = requiredParam(ctx, "table_name") catch return ctx.status(400).text("invalid table name");
        const raw_source_ordinal = requiredParam(ctx, "source_ordinal") catch return ctx.status(400).text("invalid replication source");
        const source_ordinal = std.fmt.parseInt(u32, raw_source_ordinal, 10) catch return ctx.status(400).text("invalid replication source");
        var result = self.tableOperations().reseedExactCutover(ctx.allocator, requestContext(ctx), table_name, source_ordinal) catch |err| return switch (err) {
            error.TableNotFound, error.UnknownReplicationSource => ctx.status(404).text("not found"),
            error.InvalidReplicationSourceConfig, error.UnsupportedReplicationSource => ctx.status(400).text("invalid replication source"),
            error.TableGenerationChanged => ctx.status(409).text("table generation changed"),
            error.TableTransitionActive => ctx.status(409).text("table transition active"),
            error.UnsupportedOperation => ctx.status(405).text("unsupported operation"),
            else => metadataReadError(ctx, err),
        };
        defer result.deinit(ctx.allocator);
        _ = ctx.status(202);
        return self.trackedJson(ctx, result);
    }
};

const InternalTableRestoreRequest = table_operations.RestoreRequest;

/// The ingress data node validates and content-binds the manifest using the
/// named connection. Metadata persists that authority identifier and exact
/// artifact identity without requiring backup credentials itself.
fn testInternalTableRestoreRequestBodyAlloc(
    alloc: std.mem.Allocator,
    backup_id: []const u8,
    table_name: []const u8,
    location: []const u8,
    connection: []const u8,
) ![]u8 {
    const manifest = backups_api.TableBackupManifest{
        .format = .native,
        .backup_id = backup_id,
        .table_name = table_name,
        .description = "test restore manifest",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = "{}",
        .replication_sources_json = "[]",
        .shards = &.{.{
            .group_id = 7001,
            .start_key = "",
            .end_key = null,
            .snapshot_path = "artifacts/groups/7001",
            .artifact_size_bytes = 0,
            .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        }},
    };
    return try std.json.Stringify.valueAlloc(alloc, InternalTableRestoreRequest{
        .backup_id = backup_id,
        .artifact_backup_id = backup_id,
        .location = location,
        .connection = connection,
        .manifest = manifest,
    }, .{});
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
    connection: []const u8,
    artifact_backup_id: []const u8,
    manifest: *const backups_api.TableBackupManifest,
) !RestoreMetadataSpec {
    try backups_api.validateTableManifest(alloc, manifest, manifest.backup_id);
    if (manifest.artifact_integrity_mode != .declared)
        return error.BackupIntegrityMissing;
    if (!std.mem.eql(u8, manifest.table_name, table_name)) return error.InvalidBackupRequest;
    const table = backups_api.deriveRestoreTableRecord(alloc, table_name, location_uri, manifest) catch {
        return error.InvalidBackupRequest;
    };
    errdefer metadata_table_manager.freeTable(alloc, table);
    const ranges = try backups_api.deriveRestoreRanges(
        alloc,
        table.table_id,
        location_uri,
        connection,
        artifact_backup_id,
        manifest,
    );
    errdefer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    return .{
        .table = table,
        .ranges = ranges,
    };
}

fn persistRestoreTableIntent(
    service_impl: anytype,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    location_uri: []const u8,
    connection: []const u8,
    artifact_backup_id: []const u8,
    manifest: *const backups_api.TableBackupManifest,
) !void {
    var spec = try loadRestoreMetadataSpec(
        alloc,
        table_name,
        location_uri,
        connection,
        artifact_backup_id,
        manifest,
    );
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
    relocation_generation: ?u64 = null,
    raft_applied_index: ?u64 = null,
    raft_term: ?u64 = null,
    raft_membership_index: ?u64 = null,
    doc_count: ?u64 = null,
    disk_bytes: ?u64 = null,
    disk_bytes_known: ?bool = null,
    empty: ?bool = null,
    created_at_millis: ?u64 = null,
    updated_at_millis: ?u64 = null,
    observed_reallocation_request_id: ?u128 = null,
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
    load_error: ?[]const u8 = null,
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
    dense_vector_projection_pending: ?bool = null,
    repair_status: ?metadata_table_manager.IndexRepairStatus = null,
    repair_active_generation_serviceable: ?bool = null,
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
    disk_bytes_known: ?bool = null,
    created_at_millis: ?u64 = null,
    index_count: ?u32 = null,
    enrichment: ?metadata_table_manager.RuntimeEnrichmentStatusReport = null,
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
        reporter_incarnation: ?u64 = null,
        status_generation: ?u64 = null,
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
    if (!metadata_table_manager.reporterFenceValid(
        parsed.value.reporter_incarnation orelse 0,
        parsed.value.status_generation orelse 0,
    )) return error.InvalidStoreReporterFence;
    const group_statuses = try cloneParsedGroupStatuses(alloc, parsed.value.group_statuses orelse &.{});
    errdefer metadata_table_manager.freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneParsedRuntimeGroupStatuses(alloc, parsed.value.runtime_statuses orelse &.{});
    errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    return .{
        .store_id = parsed.value.store_id,
        .node_id = parsed.value.node_id,
        .reporter_incarnation = parsed.value.reporter_incarnation orelse 0,
        .status_generation = parsed.value.status_generation orelse 0,
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
        reporter_incarnation: ?u64 = null,
        status_generation: ?u64 = null,
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
    if (!metadata_table_manager.reporterFenceValid(
        parsed.value.reporter_incarnation orelse 0,
        parsed.value.status_generation orelse 0,
    )) return error.InvalidStoreReporterFence;
    const group_statuses = try cloneParsedGroupStatuses(alloc, parsed.value.group_statuses orelse &.{});
    errdefer metadata_table_manager.freeGroupStatuses(alloc, group_statuses);
    const runtime_statuses = try cloneParsedRuntimeGroupStatuses(alloc, parsed.value.runtime_statuses orelse &.{});
    errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, runtime_statuses);
    const store_id = parsed.value.store_id orelse default_store_id orelse return error.MissingStoreID;
    if (store_id == 0) return error.InvalidNodeID;
    return .{
        .store_id = store_id,
        .reporter_incarnation = parsed.value.reporter_incarnation orelse 0,
        .status_generation = parsed.value.status_generation orelse 0,
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
            .relocation_generation = parsed.relocation_generation orelse 0,
            .raft_applied_index = parsed.raft_applied_index orelse 0,
            .raft_term = parsed.raft_term orelse 0,
            .raft_membership_index = parsed.raft_membership_index orelse 0,
            .doc_count = parsed.doc_count orelse 0,
            .disk_bytes = parsed.disk_bytes orelse 0,
            .disk_bytes_known = parsed.disk_bytes_known orelse false,
            .empty = parsed.empty orelse true,
            .created_at_millis = parsed.created_at_millis orelse 0,
            .updated_at_millis = parsed.updated_at_millis orelse 0,
            .observed_reallocation_request_id = parsed.observed_reallocation_request_id orelse 0,
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
    var enrichment = parsed.enrichment orelse metadata_table_manager.RuntimeEnrichmentStatusReport{};
    enrichment.projection_checkpoint_status = try alloc.dupe(u8, enrichment.projection_checkpoint_status);
    errdefer alloc.free(enrichment.projection_checkpoint_status);
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
        .disk_bytes_known = parsed.disk_bytes_known orelse false,
        .created_at_millis = parsed.created_at_millis orelse 0,
        .index_count = parsed.index_count orelse @intCast(indexes.len),
        .enrichment = enrichment,
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
    const load_error = if (parsed.load_error) |value| try alloc.dupe(u8, value) else null;
    errdefer if (load_error) |value| alloc.free(value);
    return .{
        .name = name,
        .kind = kind,
        .load_error = load_error,
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
        .dense_vector_projection_pending = parsed.dense_vector_projection_pending orelse false,
        .repair_status = parsed.repair_status,
        .repair_active_generation_serviceable = parsed.repair_status != null and
            (parsed.repair_active_generation_serviceable orelse false),
    };
}

test "metadata status JSON preserves compact managed index admission state" {
    const alloc = std.testing.allocator;
    const report = try parseStoreStatusReport(alloc,
        \\{"store_id":20,"runtime_statuses":[{"group_id":10,"indexes":[{"name":"thumbnail","kind":"dense_vector","repair_status":"waiting","repair_active_generation_serviceable":true,"dense_vector_projection_pending":true},{"name":"legacy","kind":"full_text","repair_active_generation_serviceable":true},{"name":"mixed_version","coverage_generation":7,"coverage_config_hash":8}]}]}
    );
    defer freeStoreStatusReport(alloc, report);

    try std.testing.expectEqual(@as(usize, 1), report.runtime_statuses.len);
    const indexes = report.runtime_statuses[0].indexes;
    try std.testing.expectEqual(@as(usize, 3), indexes.len);
    try std.testing.expectEqual(metadata_table_manager.IndexRepairStatus.waiting, indexes[0].repair_status.?);
    try std.testing.expect(indexes[0].repair_active_generation_serviceable);
    try std.testing.expect(indexes[0].dense_vector_projection_pending);
    // Proof without a repair lifecycle is not actionable and must not survive
    // normalization from a malformed or mixed-version producer.
    try std.testing.expect(indexes[1].repair_status == null);
    try std.testing.expect(!indexes[1].repair_active_generation_serviceable);
    try std.testing.expect(!indexes[1].dense_vector_projection_pending);
    // A mixed-version producer can omit both fields. Preserve that absence as
    // an incomplete identity so it cannot authorize repair-state deletion.
    try std.testing.expectEqualStrings("", indexes[2].kind);
    try std.testing.expect(!indexes[2].coverage_identity_ready);
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
    const updated = try cloneTableWithReseededExactCutoverSource(alloc, table.*, source_ordinal);
    defer alloc.free(updated.table.replication_sources_json);
    errdefer {
        alloc.free(updated.slot_name);
        alloc.free(updated.publication_name);
    }
    // Catalog publication is the fencing point. The old runtime observes the
    // changed source config and loses authority before it can publish further
    // work. The replacement runtime atomically claims a fresh authority and
    // durably retires the superseded slot/publication under the provider lock.
    // Performing provider cleanup here would race both owners and would leave
    // no durable retry intent after a process crash.
    try svc.replaceTableDefinition(table.*, updated.table);
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

fn jsonBodyOrEmptyObject(body: []const u8) []const u8 {
    return if (body.len == 0) "{}" else body;
}

fn freeStoreStatusReport(alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) void {
    node_operations.freeStoreStatusReport(alloc, report);
}

test "metadata http server reports reallocation protocol upgrade gating" {
    const UpgradeGatedSource = struct {
        fn iface(_: *@This()) AdminSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .trigger_reallocate = triggerReallocate,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return error.TestUnexpectedResult;
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn triggerReallocate(_: *anyopaque) !void {
            return error.ReallocationProtocolUpgradeRequired;
        }
    };

    var source = UpgradeGatedSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var response = try server.executeTypedHandlerForTest(.POST, routes.Routes.internal_reallocate, &.{}, MetadataHttpServer.metadataTriggerReallocate);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("metadata voter upgrade required", response.body.?);
}

test "metadata http server serves status and filtered admin routes" {
    const FakeSource = struct {
        const incarnation: metadata_api.MetadataClusterIncarnation = "77777777777777777777777777777777".*;
        const voter_set_fingerprint: metadata_api.MetadataRaftVoterSetFingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;

        fn iface(_: *@This()) AdminSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .head = head,
                    .runtime_topology = runtimeTopology,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .validate_publication = validatePublication,
                    .validate_table_publication = validateTablePublication,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn head(_: *anyopaque) !metadata_api.MetadataHead {
            return .{ .metadata_group_id = 77, .metadata_incarnation = incarnation, .metadata_epoch = 5 };
        }

        fn runtimeTopology(_: *anyopaque) !metadata_api.MetadataRuntimeTopology {
            return .{
                .metadata_group_id = 77,
                .metadata_incarnation = incarnation,
                .metadata_raft_local_node_id = 2,
                .metadata_raft_role = "follower",
                .metadata_raft_leader_id = 1,
                .metadata_raft_term = 9,
                .metadata_raft_local_voter = true,
                .metadata_raft_voter_count = 3,
                .metadata_raft_voter_set_fingerprint = voter_set_fingerprint,
                .metadata_raft_learner_count = 2,
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metadata_incarnation = incarnation,
                .metadata_epoch = 5,
                .metadata_raft_voter_set_fingerprint = voter_set_fingerprint,
                .metadata_raft_joint_consensus = true,
                .metadata_raft_learner_count = 2,
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
                .status = .{ .metadata_group_id = 77, .metadata_incarnation = incarnation, .metadata_epoch = 5, .metrics = .{} },
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

        fn validateTablePublication(_: *anyopaque, contract: metadata_api.CatalogTablePublicationContract) !bool {
            var snapshot = try adminSnapshot(undefined);
            return contract.matches(&snapshot);
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    var topology_resp = try server.executeTypedHandlerForTest(.GET, routes.Routes.runtime_topology, &.{}, MetadataHttpServer.metadataRuntimeTopology);
    defer topology_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), topology_resp.status.code);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"metadata_group_id\":77,\"metadata_raft_local_node_id\":2,\"metadata_raft_role\":\"follower\",\"metadata_raft_leader_id\":1,\"metadata_raft_term\":9,\"metadata_raft_local_voter\":true,\"metadata_raft_voter_count\":3,\"metadata_raft_learner_count\":2}",
        topology_resp.body.?,
    );
    try std.testing.expect(std.mem.indexOf(u8, topology_resp.body.?, "\"projected_tables\"") == null);

    var status_resp = try server.executeTypedHandlerForTest(.GET, routes.Routes.status, &.{}, MetadataHttpServer.metadataStatus);
    defer status_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), status_resp.status.code);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"metadata_group_id\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"metadata_raft_voter_set_fingerprint\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"metadata_raft_joint_consensus\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"metadata_raft_learner_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_tables_with_replication_sources\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_replication_sources\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_replication_source_lag_millis_max\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_replication_source_observed_lag_millis_max\":56") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_replication_source_statuses_with_source_commit_timestamp\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"projected_replication_source_statuses_reseed_recommended\":1") != null);

    const table_params = [_]httpx.RouteParam{.{ .name = "table_id", .value = "1" }};
    var ranges_resp = try server.executeTypedHandlerForTest(.GET, "/metadata/v1/tables/1/ranges", &table_params, MetadataHttpServer.metadataTableRanges);
    defer ranges_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body.?, "\"group_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body.?, "\"group_id\":11") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body.?, "\"doc_identity_shard_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, ranges_resp.body.?, "\"doc_identity_range_id\":10") != null);

    const group_params = [_]httpx.RouteParam{.{ .name = "group_id", .value = "10" }};
    var placement_resp = try server.executeTypedHandlerForTest(.GET, "/metadata/v1/groups/10/placement", &group_params, MetadataHttpServer.metadataGroupPlacement);
    defer placement_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, placement_resp.body.?, "\"group_id\":10") != null);

    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "1" }};
    var shutdown_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/1/shutdown", &node_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer shutdown_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body.?, "\"phase\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body.?, "\"safe_to_terminate\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, shutdown_resp.body.?, "\"pending_groups\":[10]") != null);

    var snapshot_resp = try server.executeTypedHandlerForTest(.GET, routes.Routes.admin_snapshot, &.{}, MetadataHttpServer.metadataSnapshot);
    defer snapshot_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"local_bootstrap_statuses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"phase\":\"failed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"backup_id\":\"snap1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"snapshot_path\":\"snap1/groups/10\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"replication_source_statuses\"") != null);

    const publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 77,
        .metadata_incarnation = FakeSource.incarnation,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(publication_body);
    var publication_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_catalog_publication_check, &.{}, publication_body, MetadataHttpServer.metadataCatalogPublicationCheck);
    defer publication_resp.deinit();
    try std.testing.expectEqual(@as(u16, 204), publication_resp.status.code);
    const table_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogTablePublicationContract{
        .metadata_group_id = 77,
        .metadata_incarnation = FakeSource.incarnation,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .topology = metadata_api.catalogTableTopology(1, (&[_]metadata_table_manager.RangeRecord{
            .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
            .{ .group_id = 11, .table_id = 1, .doc_identity_shard_id = 10, .doc_identity_range_id = 10, .start_key = "doc:m", .end_key = "doc:z" },
        })[0..]),
    }, .{});
    defer std.testing.allocator.free(table_publication_body);
    var table_publication_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_catalog_table_publication_check, &.{}, table_publication_body, MetadataHttpServer.metadataCatalogTablePublicationCheck);
    defer table_publication_resp.deinit();
    try std.testing.expectEqual(@as(u16, 204), table_publication_resp.status.code);
    const foreign_group_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 78,
        .metadata_incarnation = FakeSource.incarnation,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(foreign_group_publication_body);
    var foreign_group_publication_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_catalog_publication_check, &.{}, foreign_group_publication_body, MetadataHttpServer.metadataCatalogPublicationCheck);
    defer foreign_group_publication_resp.deinit();
    try std.testing.expectEqual(@as(u16, 409), foreign_group_publication_resp.status.code);
    const foreign_incarnation_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 77,
        .metadata_incarnation = "78787878787878787878787878787878".*,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(foreign_incarnation_publication_body);
    var foreign_incarnation_publication_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_catalog_publication_check, &.{}, foreign_incarnation_publication_body, MetadataHttpServer.metadataCatalogPublicationCheck);
    defer foreign_incarnation_publication_resp.deinit();
    try std.testing.expectEqual(@as(u16, 409), foreign_incarnation_publication_resp.status.code);
    const stale_publication_body = try std.json.Stringify.valueAlloc(std.testing.allocator, metadata_api.CatalogPublicationContract{
        .metadata_group_id = 77,
        .metadata_incarnation = FakeSource.incarnation,
        .table_id = 1,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .range = .{ .group_id = 10, .table_id = 1, .start_key = "doc:b", .end_key = "doc:m" },
    }, .{});
    defer std.testing.allocator.free(stale_publication_body);
    var stale_publication_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_catalog_publication_check, &.{}, stale_publication_body, MetadataHttpServer.metadataCatalogPublicationCheck);
    defer stale_publication_resp.deinit();
    try std.testing.expectEqual(@as(u16, 409), stale_publication_resp.status.code);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"replication_source_action_hints\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"source_kind\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"external_table\":\"users\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"cutover_mode\":\"exported_snapshot\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"slot_name\":\"antfly_postgres_users_docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"publication_name\":\"antfly_pub_postgres_users_docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"snapshot_offset\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"prepared_checkpoint\":\"lsn:0/16B6A50\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"stream_checkpoint\":\"lsn:0/16B6A50\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"lag_records\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"lag_millis\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"last_source_commit_at_ms\":1200") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"doc_identity_shard_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"doc_identity_range_id\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"merged_group_statuses\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"doc_identity_reassignment_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_resp.body.?, "\"namespace_range_id\":10") != null);

    var active_resp = try server.executeTypedHandlerForTest(.GET, routes.Routes.active_transitions, &.{}, MetadataHttpServer.metadataActiveTransitions);
    defer active_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, active_resp.body.?, "\"transition_id\":9001") != null);
    try std.testing.expect(std.mem.indexOf(u8, active_resp.body.?, "\"transition_id\":9010") != null);
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
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "memories" }};
    const index_params = [_]httpx.RouteParam{
        .{ .name = "table_name", .value = "memories" },
        .{ .name = "index_name", .value = "memory_text" },
    };

    var schema_resp = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/tables/memories/schema", &table_params, "{}", MetadataHttpServer.metadataUpdateTableSchema);
    defer schema_resp.deinit();
    try std.testing.expectEqual(@as(u16, 405), schema_resp.status.code);

    var create_index_resp = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/tables/memories/indexes/memory_text", &index_params, "{\"type\":\"full_text\"}", MetadataHttpServer.metadataCreateTableIndex);
    defer create_index_resp.deinit();
    try std.testing.expectEqual(@as(u16, 405), create_index_resp.status.code);

    var drop_index_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/tables/memories/indexes/memory_text", &index_params, MetadataHttpServer.metadataDropTableIndex);
    defer drop_index_resp.deinit();
    try std.testing.expectEqual(@as(u16, 405), drop_index_resp.status.code);

    var drop_table_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/tables/memories", &table_params, MetadataHttpServer.metadataDropTable);
    defer drop_table_resp.deinit();
    try std.testing.expectEqual(@as(u16, 405), drop_table_resp.status.code);
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
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};
    var response = try server.executeTypedHandlerWithBodyForTest(
        .PUT,
        "/internal/v1/tables/docs/definition",
        &table_params,
        \\{"expected":{"table_id":42,"name":"docs","description":"original"},"definition":{"table_id":42,"name":"docs","description":"restored"}}
    ,
        MetadataHttpServer.metadataReplaceTableDefinition,
    );
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 202), response.status.code);
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

    var node_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"node_id\":9,\"role\":\"data\"}", MetadataHttpServer.metadataRegisterNode);
    defer node_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), node_resp.status.code);
    try std.testing.expectEqual(@as(usize, 1), source.node_count);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(source.nodes[0].lifecycle));

    var draining_register_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"node_id\":99,\"role\":\"data\",\"lifecycle\":\"draining\"}", MetadataHttpServer.metadataRegisterNode);
    defer draining_register_resp.deinit();
    try std.testing.expectEqual(@as(u16, 400), draining_register_resp.status.code);
    try std.testing.expectEqual(@as(usize, 1), source.node_count);
    try std.testing.expect(!source.stores[1].drain_requested);

    const node_9_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "9" }};
    var shutdown_resp = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/nodes/9/shutdown", &node_9_params, "{\"type\":\"remove\",\"reason\":\"test\"}", MetadataHttpServer.metadataRequestNodeShutdown);
    defer shutdown_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), shutdown_resp.status.code);
    try std.testing.expect(std.mem.eql(u8, source.nodes[0].lifecycle, metadata_table_manager.node_lifecycle_draining));
    try std.testing.expect(source.stores[0].drain_requested);
    try std.testing.expect(source.reallocate_triggered);

    var register_node_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"node_id\":9,\"role\":\"data\"}", MetadataHttpServer.metadataRegisterNode);
    defer register_node_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), register_node_resp.status.code);
    try std.testing.expect(std.mem.eql(u8, source.nodes[0].lifecycle, metadata_table_manager.node_lifecycle_draining));

    var register_node_store_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}", MetadataHttpServer.metadataRegisterNode);
    defer register_node_store_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), register_node_store_resp.status.code);
    try std.testing.expect(source.stores[0].drain_requested);

    var status_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/9/shutdown", &node_9_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer status_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"phase\":\"complete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"safe_to_terminate\":true") != null);

    source.reallocate_triggered = false;
    var cancel_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9/shutdown", &node_9_params, MetadataHttpServer.metadataCancelNodeShutdown);
    defer cancel_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status.code);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(source.nodes[0].lifecycle));
    try std.testing.expect(!source.stores[0].drain_requested);
    try std.testing.expect(source.reallocate_triggered);

    var cancelled_status_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/9/shutdown", &node_9_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer cancelled_status_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, cancelled_status_resp.body.?, "\"phase\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cancelled_status_resp.body.?, "\"safe_to_terminate\":false") != null);

    var post_cancel_register_node_store_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}", MetadataHttpServer.metadataRegisterNode);
    defer post_cancel_register_node_store_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), post_cancel_register_node_store_resp.status.code);
    try std.testing.expect(!source.stores[0].drain_requested);

    source.reallocate_triggered = false;
    var retry_cancel_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9/shutdown", &node_9_params, MetadataHttpServer.metadataCancelNodeShutdown);
    defer retry_cancel_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), retry_cancel_resp.status.code);
    try std.testing.expect(!source.reallocate_triggered);

    const node_99_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "99" }};
    var retry_resp = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/nodes/99/shutdown", &node_99_params, "{\"type\":\"remove\"}", MetadataHttpServer.metadataRequestNodeShutdown);
    defer retry_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), retry_resp.status.code);
    try std.testing.expectEqual(@as(usize, 2), source.node_count);
    try std.testing.expect(std.mem.eql(u8, source.nodes[1].lifecycle, metadata_table_manager.node_lifecycle_draining));

    var register_unknown_store_resp = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":99,\"node_id\":99,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}", MetadataHttpServer.metadataRegisterNode);
    defer register_unknown_store_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), register_unknown_store_resp.status.code);
    try std.testing.expect(source.stores[1].drain_requested);

    var unknown_status_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/99/shutdown", &node_99_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer unknown_status_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, unknown_status_resp.body.?, "\"phase\":\"complete\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, unknown_status_resp.body.?, "\"safe_to_terminate\":true") != null);
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

    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "42" }};
    var status_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/42/shutdown", &node_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer status_resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), status_resp.status.code);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"phase\":\"not_found\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"safe_to_terminate\":true") != null);
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
    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "9" }};

    var cancel_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9/shutdown", &node_params, MetadataHttpServer.metadataCancelNodeShutdown);
    defer cancel_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status.code);
    try std.testing.expectEqual(@as(usize, 1), source.cancel_count);
    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);

    source.nodes[0].lifecycle = metadata_table_manager.node_lifecycle_draining;
    var request_resp = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/nodes/9/shutdown", &node_params, "{\"type\":\"remove\"}", MetadataHttpServer.metadataRequestNodeShutdown);
    defer request_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), request_resp.status.code);
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
    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "9" }};

    var active_finalize_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9", &node_params, MetadataHttpServer.metadataFinalizeNodeShutdown);
    defer active_finalize_resp.deinit();
    try std.testing.expectEqual(@as(u16, 409), active_finalize_resp.status.code);
    try std.testing.expectEqual(@as(usize, 0), source.finalize_count);
    try std.testing.expectEqual(@as(usize, 0), source.reallocate_count);

    source.nodes[0].lifecycle = metadata_table_manager.node_lifecycle_draining;
    var finalize_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9", &node_params, MetadataHttpServer.metadataFinalizeNodeShutdown);
    defer finalize_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), finalize_resp.status.code);
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
    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "9" }};

    var active_finalize_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9", &node_params, MetadataHttpServer.metadataFinalizeNodeShutdown);
    defer active_finalize_resp.deinit();
    try std.testing.expectEqual(@as(u16, 409), active_finalize_resp.status.code);
    try std.testing.expectEqual(@as(usize, 0), source.finalize_count);

    source.stores[0].drain_requested = true;
    var draining_finalize_resp = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9", &node_params, MetadataHttpServer.metadataFinalizeNodeShutdown);
    defer draining_finalize_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), draining_finalize_resp.status.code);
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
    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "9" }};

    var shutdown_resp = try server_a.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/nodes/9/shutdown", &node_params, "{\"type\":\"remove\"}", MetadataHttpServer.metadataRequestNodeShutdown);
    defer shutdown_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), shutdown_resp.status.code);
    try std.testing.expect(source.stores[0].drain_requested);

    source.stage_next_store = true;
    var stale_register_resp = try server_a.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":9,\"node_id\":9,\"role\":\"data\",\"health_class\":\"healthy\",\"live\":true}", MetadataHttpServer.metadataRegisterNode);
    defer stale_register_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), stale_register_resp.status.code);
    try std.testing.expect(source.pending_store.?.drain_requested);

    var cancel_resp = try server_b.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/9/shutdown", &node_params, MetadataHttpServer.metadataCancelNodeShutdown);
    defer cancel_resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), cancel_resp.status.code);
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

    const node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "4" }};
    var status_resp = try server.executeTypedHandlerForTest(.GET, "/internal/v1/nodes/4/shutdown", &node_params, MetadataHttpServer.metadataNodeShutdownStatus);
    defer status_resp.deinit();
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"phase\":\"blocked\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"blocked\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"blocked_reason\":\"InsufficientShardVoters\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"message\":\"Node hosts a shard with no other voters") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"safe_to_terminate\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"group_status_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_resp.body.?, "\"pending_groups\":[44]") != null);
}

test "metadata status uses the typed httpx handler" {
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
    var resp = try server.executeTypedHandlerForTest(.GET, routes.Routes.status, &.{}, MetadataHttpServer.metadataStatus);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expect(std.mem.indexOf(u8, resp.body.?, "\"metadata_group_id\":1900") != null);
}

test "metadata linearizable head uses the fenced source operation" {
    const FakeSource = struct {
        calls: usize = 0,
        const incarnation: metadata_api.MetadataClusterIncarnation = "22222222222222222222222222222222".*;

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .linearizable_head = linearizableHead,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn linearizableHead(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.MetadataHead {
            try request.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return .{ .metadata_group_id = 77, .metadata_incarnation = incarnation, .metadata_epoch = 18 };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return error.TestUnexpectedResult;
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var resp = try server.executeTypedHandlerForTest(
        .POST,
        routes.Routes.internal_linearizable_head,
        &.{},
        MetadataHttpServer.metadataLinearizableHead,
    );
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        \\{"metadata_group_id":77,"metadata_incarnation":"22222222222222222222222222222222","metadata_epoch":18}
    ,
        resp.body.?,
    );
    try std.testing.expectEqual(@as(usize, 1), source.calls);
}

test "metadata linearizable snapshot fences and frees one owned response" {
    const FakeSource = struct {
        calls: usize = 0,
        frees: usize = 0,
        const incarnation: metadata_api.MetadataClusterIncarnation = "22222222222222222222222222222222".*;

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .linearizable_snapshot = linearizableSnapshot,
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn linearizableSnapshot(ptr: *anyopaque, request: operation.RequestContext) !metadata_api.AdminSnapshot {
            try request.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return .{
                .status = .{
                    .metadata_group_id = 77,
                    .metadata_incarnation = incarnation,
                    .metadata_epoch = 3,
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

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return error.TestUnexpectedResult;
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }

        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.frees += 1;
            snapshot.* = undefined;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    var resp = try server.executeTypedHandlerForTest(
        .POST,
        routes.Routes.internal_linearizable_snapshot,
        &.{},
        MetadataHttpServer.metadataLinearizableSnapshot,
    );
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        \\{"status":{"metadata_group_id":77,"metadata_incarnation":"22222222222222222222222222222222","metadata_epoch":3},"tables":[]}
    ,
        resp.body.?,
    );
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    try std.testing.expectEqual(@as(usize, 1), source.frees);

    var legacy_server = MetadataHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    });
    var legacy_resp = try legacy_server.executeTypedHandlerForTest(
        .POST,
        routes.Routes.internal_linearizable_snapshot,
        &.{},
        MetadataHttpServer.metadataLinearizableSnapshot,
    );
    defer legacy_resp.deinit();
    try std.testing.expectEqual(@as(u16, 405), legacy_resp.status.code);
}

test "metadata linearizable snapshot detects concurrent projection changes" {
    const incarnation: metadata_api.MetadataClusterIncarnation = "22222222222222222222222222222222".*;
    const before = service.AdminSnapshotFence{
        .metadata_group_id = 77,
        .metadata_incarnation = incarnation,
        .metadata_raft_term = 4,
        .metadata_raft_commit_index = 5,
        .metadata_raft_applied_index = 5,
        .projection_epoch = 6,
        .catalog_epoch = 7,
        .placement_epoch = 8,
        .reconcile_lease_epoch = 9,
        .transition_epoch = 10,
    };
    var after = before;
    try std.testing.expect(AdminSource.sameSnapshotFence(before, after));

    after.metadata_raft_commit_index += 1;
    try std.testing.expect(!AdminSource.sameSnapshotFence(before, after));
    after.metadata_raft_commit_index = before.metadata_raft_commit_index;
    after.metadata_raft_applied_index += 1;
    try std.testing.expect(!AdminSource.sameSnapshotFence(before, after));
    after.metadata_raft_applied_index = before.metadata_raft_applied_index;
    after.projection_epoch += 1;
    try std.testing.expect(!AdminSource.sameSnapshotFence(before, after));
    after.projection_epoch = before.projection_epoch;
    after.metadata_group_id += 1;
    try std.testing.expect(!AdminSource.sameSnapshotFence(before, after));
}

test "coherent linearizable snapshot retries a torn capture and preserves request context" {
    const FakeService = struct {
        barrier_calls: usize = 0,
        fence_calls: usize = 0,
        snapshot_calls: usize = 0,
        frees: usize = 0,
        saw_request_id: bool = false,

        const incarnation: metadata_api.MetadataClusterIncarnation = "22222222222222222222222222222222".*;

        pub fn ensureLinearizableReadWithContext(self: *@This(), request: operation.RequestContext) !void {
            try request.ensureActive();
            self.barrier_calls += 1;
            self.saw_request_id = std.mem.eql(u8, request.request_id, "snapshot-test");
        }

        fn fence(epoch: u64) service.AdminSnapshotFence {
            return .{
                .metadata_group_id = 77,
                .metadata_incarnation = incarnation,
                .metadata_raft_term = 4,
                .metadata_raft_commit_index = epoch,
                .metadata_raft_applied_index = epoch,
                .projection_epoch = epoch,
                .catalog_epoch = epoch,
                .placement_epoch = epoch,
                .reconcile_lease_epoch = epoch,
                .transition_epoch = epoch,
            };
        }

        pub fn adminSnapshotFence(self: *@This()) !service.AdminSnapshotFence {
            defer self.fence_calls += 1;
            return fence(switch (self.fence_calls) {
                0 => 1,
                else => 2,
            });
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            self.snapshot_calls += 1;
            return .{
                .status = .{
                    .metadata_group_id = 77,
                    .metadata_incarnation = incarnation,
                    .metadata_epoch = self.snapshot_calls,
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

        pub fn freeAdminSnapshot(self: *@This(), snapshot: *metadata_api.AdminSnapshot) void {
            self.frees += 1;
            snapshot.* = undefined;
        }
    };

    var fake = FakeService{};
    var snapshot = try AdminSource.coherentLinearizableSnapshot(FakeService, &fake, .{
        .request_id = "snapshot-test",
    });

    try std.testing.expect(fake.saw_request_id);
    try std.testing.expectEqual(@as(usize, 1), fake.barrier_calls);
    try std.testing.expectEqual(@as(usize, 2), fake.snapshot_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.frees);
    try std.testing.expectEqual(@as(u64, 2), snapshot.status.metadata_epoch);
    fake.freeAdminSnapshot(&snapshot);
    try std.testing.expectEqual(@as(usize, 2), fake.frees);
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
            try std.testing.expectEqual(@as(usize, 1), report.group_statuses.len);
            try std.testing.expectEqual(@as(u64, 9), report.group_statuses[0].group_id);
            try std.testing.expectEqual(
                @as(u128, 0x1234_5678_9abc_def0_1234_5678_9abc_def0),
                report.group_statuses[0].observed_reallocation_request_id,
            );
            self.store_status_count += 1;
        }

        fn restoreTable(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            location_uri: []const u8,
            connection: []const u8,
            artifact_backup_id: []const u8,
            manifest: *const backups_api.TableBackupManifest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("file:///tmp/out", location_uri);
            try std.testing.expectEqualStrings("test-backups", connection);
            try std.testing.expectEqualStrings("snap1", artifact_backup_id);
            try std.testing.expectEqualStrings("snap1", manifest.backup_id);
            try std.testing.expectEqualStrings("docs", manifest.table_name);
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

    var reallocate = try server.executeTypedHandlerForTest(.POST, routes.Routes.internal_reallocate, &.{}, MetadataHttpServer.metadataTriggerReallocate);
    defer reallocate.deinit();
    try std.testing.expectEqual(@as(u16, 202), reallocate.status.code);

    var zero_node = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"node_id\":0}", MetadataHttpServer.metadataRegisterNode);
    defer zero_node.deinit();
    try std.testing.expectEqual(@as(u16, 400), zero_node.status.code);

    var zero_store = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":0,\"node_id\":0}", MetadataHttpServer.metadataRegisterNode);
    defer zero_store.deinit();
    try std.testing.expectEqual(@as(u16, 400), zero_store.status.code);

    var mismatched_store = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":7,\"node_id\":1}", MetadataHttpServer.metadataRegisterNode);
    defer mismatched_store.deinit();
    try std.testing.expectEqual(@as(u16, 400), mismatched_store.status.code);

    const zero_node_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "0" }};
    var zero_node_status = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/nodes/0/status", &zero_node_params, "{\"health_class\":\"healthy\"}", MetadataHttpServer.metadataReportNodeStatus);
    defer zero_node_status.deinit();
    try std.testing.expectEqual(@as(u16, 404), zero_node_status.status.code);

    var zero_node_shutdown = try server.executeTypedHandlerWithBodyForTest(.PUT, "/internal/v1/nodes/0/shutdown", &zero_node_params, "{\"type\":\"remove\"}", MetadataHttpServer.metadataRequestNodeShutdown);
    defer zero_node_shutdown.deinit();
    try std.testing.expectEqual(@as(u16, 404), zero_node_shutdown.status.code);

    var zero_node_finalize = try server.executeTypedHandlerForTest(.DELETE, "/internal/v1/nodes/0", &zero_node_params, MetadataHttpServer.metadataFinalizeNodeShutdown);
    defer zero_node_finalize.deinit();
    try std.testing.expectEqual(@as(u16, 404), zero_node_finalize.status.code);

    var store = try server.executeTypedHandlerWithBodyForTest(.POST, routes.Routes.internal_nodes, &.{}, "{\"store_id\":7,\"node_id\":7}", MetadataHttpServer.metadataRegisterNode);
    defer store.deinit();
    try std.testing.expectEqual(@as(u16, 202), store.status.code);

    const node_7_params = [_]httpx.RouteParam{.{ .name = "node_id", .value = "7" }};
    var store_status = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/nodes/7/status", &node_7_params, "{\"store_id\":7,\"health_class\":\"healthy\",\"group_statuses\":[{\"group_id\":9,\"observed_reallocation_request_id\":24197857203266734864793317670504947440}]}", MetadataHttpServer.metadataReportNodeStatus);
    defer store_status.deinit();
    try std.testing.expectEqual(@as(u16, 202), store_status.status.code);

    const restore_body = try testInternalTableRestoreRequestBodyAlloc(
        std.testing.allocator,
        "snap1",
        "docs",
        "file:///tmp/out",
        "test-backups",
    );
    defer std.testing.allocator.free(restore_body);
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};
    var restore = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/restore", &table_params, restore_body, MetadataHttpServer.metadataRestoreTable);
    defer restore.deinit();
    try std.testing.expectEqual(@as(u16, 202), restore.status.code);

    var split = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/split", &table_params, "{\"split_key\":\"doc:m\"}", MetadataHttpServer.metadataRequestTableSplit);
    defer split.deinit();
    try std.testing.expectEqual(@as(u16, 202), split.status.code);

    var merge = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/merge", &table_params, "{\"donor_group_id\":10,\"receiver_group_id\":9,\"allow_doc_identity_reassignment\":true}", MetadataHttpServer.metadataRequestTableMerge);
    defer merge.deinit();
    try std.testing.expectEqual(@as(u16, 202), merge.status.code);

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
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};

    var split = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/split", &table_params, "{\"split_key\":\"doc:m\"}", MetadataHttpServer.metadataRequestTableSplit);
    defer split.deinit();
    try std.testing.expectEqual(@as(u16, 409), split.status.code);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split.body.?);

    var merge = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/merge", &table_params, "{\"donor_group_id\":10,\"receiver_group_id\":9,\"allow_doc_identity_reassignment\":true}", MetadataHttpServer.metadataRequestTableMerge);
    defer merge.deinit();
    try std.testing.expectEqual(@as(u16, 409), merge.status.code);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge.body.?);

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
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};

    var split = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/split", &table_params, "{\"split_key\":\"doc:m\"}", MetadataHttpServer.metadataRequestTableSplit);
    defer split.deinit();
    try std.testing.expectEqual(@as(u16, 409), split.status.code);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split.body.?);

    var merge = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/merge", &table_params, "{\"donor_group_id\":10,\"receiver_group_id\":9}", MetadataHttpServer.metadataRequestTableMerge);
    defer merge.deinit();
    try std.testing.expectEqual(@as(u16, 409), merge.status.code);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge.body.?);

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

        fn restoreTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: []const u8, _: *const backups_api.TableBackupManifest) !void {
            return error.MissingEndpoint;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());

    const restore_body = try testInternalTableRestoreRequestBodyAlloc(
        std.testing.allocator,
        "snap1",
        "docs",
        "s3://bucket/out",
        "test-backups",
    );
    defer std.testing.allocator.free(restore_body);
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};
    var restore = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs/restore", &table_params, restore_body, MetadataHttpServer.metadataRestoreTable);
    defer restore.deinit();
    try std.testing.expectEqual(@as(u16, 400), restore.status.code);
    try std.testing.expect(std.mem.startsWith(u8, restore.headers.get("content-type").?, "text/plain"));
    try std.testing.expectEqualStrings(
        "missing S3-compatible endpoint; set AWS_ENDPOINT_URL for s3:// backups",
        restore.body.?,
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
    const reseed_params = [_]httpx.RouteParam{
        .{ .name = "table_name", .value = "docs" },
        .{ .name = "source_ordinal", .value = "1" },
    };
    var resp = try server.executeTypedHandlerForTest(.POST, "/internal/v1/tables/docs/replication-sources/1/reseed-exact-cutover", &reseed_params, MetadataHttpServer.metadataReseedReplicationSourceExactCutover);
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 202), resp.status.code);
    try std.testing.expectEqualStrings("docs", source.reseed_table_name.?);
    try std.testing.expectEqual(@as(u32, 1), source.reseed_source_ordinal.?);
    try std.testing.expect(std.mem.indexOf(u8, resp.body.?, "\"slot_name\":\"fresh_slot\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body.?, "\"publication_name\":\"fresh_pub\"") != null);
}

test "metadata http server returns retryable authority response when reconcile lease is not held" {
    const FakeSource = struct {
        create_calls: usize = 0,

        fn iface(self: *@This()) AdminSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_table = createTable,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return error.UnexpectedStatusCall;
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedAdminSnapshotCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn createTable(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            _: tables_api.CreateTableRequest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.create_calls += 1;
            return error.ReconcileLeaseNotHeld;
        }
    };

    var source = FakeSource{};
    var server = MetadataHttpServer.init(std.testing.allocator, .{}, source.iface());
    const table_params = [_]httpx.RouteParam{.{ .name = "table_name", .value = "docs" }};
    var resp = try server.executeTypedHandlerWithBodyForTest(.POST, "/internal/v1/tables/docs", &table_params, "{}", MetadataHttpServer.metadataCreateTable);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 503), resp.status.code);
    try std.testing.expectEqual(@as(usize, 1), source.create_calls);
    try std.testing.expectEqualStrings(http_common.metadata_not_leader_value, resp.headers.get(http_common.metadata_not_leader_header).?);
    try std.testing.expect(resp.headers.get(http_common.metadata_mutation_not_admitted_header) == null);
}

test "metadata mutation pre-admission responses prove proposal was not admitted" {
    const errors = [_]anyerror{
        error.NotLeader,
        error.ProposalDropped,
        error.LeaderTransferInProgress,
    };
    for (errors) |err| {
        var request = try httpx.Request.init(std.testing.allocator, .POST, "/internal/v1/reallocate");
        defer request.deinit();
        var ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &request);
        defer ctx.deinit();

        var resp = try MetadataHttpServer.metadataMutationError(&ctx, err);
        defer resp.deinit();
        try std.testing.expectEqual(@as(u16, 503), resp.status.code);
        try std.testing.expectEqualStrings(
            http_common.metadata_mutation_not_admitted_value,
            resp.headers.get(http_common.metadata_mutation_not_admitted_header).?,
        );
    }
}

test "metadata node lifecycle distinguishes pre-admission rejection from partial outcome" {
    var rejected_request = try httpx.Request.init(std.testing.allocator, .PUT, "/internal/v1/nodes/9/shutdown");
    defer rejected_request.deinit();
    var rejected_ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &rejected_request);
    defer rejected_ctx.deinit();

    var rejected = try MetadataHttpServer.nodeLifecycleMutationError(&rejected_ctx, error.ProposalDropped);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 503), rejected.status.code);
    try std.testing.expectEqualStrings(
        http_common.metadata_mutation_not_admitted_value,
        rejected.headers.get(http_common.metadata_mutation_not_admitted_header).?,
    );

    var partial_request = try httpx.Request.init(std.testing.allocator, .PUT, "/internal/v1/nodes/9/shutdown");
    defer partial_request.deinit();
    var partial_ctx = httpx.Context.init(std.testing.allocator, std.testing.io, &partial_request);
    defer partial_ctx.deinit();

    var partial = try MetadataHttpServer.nodeLifecycleMutationError(&partial_ctx, error.MetadataMutationOutcomeUnknown);
    defer partial.deinit();
    try std.testing.expectEqual(@as(u16, 503), partial.status.code);
    try std.testing.expect(partial.headers.get(http_common.metadata_mutation_not_admitted_header) == null);
    try std.testing.expectEqualStrings(
        http_common.metadata_not_leader_value,
        partial.headers.get(http_common.metadata_not_leader_header).?,
    );
}
