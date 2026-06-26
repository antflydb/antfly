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

const builtin = @import("builtin");
const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const platform_time_lib = @import("antfly_platform").time;
const metadata_openapi = @import("antfly_metadata_openapi");
const scraping = @import("antfly_scraping");
const common_secrets = @import("../common/secrets.zig");
const fs_paths = @import("../common/fs_paths.zig");
const backups_api = @import("backups.zig");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_provisioner = @import("../metadata/table_provisioner.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const backend_types = @import("../storage/backend_types.zig");
const catalog_resources = @import("catalog_resources.zig");
const hbc_mod = @import("../storage/hbc_adapter.zig");
const lsm_backend = @import("../storage/lsm_backend/mod.zig");
const portable_backup = @import("../storage/portable_backup.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const ha_write_gate_mod = @import("../storage/ha/write_gate.zig");
const storage_schema = @import("../storage/schema.zig");
const schema_mod = @import("../schema/mod.zig");
const table_catalog = @import("table_catalog.zig");
const table_reads = @import("table_reads.zig");
const table_router = @import("table_router.zig");
const tables_api = @import("tables.zig");
const relational_rows_api = @import("relational_rows.zig");
const sql_adapter = @import("../sql/mod.zig");
const indexes_api = @import("indexes.zig");
const query_api = @import("query.zig");
const runtime_status = @import("runtime_status.zig");
const http_server = @import("http_server.zig");
const http_client = @import("http_client.zig");
const http_common = @import("../raft/transport/http_common.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const table_write_bulk_ingest = @import("table_writes/bulk_ingest.zig");
const table_write_core = @import("table_writes/core.zig");
const table_write_cache = @import("table_writes/cache.zig");
const table_write_index_config = @import("table_writes/index_config.zig");
const table_write_integrity = @import("table_writes/integrity.zig");
const table_write_integrity_types = @import("table_writes/integrity_types.zig");
const table_write_managed_db = @import("table_writes/managed_db.zig");
const table_write_schema_jobs = @import("table_writes/schema_jobs.zig");
const table_write_backup_restore = @import("table_writes/backup_restore.zig");
const table_write_relational_mutation = @import("table_writes/relational_mutation.zig");
const table_write_remote_wire = @import("table_writes/remote_wire.zig");
const table_write_sources = @import("table_writes/sources.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const distributed_txn = @import("distributed_txn.zig");
const build_options = @import("build_options");
const tracing = @import("../tracing/mod.zig");
const platform_time = @import("../platform/time.zig");
const platform_clock = @import("../platform/clock.zig");
const Io = std.Io;
const sleepNs = table_write_sources.sleepNs;
const lockAtomic = table_write_sources.lockAtomic;
const publishRuntimeStatusSnapshot = table_write_sources.publishRuntimeStatusSnapshot;
const publishStartupCatchUpRuntimeStatusSnapshot = table_write_sources.publishStartupCatchUpRuntimeStatusSnapshot;
const applyStartupCatchUpAsyncOverlay = table_write_cache.applyStartupCatchUpAsyncOverlay;
pub const ForeignKeyIntegrityAction = table_write_integrity_types.ForeignKeyIntegrityAction;
pub const UniqueConstraintIntegrityAction = table_write_integrity_types.UniqueConstraintIntegrityAction;
pub const ForeignKeyIntegrityRequest = table_write_integrity_types.ForeignKeyIntegrityRequest;
pub const ForeignKeyIntegritySchemaControllerOptions = table_write_integrity_types.ForeignKeyIntegritySchemaControllerOptions;
pub const ForeignKeyIntegritySchemaControllerTableResult = table_write_integrity_types.ForeignKeyIntegritySchemaControllerTableResult;
pub const ForeignKeyIntegritySchemaControllerResult = table_write_integrity_types.ForeignKeyIntegritySchemaControllerResult;
pub const UniqueConstraintIntegrityRequest = table_write_integrity_types.UniqueConstraintIntegrityRequest;
pub const UniqueConstraintIntegritySchemaControllerOptions = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerOptions;
pub const UniqueConstraintIntegrityGroupReport = table_write_integrity_types.UniqueConstraintIntegrityGroupReport;
pub const UniqueConstraintIntegrityResult = table_write_integrity_types.UniqueConstraintIntegrityResult;
pub const UniqueConstraintIntegritySchemaControllerTableResult = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerTableResult;
pub const UniqueConstraintIntegritySchemaControllerResult = table_write_integrity_types.UniqueConstraintIntegritySchemaControllerResult;
pub const UniqueConstraintOwnerTopology = table_write_integrity_types.UniqueConstraintOwnerTopology;
pub const UniqueConstraintOwnerRange = table_write_integrity_types.UniqueConstraintOwnerRange;
pub const UniqueConstraintIntegrityProgress = table_write_integrity_types.UniqueConstraintIntegrityProgress;
pub const ForeignKeyIntegrityGroupReport = table_write_integrity_types.ForeignKeyIntegrityGroupReport;
pub const ForeignKeyIntegrityTupleValue = table_write_integrity_types.ForeignKeyIntegrityTupleValue;
pub const ForeignKeyIntegrityViolation = table_write_integrity_types.ForeignKeyIntegrityViolation;
pub const ForeignKeyIntegrityJobStatus = table_write_integrity_types.ForeignKeyIntegrityJobStatus;
pub const ForeignKeyIntegrityResult = table_write_integrity_types.ForeignKeyIntegrityResult;
pub const ForeignKeyIntegrityWorkUnit = table_write_integrity_types.ForeignKeyIntegrityWorkUnit;
pub const ForeignKeyIntegrityWorkState = table_write_integrity_types.ForeignKeyIntegrityWorkState;
pub const ForeignKeyIntegrityWorkStatus = table_write_integrity_types.ForeignKeyIntegrityWorkStatus;
pub const ForeignKeyIntegrityWorkClaim = table_write_integrity_types.ForeignKeyIntegrityWorkClaim;
pub const ForeignKeyIntegrityProgress = table_write_integrity_types.ForeignKeyIntegrityProgress;
pub const ForeignKeyActionJobStatus = table_write_integrity_types.ForeignKeyActionJobStatus;
pub const ForeignKeyActionJobResult = table_write_integrity_types.ForeignKeyActionJobResult;
pub const ForeignKeyActionJobProgressResult = table_write_integrity_types.ForeignKeyActionJobProgressResult;
pub const ForeignKeyActionScheduleStatus = table_write_integrity_types.ForeignKeyActionScheduleStatus;
pub const ForeignKeyActionScheduleProgressResult = table_write_integrity_types.ForeignKeyActionScheduleProgressResult;
pub const SecondaryIndexRebuildWorkerResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerResult;
pub const SecondaryIndexRebuildWorkerPassResult = table_write_schema_jobs.SecondaryIndexRebuildWorkerPassResult;
pub const SecondaryIndexRebuildGroupRequest = table_write_schema_jobs.SecondaryIndexRebuildGroupRequest;
pub const SchemaRewriteWorkerResult = table_write_schema_jobs.SchemaRewriteWorkerResult;
pub const SchemaRewriteWorkerPassResult = table_write_schema_jobs.SchemaRewriteWorkerPassResult;
pub const SchemaRewriteGroupRequest = table_write_schema_jobs.SchemaRewriteGroupRequest;
pub const TableWriteSource = table_write_core.TableWriteSource;
pub const RaftBatcher = table_write_core.RaftBatcher;
pub const freeForeignKeyRefChildrenPage = table_write_core.freeForeignKeyRefChildrenPage;
pub const validateIndexConfig = table_write_index_config.validateIndexConfig;
pub const validateIndexConfigWithOptions = table_write_index_config.validateIndexConfigWithOptions;
pub const normalizeManagedEmbeddingIndexDimensionJsonWithOptions = table_write_index_config.normalizeManagedEmbeddingIndexDimensionJsonWithOptions;
pub const normalizeManagedEmbeddingIndexDimensionsJsonWithOptions = table_write_index_config.normalizeManagedEmbeddingIndexDimensionsJsonWithOptions;

const GroupBatch = table_write_core.GroupBatch;
const backend_current_root_generation = table_write_core.backend_current_root_generation;
const normalizeRelationalConstraintError = table_write_core.normalizeRelationalConstraintError;
const nextTxnTimestamp = table_write_core.nextTxnTimestamp;
const nextTxnId = table_write_core.nextTxnId;
const boundConflict = table_write_core.boundConflict;
const cloneOptionalString = table_write_integrity.cloneOptionalString;
const foreignKeyActionJobStatusFromDbRecord = table_write_integrity.foreignKeyActionJobStatusFromDbRecord;
const cloneForeignKeyActionJobStatus = table_write_integrity.cloneForeignKeyActionJobStatus;
const foreignKeyActionJobProgressFromDbRecords = table_write_integrity.foreignKeyActionJobProgressFromDbRecords;
const foreignKeyActionScheduleStatusFromDbRecord = table_write_integrity.foreignKeyActionScheduleStatusFromDbRecord;
const foreignKeyActionScheduleProgressFromDbRecords = table_write_integrity.foreignKeyActionScheduleProgressFromDbRecords;
const appendForeignKeyActionJobProgressFromDb = table_write_integrity.appendForeignKeyActionJobProgressFromDb;
const appendForeignKeyActionJobStatuses = table_write_integrity.appendForeignKeyActionJobStatuses;
const appendForeignKeyActionJobStatusFromProgressByJobId = table_write_integrity.appendForeignKeyActionJobStatusFromProgressByJobId;
const countInvalidForeignKeyActionJobStatusesByJobId = table_write_integrity.countInvalidForeignKeyActionJobStatusesByJobId;
const countInvalidForeignKeyActionJobStatuses = table_write_integrity.countInvalidForeignKeyActionJobStatuses;
const cloneForeignKeyActionScheduleStatus = table_write_integrity.cloneForeignKeyActionScheduleStatus;
const appendForeignKeyActionScheduleStatuses = table_write_integrity.appendForeignKeyActionScheduleStatuses;
const cloneForeignKeyIntegrityViolation = table_write_integrity.cloneForeignKeyIntegrityViolation;
const cloneForeignKeyIntegrityResultViolation = table_write_integrity.cloneForeignKeyIntegrityResultViolation;
const cloneForeignKeyIntegrityResultViolations = table_write_integrity.cloneForeignKeyIntegrityResultViolations;
const cloneForeignKeyIntegrityProgress = table_write_integrity.cloneForeignKeyIntegrityProgress;
const cloneForeignKeyIntegrityProgressSlice = table_write_integrity.cloneForeignKeyIntegrityProgressSlice;
const cloneForeignKeyIntegrityProgressRecord = table_write_integrity.cloneForeignKeyIntegrityProgressRecord;
const cloneForeignKeyIntegrityWorkUnit = table_write_integrity.cloneForeignKeyIntegrityWorkUnit;
const cloneForeignKeyIntegrityWorkUnits = table_write_integrity.cloneForeignKeyIntegrityWorkUnits;
const foreignKeyIntegrityPlannedActionName = table_write_integrity.foreignKeyIntegrityPlannedActionName;
pub const stableForeignKeyIntegrityJobIdAlloc = table_write_integrity.stableForeignKeyIntegrityJobIdAlloc;
const foreignKeyIntegrityProgressModeNameForPlannedAction = table_write_integrity.foreignKeyIntegrityProgressModeNameForPlannedAction;
const startForeignKeyIntegrityJobOnDb = table_write_integrity.startForeignKeyIntegrityJobOnDb;
const foreignKeyIntegrityJobDiagnosticsAlloc = table_write_integrity.foreignKeyIntegrityJobDiagnosticsAlloc;
const foreignKeyIntegrityViolationSamplesContain = table_write_integrity.foreignKeyIntegrityViolationSamplesContain;
const foreignKeyIntegrityTupleValuesEqual = table_write_integrity.foreignKeyIntegrityTupleValuesEqual;
const optionalStringsEqual = table_write_integrity.optionalStringsEqual;
const finishForeignKeyIntegrityJobOnDb = table_write_integrity.finishForeignKeyIntegrityJobOnDb;
const hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb = table_write_integrity.hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb;
const attachForeignKeyIntegrityJobId = table_write_integrity.attachForeignKeyIntegrityJobId;
const decodeForeignKeyIntegrityJobViolationSamples = table_write_integrity.decodeForeignKeyIntegrityJobViolationSamples;
const cloneForeignKeyIntegrityJobStatus = table_write_integrity.cloneForeignKeyIntegrityJobStatus;
const cloneForeignKeyIntegrityJobRecord = table_write_integrity.cloneForeignKeyIntegrityJobRecord;
const cloneForeignKeyIntegrityWorkStatus = table_write_integrity.cloneForeignKeyIntegrityWorkStatus;
const cloneForeignKeyIntegrityWorkClaim = table_write_integrity.cloneForeignKeyIntegrityWorkClaim;
const cloneForeignKeyIntegrityWorkClaimSlice = table_write_integrity.cloneForeignKeyIntegrityWorkClaimSlice;
const cloneForeignKeyIntegrityResultForWorkerSnapshot = table_write_integrity.cloneForeignKeyIntegrityResultForWorkerSnapshot;
const cloneForeignKeyIntegrityResultForWorkerExecution = table_write_integrity.cloneForeignKeyIntegrityResultForWorkerExecution;
const cloneForeignKeyIntegrityWorkClaimRecord = table_write_integrity.cloneForeignKeyIntegrityWorkClaimRecord;
const foreignKeyIntegrityProgressMatchesUnit = table_write_integrity.foreignKeyIntegrityProgressMatchesUnit;
const foreignKeyIntegrityClaimKey = table_write_integrity.foreignKeyIntegrityClaimKey;
const foreignKeyIntegrityWorkStatusFromUnit = table_write_integrity.foreignKeyIntegrityWorkStatusFromUnit;
const buildForeignKeyIntegrityWorkStatuses = table_write_integrity.buildForeignKeyIntegrityWorkStatuses;
const foreignKeyIntegritySingleWorkUnit = table_write_integrity.foreignKeyIntegritySingleWorkUnit;
const foreignKeyIntegritySingleWorkUnitForPhase = table_write_integrity.foreignKeyIntegritySingleWorkUnitForPhase;
const foreignKeyIntegrityWorkerActionSupported = table_write_integrity.foreignKeyIntegrityWorkerActionSupported;
const foreignKeyIntegrityWorkStatusClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusClaimable;
const foreignKeyIntegrityWorkStatusesValid = table_write_integrity.foreignKeyIntegrityWorkStatusesValid;
const foreignKeyIntegrityWorkStatusesHaveClaimable = table_write_integrity.foreignKeyIntegrityWorkStatusesHaveClaimable;
const foreignKeyIntegrityPlannedUnitsContainGroupBefore = table_write_integrity.foreignKeyIntegrityPlannedUnitsContainGroupBefore;
const mergeForeignKeyIntegrityReport = table_write_integrity.mergeForeignKeyIntegrityReport;
const appendForeignKeyIntegrityResult = table_write_integrity.appendForeignKeyIntegrityResult;
const appendForeignKeyIntegrityProgressAndClaims = table_write_integrity.appendForeignKeyIntegrityProgressAndClaims;
const appendForeignKeyIntegrityExecutedResult = table_write_integrity.appendForeignKeyIntegrityExecutedResult;
const mergeForeignKeyDeletePlans = table_write_integrity.mergeForeignKeyDeletePlans;
const cloneUniqueConstraintIntegrityProgress = table_write_integrity.cloneUniqueConstraintIntegrityProgress;
const cloneUniqueConstraintIntegrityProgressRecord = table_write_integrity.cloneUniqueConstraintIntegrityProgressRecord;
const mergeUniqueConstraintIntegrityReport = table_write_integrity.mergeUniqueConstraintIntegrityReport;
const appendUniqueConstraintIntegrityResult = table_write_integrity.appendUniqueConstraintIntegrityResult;
const uniqueConstraintIntegrityProgressModeForAction = table_write_integrity.uniqueConstraintIntegrityProgressModeForAction;
const emptyForeignKeyIntegrityControllerResult = table_write_integrity.emptyForeignKeyIntegrityControllerResult;
const validateUniqueConstraintIntegritySchemaControllerOptions = table_write_integrity.validateUniqueConstraintIntegritySchemaControllerOptions;
const appendUniqueConstraintIntegritySchemaControllerTableResult = table_write_integrity.appendUniqueConstraintIntegritySchemaControllerTableResult;
const finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult = table_write_integrity.finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult;
const shouldPromoteUniqueConstraintAfterSchemaControllerResult = table_write_integrity.shouldPromoteUniqueConstraintAfterSchemaControllerResult;
const validateForeignKeyIntegritySchemaControllerOptions = table_write_integrity.validateForeignKeyIntegritySchemaControllerOptions;
const appendForeignKeyIntegritySchemaControllerTableResult = table_write_integrity.appendForeignKeyIntegritySchemaControllerTableResult;
const foreignKeyIntegrityActionFromJobStatus = table_write_integrity.foreignKeyIntegrityActionFromJobStatus;
const foreignKeyIntegritySchemaControllerResultsContainJobId = table_write_integrity.foreignKeyIntegritySchemaControllerResultsContainJobId;
const finalizeForeignKeyIntegritySchemaControllerMaintenanceResult = table_write_integrity.finalizeForeignKeyIntegritySchemaControllerMaintenanceResult;
const resultForeignKeyConstraintName = table_write_integrity.resultForeignKeyConstraintName;
const shouldPromoteForeignKeyAfterSchemaControllerResult = table_write_integrity.shouldPromoteForeignKeyAfterSchemaControllerResult;
const runUniqueConstraintIntegritySchemaControllerMaintenanceForTable = table_write_integrity.runUniqueConstraintIntegritySchemaControllerMaintenanceForTable;
const promoteLocalUniqueConstraintAfterSchemaControllerResult = table_write_integrity.promoteLocalUniqueConstraintAfterSchemaControllerResult;
const foreignKeyIntegritySchemaControllerPassWithSchemaJson = table_write_integrity.foreignKeyIntegritySchemaControllerPassWithSchemaJson;
const runForeignKeyIntegritySchemaControllerMaintenanceForTable = table_write_integrity.runForeignKeyIntegritySchemaControllerMaintenanceForTable;
const runForeignKeyIntegrityJobControllerMaintenanceForTable = table_write_integrity.runForeignKeyIntegrityJobControllerMaintenanceForTable;
const runForeignKeyActionScheduleControllerMaintenanceForTable = table_write_integrity.runForeignKeyActionScheduleControllerMaintenanceForTable;
const runForeignKeyActionJobControllerMaintenanceForTable = table_write_integrity.runForeignKeyActionJobControllerMaintenanceForTable;
const foreignKeyActionCanRunGroupDbLocal = table_write_integrity.foreignKeyActionCanRunGroupDbLocal;
const foreignKeyActionOwnerParentTableNameAlloc = table_write_integrity.foreignKeyActionOwnerParentTableNameAlloc;
const promoteLocalForeignKeyAfterSchemaControllerResult = table_write_integrity.promoteLocalForeignKeyAfterSchemaControllerResult;
const runCatalogForeignKeyIntegritySchemaControllerMaintenancePass = table_write_integrity.runCatalogForeignKeyIntegritySchemaControllerMaintenancePass;
const runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass = table_write_integrity.runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass;
const planForeignKeyIntegrityWorkUnits = table_write_integrity.planForeignKeyIntegrityWorkUnits;
const planForeignKeyIntegrityWorkerWorkUnits = table_write_integrity.planForeignKeyIntegrityWorkerWorkUnits;
const appendForeignKeyIntegrityOwnerRangeWorkUnits = table_write_integrity.appendForeignKeyIntegrityOwnerRangeWorkUnits;
const foreignKeyReferenceRangeMatchesEnforcedSchema = table_write_integrity.foreignKeyReferenceRangeMatchesEnforcedSchema;
const findForeignKeyIntegritySnapshotTableById = table_write_integrity.findForeignKeyIntegritySnapshotTableById;
const sortRangeRefsForForeignKeyIntegrityPlan = table_write_integrity.sortRangeRefsForForeignKeyIntegrityPlan;
const rangeOverlapsForeignKeyIntegritySpan = table_write_integrity.rangeOverlapsForeignKeyIntegritySpan;
const rangeOverlapsForeignKeyOwnerSpan = table_write_integrity.rangeOverlapsForeignKeyOwnerSpan;
const maxLowerDocKey = table_write_integrity.maxLowerDocKey;
const minUpperDocKey = table_write_integrity.minUpperDocKey;
const foreignKeyIntegrityNowNs = table_write_integrity.foreignKeyIntegrityNowNs;
const resolveForeignKeyIntegrityGroupsEventually = table_write_integrity.resolveForeignKeyIntegrityGroupsEventually;
const foreignKeyIntegrityProgressModeForAction = table_write_integrity.foreignKeyIntegrityProgressModeForAction;
const foreignKeyIntegrityResultFromRoutedExplain = table_write_integrity.foreignKeyIntegrityResultFromRoutedExplain;
const runForeignKeyIntegrityOnDb = table_write_integrity.runForeignKeyIntegrityOnDb;
const runForeignKeyIntegrityClaimedWorkUnitOnDb = table_write_integrity.runForeignKeyIntegrityClaimedWorkUnitOnDb;
const runUniqueConstraintIntegrityOnDb = table_write_integrity.runUniqueConstraintIntegrityOnDb;
const stableForeignKeyActionPageTxnId = table_write_integrity.stableForeignKeyActionPageTxnId;
const hashForeignKeyActionPageTxnIdentity = table_write_integrity.hashForeignKeyActionPageTxnIdentity;
const hashActionPageTxnField = table_write_integrity.hashActionPageTxnField;
const hashActionPageTxnOptionalField = table_write_integrity.hashActionPageTxnOptionalField;
const hashActionPageTxnU64 = table_write_integrity.hashActionPageTxnU64;
const appendUniqueGroupId = table_write_integrity.appendUniqueGroupId;
const collectForeignKeyActionJobProgressGroupIds = table_write_integrity.collectForeignKeyActionJobProgressGroupIds;
const runSecondaryIndexRebuildRangeGroupLocal = table_write_schema_jobs.runSecondaryIndexRebuildRangeGroupLocal;
const runSecondaryIndexRebuildWorkerPassForCatalog = table_write_schema_jobs.runSecondaryIndexRebuildWorkerPassForCatalog;
const runSchemaRewriteJobGroupLocal = table_write_schema_jobs.runSchemaRewriteJobGroupLocal;
const runSchemaRewriteWorkerPassForCatalog = table_write_schema_jobs.runSchemaRewriteWorkerPassForCatalog;

const nativeCatalogTableNameAlloc = table_catalog.nativeTableNameForCatalogTargetAlloc;
const nativeCatalogTableNameForCreateAlloc = table_catalog.nativeTableNameForCatalogCreateTargetAlloc;

const max_cached_write_tables = 64;
const auto_bulk_ingest_min_batch_ops = table_write_bulk_ingest.min_batch_ops;
const auto_bulk_ingest_max_window_ops = table_write_bulk_ingest.max_window_ops;
const auto_bulk_ingest_max_idle_ns = table_write_bulk_ingest.max_idle_ns;
const auto_bulk_ingest_finish_options = table_write_bulk_ingest.finish_options;
const shouldDrainManagedDbAfterBatch = table_write_bulk_ingest.shouldDrainManagedDbAfterBatch;
const shouldDrainCachedManagedDbAfterBatch = table_write_bulk_ingest.shouldDrainCachedManagedDbAfterBatch;
const autoBulkIngestBatchOps = table_write_bulk_ingest.autoBulkIngestBatchOps;
const autoBulkIngestGroupBatchOps = table_write_bulk_ingest.autoBulkIngestGroupBatchOps;
const ensureGroupBatch = table_write_bulk_ingest.ensureGroupBatch;
const WriteCoalesceQueue = table_write_bulk_ingest.WriteCoalesceQueue;
const WriteCoalesceEntry = table_write_bulk_ingest.WriteCoalesceEntry;
const coalescedEntryBatchRequest = table_write_bulk_ingest.coalescedEntryBatchRequest;
const totalCoalescedWrites = table_write_bulk_ingest.totalCoalescedWrites;
const totalCoalescedDeletes = table_write_bulk_ingest.totalCoalescedDeletes;
const cloneWriteCoalesceGroupBatch = table_write_bulk_ingest.cloneWriteCoalesceGroupBatch;
const freeWriteCoalesceGroupBatch = table_write_bulk_ingest.freeWriteCoalesceGroupBatch;
const accumulateTextMemoryAttributionStats = table_write_cache.accumulateTextMemoryAttributionStats;

const moveDroppedGroupPathToTrash = table_write_backup_restore.moveDroppedGroupPathToTrash;
const deleteGroupPathIfPresent = table_write_backup_restore.deleteGroupPathIfPresent;
const readBackupFileAlloc = table_write_backup_restore.readBackupFileAlloc;
const freeBackupShards = table_write_backup_restore.freeBackupShards;
const cloneShardSnapshots = table_write_backup_restore.cloneShardSnapshots;

pub const mutateRowsJoinedFromRecursiveCtePlanAlloc = table_write_relational_mutation.mutateRowsJoinedFromRecursiveCtePlanAlloc;
pub const mutateRowsJoinedFromRecursiveCtePlanWithSessionAlloc = table_write_relational_mutation.mutateRowsJoinedFromRecursiveCtePlanWithSessionAlloc;
pub const mutateRowsJoinedFromRecursiveCtePlanAutocommitAlloc = table_write_relational_mutation.mutateRowsJoinedFromRecursiveCtePlanAutocommitAlloc;
pub const mutateRowsJoinedFromRecursiveCtePlanAutocommitWithSessionAlloc = table_write_relational_mutation.mutateRowsJoinedFromRecursiveCtePlanAutocommitWithSessionAlloc;
pub const mergeRowsFromRecursiveCtePlanAlloc = table_write_relational_mutation.mergeRowsFromRecursiveCtePlanAlloc;
pub const mergeRowsFromRecursiveCtePlanWithSessionAlloc = table_write_relational_mutation.mergeRowsFromRecursiveCtePlanWithSessionAlloc;
const mutateRowsFromSourceAutocommitOnDb = table_write_relational_mutation.mutateRowsFromSourceAutocommitOnDb;
const mutateRowsJoinedFromSourceRowsOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsOnDb;
const mutateRowsJoinedFromSourceRowsAutocommitOnDb = table_write_relational_mutation.mutateRowsJoinedFromSourceRowsAutocommitOnDb;
const mergeRowsFromSourceRowsOnDb = table_write_relational_mutation.mergeRowsFromSourceRowsOnDb;
const ManagedDbOpenMode = table_write_managed_db.ManagedDbOpenMode;
const ManagedDbOpenOptions = table_write_managed_db.ManagedDbOpenOptions;
const haMirrorForManagedDbOpenMode = table_write_managed_db.haMirrorForManagedDbOpenMode;
const loadLocalTableSchemaJson = table_write_managed_db.loadLocalTableSchemaJson;
const applyLocalTableSchemaJson = table_write_managed_db.applyLocalTableSchemaJson;
const rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate = table_write_managed_db.rebuildEmptyVersionedFullTextIndexesAfterSchemaUpdate;
const corruptEmbeddingArtifactInDb = table_write_managed_db.corruptEmbeddingArtifactInDb;
const catchUpManagedIndexCreate = table_write_managed_db.catchUpManagedIndexCreate;
const seedManagedIndexReplayFromStoredDocsIfNeeded = table_write_managed_db.seedManagedIndexReplayFromStoredDocsIfNeeded;
const reconcileLocalTableIndexes = table_write_managed_db.reconcileLocalTableIndexes;
const dropLocalTableIndex = table_write_managed_db.dropLocalTableIndex;
const replayManagedIndexForTableIfNeeded = table_write_managed_db.replayManagedIndexForTableIfNeeded;
const drainManagedDbBeforeClose = table_write_managed_db.drainManagedDbBeforeClose;
const isTransientReplayVisibilityError = table_write_managed_db.isTransientReplayVisibilityError;
const loadTableIndexesJson = table_write_managed_db.loadTableIndexesJson;
const loadTableManagedMetadata = table_write_managed_db.loadTableManagedMetadata;
const loadTableIdentityNamespaceForGroup = table_write_managed_db.loadTableIdentityNamespaceForGroup;
const findTableRecord = table_write_managed_db.findTableRecord;
const findRangeRecord = table_write_managed_db.findRangeRecord;
const validateProvisionedDbIdentityNamespaceExpected = table_write_managed_db.validateProvisionedDbIdentityNamespaceExpected;
const validateProvisionedDbIdentityNamespace = table_write_managed_db.validateProvisionedDbIdentityNamespace;
const loadTableSchemaJson = table_write_managed_db.loadTableSchemaJson;
const validateTableWritesAgainstLocalSchema = table_write_managed_db.validateTableWritesAgainstLocalSchema;
const validateTableBatchAgainstLocalSchema = table_write_managed_db.validateTableBatchAgainstLocalSchema;
const validateTransactionAgainstLocalSchema = table_write_managed_db.validateTransactionAgainstLocalSchema;
const validateTableWritesAgainstCatalogSchema = table_write_managed_db.validateTableWritesAgainstCatalogSchema;
const validateTableBatchAgainstCatalogSchema = table_write_managed_db.validateTableBatchAgainstCatalogSchema;
const validateTableBatchAgainstSchemaJson = table_write_managed_db.validateTableBatchAgainstSchemaJson;
const validateTransactionAgainstCatalogSchema = table_write_managed_db.validateTransactionAgainstCatalogSchema;
const openManagedDbForStatusWithCache = table_write_managed_db.openManagedDbForStatusWithCache;
pub const openManagedDbForStatusWithIndexesJsonAndCache = table_write_managed_db.openManagedDbForStatusWithIndexesJsonAndCache;
const openManagedDbWithIndexesJson = table_write_managed_db.openManagedDbWithIndexesJson;
const openManagedDbWithIndexesJsonAndCache = table_write_managed_db.openManagedDbWithIndexesJsonAndCache;
const openManagedDbWithIndexesJsonAndCacheMode = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheMode;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntime = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntime;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndIdentity;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntfly = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntfly;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentity;
const openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions = table_write_managed_db.openManagedDbWithIndexesJsonAndCacheModeWithRuntimeAndLocalAntflyAndIdentityWithOptions;
const openManagedDbForTable = table_write_managed_db.openManagedDbForTable;
const openManagedDbForTableWithRuntime = table_write_managed_db.openManagedDbForTableWithRuntime;
const openManagedDbForTableGroupWithRuntime = table_write_managed_db.openManagedDbForTableGroupWithRuntime;
const openManagedDbForTableGroupWithRuntimeAndHAWriteGate = table_write_managed_db.openManagedDbForTableGroupWithRuntimeAndHAWriteGate;
const openManagedDbForTableWithCache = table_write_managed_db.openManagedDbForTableWithCache;
const openManagedDbForTableWithIndexesJson = table_write_managed_db.openManagedDbForTableWithIndexesJson;
const openManagedDbForTableWithCacheAndRuntime = table_write_managed_db.openManagedDbForTableWithCacheAndRuntime;
const openManagedDbForTableGroupWithCacheAndRuntime = table_write_managed_db.openManagedDbForTableGroupWithCacheAndRuntime;
const openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate = table_write_managed_db.openManagedDbForTableGroupWithCacheAndRuntimeAndHAWriteGate;
const openManagedDbForTableWithIndexesJsonAndCache = table_write_managed_db.openManagedDbForTableWithIndexesJsonAndCache;
const openManagedDbForTableWithIndexesJsonAndCacheAndRuntime = table_write_managed_db.openManagedDbForTableWithIndexesJsonAndCacheAndRuntime;

fn mergeDocumentArtifactTableReprocessResult(
    alloc: std.mem.Allocator,
    dst: *db_mod.types.DocumentArtifactTableReprocessResult,
    dst_failures: *std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessFailure),
    dst_shard_cursors: *std.ArrayListUnmanaged(db_mod.types.DocumentArtifactReprocessShardCursor),
    group_id: ?u64,
    src: db_mod.types.DocumentArtifactTableReprocessResult,
) !void {
    dst.scanned += src.scanned;
    dst.reprocessed += src.reprocessed;
    dst.skipped += src.skipped;
    dst.failed += src.failed;
    dst.limit += src.limit;
    if (dst.next_key == null) {
        if (src.next_key) |next_key| dst.next_key = try alloc.dupe(u8, next_key);
    }
    if (src.shard_cursors.len > 0) {
        for (src.shard_cursors) |cursor| {
            const next_key_copy = try alloc.dupe(u8, cursor.next_key);
            errdefer alloc.free(next_key_copy);
            try dst_shard_cursors.append(alloc, .{
                .group_id = cursor.group_id orelse group_id,
                .next_key = next_key_copy,
                .scanned = cursor.scanned,
                .reprocessed = cursor.reprocessed,
                .skipped = cursor.skipped,
                .failed = cursor.failed,
                .limit = cursor.limit,
            });
        }
    } else if (src.next_key) |next_key| {
        const next_key_copy = try alloc.dupe(u8, next_key);
        errdefer alloc.free(next_key_copy);
        try dst_shard_cursors.append(alloc, .{
            .group_id = group_id,
            .next_key = next_key_copy,
            .scanned = src.scanned,
            .reprocessed = src.reprocessed,
            .skipped = src.skipped,
            .failed = src.failed,
            .limit = src.limit,
        });
    }
    for (src.failures) |failure| {
        const key = try alloc.dupe(u8, failure.key);
        errdefer alloc.free(key);
        const error_code = try alloc.dupe(u8, failure.error_code);
        errdefer alloc.free(error_code);
        try dst_failures.append(alloc, .{
            .key = key,
            .error_code = error_code,
        });
    }
}

fn documentArtifactReprocessRequestForCursor(
    req: db_mod.types.DocumentArtifactTableReprocessRequest,
    cursor: db_mod.types.DocumentArtifactReprocessShardResume,
) db_mod.types.DocumentArtifactTableReprocessRequest {
    return .{
        .from_key = cursor.next_key,
        .to_key = req.to_key,
        .limit = if (cursor.limit != 0) cursor.limit else req.limit,
    };
}

pub const ProvisionedTableWriteCache = table_write_cache.ProvisionedTableWriteCache;
const HostedManagedDbCache = table_write_cache.HostedManagedDbCache;
pub const HostedManagedDbCacheDiagnostics = table_write_cache.HostedManagedDbCacheDiagnostics;
pub const closeHostedManagedDbCacheForRoot = table_write_cache.closeHostedManagedDbCacheForRoot;
const hostedManagedDbCacheForRoot = table_write_cache.hostedManagedDbCacheForRoot;
const hostedManagedDbCacheForRootIfPresent = table_write_cache.hostedManagedDbCacheForRootIfPresent;
pub const hostedManagedDbCacheDiagnosticsForRoot = table_write_cache.hostedManagedDbCacheDiagnosticsForRoot;

fn parseJsonBodyIgnoreUnknown(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{ .ignore_unknown_fields = true });
}

fn jsonValueContainsText(value: std.json.Value, needle: []const u8) bool {
    switch (value) {
        .string => |text| return std.mem.indexOf(u8, text, needle) != null,
        .array => |items| {
            for (items.items) |item| {
                if (jsonValueContainsText(item, needle)) return true;
            }
            return false;
        },
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (std.mem.indexOf(u8, entry.key_ptr.*, needle) != null) return true;
                if (jsonValueContainsText(entry.value_ptr.*, needle)) return true;
            }
            return false;
        },
        else => return false,
    }
}

const TestEmbeddingRequest = struct {
    model: std.json.Value,
    input: std.json.Value,
};

pub const BoundTableWriteSource = table_write_sources.BoundTableWriteSource;
pub const ProvisionedTableWriteSource = table_write_sources.ProvisionedTableWriteSource;
pub const HostedProvisionedTableWriteSource = table_write_sources.HostedProvisionedTableWriteSource;

test "docid focused provisioned secondary index rebuild worker pass repairs projected catalog range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-secondary-index-rebuild-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 9001);
    defer alloc.free(db_path);

    const building_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    const range: metadata_table_manager.RangeRecord = .{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const namespace: doc_identity.Namespace = .{
        .table_id = 77,
        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
    };
    {
        var db = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace });
        defer db.close();
        try db.applyTableSchemaJson(alloc, building_schema_json, .{});
        try db.batch(.{ .writes = &.{
            .{ .key = "row:active", .value = "{\"id\":\"active\",\"amount\":1,\"status\":\"active\"}" },
            .{ .key = "row:inactive", .value = "{\"id\":\"inactive\",\"amount\":2,\"status\":\"inactive\"}" },
        } });
        const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
        defer alloc.free(inactive_amount_key);
        try db.core.store.put(inactive_amount_key, "");
    }

    const Catalog = struct {
        const ready_schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"ready","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ;

        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "orders",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = building_schema_json,
        },
        range: metadata_table_manager.RangeRecord = range,
        rebuild: metadata_table_manager.SecondaryIndexRebuildRangeRecord = .{
            .table_id = 77,
            .index_name = "amount",
            .index_generation = 9,
            .start_row_key = "",
            .end_row_key = null,
            .group_id = 9001,
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_secondary_index_rebuild_range = beginSecondaryIndexRebuildRange,
                    .finish_secondary_index_rebuild_range = finishSecondaryIndexRebuildRange,
                    .invalidate_secondary_index_rebuild_range = invalidateSecondaryIndexRebuildRange,
                    .promote_secondary_index_ready = promoteSecondaryIndexReady,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .secondary_index_rebuild_ranges = @as([*]metadata_table_manager.SecondaryIndexRebuildRangeRecord, @ptrCast(&self.rebuild))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn selectorMatches(self: *@This(), selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector) bool {
            return selector.table_id == self.rebuild.table_id and
                selector.index_generation == self.rebuild.index_generation and
                std.mem.eql(u8, selector.index_name, self.rebuild.index_name) and
                std.mem.eql(u8, selector.start_row_key, self.rebuild.start_row_key);
        }

        fn beginSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            if (!std.mem.eql(u8, self.rebuild.state, metadata_table_manager.secondary_index_rebuild_declared)) return error.SecondaryIndexRebuildRangeClaimBusy;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_building;
            self.rebuild.lease_owner = request.lease_owner;
            self.rebuild.lease_expires_at_ms = request.lease_expires_at_ms;
        }

        fn finishSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_ready;
            self.rebuild.completed_row_count = request.completed_row_count;
            self.rebuild.progress_row_key = request.progress_row_key;
        }

        fn invalidateSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.selectorMatches(request.selector)) return error.SecondaryIndexRebuildRangeNotFound;
            self.rebuild.state = metadata_table_manager.secondary_index_rebuild_invalid;
            self.rebuild.last_error = request.last_error;
        }

        fn promoteSecondaryIndexReady(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            expected_generation: u64,
        ) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, table_name, self.table.name)) return false;
            const updated = tables_api.schemaWithSecondaryIndexReadyAlloc(
                allocator,
                self.table.schema_json,
                index_name,
                expected_generation,
            ) catch |err| switch (err) {
                error.SecondaryIndexNotBuilding,
                error.SecondaryIndexGenerationMismatch,
                error.SecondaryIndexNotFound,
                => return false,
                else => return err,
            };
            defer allocator.free(updated);
            self.table.schema_json = ready_schema_json;
            return true;
        }
    };

    var catalog = Catalog{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    defer source.deinit();

    var pass = (try source.source().secondaryIndexRebuildWorkerPass(alloc, "orders", "worker-a", 500, 1)).?;
    defer pass.deinit(alloc);
    try std.testing.expect(pass.complete);
    try std.testing.expectEqual(@as(u64, 1), pass.ranges_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.ranges_completed);
    try std.testing.expectEqual(@as(u64, 1), pass.indexes_promoted);
    try std.testing.expectEqual(@as(u64, 2), pass.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), pass.report.indexed_rows);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, catalog.rebuild.state);
    try std.testing.expectEqual(@as(u64, 2), catalog.rebuild.completed_row_count);
    try std.testing.expect(std.mem.indexOf(u8, catalog.table.schema_json, "\"x-antfly-index-lifecycle\":\"ready\"") != null);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace, .prefer_existing_identity_namespace = true });
    defer reopened.close();
    const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
    defer alloc.free(inactive_amount_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, inactive_amount_key));
}

test "docid focused provisioned schema rewrite worker pass drains projected catalog range job" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-schema-rewrite-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 9001);
    defer alloc.free(db_path);

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["title","status"],"additionalProperties":false}}}}
    ;

    const range: metadata_table_manager.RangeRecord = .{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const namespace: doc_identity.Namespace = .{
        .table_id = 77,
        .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
        .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
    };
    {
        var db = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_v1, .{});
        try db.batch(.{ .writes = &.{.{ .key = "row:a", .value = "{\"title\":\"one\",\"status\":\"ACTIVE\"}" }} });
        try db.applyTableSchemaJson(alloc, schema_v2, .{});
    }

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = schema_v2,
        },
        range: metadata_table_manager.RangeRecord = range,
        job: metadata_table_manager.SchemaRewriteJobRecord = .{
            .job_id = 8101,
            .table_id = 77,
            .group_id = 9001,
            .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .end_row_key = null,
            .target_column = "status_key",
            .expression = .{
                .kind = .lower,
                .operands = &.{.{ .kind = .field, .field = "status" }},
            },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_schema_rewrite_job = beginSchemaRewriteJob,
                    .finish_schema_rewrite_job = finishSchemaRewriteJob,
                    .invalidate_schema_rewrite_job = invalidateSchemaRewriteJob,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .schema_rewrite_jobs = @as([*]metadata_table_manager.SchemaRewriteJobRecord, @ptrCast(&self.job))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            if (!std.mem.eql(u8, self.job.state, metadata_table_manager.schema_rewrite_declared)) return error.SchemaRewriteJobClaimBusy;
            self.job.state = metadata_table_manager.schema_rewrite_running;
            self.job.lease_owner = request.lease_owner;
            self.job.lease_expires_at_ms = request.lease_expires_at_ms;
            self.job.attempts += 1;
        }

        fn finishSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            if (!std.mem.eql(u8, self.job.lease_owner, request.lease_owner)) return error.SchemaRewriteJobLeaseMismatch;
            self.job.state = metadata_table_manager.schema_rewrite_ready;
            self.job.lease_owner = "";
            self.job.lease_expires_at_ms = 0;
            self.job.completed_row_count = request.completed_row_count;
            self.job.progress_row_key = "";
        }

        fn invalidateSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != self.job.job_id) return error.UnknownSchemaRewriteJob;
            self.job.state = metadata_table_manager.schema_rewrite_invalid;
            self.job.lease_owner = "";
            self.job.lease_expires_at_ms = 0;
            self.job.last_error = request.last_error;
        }
    };

    var catalog = Catalog{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    defer source.deinit();

    var pass = (try source.source().schemaRewriteWorkerPass(alloc, "events", "worker-a", 500, 1)).?;
    defer pass.deinit(alloc);
    try std.testing.expect(pass.complete);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_completed);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_ready, catalog.job.state);
    try std.testing.expectEqual(@as(u64, 1), catalog.job.completed_row_count);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .identity_namespace = namespace, .prefer_existing_identity_namespace = true });
    defer reopened.close();
    const materialized = (try reopened.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(materialized);
    try std.testing.expect(std.mem.indexOf(u8, materialized, "\"status_key\":\"active\"") != null);
}

test "provisioned table write source create table clears stale local group state" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-create-table-clears-stale-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var stale_db = try db_mod.DB.open(alloc, db_path, .{});
        defer stale_db.close();
        try stale_db.batch(.{
            .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
        });
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const stale_change_journal_dir = try std.fmt.allocPrint(alloc, "{s}/change_journal", .{db_path});
    defer alloc.free(stale_change_journal_dir);
    try fs_paths.createDirPathPortable(io_impl.io(), stale_change_journal_dir);
    const stale_marker_path = try std.fmt.allocPrint(alloc, "{s}/stale.marker", .{stale_change_journal_dir});
    defer alloc.free(stale_marker_path);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = stale_marker_path, .data = "stale" });

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});

    var recreated_db = try db_mod.DB.open(alloc, db_path, .{});
    defer recreated_db.close();
    try std.testing.expect((try recreated_db.lookup(alloc, "doc:stale", .{})) == null);
    try std.testing.expect(recreated_db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), stale_marker_path, .{}));
}

test "provisioned table write source seeds doc identity namespace from table range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-identity-namespace-root", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
        defer source.deinit();
        _ = try source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        });
    }

    var db = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer db.close();
    try std.testing.expect(db.core.identity_namespace.eql(.{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 7101,
    }));
}

test "docid focused provisioned table write source rejects stale doc identity namespace before write" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const stale_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 7001,
        .range_id = 9999,
    };

    const setupStaleDb = struct {
        fn run(allocator: std.mem.Allocator, path: []const u8, namespace: doc_identity.Namespace) !void {
            var db = try db_mod.DB.open(allocator, path, .{ .identity_namespace = namespace });
            defer db.close();
            try db.batch(.{
                .writes = &.{.{ .key = "doc:stale", .value = "{\"title\":\"stale\"}" }},
            });
        }
    }.run;

    const uncached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-uncached-root", .{tmp.sub_path});
    defer alloc.free(uncached_root);
    const uncached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, uncached_root, 7001);
    defer alloc.free(uncached_db_path);
    try setupStaleDb(alloc, uncached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(uncached_root, Catalog.iface());
        defer source.deinit();
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var uncached_db = try db_mod.DB.open(alloc, uncached_db_path, .{ .start_index_workers = false });
    defer uncached_db.close();
    try std.testing.expect((try uncached_db.lookup(alloc, "doc:b", .{})) == null);

    const cached_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-stale-identity-cached-root", .{tmp.sub_path});
    defer alloc.free(cached_root);
    const cached_db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, cached_root, 7001);
    defer alloc.free(cached_db_path);
    try setupStaleDb(alloc, cached_db_path, stale_namespace);

    {
        var source = ProvisionedTableWriteSource.init(cached_root, Catalog.iface());
        defer source.deinit();
        var write_cache = ProvisionedTableWriteCache.init(alloc);
        defer write_cache.deinit();
        source.write_cache = &write_cache;
        try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.source().batch(alloc, "docs", .{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        }));
    }

    var cached_db = try db_mod.DB.open(alloc, cached_db_path, .{ .start_index_workers = false });
    defer cached_db.close();
    try std.testing.expect((try cached_db.lookup(alloc, "doc:b", .{})) == null);
}

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

test "provisioned table write source backs up and restores a local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "docid focused provisioned table write source backs up a portable local table" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup-out");
    defer alloc.free(backup_root);
    const restore_path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-portable-backup-restore");
    defer alloc.free(restore_path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_path) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_path) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "portable-snap",
        .format = .portable,
    })).?;
    defer freeBackupShards(alloc, shards);
    try std.testing.expectEqual(@as(usize, 1), shards.len);
    try std.testing.expectEqualStrings("portable-snap.afb", shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, shards[0].snapshot_path });
    defer alloc.free(afb_path);
    const afb = try readBackupFileAlloc(alloc, afb_path);
    defer alloc.free(afb);

    var restored_db = try db_mod.DB.open(alloc, restore_path, .{});
    defer restored_db.close();
    try portable_backup.importPortable(alloc, restored_db.core.store, afb);
    var restored = (try restored_db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "docid focused provisioned table restore rejects mismatched doc identity namespace" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-docid-mismatch");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-table-backup-restore-docid-mismatch-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const source_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 97001 };
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = source_namespace,
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .timestamp_ns = 1,
        });
        _ = try db.snapshot("snap1-local");
    }

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/snap1-local", .{db_path});
    defer alloc.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(alloc, backup_root, "snap1", 7001);
    defer alloc.free(dest_root);
    try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = 7001,
        .start_key = try alloc.dupe(u8, ""),
        .end_key = null,
        .snapshot_path = try backups_api.shardSnapshotRelPath(alloc, "snap1", 7001),
    };
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                    .range_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    try std.testing.expectError(error.IdentityNamespaceMismatch, source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    }));
}

test "provisioned table write source backs up and restores full_text writes from the write cache" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-write-cache-backup-restore");
    defer alloc.free(path);
    const backup_root = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-write-cache-backup-restore-out");
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"content\":\"distributed consensus\"}" }},
        .timestamp_ns = 1,
        .sync_level = .full_text,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\",\"content\":\"vector search\"}" }},
        .timestamp_ns = 2,
        .sync_level = .full_text,
    });

    _ = try source.source().restoreTable(alloc, "docs", .{
        .backup_root = backup_root,
        .manifest = &manifest,
    });

    var read_source = table_reads.ProvisionedTableReadSource.init(
        path,
        FakeCatalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );

    var restored_lookup = (try read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer restored_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_lookup.json, "\"alpha\"") != null);

    var restored_scan = (try read_source.source().scan(alloc, "docs", "", "", .{
        .limit = 10,
        .include_documents = true,
    }, .read_index)).?;
    defer restored_scan.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, restored_scan.ndjson, "\"alpha\"") != null);

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});

    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned query visibility read preparation invalidates readers without closing dirty writer cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-cache-read-prep";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("other", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    const large_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (large_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(large_writes);
    }
    for (large_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk:{d}", .{i}),
            .value = "{\"title\":\"bulk\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = large_writes,
        .timestamp_ns = 2,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    const next_writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (next_writes) |write| alloc.free(@constCast(write.key));
        alloc.free(next_writes);
    }
    for (next_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-next:{d}", .{i}),
            .value = "{\"title\":\"bulk next\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = next_writes,
        .timestamp_ns = 3,
        .sync_level = .write,
    });

    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].*.auto_bulk_ingest_ops);

    source.readPreparation().prepareForRead("docs", .general);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "auto bulk best-effort finish does not spin when writer cache lock is busy" {
    var source = ProvisionedTableWriteSource.init(
        "/tmp/unused-antfly-auto-bulk-finish-busy",
        table_catalog.emptyCatalogSource(),
    );

    try std.testing.expectEqual(@as(?bool, false), source.tryFinishExpiredAutoBulkIngest());

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(?bool, null), source.tryFinishExpiredAutoBulkIngest());
    try std.testing.expect(!source.finishExpiredAutoBulkIngestBestEffort());
}

test "auto bulk max-window request waits for idle finish" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-auto-bulk-roll-without-next-write";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-roll:{d}", .{i}),
            .value = "{\"title\":\"bulk roll\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);

    const requested_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + 1;
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, requested_finish_ns);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);

    try std.testing.expect(!try write_cache.finishExpiredAutoBulkIngestLocked(requested_finish_ns));
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_finish_requested);
    try std.testing.expect(write_cache.entries.items[0].*.auto_bulk_ingest_ops >= auto_bulk_ingest_max_window_ops);

    const idle_finish_ns = write_cache.entries.items[0].*.auto_bulk_ingest_last_ns + auto_bulk_ingest_max_idle_ns;
    try std.testing.expect(try write_cache.finishExpiredAutoBulkIngestLocked(idle_finish_ns));
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.active_bulk_ingest_sessions.items.len);
}

test "auto bulk background finish skips entries with active foreground leases" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-auto-bulk-active-lease-skip", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:auto-bulk-active-lease:{d}", .{i}),
            .value = "{\"title\":\"active lease\"}",
        };
    }

    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    const entry = write_cache.entries.items[0];
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try write_cache.recordAutoBulkIngestOpsLocked(7001, "docs", auto_bulk_ingest_max_window_ops, platform_time.monotonicNs());
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    entry.active_leases += 1;
    const skipped = try write_cache.finishExpiredAutoBulkIngestLocked(platform_time.monotonicNs());
    try std.testing.expect(!skipped);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(entry.auto_bulk_ingest_finish_requested);

    const foreground_roll = try write_cache.rollRequestedAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(foreground_roll);
    try std.testing.expect(entry.auto_bulk_ingest_session_open);
    try std.testing.expect(!entry.auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), entry.auto_bulk_ingest_ops);

    entry.active_leases -= 1;
}

test "auto bulk group writes release leases so idle finish can publish" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-group-write-release", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = auto_bulk_ingest_min_batch_ops;
    const total_docs = batch_size;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"body\":\"auto bulk doc {d}\"}}", .{i}),
        };
    }

    var offset: usize = 0;
    while (offset < total_docs) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .full_index,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

        if (try source.source().localRuntimeStatuses(alloc, "docs")) |statuses| {
            var owned = statuses;
            owned.deinit(alloc);
        }
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try std.testing.expect(replay_target > 0);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
}

test "weak-sync group writes publish all docs after background dense catch-up" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/auto-bulk-threshold-visible", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const batch_size = 250;
    const expected_docs = 1000;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
    defer {
        for (writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"_embeddings\":{{\"dense_idx\":[1.0,0.0]}}}}", .{}),
        };
    }

    var offset: usize = 0;
    while (offset < expected_docs) : (offset += batch_size) {
        for (writes, 0..) |*write, i| {
            alloc.free(@constCast(write.key));
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{offset + i});
        }

        _ = try source.source().batchGroupLocal(alloc, 7001, "docs", .{
            .writes = writes,
            .timestamp_ns = @intCast(offset + 1),
            .sync_level = .write,
        });

        try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    }

    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expect(!write_cache.entries.items[0].auto_bulk_ingest_finish_requested);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].auto_bulk_ingest_ops);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const replay_target = write_cache.entries.items[0].db.core.nextDerivedSequence();
    try write_cache.entries.items[0].db.executor.waitForAll(replay_target);
    const stats = try write_cache.entries.items[0].db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, expected_docs), stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, stats.indexes[0].replay_applied_sequence);

    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &write_cache.entries.items[0].db));
    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, expected_docs), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(replay_target, statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "dirty auto bulk writer publishes runtime status before read invalidation closes it" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-cache-publish-before-invalidate", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    const writes = try alloc.alloc(db_mod.types.BatchWrite, auto_bulk_ingest_min_batch_ops);
    defer {
        for (writes) |write| alloc.free(@constCast(write.key));
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:bulk-status:{d}", .{i}),
            .value = "{\"title\":\"bulk status\"}",
        };
    }
    _ = try source.source().batch(alloc, "docs", .{
        .writes = writes,
        .timestamp_ns = 1,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!write_cache.entries.items[0].*.auto_bulk_ingest_session_open);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    source.readPreparation().prepareForRead("docs", .general);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expect(statuses.items[0].stats.index_count > 0);
}

test "provisioned query visibility managed publish hook updates runtime status cache from live writer" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-managed-visibility-publish-status", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, table_catalog.emptyCatalogSource());
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" },
        },
        .sync_level = .full_index,
    });

    const hook = source.managedDerivedVisibilityHook("docs", 7001, &db);
    hook.notify(.publish);

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, statuses.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(statuses.items[0]));
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned query visibility read preparation does not block on same-table batch after early dirty publication" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-provisioned-read-prep-active-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        alloc: std.mem.Allocator,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batch(self.alloc, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
                .timestamp_ns = 1,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    {
        var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default, null, null);
        defer cached.deinit(alloc);
    }

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    var probe = BatchProbe{};
    table_write_sources.test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer table_write_sources.test_before_batch_execution_hook = null;

    var batch_worker = BatchWorker{ .source = &source, .alloc = alloc };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    defer batch_thread.join();

    const ReadWorker = struct {
        source: *ProvisionedTableWriteSource,
        started: std.atomic.Value(bool) = .init(false),
        completed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.started.store(true, .release);
            self.source.readPreparation().prepareForRead("docs", .general);
            self.completed.store(true, .release);
        }
    };

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var read_worker = ReadWorker{ .source = &source };
    const read_thread = try std.Thread.spawn(.{}, ReadWorker.run, .{&read_worker});
    defer read_thread.join();

    while (!read_worker.started.load(.acquire)) std.atomic.spinLoopHint();
    for (0..1000) |_| std.atomic.spinLoopHint();
    try std.testing.expect(read_worker.completed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));

    probe.release.store(true, .release);
    if (batch_worker.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));
}

test "provisioned txn resolve invalidates cached writer state on commit" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-txn-resolve-invalidates-write-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");
    _ = try source.source().txnBeginGroupLocal(alloc, 7001, "docs", txn_id, 10_000, 0, &.{"group:7001"});
    _ = try source.source().txnPrepareGroupLocal(alloc, 7001, "docs", txn_id, 0, .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
    });
    _ = try source.source().txnResolveGroupLocal(alloc, 7001, "docs", txn_id, .committed, 10_001);

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "provisioned table write source routes batch writes across ranges" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-batch";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" },
        },
    });

    left_db.close();
    left_db = try db_mod.DB.open(alloc, left_path, .{});
    right_db.close();
    right_db = try db_mod.DB.open(alloc, right_path, .{});

    var left = (try left_db.lookup(alloc, "doc:a", .{})).?;
    defer left.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, left.json, "\"alpha\"") != null);

    var right = (try right_db.lookup(alloc, "doc:z", .{})).?;
    defer right.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, right.json, "\"zeta\"") != null);
}

test "docid focused provisioned table write source routes same-owner identity rewrites and rejects cross-owner rewrites" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-identity-rewrite-routing";

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Recorder = struct {
        calls: usize = 0,
        group_id: u64 = 0,
        identity_rewrites: usize = 0,

        fn batcher(self: *@This()) RaftBatcher {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch_group = batchGroup,
                    .batch_group_local = batchGroupLocal,
                },
            };
        }

        fn batchGroup(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 0), req.writes.len);
            try std.testing.expectEqual(@as(usize, 0), req.deletes.len);
            try std.testing.expectEqual(@as(usize, 1), req.relational_identity_rewrites.len);
            try std.testing.expectEqualStrings("doc:a", req.relational_identity_rewrites[0].old_key);
            try std.testing.expectEqualStrings("doc:b", req.relational_identity_rewrites[0].new_key);
            self.calls += 1;
            self.group_id = group_id;
            self.identity_rewrites = req.relational_identity_rewrites.len;
        }

        fn batchGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) anyerror!void {
            return error.TestUnexpectedResult;
        }
    };

    var recorder = Recorder{};
    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    defer source.deinit();
    _ = source.withRaftBatcher(recorder.batcher());

    _ = try source.source().batch(alloc, "docs", .{
        .relational_identity_rewrites = &.{.{
            .old_key = "doc:a",
            .new_key = "doc:b",
            .value = "{\"id\":\"doc:b\"}",
        }},
    });
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 7001), recorder.group_id);
    try std.testing.expectEqual(@as(usize, 1), recorder.identity_rewrites);

    try std.testing.expectError(error.UnsupportedOperation, source.source().batch(alloc, "docs", .{
        .relational_identity_rewrites = &.{.{
            .old_key = "doc:a",
            .new_key = "doc:z",
            .value = "{\"id\":\"doc:z\"}",
        }},
    }));
}

const ProvisionedWriteCoalesceTestCatalog = struct {
    fn iface() table_catalog.CatalogSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                .table_id = 7,
                .name = "docs",
                .placement_role = "data",
                .indexes_json = tables_api.default_indexes_json,
            }})[0..]),
            .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                .group_id = 7001,
                .table_id = 7,
                .start_key = "",
                .end_key = null,
            }})[0..]),
            .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
            .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
            .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
            .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const ProvisionedWriteCoalesceProbe = struct {
    calls: std.atomic.Value(u32) = .init(0),
    first_entered: std.atomic.Value(bool) = .init(false),
    release_first: std.atomic.Value(bool) = .init(false),

    fn beforeBatch(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const call = self.calls.fetchAdd(1, .acq_rel) + 1;
        if (call == 1) {
            self.first_entered.store(true, .release);
            while (!self.release_first.load(.acquire)) std.atomic.spinLoopHint();
        }
    }
};

const ProvisionedWriteCoalesceBatchWorker = struct {
    source: *ProvisionedTableWriteSource,
    key: []const u8,
    value: []const u8,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        _ = self.source.source().batch(std.heap.page_allocator, "docs", .{
            .writes = &.{.{ .key = self.key, .value = self.value }},
            .sync_level = .write,
        }) catch |err| {
            self.err = err;
        };
    }
};

test "docid focused provisioned table write source coalesces same-group waiters" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch-coalesce-waiters");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, ProvisionedWriteCoalesceTestCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    var probe = ProvisionedWriteCoalesceProbe{};
    table_write_sources.test_before_batch_execution_hook = .{ .ptr = &probe, .run = ProvisionedWriteCoalesceProbe.beforeBatch };
    defer table_write_sources.test_before_batch_execution_hook = null;

    var first = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:a", .value = "{\"title\":\"alpha\"}" };
    const first_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&first});
    while (!probe.first_entered.load(.acquire)) std.atomic.spinLoopHint();

    var second = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:b", .value = "{\"title\":\"beta\"}" };
    var third = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:c", .value = "{\"title\":\"gamma\"}" };
    const second_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&second});
    const third_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&third});

    try source.testingWaitForWriteCoalesceQueueEntries("docs", 7001, 2);
    probe.release_first.store(true, .release);

    first_thread.join();
    second_thread.join();
    third_thread.join();
    if (first.err) |err| return err;
    if (second.err) |err| return err;
    if (third.err) |err| return err;

    try std.testing.expectEqual(@as(u32, 2), probe.calls.load(.acquire));

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, db_path, 7001, "docs", .default, null, null);
    defer cached.deinit(alloc);
    try drainManagedDbBeforeClose(cached.db);
    var beta = (try cached.db.lookup(alloc, "doc:b", .{})).?;
    defer beta.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, beta.json, "\"beta\"") != null);
    var gamma = (try cached.db.lookup(alloc, "doc:c", .{})).?;
    defer gamma.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, gamma.json, "\"gamma\"") != null);
}

test "docid focused provisioned table write coalescer isolates failed waiters" {
    const alloc = std.testing.allocator;
    const path = try uniqueTestTmpPathAlloc(alloc, "antfly-api-provisioned-batch-coalesce-failure-isolation");
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, ProvisionedWriteCoalesceTestCatalog.iface());
    defer source.deinit();
    source.write_cache = &write_cache;

    _ = try source.source().createTable(alloc, "docs", .{});

    var probe = ProvisionedWriteCoalesceProbe{};
    table_write_sources.test_before_batch_execution_hook = .{ .ptr = &probe, .run = ProvisionedWriteCoalesceProbe.beforeBatch };
    defer table_write_sources.test_before_batch_execution_hook = null;

    var first = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:a", .value = "{\"title\":\"alpha\"}" };
    const first_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&first});
    while (!probe.first_entered.load(.acquire)) std.atomic.spinLoopHint();

    var invalid = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:b", .value = "{\"title\":" };
    var valid = ProvisionedWriteCoalesceBatchWorker{ .source = &source, .key = "doc:c", .value = "{\"title\":\"gamma\"}" };
    const invalid_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&invalid});
    const valid_thread = try std.Thread.spawn(.{}, ProvisionedWriteCoalesceBatchWorker.run, .{&valid});

    try source.testingWaitForWriteCoalesceQueueEntries("docs", 7001, 2);
    probe.release_first.store(true, .release);

    first_thread.join();
    invalid_thread.join();
    valid_thread.join();
    if (first.err) |err| return err;
    try std.testing.expect(invalid.err != null);
    if (valid.err) |err| return err;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, db_path, 7001, "docs", .default, null, null);
    defer cached.deinit(alloc);
    try drainManagedDbBeforeClose(cached.db);
    try std.testing.expect((try cached.db.lookup(alloc, "doc:b", .{})) == null);
    var gamma = (try cached.db.lookup(alloc, "doc:c", .{})).?;
    defer gamma.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, gamma.json, "\"gamma\"") != null);
}

test "provisioned table write source rejects writes that violate enforced document schemas" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-batch-schema";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data", .schema_json = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    try std.testing.expectError(error.InvalidBatchRequest, source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }},
    }));
}

test "provisioned table write source drains managed dense enrichment before close" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-drain";

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    try std.testing.expect(reopened.core.index_manager.denseIndex("semantic_idx").?.index.metadata.active_count > 0);

    const query_vec = [_]f32{ 1, 0, 0 };
    var result = try reopened.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{
            .vector = query_vec[0..],
            .k = 1,
        },
        .limit = 1,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "provisioned table write cache eventually runs managed dense enrichment for write sync" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-write-cache";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            _ = request_count.fetchAdd(1, .monotonic);

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha body"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" }},
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.request_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.request_count.load(.monotonic) > 0);
}

test "provisioned write cache invalidation closes failed managed enrichment db without aborting" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-write-cache-failed-close";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var rate_limited_count: std.atomic.Value(u32) = .init(0);

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const request_index = request_count.fetchAdd(1, .monotonic);
            if (request_index == 0) {
                const body = try std.fmt.allocPrint(
                    arena,
                    "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":[1,0,0]}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                    .{},
                );
                return .{
                    .status = 200,
                    .content_type = try arena.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            _ = rate_limited_count.fetchAdd(1, .monotonic);
            const body = try arena.dupe(u8,
                \\{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}
            );
            return .{
                .status = 429,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.rate_limited_count.store(0, .monotonic);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.write_cache = &write_cache;
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.rate_limited_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }

    try std.testing.expect(FakeEmbeddingProvider.rate_limited_count.load(.monotonic) > 0);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);

    source.invalidateWriteCache("docs");

    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);
}

test "provisioned query visibility table write source invalidates cached query db after managed dense replay becomes visible" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-dense-query-visibility";

    const FakeEmbeddingProvider = struct {
        var request_count: std.atomic.Value(u32) = .init(0);
        var rate_limited_count: std.atomic.Value(u32) = .init(0);
        var allow_all: std.atomic.Value(bool) = .init(false);

        fn vectorForInput(input: std.json.Value) []const u8 {
            if (jsonValueContainsText(input, "alpha")) return "[1,0,0]";
            if (jsonValueContainsText(input, "beta")) return "[0,1,0]";
            return "[0,0,1]";
        }

        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const request_index = request_count.fetchAdd(1, .monotonic);
            if (request_index != 0 and !allow_all.load(.acquire)) {
                _ = rate_limited_count.fetchAdd(1, .monotonic);
                const body = try arena.dupe(u8,
                    \\{"error":{"message":"rate limited","type":"rate_limit_exceeded"}}
                );
                return .{
                    .status = 429,
                    .content_type = try arena.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vectorForInput(parsed_req.value.input)},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }

        fn allowAll() void {
            allow_all.store(true, .release);
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    FakeEmbeddingProvider.request_count.store(0, .monotonic);
    FakeEmbeddingProvider.rate_limited_count.store(0, .monotonic);
    FakeEmbeddingProvider.allow_all.store(false, .monotonic);

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    source.read_cache = &read_cache;
    source.write_cache = &write_cache;

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha body\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta body\"}" },
            .{ .key = "doc:c", .value = "{\"body\":\"gamma body\"}" },
        },
        .sync_level = .write,
    });

    var attempts: usize = 0;
    while (attempts < 100 and FakeEmbeddingProvider.rate_limited_count.load(.monotonic) == 0) : (attempts += 1) {
        sleepNs(50 * std.time.ns_per_ms);
    }
    try std.testing.expect(FakeEmbeddingProvider.rate_limited_count.load(.monotonic) > 0);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    {
        var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        var initial = try read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{
                .vector = &.{ 1.0, 0.0, 0.0 },
                .k = 3,
            },
            .limit = 3,
        });
        defer initial.deinit();
        try std.testing.expect(initial.total_hits < 3);
    }

    FakeEmbeddingProvider.allowAll();

    var ready = false;
    attempts = 0;
    while (attempts < 200) : (attempts += 1) {
        {
            var read_lease = try read_cache.getOrOpen(db_path, FakeCatalog.iface(), 7001, 0, "docs");
            defer read_lease.release();

            var result = try read_lease.db.search(alloc, .{
                .index_name = "semantic_idx",
                .dense = .{
                    .vector = &.{ 1.0, 0.0, 0.0 },
                    .k = 3,
                },
                .limit = 3,
            });
            defer result.deinit();
            if (result.total_hits == 3 and result.hits.len == 3) {
                try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
                ready = true;
                break;
            }
        }

        sleepNs(25 * std.time.ns_per_ms);
    }

    try std.testing.expect(ready);
}

test "provisioned table write source persists chunk artifacts when chunker enables full text indexing" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-managed-chunk-full-text";

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, arena: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));

            var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, arena, req.body);
            defer parsed_req.deinit();

            const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha"))
                "[1,0,0]"
            else
                "[0,0,1]";

            const body = try std.fmt.allocPrint(
                arena,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try arena.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeCatalog = struct {
        var indexes_json_buf: []const u8 = "";
        var table_records = [_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .placement_role = "data",
        }};
        var range_records = [_]metadata_table_manager.RangeRecord{.{
            .group_id = 7001,
            .table_id = 7,
            .start_key = "",
            .end_key = null,
        }};

        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            table_records[0].indexes_json = indexes_json_buf;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = table_records[0..],
                .ranges = range_records[0..],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer listener.deinit();
    try listener.start();
    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    FakeCatalog.indexes_json_buf = try std.fmt.allocPrint(alloc,
        \\{{"semantic_chunked_idx":{{"type":"embeddings","field":"body","dimension":3,"embedder":{{"provider":"openai","model":"test-embed","url":"{s}"}},"chunker":{{"provider":"antfly","model":"fixed-bert-tokenizer","store_chunks":false,"full_text_index":{{}},"text":{{"target_tokens":4,"overlap_tokens":1,"separator":" "}}}}}}}}
    , .{base_uri});
    defer alloc.free(FakeCatalog.indexes_json_buf);

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"Alpha with full text chunks\",\"body\":\"alpha alpha alpha alpha beta beta beta beta beta beta\"}" }},
        .sync_level = .write,
    });

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var reopened = try db_mod.DB.open(alloc, db_path, .{});
    defer reopened.close();

    const chunk_prefix = try db_mod.internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "semantic_chunked_idx_chunks");
    defer alloc.free(chunk_prefix);
    const artifacts = try reopened.core.store.scanPrefix(alloc, chunk_prefix);
    defer db_mod.docstore.DocStore.freeResults(alloc, artifacts);

    var chunk_count: usize = 0;
    for (artifacts) |entry| {
        if (db_mod.internal_keys.isChunkArtifactRecordKey(entry.key)) chunk_count += 1;
    }

    try std.testing.expect(chunk_count >= 2);
}

test "provisioned table write source runtime status is best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    const statuses = try source.source().localRuntimeStatuses(std.testing.allocator, "docs");
    try std.testing.expect(statuses == null);
}

test "provisioned table write source runtime statuses reconcile empty embeddings indexes" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/antfly-api-provisioned-write-runtime-status-managed", .{tmp.sub_path});
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, FakeCatalog.iface());
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;
    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, FakeCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);
    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, cached.db);
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expectEqual(false, statuses.items[0].stats.indexes[0].backfill_active);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status stays cache-only without shared snapshot" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-cache";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;

    lockAtomic(&source.local_db_mutex);
    var cached = try write_cache.getOrOpenLocked(group_path, WarmCatalog.iface(), 7001, 0, "docs");
    source.local_db_mutex.unlock();
    defer cached.deinit(alloc);

    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status prefers shared snapshot cache" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-write-runtime-prefers-snapshot";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const group_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(group_path);
    var db = try db_mod.DB.open(alloc, group_path, .{});
    defer db.close();

    const WarmCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"cached_handle_idx\":{\"type\":\"embeddings\",\"dimension\":3,\"external\":true}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var cached = try write_cache.getOrOpenLocked(path, WarmCatalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "snapshot_idx"),
        .kind = .dense_vector,
        .doc_count = 42,
    };
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 42,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    defer status.deinit(alloc);
    try snapshot_cache.upsertGroupStatus("docs", status);

    var source = ProvisionedTableWriteSource.init(path, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 42), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("snapshot_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source structural mutation invalidates shared runtime status cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    try snapshot_cache.upsertGroupStatus("docs", .{
        .group_id = 7001,
        .stats = .{},
    });
    {
        var before = (try snapshot_cache.snapshot(alloc, "docs")).?;
        defer before.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), before.items.len);
    }

    var source = ProvisionedTableWriteSource.init("/tmp/antfly-runtime-status-structural-invalidation", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginLocalStructuralMutation("docs");
    source.finishLocalStructuralMutation("docs");

    try std.testing.expect((try snapshot_cache.snapshot(alloc, "docs")) == null);
}

test "provisioned table write source runtime status falls back to shared snapshot cache" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
}

test "provisioned table write source cached runtime status does not fetch catalog coverage" {
    const alloc = std.testing.allocator;

    const CountingCatalog = struct {
        calls: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var cached_status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer cached_status.deinit(alloc);
    cached_status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 101,
        .replay_catch_up_required = true,
        .backfill_active = true,
    };
    try snapshot_cache.upsertGroupStatus("docs", cached_status);

    var catalog = CountingCatalog{};
    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-no-catalog-coverage", catalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 25_000), statuses.items[0].stats.doc_count);
    try std.testing.expectEqualStrings("dense_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "provisioned table write source runtime status does not cold-open uncached db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-uncached-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        _ = try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    try std.testing.expect((try source.source().localRuntimeStatuses(alloc, "docs")) == null);
}

test "provisioned table write source runtime status serves cached snapshot during active same-table work" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and request busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-request-busy-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves sibling groups while one group is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    items[1] = .{
        .group_id = 7002,
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[1].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-active-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 7002), statuses.items[1].group_id);
    try std.testing.expectEqual(@as(u64, 3), statuses.items[1].stats.doc_count);
}

test "provisioned table write source runtime status filtering transfers owned statuses" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-status-filter-ownership", NoCatalog.iface());

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    errdefer alloc.free(items);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    errdefer items[0].deinit(alloc);
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    var owned: runtime_status.LocalTableRuntimeStatuses = .{ .items = items };
    defer owned.deinit(alloc);

    var filtered = (try source.takeStatusesWithoutActiveGroups(alloc, &owned, &.{7002})).?;
    defer filtered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), owned.items.len);
    try std.testing.expectEqual(@as(usize, 1), filtered.items.len);
    try std.testing.expectEqual(@as(u64, 7001), filtered.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), filtered.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves unrelated table snapshot while source mutex is busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-unrelated-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves cached snapshot while dirty without source contention" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-unlocked", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status still serves clean sibling table while another table is dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const docs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    docs_items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    docs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const logs_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    logs_items[0] = .{
        .group_id = 8001,
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    logs_items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 4,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 2);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = docs_items },
    };
    snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = logs_items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-dirty-sibling", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "logs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 8001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 4), statuses.items[0].stats.doc_count);
}

test "provisioned table write source runtime status serves shared snapshot cache while clean and busy" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-clean-busy", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
}

test "provisioned table write source serializes same-table same-group operations" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(!source.tryBeginGroupOperation("docs", 7001));
}

test "provisioned table write source structural activity waits for table request lease" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request", NoCatalog.iface());
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
}

test "provisioned table write source keeps structural activity blocked until last table request exits" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-request-refcount", NoCatalog.iface());
    source.beginTableRequest("docs");
    source.beginTableRequest("docs");

    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(!source.tryBeginStructuralTableActivity("docs"));

    source.endTableRequest("docs");
    try std.testing.expect(source.tryBeginStructuralTableActivity("docs"));
    source.endStructuralTableActivity("docs");
}

test "provisioned table write source allows different groups of same table to proceed independently" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-table-activity-different-groups", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.tryBeginGroupOperation("docs", 7002));
    source.endGroupOperation("docs", 7002);
}

test "provisioned table write source reports only same-group activity as busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-group-activity", NoCatalog.iface());
    source.beginGroupOperation("docs", 7001);
    defer source.endGroupOperation("docs", 7001);

    try std.testing.expect(source.hasGroupActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasGroupActivityBestEffort("docs", 7002));
    try std.testing.expect(source.hasReadBlockingActivityBestEffort("docs", 7001));
    try std.testing.expect(!source.hasReadBlockingActivityBestEffort("docs", 7002));
}

test "provisioned table write source managed writer probe is unknown while source mutex is busy" {
    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-managed-writer-state", NoCatalog.iface());
    lockAtomic(&source.local_db_mutex);
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(ProvisionedTableWriteSource.ManagedWriterGroupProbe, .unknown), source.probeManagedWriterGroupBestEffort("docs", 7001));
}

test "provisioned table write source runtime status stays null while dirty and busy during startup catch-up" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .artifact_rebuild,
                    .wal_retention_known = true,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 4,
                    .current_target_sequence = 9,
                    .current_scanned_entries = 12,
                    .current_applied_entries = 4,
                    .progress_updates = 3,
                },
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-snapshot-startup", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");
    source.startup_catch_up_active.store(true, .monotonic);

    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);

    const async_stats = source.asyncIndexingStatsBestEffort();
    try std.testing.expect(async_stats.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, async_stats.startup.phase);
    try std.testing.expectEqual(@as(u64, 99), async_stats.startup.wal_retained_bytes);
    try std.testing.expect(async_stats.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 9), async_stats.dense_catch_up.current_target_sequence);
}

test "provisioned table write source runtime status serves cached snapshot when request is idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-snapshot", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only when dirty and idle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned query visibility table write source runtime status does not inspect read cache hbc stats when dirty" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-hbc", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.read_cache = &read_cache;
    source.runtime_status_cache = &snapshot_cache;
    source.markWriteCacheDirty("docs");

    const read_cache_stats_before = read_cache.cacheStats();
    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    const read_cache_stats_after = read_cache.cacheStats();

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.total_bytes);
    try std.testing.expectEqual(@as(u64, 0), statuses.items[0].stats.indexes[0].hbc_cache.vector.used_bytes);
    try std.testing.expectEqual(read_cache_stats_before.hit_count, read_cache_stats_after.hit_count);
    try std.testing.expectEqual(read_cache_stats_before.miss_count, read_cache_stats_after.miss_count);
}

test "provisioned query visibility table write source read cache overlay preserves live replay status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-read-cache-preserve-replay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        const req: db_mod.types.SearchRequest = .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
        };
        var profiled = try read_lease.db.searchDenseProfiled(alloc, req, req.dense.?);
        defer profiled.result.deinit();
        try std.testing.expect(profiled.result.hits.len >= 1);
    }

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.read_cache = &read_cache;

    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer status.deinit(alloc);
    status.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .replay_applied_sequence = 42,
        .replay_target_sequence = 42,
        .replay_catch_up_required = false,
        .backfill_active = false,
    };

    try source.overlayReadCacheIndexVisibilityBestEffort(alloc, "docs", 7001, &status);

    try std.testing.expectEqual(@as(u64, 1), status.stats.indexes[0].doc_count);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.total_bytes > 0);
    try std.testing.expect(status.stats.indexes[0].hbc_cache.vector.used_bytes > 0);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 42), status.stats.indexes[0].replay_target_sequence);
    try std.testing.expect(!status.stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(!status.stats.indexes[0].backfill_active);
}

test "provisioned table write source restore repair completion retires cached vector read state" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-read-state", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" }},
            .sync_level = .write,
        });
    }

    var hbc_cache = hbc_mod.Cache.init(alloc);
    defer hbc_cache.deinit();

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();
    read_cache.hbc_cache = &hbc_cache;

    {
        var read_lease = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer read_lease.release();

        const query_vec = [_]f32{ 1.0, 2.0 };
        var result = try read_lease.db.search(alloc, .{
            .index_name = "semantic_idx",
            .dense = .{ .vector = query_vec[0..], .k = 1 },
            .limit = 1,
        });
        defer result.deinit();
        try std.testing.expect(result.hits.len >= 1);
    }

    try std.testing.expect(hbc_cache.global_stats.total_bytes > 0);
    const stats_before = read_cache.cacheStats();
    {
        var repair_db = try openManagedDbWithIndexesJsonAndCache(
            alloc,
            path,
            "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
            null,
            &hbc_cache,
            0,
            null,
        );
        defer repair_db.close();
        repair_db.clearDenseHbcCaches();
    }

    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,

        fn onChange(ptr: *anyopaque, _: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
        }
    };
    var hook = Hook{};
    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    defer source.deinit();
    source.read_cache = &read_cache;
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    source.enqueueRestoreRepairComplete("docs");
    source.restore_repair_completion_group.await(source.table_activity_threaded.io()) catch {};

    try std.testing.expectEqual(@as(u64, 0), hbc_cache.global_stats.total_bytes);
    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);

    {
        var reopened = try read_cache.getOrOpen(path, NoCatalog.iface(), 7001, 0, "docs");
        defer reopened.release();
    }
    const stats_after = read_cache.cacheStats();
    try std.testing.expectEqual(stats_before.miss_count + 1, stats_after.miss_count);
}

test "docid focused provisioned restore repair open rejects stale doc identity namespace" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .range_id = 97001,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-repair-stale-docid", .{tmp.sub_path});
    defer alloc.free(path);

    const stale_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 7001, .range_id = 71001 };
    {
        var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = stale_namespace });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"stale\"}" }},
            .sync_level = .write,
        });
    }

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-stale-docid", Catalog.iface());
    defer source.deinit();

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, source.openRestoreRepairDbForGroup(
        alloc,
        path,
        7001,
        "docs",
        "{}",
    ));
}

test "provisioned table write source visibility hook publishes owner db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-visibility-hook", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .full_index,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,
        table_name: []const u8 = "",

        fn onChange(ptr: *anyopaque, table_name: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
            self.table_name = table_name;
        }
    };
    var hook = Hook{};
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, &db, .publish);

    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);

    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, null, .invalidate);

    try std.testing.expectEqual(@as(usize, 2), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
    try std.testing.expectEqualStrings("docs", hook.table_name);
    var retained = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer retained.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), retained.items.len);
    try std.testing.expectEqual(@as(u64, 1), retained.items[0].stats.doc_count);
}

test "provisioned replicated sync marks local runtime status dirty" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Hook = struct {
        calls: usize = 0,
        kind: ?ProvisionedTableWriteSource.LocalChangeKind = null,

        fn onChange(ptr: *anyopaque, _: []const u8, kind: ProvisionedTableWriteSource.LocalChangeKind) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.kind = kind;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/replicated-sync-runtime-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var hook = Hook{};
    source.setLocalChangeHook(.{
        .ptr = &hook,
        .on_change = Hook.onChange,
    });

    _ = try source.applyReplicatedBatchGroupLocal(alloc, 7001, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .full_index,
    });
    try std.testing.expectEqual(@as(usize, 1), hook.calls);

    hook.calls = 0;
    hook.kind = null;
    try source.syncReplicatedBatchGroupLocal(alloc, 7001, "docs", .full_index);

    try std.testing.expectEqual(@as(usize, 1), hook.calls);
    try std.testing.expectEqual(ProvisionedTableWriteSource.LocalChangeKind.data, hook.kind.?);
}

test "docid focused provisioned table write source consistent visibility hook does not block on busy apply lock" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-consistent-hook-busy", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"search_idx\":{\"type\":\"full_text\",\"config\":{}}}",
    );
    defer db.close();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    db.core.lockApplyExclusive();
    ProvisionedTableWriteSource.onManagedDerivedVisibilityChanged(&source, "docs", 7001, &db, .publish_consistent);
    db.core.unlockApplyExclusive();

    const published = try snapshot_cache.snapshot(alloc, "docs");
    try std.testing.expect(published == null);
}

test "docid focused provisioned table write source consistent visibility refreshes stale dense status" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-consistent-dense-refresh", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, table_catalog.emptyCatalogSource());
    source.runtime_status_cache = &snapshot_cache;
    const hook = source.managedDerivedVisibilityHook("docs", 7001, &db);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });
    hook.notify(.publish);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":[0,1]}}" }},
        .sync_level = .full_index,
    });
    hook.notify(.publish_consistent);

    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(@as(usize, 1), published.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.indexes[0].doc_count);
    try std.testing.expectEqual(@as(u64, 2), published.items[0].stats.doc_count);
}

test "provisioned table write source promotes synthetic placeholder when publishing live db status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-promote-synthetic", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var placeholder = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .metadata = .{
            .updated_at_ns = 1,
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    defer placeholder.deinit(alloc);
    placeholder.stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
    };
    try snapshot_cache.upsertGroupStatus("docs", placeholder);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    var db = try openManagedDbWithIndexesJson(
        alloc,
        path,
        "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
    );
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    try publishRuntimeStatusSnapshot(&source, alloc, "docs", 7001, &db);

    var published = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer published.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), published.items.len);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, published.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, published.items[0].metadata.freshness);
    try std.testing.expect(runtime_status.statusHasRuntimeFacts(published.items[0]));
    try std.testing.expectEqual(@as(u64, 1), published.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source publishes replay debt from owner db handle" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-replay-debt-publish", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var db = try openManagedDbWithIndexesJsonAndCacheMode(
        alloc,
        path,
        "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        null,
        null,
        0,
        null,
        .writer_no_replay,
    );
    defer db.close();
    _ = try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
        .sync_level = .write,
    });

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &db));

    var statuses = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("dv_v1", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_target_sequence > statuses.items[0].stats.indexes[0].replay_applied_sequence);
}

test "provisioned runtime status overlays live writer replay target without republishing stats" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-live-target-overlay", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var seeded = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer seeded.close();
        _ = try seeded.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    {
        var initial = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer initial.close();
        try std.testing.expect(source.publishManagedRuntimeStatusBestEffort("docs", 7001, &initial));
    }

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);
    _ = try cached.db.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"embedding\":[2,3]}" }},
        .sync_level = .write,
    });

    var cached_only = (try snapshot_cache.snapshot(alloc, "docs")).?;
    defer cached_only.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), cached_only.items[0].stats.indexes[0].replay_target_sequence);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "provisioned table read source serves profiled dense query without runtime status warmup" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-warmed-read-cache-profiled-query", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
                .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
            },
            .sync_level = .full_index,
        });
    }

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    _ = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;
    write_source.markWriteCacheDirty("docs");

    try std.testing.expect((try write_source.source().localRuntimeStatuses(alloc, "docs")) == null);

    var read_source = table_reads.ProvisionedTableReadSource.init(replica_root_dir, Catalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    read_source.cache = &read_cache;

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try read_source.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.responses.?[0].hits != null);
    try std.testing.expect(parsed.value.responses.?[0].profile != null);
}

test "hosted provisioned table read source serves profiled dense query after external write-sync batch without index-not-found" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7001) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 1 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            _ = node_id;
            return if (group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const ExecutorState = struct {
        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    _ = try write_source.source().batch(alloc, "docs", .{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"semantic_idx\":[1,2]}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"semantic_idx\":[2,1]}}" },
        },
        .sync_level = .write,
    });

    var executor_state = ExecutorState{};
    var hosted = table_reads.HostedProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
        FakeRouter.iface(),
        executor_state.iface(),
    );

    var owned = try query_api.parseQueryRequest(alloc, null, "docs",
        \\{"embeddings":{"semantic_idx":[1.0,2.0]},"indexes":["semantic_idx"],"limit":2,"profile":true}
    );
    defer owned.deinit(alloc);

    var response = (try hosted.source().query(alloc, "docs", owned.req, .read_index)).?;
    defer response.deinit(alloc);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}

test "provisioned table read source survives many external write-sync batches before first profiled dense query" {
    const alloc = std.testing.allocator;
    const total_docs: usize = 50_000;
    const batch_size: usize = 250;
    const dims: usize = 384;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":384}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeStatusSource = struct {
        fn iface() http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return try Catalog.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            Catalog.freeAdminSnapshot(undefined, snapshot);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-profiled-external-write-sync-many", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var read_cache = table_reads.ProvisionedTableReadCache.init(alloc);
    defer read_cache.deinit();

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var write_source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    write_source.read_cache = &read_cache;
    write_source.write_cache = &write_cache;
    write_source.runtime_status_cache = &snapshot_cache;

    const dense_doc_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"_embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1");
        }
        try out.writer.writeAll("]}}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(dense_doc_json);

    const query_json = blk: {
        var out: std.Io.Writer.Allocating = .init(alloc);
        defer out.deinit();
        try out.writer.writeAll("{\"embeddings\":{\"semantic_idx\":[");
        for (0..dims) |i| {
            if (i != 0) try out.writer.writeByte(',');
            try out.writer.writeAll("1.0");
        }
        try out.writer.writeAll("]},\"indexes\":[\"semantic_idx\"],\"limit\":10,\"profile\":true}");
        break :blk try out.toOwnedSlice();
    };
    defer alloc.free(query_json);

    {
        var cold_read_source = table_reads.ProvisionedTableReadSource.init(
            replica_root_dir,
            Catalog.iface(),
            raft_mod.read_gate.noopReadableLeaseRequester(),
        );
        cold_read_source.cache = &read_cache;

        var cold_owned = try query_api.parseQueryRequest(alloc, null, "docs", query_json);
        defer cold_owned.deinit(alloc);

        var cold_response = (try cold_read_source.source().query(alloc, "docs", cold_owned.req, .read_index)).?;
        defer cold_response.deinit(alloc);
    }

    for (0..(total_docs / batch_size)) |batch_idx| {
        const writes = try alloc.alloc(db_mod.types.BatchWrite, batch_size);
        defer {
            for (writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            alloc.free(writes);
        }
        for (writes, 0..) |*write, i| {
            const doc_idx = batch_idx * batch_size + i;
            write.key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx});
            write.value = try alloc.dupe(u8, dense_doc_json);
        }
        _ = try write_source.source().batch(alloc, "docs", .{
            .writes = writes,
            .sync_level = .write,
        });
    }

    var read_source = table_reads.ProvisionedTableReadSource.init(
        replica_root_dir,
        Catalog.iface(),
        raft_mod.read_gate.noopReadableLeaseRequester(),
    );
    read_source.cache = &read_cache;

    var server = http_server.ApiHttpServer.init(
        alloc,
        .{},
        FakeStatusSource.iface(),
        read_source.source(),
        write_source.source(),
    );

    const IndexDetail = struct {
        status: ?struct {
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
            replay_target_sequence: ?u64 = null,
            replay_applied_sequence: ?u64 = null,
            replay_catch_up_required: ?bool = null,
            backfill_active: ?bool = null,
            rebuilding: ?bool = null,
        } = null,
    };

    var ready = false;
    for (0..200) |_| {
        var detail = try server.handlePublicTableGetIndex("docs", "semantic_idx");
        defer detail.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), detail.status);
        var parsed_detail = try std.json.parseFromSlice(IndexDetail, alloc, detail.body, .{ .ignore_unknown_fields = true });
        defer parsed_detail.deinit();
        if (parsed_detail.value.status) |idx| {
            if ((idx.doc_count orelse 0) == total_docs and
                (idx.total_indexed orelse 0) == total_docs and
                (idx.replay_applied_sequence orelse 0) == (idx.replay_target_sequence orelse 0) and
                !(idx.replay_catch_up_required orelse false) and
                !(idx.backfill_active orelse false) and
                !(idx.rebuilding orelse false))
            {
                ready = true;
                break;
            }
        }
        sleepNs(10 * std.time.ns_per_ms);
    }
    try std.testing.expect(ready);

    var response = try server.handlePublicTableQuery("docs", query_json, null);
    defer response.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("responses") != null);
}

test "provisioned table write source runtime status keeps cached snapshot while request is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-leased-request-busy", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    source.beginTableRequest("docs");
    defer source.endTableRequest("docs");

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status remains cache-only while bulk ingest session is active" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-bulk-ingest-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    {
        var db = try openManagedDbWithIndexesJson(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
        );
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    try write_cache.beginBulkIngestLocked("docs");

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.indexes[0].doc_count);
}

test "provisioned table write source runtime status does not lease writer during auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "provisioned query visibility read preparation keeps write cache dirty while auto bulk ingest is active" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/read-prep-auto-bulk-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", platform_time.monotonicNs());
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    source.markWriteCacheDirty("docs");

    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
    source.readPreparation().prepareForRead("docs", .dense_query);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "runtime status request does not finish expired auto bulk ingest" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/runtime-status-auto-bulk-expired", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    var cached = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached.deinit(alloc);

    const now_ns = platform_time.monotonicNs();
    try write_cache.ensureAutoBulkIngestLocked(7001, "docs", now_ns -| auto_bulk_ingest_max_idle_ns);
    write_cache.entries.items[0].auto_bulk_ingest_last_ns = now_ns -| auto_bulk_ingest_max_idle_ns;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{},
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(write_cache.entries.items[0].auto_bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items[0].active_leases);
    try write_cache.finishAutoBulkIngestLocked(7001, "docs");
}

test "runtime status refresh preserves current snapshot on allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const current_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            var current = runtime_status.LocalTableRuntimeStatuses{ .items = current_items };
            defer current.deinit(alloc);
            current.items[0] = .{
                .group_id = 7001,
                .stats = .{ .doc_count = 11 },
            };

            const refresh_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            var refresh = runtime_status.LocalTableRuntimeStatuses{ .items = refresh_items };
            defer refresh.deinit(alloc);
            refresh.items[0] = .{
                .group_id = 7001,
                .stats = .{ .doc_count = 99 },
            };

            ProvisionedTableWriteSource.replaceRuntimeStatusesWithMergedRefresh(alloc, &current, &refresh) catch |err| switch (err) {
                error.OutOfMemory => {
                    try std.testing.expectEqual(@as(usize, 1), current.items.len);
                    try std.testing.expectEqual(@as(u64, 11), current.items[0].stats.doc_count);
                    return;
                },
            };

            try std.testing.expectEqual(@as(usize, 1), current.items.len);
            try std.testing.expectEqual(@as(u64, 99), current.items[0].stats.doc_count);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "provisioned table write source startup snapshot preserves existing group status" {
    const alloc = std.testing.allocator;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();

    const items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1),
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 9,
    };

    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    defer alloc.free(snapshots);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    snapshot_cache.replaceOwned(snapshots);

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-runtime-startup-overlay", NoCatalog.iface());
    source.runtime_status_cache = &snapshot_cache;

    try publishStartupCatchUpRuntimeStatusSnapshot(&source, alloc, "docs", 7001, .{
        .active = true,
        .phase = .opening_db,
        .wal_retention_known = true,
        .wal_retained_segments = 5,
        .wal_retained_bytes = 123,
    }, null, null);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 9), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("semantic_idx", statuses.items[0].stats.indexes[0].name);
    try std.testing.expect(statuses.items[0].stats.async_indexing.startup.active);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.opening_db, statuses.items[0].stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 5), statuses.items[0].stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 123), statuses.items[0].stats.async_indexing.startup.wal_retained_bytes);
}

test "startup async overlay replaces async stats while preserving cached table stats" {
    var status = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7001,
        .stats = .{
            .doc_count = 9,
            .index_count = 1,
            .indexes = &.{},
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retention_known = true,
                    .wal_retained_segments = 5,
                    .wal_retained_bytes = 123,
                },
                .dense_catch_up = .{
                    .active = false,
                    .current_sequence = 1,
                    .current_target_sequence = 2,
                    .current_scanned_entries = 3,
                    .current_applied_entries = 4,
                    .progress_updates = 5,
                },
            },
        },
    };

    applyStartupCatchUpAsyncOverlay(&status, .{
        .startup = .{
            .active = true,
            .phase = .artifact_rebuild,
            .configured_indexes = 2,
            .opened_indexes = 2,
        },
        .dense_catch_up = .{
            .active = true,
            .current_sequence = 10880,
            .current_target_sequence = 1001001,
            .current_scanned_entries = 10880,
            .current_applied_entries = 10880,
            .progress_updates = 91,
        },
    }, .{
        .active = true,
        .phase = .artifact_rebuild,
        .wal_retention_known = true,
        .wal_retained_segments = 7,
        .wal_retained_bytes = 456,
    });

    try std.testing.expectEqual(@as(u64, 9), status.stats.doc_count);
    try std.testing.expectEqual(db_mod.types.StartupCatchUpPhase.artifact_rebuild, status.stats.async_indexing.startup.phase);
    try std.testing.expectEqual(@as(u64, 7), status.stats.async_indexing.startup.wal_retained_segments);
    try std.testing.expectEqual(@as(u64, 456), status.stats.async_indexing.startup.wal_retained_bytes);
    try std.testing.expect(status.stats.async_indexing.dense_catch_up.active);
    try std.testing.expectEqual(@as(u64, 10880), status.stats.async_indexing.dense_catch_up.current_applied_entries);
}

test "provisioned table write source maintenance probes are best effort when local db is busy" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-maintenance-probe", FakeCatalog.iface());
    try std.testing.expect(source.local_db_mutex.tryLock());
    defer source.local_db_mutex.unlock();

    try std.testing.expectEqual(@as(u64, 0), source.lsmMaintenanceScoreBestEffort());
    try std.testing.expect(source.hasActiveBulkIngestSession());
    try std.testing.expect(!try source.runLsmMaintenanceRoundBestEffort());
}

test "docid focused provisioned foreign key action job drains owner range page" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-fk-action-job-owner-range";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = .{
                .table_id = 42,
                .shard_id = 7001,
                .range_id = 7101,
            },
        });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_json, .{});
        try db.batch(.{
            .writes = &.{
                .{ .key = "customer:owner", .value = "{\"_type\":\"customers\"}" },
                .{ .key = "order:owner", .value = "{\"customer_id\":\"customer:owner\",\"status\":\"open\"}" },
            },
            .sync_level = .write,
        });
        _ = try db.repairForeignKeyRefsInRange("", "");
    }

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 42,
                        .name = "orders",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 43,
                        .name = "customers",
                        .placement_role = "data",
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 42,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 42,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 43,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    var result = (try source.source().foreignKeyActionJobPage(
        alloc,
        "orders",
        "fk-action:set-null:owner-range",
        "set_null",
        "worker:fk-action",
        "orders_customer_id_fkey",
        "customers",
        "customer:owner",
        null,
        1,
        60_000,
    )) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    try std.testing.expect(result.complete);
    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(u64, 7001), result.groups[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), result.groups[0].applied_children);
    try std.testing.expectEqualStrings("complete", result.groups[0].status);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer reopened.close();
    const child = (try reopened.get(alloc, "order:owner")) orelse return error.TestUnexpectedResult;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", child);
}

test "docid focused provisioned same-table foreign key action job routes runtime parent through catalog owner range" {
    const alloc = std.testing.allocator;
    const replica_root_dir = "/tmp/antfly-api-fk-action-job-same-table-owner-range";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root_dir) catch {};

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"manager_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"row_manager_id_fkey","columns":["manager_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ;

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(db_path);
    {
        var db = try db_mod.DB.open(alloc, db_path, .{
            .identity_namespace = .{
                .table_id = 42,
                .shard_id = 7001,
                .range_id = 7101,
            },
        });
        defer db.close();
        try db.applyTableSchemaJson(alloc, schema_json, .{});
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:manager", .value = "{\"status\":\"lead\"}" },
                .{ .key = "doc:report", .value = "{\"manager_id\":\"doc:manager\",\"status\":\"open\"}" },
            },
            .sync_level = .write,
        });
        _ = try db.repairForeignKeyRefsInRange("", "");
    }

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 42,
                    .name = "docs",
                    .placement_role = "data",
                    .schema_json = schema_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .range_id = 7101,
                    .table_id = 42,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 42,
                    .constraint_name = "row_manager_id_fkey",
                    .parent_table_id = 42,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 7001,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    defer source.deinit();
    var result = (try source.source().foreignKeyActionJobPage(
        alloc,
        "docs",
        "fk-action:set-null:same-table-owner-range",
        "set_null",
        "worker:fk-action",
        "row_manager_id_fkey",
        "row",
        "doc:manager",
        null,
        1,
        60_000,
    )) orelse return error.TestUnexpectedResult;
    defer result.deinit(alloc);
    try std.testing.expect(result.complete);
    try std.testing.expectEqual(@as(usize, 1), result.groups.len);
    try std.testing.expectEqual(@as(u64, 7001), result.groups[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), result.groups[0].applied_children);
    try std.testing.expectEqualStrings("complete", result.groups[0].status);

    var reopened = try db_mod.DB.open(alloc, db_path, .{ .start_index_workers = false });
    defer reopened.close();
    const child = (try reopened.get(alloc, "doc:report")) orelse return error.TestUnexpectedResult;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", child);
}

test "managed startup catch-up uses provided indexes json without catalog fetch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-provided-indexes", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);
    const indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}";

    {
        var db = try openManagedDbWithIndexesJsonAndCacheMode(alloc, path, indexes_json, null, null, backend_current_root_generation, null, .default);
        defer db.close();
    }

    const CountingCatalog = struct {
        calls: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = CountingCatalog{};
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, catalog.iface());
    source.runtime_status_cache = &snapshot_cache;
    const result = try source.catchUpTableGroupBestEffortWithIndexesJson(alloc, 7001, "docs", indexes_json);

    try std.testing.expectEqual(@as(usize, 0), catalog.calls);
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(!statuses.items[0].stats.async_indexing.startup.active);
}

test "managed startup catch-up bypasses shared write cache" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

test "managed startup catch-up invalidates stale cached writer status after replay clears debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-invalidates-stale-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    {
        var seeded = try openManagedDbWithIndexesJsonAndCacheMode(
            alloc,
            path,
            "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
            null,
            null,
            0,
            null,
            .writer_no_replay,
        );
        defer seeded.close();
        _ = try seeded.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"embedding\":[1,2]}" }},
            .sync_level = .write,
        });
    }

    var db = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    const before = try db.db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, before);
    try std.testing.expectEqual(@as(usize, 1), before.indexes.len);
    try std.testing.expect(before.indexes[0].replay_catch_up_required);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(result.had_debt);
    try std.testing.expect(result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), write_cache.entries.items.len);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expectEqual(@as(u64, statuses.items[0].stats.indexes[0].replay_target_sequence), statuses.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expect(!statuses.items[0].stats.indexes[0].replay_catch_up_required);
}

test "managed startup catch-up defers while shared writer cache owns the table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-shared-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    var cached = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached.deinit(alloc);

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "managed startup catch-up defers while foreground writer state is dirty" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-defers-dirty-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    source.markWriteCacheDirty("docs");

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(result.busy);
    try std.testing.expect(!result.had_debt);
    try std.testing.expect(!result.cleared_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
}

test "provisioned table write source group batch does not hold local db mutex during db batch" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-group-batch-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const BatchProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeBatch(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var probe = BatchProbe{};
    table_write_sources.test_before_batch_execution_hook = .{
        .ptr = &probe,
        .run = BatchProbe.beforeBatch,
    };
    defer table_write_sources.test_before_batch_execution_hook = null;

    var worker = BatchWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    var result = (try db.lookup(alloc, "doc:a", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"alpha\"") != null);
}

test "docid focused provisioned table write source drop table does not hold local db mutex during background delete" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DropProbe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn beforeDeleteTree(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().createDirPath(io_impl.io(), path);
    const marker_path = try std.fmt.allocPrint(alloc, "{s}/marker.txt", .{path});
    defer alloc.free(marker_path);
    var marker = try std.Io.Dir.cwd().createFile(io_impl.io(), marker_path, .{});
    marker.close(io_impl.io());

    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();

    var source = ProvisionedTableWriteSource.init(replica_root_dir, NoCatalog.iface());
    source.backend_runtime = runtime.ptr();
    var probe = DropProbe{};
    table_write_sources.test_before_drop_table_delete_hook = .{
        .ptr = &probe,
        .run = DropProbe.beforeDeleteTree,
    };
    defer table_write_sources.test_before_drop_table_delete_hook = null;

    var worker = DropWorker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, DropWorker.run, .{&worker});
    thread.join();

    if (worker.err) |err| return err;

    while (!probe.entered.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    source.drainDroppedTableDeletes();

    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), path, .{}));
}

test "docid focused provisioned table write source drop table waits for in-flight group batch on same table" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-table-waits-for-batch", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const BatchWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().batchGroupLocal(std.heap.page_allocator, 7001, "docs", .{
                .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            }) catch |err| {
                self.err = err;
            };
        }
    };

    const DropWorker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropTable(std.heap.page_allocator, "docs", &.{7001}) catch |err| {
                self.err = err;
            };
        }
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var batch_probe = Probe{};
    var drop_probe = Probe{};
    table_write_sources.test_before_batch_execution_hook = .{ .ptr = &batch_probe, .run = Probe.run };
    defer table_write_sources.test_before_batch_execution_hook = null;
    table_write_sources.test_before_drop_table_delete_hook = .{ .ptr = &drop_probe, .run = Probe.run };
    defer table_write_sources.test_before_drop_table_delete_hook = null;

    var batch_worker = BatchWorker{ .source = &source };
    const batch_thread = try std.Thread.spawn(.{}, BatchWorker.run, .{&batch_worker});
    while (!batch_probe.entered.load(.acquire)) std.atomic.spinLoopHint();

    var drop_worker = DropWorker{ .source = &source };
    const drop_thread = try std.Thread.spawn(.{}, DropWorker.run, .{&drop_worker});

    sleepNs(10 * std.time.ns_per_ms);
    try std.testing.expect(!drop_probe.entered.load(.acquire));

    batch_probe.release.store(true, .release);
    batch_thread.join();
    if (batch_worker.err) |err| return err;

    while (!drop_probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    drop_probe.release.store(true, .release);
    drop_thread.join();
    if (drop_worker.err) |err| return err;
}

test "provisioned table write source drop index does not hold local db mutex during index deletion work" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/provisioned-drop-index-mutex", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = tables_api.default_indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().dropIndex(std.heap.page_allocator, "docs", "full_text_index_v0") catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    var probe = Probe{};
    table_write_sources.test_before_drop_index_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer table_write_sources.test_before_drop_index_work_hook = null;

    var worker = Worker{ .source = &source };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    var reopened = try db_mod.DB.open(alloc, path, .{});
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.textIndex("full_text_index_v0") == null);
}

test "provisioned table write source create table provisions local indexes and schema" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-create-schema";
    const schema_json =
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = schema_json,
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    var req = tables_api.CreateTableRequest{
        .schema_json = try alloc.dupe(u8, schema_json),
    };
    defer req.deinit(alloc);

    _ = try source.source().createTable(alloc, "docs", req);

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    try std.testing.expect(db.core.index_manager.textIndex("full_text_index_v0") != null);
    try std.testing.expect(db.core.schema != null);
}

test "provisioned table write source restore table does not hold local db mutex during restore work" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-provisioned-table-restore-mutex";
    const backup_root = "/tmp/antfly-api-provisioned-table-restore-mutex-out";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    defer {
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};
    }

    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(db_path);
    var db = try db_mod.DB.open(alloc, db_path, .{});
    defer db.close();

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "",
                    .read_schema_json = "",
                    .indexes_json = tables_api.default_indexes_json,
                    .replication_sources_json = "[]",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Probe = struct {
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.atomic.spinLoopHint();
        }
    };

    const Worker = struct {
        source: *ProvisionedTableWriteSource,
        manifest: *const backups_api.TableBackupManifest,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.source.source().restoreTable(std.heap.page_allocator, "docs", .{
                .backup_root = backup_root,
                .manifest = self.manifest,
            }) catch |err| {
                self.err = err;
            };
        }
    };

    var source = ProvisionedTableWriteSource.init(path, Catalog.iface());
    _ = try source.source().createTable(alloc, "docs", .{});
    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    const shards = (try source.source().backupTable(alloc, "docs", .{
        .backup_root = backup_root,
        .backup_id = "snap1",
    })).?;
    defer freeBackupShards(alloc, shards);

    var manifest = try backups_api.createManifest(alloc, "snap1", &.{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = tables_api.default_indexes_json,
        .replication_sources_json = "[]",
    }, shards);
    defer manifest.deinit(alloc);

    _ = try source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });

    var probe = Probe{};
    table_write_sources.test_before_restore_work_hook = .{
        .ptr = &probe,
        .run = Probe.run,
    };
    defer table_write_sources.test_before_restore_work_hook = null;

    var worker = Worker{
        .source = &source,
        .manifest = &manifest,
    };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});

    while (!probe.entered.load(.acquire)) std.atomic.spinLoopHint();
    try std.testing.expect(source.local_db_mutex.tryLock());
    source.local_db_mutex.unlock();

    probe.release.store(true, .release);
    thread.join();

    if (worker.err) |err| return err;

    db.close();
    db = try db_mod.DB.open(alloc, db_path, .{});
    var restored = (try db.lookup(alloc, "doc:a", .{})).?;
    defer restored.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, restored.json, "\"alpha\"") != null);
}

test "provisioned query visibility table write source deinit drains restore repair work group" {
    if (builtin.os.tag == .freestanding) return;

    const NoCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogFetch;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const DrainCtx = struct {
        started: std.atomic.Value(u32) = .init(0),
        finished: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This()) !void {
            _ = self.started.fetchAdd(1, .release);
            sleepNs(20 * std.time.ns_per_ms);
            _ = self.finished.fetchAdd(1, .release);
        }
    };

    var source = ProvisionedTableWriteSource.init("/tmp/unused-antfly-restore-repair-drain", NoCatalog.iface());

    var ctx = DrainCtx{};
    try source.restore_repair_work_group.concurrent(source.table_activity_threaded.io(), DrainCtx.run, .{&ctx});

    while (ctx.started.load(.acquire) == 0) std.Thread.yield() catch {};
    source.deinit();

    try std.testing.expectEqual(@as(u32, 1), ctx.started.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.finished.load(.acquire));
}

test "managed startup catch-up repairs external dense doc gaps from stored artifacts without replay debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-dense-artifacts", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[{\"name\":\"dense_idx\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}},{\"name\":\"ft_v1\",\"type\":\"full_text\",\"config\":{}}]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();

        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0.9,0.1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{path});
    defer alloc.free(dense_index_path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), dense_index_path);

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var snapshot_cache = runtime_status.TableRuntimeSnapshotCache.init(alloc);
    defer snapshot_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.runtime_status_cache = &snapshot_cache;

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(result.had_debt);
    try std.testing.expect(result.cleared_debt);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);

    var dense_doc_count: ?u64 = null;
    for (statuses.items[0].stats.indexes) |index| {
        if (!std.mem.eql(u8, index.name, "dense_idx")) continue;
        dense_doc_count = index.doc_count;
    }
    try std.testing.expectEqual(@as(?u64, 3), dense_doc_count);
}

test "managed startup catch-up defers while shared bulk ingest state is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-bulk", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;

    try write_cache.beginBulkIngestLocked("docs");
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer cached_seed.deinit(alloc);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    const hits_before = write_cache.hit_count.load(.monotonic);

    const first = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(first.busy);
    try std.testing.expect(!first.had_debt);
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    const startup_hits_before = startup_write_cache.hit_count.load(.monotonic);

    const second = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(second.busy);
    try std.testing.expect(!second.had_debt);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].bulk_ingest_session_open);
    try std.testing.expectEqual(@as(usize, 1), write_cache.active_bulk_ingest_sessions.items.len);
    try std.testing.expectEqual(hits_before, write_cache.hit_count.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), startup_write_cache.entries.items.len);
    try std.testing.expectEqual(startup_hits_before, startup_write_cache.hit_count.load(.monotonic));
}

test "provisioned query visibility managed startup catch-up ignores stale dirty bit after writer cache entry is gone" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-startup-catch-up-stale-dirty", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
    }

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var startup_write_cache = ProvisionedTableWriteCache.init(alloc);
    defer startup_write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    source.startup_write_cache = &startup_write_cache;
    source.markWriteCacheDirty("docs");
    try std.testing.expect(source.isWriteCacheDirtyForTable("docs"));

    const result = try source.catchUpTableGroupBestEffort(alloc, 7001, "docs");
    try std.testing.expect(!result.busy);
    try std.testing.expect(!source.isWriteCacheDirtyForTable("docs"));
}

test "managed source status-only open bypasses shared writer cache entry" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/managed-source-status-only", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;

    var seeded = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, 0, "docs");
    defer seeded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);

    var status_only = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .status_only, null, null);
    defer status_only.deinit(alloc);

    try std.testing.expect(status_only.owned_db != null);
    try std.testing.expect(status_only.entry == null);
    try std.testing.expect(!status_only.db.start_index_workers);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].db.start_index_workers);
}

fn testingVisibleRootGenerationSource(value: *u64) table_reads.GroupVisibleRootGenerationSource {
    return .{
        .ptr = value,
        .visible_root_generation_for_group = testingVisibleRootGenerationForGroup,
    };
}

fn testingVisibleRootGenerationForGroup(ptr: *anyopaque, _: u64) u64 {
    return (@as(*u64, @ptrCast(@alignCast(ptr)))).*;
}

test "write cache prunes stale visible root generations instead of clearing current entries" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-generations", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var cached_first = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_first.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), write_cache.entries.items[0].lsm_root_generation);

    generation = 2;
    var cached_second = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_second.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);

    var statuses = (try source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 7001), statuses.items[0].group_id);
}

test "hosted write cache opens current visible root generation" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/hosted-write-cache-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 7001);
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Router = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (group_id == 7001) .active else .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 1 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            _ = node_id;
            return if (group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const Executor = struct {
        fn iface() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.UnexpectedHttpRequest;
        }
    };

    var generation: u64 = 1;
    var source = HostedProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface(), Router.iface(), Executor.iface());
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));
    defer source.invalidateManagedCache("docs");

    const hosted_cache = try hostedManagedDbCacheForRoot(replica_root_dir);
    var cached_first = try source.getOrOpenCachedDbMode(hosted_cache, path, 7001, "docs", .default_async);
    cached_first.deinit(hosted_cache.write_cache.alloc);
    try std.testing.expectEqual(@as(usize, 1), hosted_cache.write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), hosted_cache.write_cache.entries.items[0].lsm_root_generation);

    generation = 2;
    var cached_second = try source.getOrOpenCachedDbMode(hosted_cache, path, 7001, "docs", .default_async);
    cached_second.deinit(hosted_cache.write_cache.alloc);
    try std.testing.expectEqual(@as(usize, 1), hosted_cache.write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), hosted_cache.write_cache.entries.items[0].lsm_root_generation);
}

test "write cache adopts just-created db across reconcile generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-created-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var cached_seed = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    cached_seed.deinit(alloc);
    write_cache.entries.items[0].allow_generation_adoption = true;
    try std.testing.expectEqual(@as(u64, 1), write_cache.entries.items[0].lsm_root_generation);
    const misses_before = write_cache.miss_count.load(.monotonic);

    generation = 2;
    source.pruneStaleWriteCacheLocked();

    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);
    try std.testing.expect(!write_cache.entries.items[0].allow_generation_adoption);

    var cached_after_bump = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_after_bump.deinit(alloc);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));
    try std.testing.expect(write_cache.hit_count.load(.monotonic) > 0);
}

test "docid focused primary lookup adopts seeded write cache across visible generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/primary-lookup-write-cache-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    var seeded = try write_cache.getOrOpenLocked(path, Catalog.iface(), 7001, generation, "docs");
    try seeded.db.batch(.{
        .writes = &.{.{ .key = "doc:gold", .value = "{\"title\":\"gold doc\"}" }},
        .sync_level = .write,
    });
    seeded.deinit(alloc);

    const primary_lookup = source.primaryLookupDbSource();
    generation = 2;
    try std.testing.expect((try primary_lookup.leaseGroup(alloc, "docs", 7001, generation)) == null);

    write_cache.entries.items[0].allow_generation_adoption = true;
    const misses_before = write_cache.miss_count.load(.monotonic);

    var lease = (try primary_lookup.leaseGroup(alloc, "docs", 7001, generation)).?;
    defer lease.release(alloc);

    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);
    try std.testing.expect(!write_cache.entries.items[0].allow_generation_adoption);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));

    var result = (try lease.db.lookup(alloc, "doc:gold", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"gold doc\"") != null);
}

test "replica root reconcile seeds write cache across generation bump" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/write-cache-reconcile-generation", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-7001/table-db", .{replica_root_dir});
    defer alloc.free(path);

    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"indexes\":[]}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const tables = [_]metadata_table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
        .indexes_json = "{\"indexes\":[]}",
    }};
    const ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 7001,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const hosted_groups = [_]u64{7001};

    var generation: u64 = 1;
    var write_cache = ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
    var source = ProvisionedTableWriteSource.init(replica_root_dir, Catalog.iface());
    source.write_cache = &write_cache;
    _ = source.withGroupVisibleRootGeneration(testingVisibleRootGenerationSource(&generation));

    const summary = try source.reconcileReplicaRootTablesWithWriteCacheLocked(
        alloc,
        1,
        &hosted_groups,
        &tables,
        &ranges,
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), summary.groups_considered);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expect(write_cache.entries.items[0].allow_generation_adoption);
    const misses_before = write_cache.miss_count.load(.monotonic);

    generation = 2;
    source.pruneStaleWriteCacheLocked();
    try std.testing.expectEqual(@as(usize, 1), write_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 2), write_cache.entries.items[0].lsm_root_generation);

    var cached_after_bump = try source.getOrOpenCachedDbMode(alloc, &write_cache, path, 7001, "docs", .default_async, null, null);
    cached_after_bump.deinit(alloc);
    try std.testing.expectEqual(misses_before, write_cache.miss_count.load(.monotonic));
}
