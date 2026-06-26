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

pub const BoundTableWriteSource = table_write_sources.BoundTableWriteSource;
pub const ProvisionedTableWriteSource = table_write_sources.ProvisionedTableWriteSource;
pub const HostedProvisionedTableWriteSource = table_write_sources.HostedProvisionedTableWriteSource;

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
