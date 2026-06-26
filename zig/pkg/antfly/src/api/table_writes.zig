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

fn uniqueTestTmpPathAlloc(alloc: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "/tmp/{s}-{d}", .{ prefix, platform_time.monotonicNs() });
}

test "api.table_writes.query_visibility table write source invalidates cached query db after managed dense replay becomes visible" {
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

test "api.table_writes.query_visibility table write source runtime status does not inspect read cache hbc stats when dirty" {
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

test "api.table_writes.query_visibility table write source read cache overlay preserves live replay status" {
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

test "api.table_writes.docid provisioned restore repair open rejects stale doc identity namespace" {
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

test "api.table_writes.docid provisioned table write source consistent visibility hook does not block on busy apply lock" {
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

test "api.table_writes.docid provisioned table write source consistent visibility refreshes stale dense status" {
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

test "api.table_writes.query_visibility read preparation keeps write cache dirty while auto bulk ingest is active" {
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

test "api.table_writes.docid provisioned foreign key action job drains owner range page" {
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

test "api.table_writes.docid provisioned same-table foreign key action job routes runtime parent through catalog owner range" {
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

test "api.table_writes.docid provisioned table write source drop table does not hold local db mutex during background delete" {
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

test "api.table_writes.docid provisioned table write source drop table waits for in-flight group batch on same table" {
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

test "api.table_writes.query_visibility table write source deinit drains restore repair work group" {
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

test "api.table_writes.query_visibility managed startup catch-up ignores stale dirty bit after writer cache entry is gone" {
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

test "api.table_writes.docid primary lookup adopts seeded write cache across visible generation bump" {
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
