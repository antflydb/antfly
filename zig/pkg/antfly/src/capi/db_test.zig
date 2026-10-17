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

//! Integration coverage for public handles and private server owners.
const public = @import("antfly_local_sources").capi_db;
const server_api = @import("server_owner.zig");
const shared = @import("antfly_local_sources").capi_handles;
const antfly = server_api.antfly;
const storage_root = server_api.storage_root;
const replication_ingress = server_api.replication_ingress;
const std = shared.std;
const builtin = shared.builtin;
const local_write = shared.local_write;
const raft_engine = server_api.raft_engine;
const TestDirectory = public.TestDirectory;
const TestDirectoryType = public.TestDirectoryType;
const vector_mod = public.vector_mod;
const capi = shared.capi;
const ApiTypes = public.ApiTypes;
const search_wire = public.search_wire;
const kernel_owner_abi = shared.kernel_owner_abi;
const kernel_error_identity = server_api.kernel_error_identity;
const local_query_client = shared.local_query_client;
const capi_build_options = shared.capi_build_options;
const kernel_wal_owner = server_api.kernel_wal_owner;
const storageWalOpen = server_api.storageWalOpen;
const storageWalClose = server_api.storageWalClose;
const storageWalAppend = server_api.storageWalAppend;
const storageWalAppendIdempotent = server_api.storageWalAppendIdempotent;
const storageWalSync = server_api.storageWalSync;
const storageWalTruncatePrefix = server_api.storageWalTruncatePrefix;
const storageWalTruncateSuffix = server_api.storageWalTruncateSuffix;
const storageWalIterate = server_api.storageWalIterate;
const storageWalRead = server_api.storageWalRead;
const storageWalStatsSnapshot = server_api.storageWalStatsSnapshot;
const storageWalLastLsn = server_api.storageWalLastLsn;
const db_mod = shared.db_mod;
const backend_types = server_api.backend_types;
const raft_mod = server_api.raft_mod;
const read_consistency = shared.read_consistency;
const hbc = public.hbc;
const graph_mod = public.graph_mod;
const traversal_mod = public.traversal_mod;
const paths_mod = public.paths_mod;
const graph_query_mod = public.graph_query_mod;
const graph_pattern_mod = public.graph_pattern_mod;
const hot_standby_seed_activation = server_api.hot_standby_seed_activation;
const transactions_mod = shared.transactions_mod;
const aggregations_mod = shared.aggregations_mod;
const aggregations_contract = server_api.aggregations_contract;
const search_agg_mod = shared.search_agg_mod;
const geo_mod = shared.geo_mod;
const lite_backend = shared.lite_backend;
const lite_restore_staging = public.lite_restore_staging;
const portable_backup = public.portable_backup;
const batch_api = shared.batch_api;
const query_api = shared.query_api;
const tables_api = shared.tables_api;
const table_reads_api = shared.table_reads_api;
const runtime_status = server_api.runtime_status;
const shard_state_store = server_api.shard_state_store;
const data_raft_apply = server_api.data_raft_apply;
const metadata_raft_apply = server_api.metadata_raft_apply;
const metadata_table_manager = server_api.metadata_table_manager;
const metadata_table_provisioner = server_api.metadata_table_provisioner;
const data_raft_projection_wire = server_api.data_raft_projection_wire;
const backups_api = server_api.backups_api;
const backup_restore = server_api.backup_restore;
const common_config = server_api.common_config;
const common_secrets = server_api.common_secrets;
const scraping = server_api.scraping;
const inference_provider = shared.inference_provider;
const managed_embedder = shared.managed_embedder;
const raft_catalog = server_api.raft_catalog;
const indexes_api = public.indexes_api;
const Allocator = shared.Allocator;
const abi_version = shared.abi_version;
const kernel_runtime_services = server_api.kernel_runtime_services;
const StorageOwnerContext = server_api.StorageOwnerContext;
const SystemStoreHandle = server_api.SystemStoreHandle;
const SystemReadTxnHandle = server_api.SystemReadTxnHandle;
const SystemCurrentScanTxnHandle = server_api.SystemCurrentScanTxnHandle;
const SystemWriteTxnHandle = server_api.SystemWriteTxnHandle;
const SystemCursorHandle = server_api.SystemCursorHandle;
const DataApplyStoreHandle = server_api.DataApplyStoreHandle;
const MetadataApplyStoreHandle = server_api.MetadataApplyStoreHandle;
const MetadataPreparedSnapshotHandle = server_api.MetadataPreparedSnapshotHandle;
const MetadataListenerBridge = server_api.MetadataListenerBridge;
const DataApplyGroupTransitionHandle = server_api.DataApplyGroupTransitionHandle;
const DataApplyPreparedSnapshotHandle = server_api.DataApplyPreparedSnapshotHandle;
const StorageOwnerTransactionRecovery = server_api.StorageOwnerTransactionRecovery;
const StorageOwnerRuntimeHooks = server_api.StorageOwnerRuntimeHooks;
const monotonicNowNs = public.monotonicNowNs;
const Handle = shared.Handle;
const StorageSnapshot = server_api.StorageSnapshot;
const startLiteEmbeddedInference = public.startLiteEmbeddedInference;
const stopLiteEmbeddedInference = shared.stopLiteEmbeddedInference;
const liteManagedEmbeddingIndexConfigJson = public.liteManagedEmbeddingIndexConfigJson;
const registerLiteIndexEnrichments = public.registerLiteIndexEnrichments;
const LiteCatalogRollback = public.LiteCatalogRollback;
const registerLiteIndexResolvers = public.registerLiteIndexResolvers;
const needsDefaultEmbeddingField = public.needsDefaultEmbeddingField;
const litePhysicalIndexConfigJson = public.litePhysicalIndexConfigJson;
const liteMergedIndexesJsonAlloc = public.liteMergedIndexesJsonAlloc;
const liteEnrichmentCatalogEntryJsonAlloc = public.liteEnrichmentCatalogEntryJsonAlloc;
const refreshLiteManagedEmbeddingRuntime = public.refreshLiteManagedEmbeddingRuntime;
const closeHandle = shared.closeHandle;
const liteOpenModeCanWrite = shared.liteOpenModeCanWrite;
const currentIdentityReadGenerationForHandle = shared.currentIdentityReadGenerationForHandle;
const stampSearchRequestIdentityGeneration = shared.stampSearchRequestIdentityGeneration;
const stamped_generation_optimistic_attempts = public.stamped_generation_optimistic_attempts;
const runAtStampedGeneration = public.runAtStampedGeneration;
const StampedGenerationOptions = public.StampedGenerationOptions;
const runAtStampedGenerationWithOptions = public.runAtStampedGenerationWithOptions;
const LocalSearchQuery = public.LocalSearchQuery;
const executeLocalSearch = public.executeLocalSearch;
const cancellationTokenRequested = public.cancellationTokenRequested;
const ReadableLeaseHookFn = shared.ReadableLeaseHookFn;
const ReadableLeaseHook = shared.ReadableLeaseHook;
const localInferenceRuntimeAvailable = public.localInferenceRuntimeAvailable;
const asHandle = shared.asHandle;
const HandleRegistryOf = shared.HandleRegistryOf;
const HandleRegistry = shared.HandleRegistry;
const handle_registry = &shared.handle_registry;
const publishHandle = public.publishHandle;
const closeHandleId = shared.closeHandleId;
const HandleAccess = public.HandleAccess;
const HandleGuard = public.HandleGuard;
const handleLockIo = shared.handleLockIo;
const enterHandle = public.enterHandle;
const beginWithIdAndParticipants = public.beginWithIdAndParticipants;
const writeIntentsInternal = public.writeIntentsInternal;
const batchInternal = public.batchInternal;
const dupBytes = shared.dupBytes;
const stringifyJson = public.stringifyJson;
const dupBase64 = public.dupBase64;
const decodeBase64Alloc = public.decodeBase64Alloc;
const parseEnrichmentKind = public.parseEnrichmentKind;
const graphFreeEdges = public.graphFreeEdges;
const traversalFreeResults = public.traversalFreeResults;
const JsonRange = public.JsonRange;
const JsonSplitState = public.JsonSplitState;
const JsonSplitDeltaWrite = public.JsonSplitDeltaWrite;
const JsonSplitDeltaEntry = public.JsonSplitDeltaEntry;
const JsonIndexConfig = public.JsonIndexConfig;
const JsonScanHash = public.JsonScanHash;
const JsonScanDocument = public.JsonScanDocument;
const JsonScanResult = public.JsonScanResult;
const JsonDBStats = public.JsonDBStats;
const JsonDBIndexStats = public.JsonDBIndexStats;
const JsonEnrichmentStats = public.JsonEnrichmentStats;
const JsonTTLCleanupStats = public.JsonTTLCleanupStats;
const JsonTransactionRecoveryStats = public.JsonTransactionRecoveryStats;
const JsonTextMergeStats = public.JsonTextMergeStats;
const JsonChunkHit = public.JsonChunkHit;
const JsonSearchHit = public.JsonSearchHit;
const JsonSearchResult = public.JsonSearchResult;
const JsonAggregateHitsRequest = public.JsonAggregateHitsRequest;
const JsonGraphNodeSelectorRequest = public.JsonGraphNodeSelectorRequest;
const JsonGraphQueryRequest = public.JsonGraphQueryRequest;
const JsonNamedGraphInputSetRequest = public.JsonNamedGraphInputSetRequest;
const JsonGraphSearchResult = public.JsonGraphSearchResult;
const JsonSearchAggregationRequest = shared.JsonSearchAggregationRequest;
const JsonNumericRangeRequest = shared.JsonNumericRangeRequest;
const JsonDateRangeRequest = shared.JsonDateRangeRequest;
const JsonDistanceRangeRequest = shared.JsonDistanceRangeRequest;
const JsonSearchAggregationBucket = shared.JsonSearchAggregationBucket;
const JsonSearchAggregationResult = shared.JsonSearchAggregationResult;
const toAggregationRequest = public.toAggregationRequest;
const freeAggregationRequests = shared.freeAggregationRequests;
const toJsonAggregationResults = public.toJsonAggregationResults;
const artifactKindLabel = public.artifactKindLabel;
const JsonArtifactSourceRef = public.JsonArtifactSourceRef;
const JsonArtifactRef = public.JsonArtifactRef;
const JsonArtifactWrite = public.JsonArtifactWrite;
const JsonDenseEnrichmentWrite = public.JsonDenseEnrichmentWrite;
const JsonSparseEnrichmentWrite = public.JsonSparseEnrichmentWrite;
const JsonGraphWrite = public.JsonGraphWrite;
const JsonDocumentEnrichmentWrite = public.JsonDocumentEnrichmentWrite;
const JsonExtractEnrichmentsResult = public.JsonExtractEnrichmentsResult;
const JsonComputeEnrichmentsResult = public.JsonComputeEnrichmentsResult;
const buildJsonExtractEnrichmentsResult = public.buildJsonExtractEnrichmentsResult;
const buildJsonComputeEnrichmentsResult = public.buildJsonComputeEnrichmentsResult;
const freeOwnedBatchWrites = public.freeOwnedBatchWrites;
const decodeBatchWritesRequest = public.decodeBatchWritesRequest;
const JsonEdge = public.JsonEdge;
const JsonTraversalResult = public.JsonTraversalResult;
const JsonPathEdge = public.JsonPathEdge;
const JsonPath = public.JsonPath;
const JsonPatternBinding = public.JsonPatternBinding;
const JsonPatternMatch = public.JsonPatternMatch;
const JsonGraphNode = public.JsonGraphNode;
const antfly_db_open = public.antfly_db_open;
const asStorageOwnerContext = server_api.asStorageOwnerContext;
const storageOwnerContextCreate = server_api.storageOwnerContextCreate;
const storageOwnerContextCreateWithRuntime = server_api.storageOwnerContextCreateWithRuntime;
const createStorageOwnerContext = server_api.createStorageOwnerContext;
const storageOwnerContextDestroy = server_api.storageOwnerContextDestroy;
const storageContextAttachInferenceProvider = server_api.storageContextAttachInferenceProvider;
const storageOwnerContextConfigureSecrets = server_api.storageOwnerContextConfigureSecrets;
const storageOwnerContextConfigureRemoteContentSecurity = server_api.storageOwnerContextConfigureRemoteContentSecurity;
const storageOwnerContextCacheKindStats = server_api.storageOwnerContextCacheKindStats;
const storageOwnerContextMetrics = server_api.storageOwnerContextMetrics;
const storageOwnerContextInvalidateCaches = server_api.storageOwnerContextInvalidateCaches;
const asSystemStore = server_api.asSystemStore;
const asSystemReadTxn = server_api.asSystemReadTxn;
const asSystemCurrentScanTxn = server_api.asSystemCurrentScanTxn;
const asSystemWriteTxn = server_api.asSystemWriteTxn;
const asSystemCursor = server_api.asSystemCursor;
const storageContextSystemStoreOpen = server_api.storageContextSystemStoreOpen;
const storageSystemStoreClose = server_api.storageSystemStoreClose;
const storageSystemStoreSync = server_api.storageSystemStoreSync;
const storageSystemStoreBeginRead = server_api.storageSystemStoreBeginRead;
const storageSystemStoreBeginCurrentScan = server_api.storageSystemStoreBeginCurrentScan;
const storageSystemStoreBeginWrite = server_api.storageSystemStoreBeginWrite;
const storageSystemReadGet = server_api.storageSystemReadGet;
const storageSystemReadOpenCursor = server_api.storageSystemReadOpenCursor;
const storageSystemReadAbort = server_api.storageSystemReadAbort;
const storageSystemCurrentScanOpenCursor = server_api.storageSystemCurrentScanOpenCursor;
const storageSystemCurrentScanAbort = server_api.storageSystemCurrentScanAbort;
const storageSystemWriteGet = server_api.storageSystemWriteGet;
const storageSystemWritePut = server_api.storageSystemWritePut;
const storageSystemWriteDelete = server_api.storageSystemWriteDelete;
const storageSystemWriteOpenCursor = server_api.storageSystemWriteOpenCursor;
const storageSystemWriteCommit = server_api.storageSystemWriteCommit;
const storageSystemWriteAbort = server_api.storageSystemWriteAbort;
const storageSystemCursorMove = server_api.storageSystemCursorMove;
const storageSystemCursorClose = server_api.storageSystemCursorClose;
const contextLiteBackend = server_api.contextLiteBackend;
const storageContextLiteAdoptionProbe = server_api.storageContextLiteAdoptionProbe;
const storageContextLiteAdoptAndVerify = server_api.storageContextLiteAdoptAndVerify;
const storageContextLiteMarkStandalone = server_api.storageContextLiteMarkStandalone;
const storageContextMaintenanceStatus = server_api.storageContextMaintenanceStatus;
const storageContextMaintenanceRun = server_api.storageContextMaintenanceRun;
const asDataApplyStore = server_api.asDataApplyStore;
const asMetadataApplyStore = server_api.asMetadataApplyStore;
const asMetadataPreparedSnapshot = server_api.asMetadataPreparedSnapshot;
const asDataApplyGroupTransition = server_api.asDataApplyGroupTransition;
const asDataApplyPreparedSnapshot = server_api.asDataApplyPreparedSnapshot;
const metadataProjectionJson = server_api.metadataProjectionJson;
const metadataProjectionStatusFromError = server_api.metadataProjectionStatusFromError;
const metadataApplyStoreOpen = server_api.metadataApplyStoreOpen;
const metadataApplyStoreClose = server_api.metadataApplyStoreClose;
const metadataApplyStoreApplyBatch = server_api.metadataApplyStoreApplyBatch;
const metadataApplyStoreBuildSnapshot = server_api.metadataApplyStoreBuildSnapshot;
const metadataApplyStoreInstallSnapshot = server_api.metadataApplyStoreInstallSnapshot;
const metadataApplyStorePrepareSnapshot = server_api.metadataApplyStorePrepareSnapshot;
const metadataApplyPreparedSnapshotMaterialize = server_api.metadataApplyPreparedSnapshotMaterialize;
const metadataApplyPreparedSnapshotCancel = server_api.metadataApplyPreparedSnapshotCancel;
const metadataApplyPreparedSnapshotDestroy = server_api.metadataApplyPreparedSnapshotDestroy;
const metadataApplyStoreAddListeners = server_api.metadataApplyStoreAddListeners;
const metadataApplyStoreRemoveListeners = server_api.metadataApplyStoreRemoveListeners;
const metadataApplyStoreBindHotStandby = server_api.metadataApplyStoreBindHotStandby;
const metadataApplyStoreProjection = server_api.metadataApplyStoreProjection;
const metadataReconcileReplicaRoot = server_api.metadataReconcileReplicaRoot;
const dataApplyStoreOpen = server_api.dataApplyStoreOpen;
const dataApplyStoreClose = server_api.dataApplyStoreClose;
const dataApplyStoreApplyBatch = server_api.dataApplyStoreApplyBatch;
const dataApplyStoreBuildSnapshot = server_api.dataApplyStoreBuildSnapshot;
const dataApplyStoreInstallSnapshot = server_api.dataApplyStoreInstallSnapshot;
const dataApplyStorePrepareSnapshot = server_api.dataApplyStorePrepareSnapshot;
const dataApplyPreparedSnapshotMaterialize = server_api.dataApplyPreparedSnapshotMaterialize;
const dataApplyPreparedSnapshotCancel = server_api.dataApplyPreparedSnapshotCancel;
const dataApplyPreparedSnapshotDestroy = server_api.dataApplyPreparedSnapshotDestroy;
const dataApplyStoreLatest = server_api.dataApplyStoreLatest;
const dataApplyStoreLatestForTransition = server_api.dataApplyStoreLatestForTransition;
const dataApplyStoreRaftBatchProtocolVersion = server_api.dataApplyStoreRaftBatchProtocolVersion;
const dataApplyStoreProjection = server_api.dataApplyStoreProjection;
const dataApplyStoreReconcileOwner = server_api.dataApplyStoreReconcileOwner;
const dataApplyLatestResult = server_api.dataApplyLatestResult;
const dataApplyExpectedBatch = server_api.dataApplyExpectedBatch;
const dataApplyStoreRetainGroups = server_api.dataApplyStoreRetainGroups;
const dataApplyStoreBeginGroupTransition = server_api.dataApplyStoreBeginGroupTransition;
const dataApplyStoreCommitGroupTransition = server_api.dataApplyStoreCommitGroupTransition;
const dataApplyStoreAbortGroupTransition = server_api.dataApplyStoreAbortGroupTransition;
const dataApplyStoreDestroyGroupTransition = server_api.dataApplyStoreDestroyGroupTransition;
const releaseBorrowedTransitionOwner = server_api.releaseBorrowedTransitionOwner;
const validateLocalTransitionOwner = server_api.validateLocalTransitionOwner;
const localTransitionIdentity = server_api.localTransitionIdentity;
const localTransitionSplitResult = server_api.localTransitionSplitResult;
const localTransitionMergeResult = server_api.localTransitionMergeResult;
const storageOwnerLocalTransition = server_api.storageOwnerLocalTransition;
const storageOwnerTargetAdvanced = server_api.storageOwnerTargetAdvanced;
const storageOwnerOpen = server_api.storageOwnerOpen;
const storageOwnerClose = server_api.storageOwnerClose;
const storageOwnerConfigure = server_api.storageOwnerConfigure;
const OwnerRepairControls = server_api.OwnerRepairControls;
const storageOwnerReconcile = server_api.storageOwnerReconcile;
const storageOwnerPreflightWriteAdmission = server_api.storageOwnerPreflightWriteAdmission;
const storageOwnerFindMedianKey = server_api.storageOwnerFindMedianKey;
const StorageOwnerBulkCallbacks = server_api.StorageOwnerBulkCallbacks;
const storageOwnerBulkBegin = server_api.storageOwnerBulkBegin;
const storageOwnerBulkFinish = server_api.storageOwnerBulkFinish;
const storageOwnerBulkAbort = server_api.storageOwnerBulkAbort;
const storageOwnerOperationTableName = server_api.storageOwnerOperationTableName;
const storageHotStandbySeedFailure = server_api.storageHotStandbySeedFailure;
const validateHotStandbySeedRequest = server_api.validateHotStandbySeedRequest;
const storageHotStandbySeedActivateJson = server_api.storageHotStandbySeedActivateJson;
const storageHotStandbySeedValidateJson = server_api.storageHotStandbySeedValidateJson;
const storageHotStandbySeedPruneJson = server_api.storageHotStandbySeedPruneJson;
const StorageOwnerDocumentChildRangeDispatch = server_api.StorageOwnerDocumentChildRangeDispatch;
const StorageOwnerCommittedBatchEffects = server_api.StorageOwnerCommittedBatchEffects;
const storageOwnerCallbackStatusToError = server_api.storageOwnerCallbackStatusToError;
const storageOwnerTableName = server_api.storageOwnerTableName;
const storageOwnerBatchJson = server_api.storageOwnerBatchJson;
const storageOwnerReplicatedBatchJson = server_api.storageOwnerReplicatedBatchJson;
const storageOwnerReplicatedBatchAtRaftEntryJson = server_api.storageOwnerReplicatedBatchAtRaftEntryJson;
const storageOwnerNativeFkGenerationControlJson = server_api.storageOwnerNativeFkGenerationControlJson;
const storageOwnerNativeInitialChildControlJson = server_api.storageOwnerNativeInitialChildControlJson;
const storageOwnerTransactionStatus = server_api.storageOwnerTransactionStatus;
const storageOwnerWaitForSync = server_api.storageOwnerWaitForSync;
const storageOwnerApplyHotStandbyReplicationRecord = server_api.storageOwnerApplyHotStandbyReplicationRecord;
const backup_pin_diagnostic_gate = server_api.backup_pin_diagnostic_gate;
const storageOwnerOnlineMergeIoJson = server_api.storageOwnerOnlineMergeIoJson;
const storageOwnerSourceArtifactJson = server_api.storageOwnerSourceArtifactJson;
const storageOwnerSourcePinPublicationJson = server_api.storageOwnerSourcePinPublicationJson;
const storageOwnerBackupPinControlJson = server_api.storageOwnerBackupPinControlJson;
const storageBackupPinReclaimJson = server_api.storageBackupPinReclaimJson;
const storageOwnerBackupJson = server_api.storageOwnerBackupJson;
const RestoreRequestScope = server_api.RestoreRequestScope;
const prepareStorageRestore = server_api.prepareStorageRestore;
const storageRestorePrepare = server_api.storageRestorePrepare;
const storageRestoreReconcile = server_api.storageRestoreReconcile;
const storageRestoreApplyBootstrap = server_api.storageRestoreApplyBootstrap;
const storageOwnerRestoreRepair = server_api.storageOwnerRestoreRepair;
const prepareStorageSnapshot = server_api.prepareStorageSnapshot;
const prepareNativeStorageSnapshot = server_api.prepareNativeStorageSnapshot;
const NativeSnapshotCapture = server_api.NativeSnapshotCapture;
const storageOwnerSnapshotCapture = server_api.storageOwnerSnapshotCapture;
const storageSnapshotCaptureDestroy = server_api.storageSnapshotCaptureDestroy;
const dataApplyPreparedSnapshotAttachNative = server_api.dataApplyPreparedSnapshotAttachNative;
const storageSnapshotCaptureBindLease = server_api.storageSnapshotCaptureBindLease;
const dataApplyPreparedSnapshotRequiresNative = server_api.dataApplyPreparedSnapshotRequiresNative;
const storageSnapshotPrepare = server_api.storageSnapshotPrepare;
const storageSnapshotPublishPrepared = server_api.storageSnapshotPublishPrepared;
const storageSnapshotPromote = server_api.storageSnapshotPromote;
const storageSnapshotCommit = server_api.storageSnapshotCommit;
const storageSnapshotRollback = server_api.storageSnapshotRollback;
const storageSnapshotDestroy = server_api.storageSnapshotDestroy;
const batchStorageKernelJson = server_api.batchStorageKernelJson;
const replicatedBatchStorageKernelJson = server_api.replicatedBatchStorageKernelJson;
const replicatedBatchStorageKernelJsonAtRaftEntry = server_api.replicatedBatchStorageKernelJsonAtRaftEntry;
const storageOwnerQueryJson = server_api.storageOwnerQueryJson;
const storageOwnerLookupJson = server_api.storageOwnerLookupJson;
const storageOwnerRelationalReadProvider = server_api.storageOwnerRelationalReadProvider;
const StorageRelationalRead = server_api.StorageRelationalRead;
const storageOwnerScanStream = server_api.storageOwnerScanStream;
const storageOwnerScanNdjson = server_api.storageOwnerScanNdjson;
const storageOwnerGraphMetricMaintenanceJson = server_api.storageOwnerGraphMetricMaintenanceJson;
const storageOwnerPreflightJson = server_api.storageOwnerPreflightJson;
const storageOwnerTextStatsJson = server_api.storageOwnerTextStatsJson;
const storageOwnerAlgebraicPartialsJson = server_api.storageOwnerAlgebraicPartialsJson;
const executeStorageOwnerCompiledQueryOperation = server_api.executeStorageOwnerCompiledQueryOperation;
const storageAggregateJson = server_api.storageAggregateJson;
const storageOwnerGraphExpandJson = server_api.storageOwnerGraphExpandJson;
const storageOwnerGraphHydrateJson = server_api.storageOwnerGraphHydrateJson;
const storageOwnerGraphEdgesJson = server_api.storageOwnerGraphEdgesJson;
const executeStorageOwnerCompiledGraph = server_api.executeStorageOwnerCompiledGraph;
const storageOwnerDocumentArtifactManifestJson = server_api.storageOwnerDocumentArtifactManifestJson;
const storageOwnerDocumentArtifactManifestsJson = server_api.storageOwnerDocumentArtifactManifestsJson;
const StorageOwnerArtifactCancellation = server_api.StorageOwnerArtifactCancellation;
const storageOwnerArtifactJsonResponse = server_api.storageOwnerArtifactJsonResponse;
const storageOwnerVectorMigrationJson = server_api.storageOwnerVectorMigrationJson;
const storageOwnerArtifactOperationJson = server_api.storageOwnerArtifactOperationJson;
const storageOwnerRuntimeStatusJson = server_api.storageOwnerRuntimeStatusJson;
const StorageOwnerObservationCancellation = server_api.StorageOwnerObservationCancellation;
const storageOwnerObservedDynamicFieldCapabilitySetsJson = server_api.storageOwnerObservedDynamicFieldCapabilitySetsJson;
const storageOwnerRestoreStateJson = server_api.storageOwnerRestoreStateJson;
const storageOwnerTextMemoryJson = server_api.storageOwnerTextMemoryJson;
const storageOwnerMaintenance = server_api.storageOwnerMaintenance;
const storageOwnerBufferDestroy = server_api.storageOwnerBufferDestroy;
const storageOwnerStatusFromError = server_api.storageOwnerStatusFromError;
const openDefaultDirectoryHandle = public.openDefaultDirectoryHandle;
const antfly_db_close = public.antfly_db_close;
const antfly_threading_mode = public.antfly_threading_mode;
const antfly_abi_version = public.antfly_abi_version;
const antfly_open_options_size = public.antfly_open_options_size;
const antfly_error_code_name = public.antfly_error_code_name;
const antfly_error_code_description = public.antfly_error_code_description;
const antfly_open_options_init = public.antfly_open_options_init;
const open_known_flags = public.open_known_flags;
const StorageKind = public.StorageKind;
const LiteResolvedOpenOptions = public.LiteResolvedOpenOptions;
const optionFieldType = public.optionFieldType;
const optionHasField = public.optionHasField;
const optionFieldPresent = public.optionFieldPresent;
const readOptionField = public.readOptionField;
const validateOpenOptionsReserved = public.validateOpenOptionsReserved;
const openModeFromU32 = public.openModeFromU32;
const profileFromU32 = public.profileFromU32;
const validateResolvedOpenOptions = public.validateResolvedOpenOptions;
const resolveOpenOptions = public.resolveOpenOptions;
const openLiteHandle = public.openLiteHandle;
const openLiteHandleAlloc = public.openLiteHandleAlloc;
const HostLiteOpenOptions = public.HostLiteOpenOptions;
const openLiteHandleWithRuntime = public.openLiteHandleWithRuntime;
const closeLiteRuntimeHandle = public.closeLiteRuntimeHandle;
const openLiteHandleAllocWithRuntime = public.openLiteHandleAllocWithRuntime;
const dbOpenOptionsFromResolved = public.dbOpenOptionsFromResolved;
const openDirectoryHandle = public.openDirectoryHandle;
const openDirectoryHandleAlloc = public.openDirectoryHandleAlloc;
const openGenericHandle = public.openGenericHandle;
const openGenericHandleOnce = public.openGenericHandleOnce;
const cStringSpan = public.cStringSpan;
const antfly_db_open_with_options = public.antfly_db_open_with_options;
const antfly_db_create_with_options = public.antfly_db_create_with_options;
const antfly_lite_open = public.antfly_lite_open;
const antfly_lite_create = public.antfly_lite_create;
const antfly_lite_open_hosted = public.antfly_lite_open_hosted;
const antfly_lite_create_hosted = public.antfly_lite_create_hosted;
const antfly_lite_open_readonly = public.antfly_lite_open_readonly;
const antfly_lite_open_status_only = public.antfly_lite_open_status_only;
const resetOutBuffer = public.resetOutBuffer;
const antfly_db_capabilities_json = public.antfly_db_capabilities_json;
const directory_storage_status = public.directory_storage_status;
const antfly_db_status_json = public.antfly_db_status_json;
const antfly_db_backup = public.antfly_db_backup;
const antfly_db_import_backup = public.antfly_db_import_backup;
const RestoreReport = public.RestoreReport;
const restoreFormatName = public.restoreFormatName;
const antfly_restore_backup_json = public.antfly_restore_backup_json;
const antfly_restore_backup_file_json = public.antfly_restore_backup_file_json;
const antfly_lite_check_json = public.antfly_lite_check_json;
const antfly_lite_check_file_json = public.antfly_lite_check_file_json;
const antfly_lite_copy_stable_snapshot_json = public.antfly_lite_copy_stable_snapshot_json;
const antfly_lite_copy_stable_snapshot_file_json = public.antfly_lite_copy_stable_snapshot_file_json;
const LiteCompactReport = public.LiteCompactReport;
const prepareLiteCompact = public.prepareLiteCompact;
const antfly_lite_compact_json = public.antfly_lite_compact_json;
const antfly_lite_vacuum_json = public.antfly_lite_vacuum_json;
const LiteReplayGeneratedEnrichmentsReport = public.LiteReplayGeneratedEnrichmentsReport;
const antfly_db_replay_generated_enrichments_json = public.antfly_db_replay_generated_enrichments_json;
const restorePortableBackupToLiteFileWithRuntime = public.restorePortableBackupToLiteFileWithRuntime;
const restorePortableBackupToLiteFile = public.restorePortableBackupToLiteFile;
const restorePortableBackupToDirectory = public.restorePortableBackupToDirectory;
const restorePortableBackupPathToDirectory = public.restorePortableBackupPathToDirectory;
const restorePortableBackupPathToLiteFile = public.restorePortableBackupPathToLiteFile;
const restorePortableSourceToLiteFile = public.restorePortableSourceToLiteFile;
const capiPathExists = public.capiPathExists;
const capiDeleteFileIfExists = public.capiDeleteFileIfExists;
const capiRenameFilePath = public.capiRenameFilePath;
const capiDeleteFilePath = public.capiDeleteFilePath;
const antfly_db_set_readable_lease_hook = public.antfly_db_set_readable_lease_hook;
const freeRawBuffer = shared.freeRawBuffer;
const antfly_buffer_free = public.antfly_buffer_free;
const wipeBufferBytes = public.wipeBufferBytes;
const antfly_buffer_free_zero = public.antfly_buffer_free_zero;
const antfly_dense_search_result_free = public.antfly_dense_search_result_free;
const antfly_packed_dense_search_result_free = public.antfly_packed_dense_search_result_free;
const packDenseHits = public.packDenseHits;
const DenseOwnedResult = public.DenseOwnedResult;
const DenseOwnedProfile = public.DenseOwnedProfile;
const DenseResolvedHit = public.DenseResolvedHit;
const DenseResolvedHits = public.DenseResolvedHits;
const DenseWireOwnedProfile = public.DenseWireOwnedProfile;
const resolveDenseHitsFromProfiled = public.resolveDenseHitsFromProfiled;
const packResolvedDenseHits = public.packResolvedDenseHits;
const encodeResolvedDenseWireResponse = public.encodeResolvedDenseWireResponse;
const searchDensePackedFast = public.searchDensePackedFast;
const searchDenseWireFast = public.searchDenseWireFast;
const searchDenseWireOwnedProfiled = public.searchDenseWireOwnedProfiled;
const searchDenseOwned = public.searchDenseOwned;
const searchDenseOwnedProfiled = public.searchDenseOwnedProfiled;
const searchTextMatchOwned = public.searchTextMatchOwned;
const searchTextTermOwned = public.searchTextTermOwned;
const searchTextMatchPhraseOwned = public.searchTextMatchPhraseOwned;
const searchTextOwned = public.searchTextOwned;
const antfly_scan_hash_result_free = public.antfly_scan_hash_result_free;
const antfly_db_begin_transaction_with_id = public.antfly_db_begin_transaction_with_id;
const antfly_db_write_transaction = public.antfly_db_write_transaction;
const antfly_db_batch = public.antfly_db_batch;
const antfly_db_batch_json = public.antfly_db_batch_json;
const antfly_db_resolve_intents = public.antfly_db_resolve_intents;
const antfly_db_get_transaction_status = public.antfly_db_get_transaction_status;
const antfly_db_get_commit_version = public.antfly_db_get_commit_version;
const antfly_db_get_timestamp = public.antfly_db_get_timestamp;
const antfly_db_lookup_json = public.antfly_db_lookup_json;
const antfly_db_get_raw = public.antfly_db_get_raw;
const antfly_db_lookup_artifact_json = public.antfly_db_lookup_artifact_json;
const antfly_decode_artifact_id_json = public.antfly_decode_artifact_id_json;
const antfly_db_get_schema_json = public.antfly_db_get_schema_json;
const antfly_db_set_schema_json = public.antfly_db_set_schema_json;
const antfly_db_run_until_idle = public.antfly_db_run_until_idle;
const antfly_db_run_until_idle_json = public.antfly_db_run_until_idle_json;
const writeRunUntilIdleNoProgressDiagnosticIfAny = public.writeRunUntilIdleNoProgressDiagnosticIfAny;
const antfly_db_pending_work_stats_json = public.antfly_db_pending_work_stats_json;
const antflyDbExtractEnrichmentsJson = public.antflyDbExtractEnrichmentsJson;
const antflyDbComputeEnrichmentsJson = public.antflyDbComputeEnrichmentsJson;
const antfly_db_update_range = public.antfly_db_update_range;
const antfly_db_get_range_json = public.antfly_db_get_range_json;
const antfly_db_get_split_state_json = public.antfly_db_get_split_state_json;
const antfly_db_set_split_state_json = public.antfly_db_set_split_state_json;
const antfly_db_clear_split_state = public.antfly_db_clear_split_state;
const antfly_db_get_split_delta_seq = public.antfly_db_get_split_delta_seq;
const antfly_db_get_split_delta_final_seq = public.antfly_db_get_split_delta_final_seq;
const antfly_db_set_split_delta_final_seq = public.antfly_db_set_split_delta_final_seq;
const antfly_db_clear_split_delta_final_seq = public.antfly_db_clear_split_delta_final_seq;
const antfly_db_list_split_delta_entries_after_json = public.antfly_db_list_split_delta_entries_after_json;
const antfly_db_clear_split_delta_entries = public.antfly_db_clear_split_delta_entries;
const antfly_db_list_indexes_json = public.antfly_db_list_indexes_json;
const antfly_db_list_enrichments_json = public.antfly_db_list_enrichments_json;
const antfly_db_scan_json = public.antfly_db_scan_json;
const antfly_db_scan_hashes = public.antfly_db_scan_hashes;
const antfly_db_stats_json = public.antfly_db_stats_json;
const dbStatsJsonAlloc = public.dbStatsJsonAlloc;
const dbIndexStatsProjectionAlloc = public.dbIndexStatsProjectionAlloc;
const jsonDBStatsProjection = public.jsonDBStatsProjection;
const requestLooksLikePublicQueryJson = public.requestLooksLikePublicQueryJson;
const searchStorageKernelQueryJson = server_api.searchStorageKernelQueryJson;
const ownerQueryCancellation = server_api.ownerQueryCancellation;
const storageOwnerQueryFailure = server_api.storageOwnerQueryFailure;
const LiteSemanticResolver = public.LiteSemanticResolver;
const searchPublicQueryJson = public.searchPublicQueryJson;
const antfly_db_sql_json = public.antfly_db_sql_json;
const executeEmbeddedSql = public.executeEmbeddedSql;
const embeddedSqlUnknownReceipt = public.embeddedSqlUnknownReceipt;
const embeddedSqlCommitReceipt = public.embeddedSqlCommitReceipt;
const antfly_db_search_json = public.antfly_db_search_json;
const antfly_db_search_dense = public.antfly_db_search_dense;
const antfly_db_search_dense_profile = public.antfly_db_search_dense_profile;
const antfly_db_dense_noop = public.antfly_db_dense_noop;
const antfly_db_dense_fixed_packed_result = public.antfly_db_dense_fixed_packed_result;
const antfly_db_search_dense_wire = public.antfly_db_search_dense_wire;
const antfly_db_search_dense_wire_profile = public.antfly_db_search_dense_wire_profile;
const antfly_db_search_text_match = public.antfly_db_search_text_match;
const antfly_db_search_text_match_wire = public.antfly_db_search_text_match_wire;
const antfly_db_search_text_term_wire = public.antfly_db_search_text_term_wire;
const antfly_db_search_text_match_phrase_wire = public.antfly_db_search_text_match_phrase_wire;
const antfly_db_search_hits_json = public.antfly_db_search_hits_json;
const parseTextQueryJson = public.parseTextQueryJson;
const parseMinShouldJson = public.parseMinShouldJson;
const parseOptionalBoostJson = public.parseOptionalBoostJson;
const parseStringArrayJson = public.parseStringArrayJson;
const parseStringMatrixJson = public.parseStringMatrixJson;
const parseGeoPointJson = public.parseGeoPointJson;
const parseGeoPointArrayJson = public.parseGeoPointArrayJson;
const parseGeoShapePolygonsJson = public.parseGeoShapePolygonsJson;
const parseGeoShapeRelation = public.parseGeoShapeRelation;
const parseTextQueryArrayJson = public.parseTextQueryArrayJson;
const appendTextQueryArrayJson = public.appendTextQueryArrayJson;
const antfly_db_execute_graph_queries_json = public.antfly_db_execute_graph_queries_json;
const antfly_db_aggregate_hits_json = public.antfly_db_aggregate_hits_json;
const parseNamedGraphQueries = public.parseNamedGraphQueries;
const parseNamedGraphInputSets = public.parseNamedGraphInputSets;
const parseGraphQueryRequestOwned = public.parseGraphQueryRequestOwned;
const parseGraphNodeSelectorRequestOwned = public.parseGraphNodeSelectorRequestOwned;
const decodeGraphKeys = public.decodeGraphKeys;
const cloneGraphEdgeTypes = public.cloneGraphEdgeTypes;
const decodeGraphHitIds = public.decodeGraphHitIds;
const deinitOwnedNodeSelector = public.deinitOwnedNodeSelector;
const deinitOwnedGraphQuery = public.deinitOwnedGraphQuery;
const deinitOwnedNamedGraphQuery = public.deinitOwnedNamedGraphQuery;
const freeOwnedNamedGraphQueries = public.freeOwnedNamedGraphQueries;
const deinitOwnedNamedGraphInputSet = public.deinitOwnedNamedGraphInputSet;
const freeOwnedNamedGraphInputSets = public.freeOwnedNamedGraphInputSets;
const parseGraphDirection = public.parseGraphDirection;
const parseGraphWeightMode = public.parseGraphWeightMode;
const legacyGraphWeightBound = public.legacyGraphWeightBound;
const computeSearchAggregations = shared.computeSearchAggregations;
const computeSingleAggregation = shared.computeSingleAggregation;
const NumericMetricKind = shared.NumericMetricKind;
const computeNumericMetricAggregation = shared.computeNumericMetricAggregation;
const computeCardinalityAggregation = shared.computeCardinalityAggregation;
const computeTermsAggregation = shared.computeTermsAggregation;
const computeHistogramAggregation = shared.computeHistogramAggregation;
const computeDateHistogramAggregation = shared.computeDateHistogramAggregation;
const computeRangeAggregation = shared.computeRangeAggregation;
const matchesNumericRangeValue = shared.matchesNumericRangeValue;
const matchesDateRangeValue = shared.matchesDateRangeValue;
const matchesGeoDistanceValue = shared.matchesGeoDistanceValue;
const accumulateNumericJsonValue = shared.accumulateNumericJsonValue;
const collectCardinalityValues = shared.collectCardinalityValues;
const appendTermAggregationValuesZig = shared.appendTermAggregationValuesZig;
const jsonValueToTermKey = shared.jsonValueToTermKey;
const stringifyJsonValueCompact = shared.stringifyJsonValueCompact;
const distanceToMeters = shared.distanceToMeters;
const extractGeoPointFieldFromStoredJson = shared.extractGeoPointFieldFromStoredJson;
const jsonValueToF64 = shared.jsonValueToF64;
const fillHistogramBucketKeys = shared.fillHistogramBucketKeys;
const fillDateHistogramBucketKeys = shared.fillDateHistogramBucketKeys;
const nextDateHistogramBucketKey = shared.nextDateHistogramBucketKey;
const addCalendarMonths = shared.addCalendarMonths;
const addCalendarYears = shared.addCalendarYears;
const civilDateToBucketNs = shared.civilDateToBucketNs;
const extractNumericFieldFromStoredJson = shared.extractNumericFieldFromStoredJson;
const extractTimestampFieldFromStoredJson = shared.extractTimestampFieldFromStoredJson;
const parseDateInterval = shared.parseDateInterval;
const parseRfc3339ToNs = shared.parseRfc3339ToNs;
const daysFromCivil = shared.daysFromCivil;
const formatRfc3339Bucket = shared.formatRfc3339Bucket;
const civilFromDays = shared.civilFromDays;
const extractValueAtPath = shared.extractValueAtPath;
const antfly_db_add_index_json = public.antfly_db_add_index_json;
const antfly_db_delete_index = public.antfly_db_delete_index;
const antfly_db_add_enrichment_json = public.antfly_db_add_enrichment_json;
const antfly_db_delete_enrichment = public.antfly_db_delete_enrichment;
const antfly_db_get_edges_json = public.antfly_db_get_edges_json;
const antfly_db_traverse_edges_json = public.antfly_db_traverse_edges_json;
const antfly_db_get_neighbors_json = public.antfly_db_get_neighbors_json;
const antfly_db_find_shortest_path_json = public.antfly_db_find_shortest_path_json;
const antfly_db_find_k_shortest_paths_json = public.antfly_db_find_k_shortest_paths_json;
const antfly_db_match_pattern_json = public.antfly_db_match_pattern_json;
const antfly_db_create_shadow_index_manager = public.antfly_db_create_shadow_index_manager;
const antfly_db_close_shadow_index_manager = public.antfly_db_close_shadow_index_manager;
const antfly_db_get_shadow_index_dir = public.antfly_db_get_shadow_index_dir;
const antfly_db_find_median_key = public.antfly_db_find_median_key;
const antfly_db_split = public.antfly_db_split;
const antfly_db_finalize_split = public.antfly_db_finalize_split;
const antfly_db_snapshot = public.antfly_db_snapshot;
const storageOwnerRestoreControlJson = server_api.storageOwnerRestoreControlJson;
const storageOwnerRelationalTransitionRead = server_api.storageOwnerRelationalTransitionRead;
const hiddenRestoreJson = server_api.hiddenRestoreJson;
const captureOwnerSeedSnapshot = server_api.captureOwnerSeedSnapshot;
const storageOwnerHiddenRestoreJson = server_api.storageOwnerHiddenRestoreJson;
const storageOwnerMergeArtifactsPage = server_api.storageOwnerMergeArtifactsPage;
const storageOwnerMergeCleanupKeysPage = server_api.storageOwnerMergeCleanupKeysPage;
const cleanupServerHandle = server_api.cleanupServerHandle;
const releaseServerContext = server_api.releaseServerContext;
const backup_codec = antfly.backup_codec;
const distributed_graph = antfly.public_api.distributed_graph;
const capi_min_thread_stack_size = 8 * 1024 * 1024;

fn tempTestPath(alloc: Allocator, root: []const u8, label: []const u8) ![:0]u8 {
    return try std.fmt.allocPrintSentinel(alloc, "{s}-{s}", .{ root, label }, 0);
}

fn tempTestAflitePath(alloc: Allocator, root: []const u8, label: []const u8) ![:0]u8 {
    const base = try tempTestPath(alloc, root, label);
    defer alloc.free(base);
    const path = try std.fmt.allocPrint(alloc, "{s}.aflite", .{base});
    defer alloc.free(path);
    return try alloc.dupeSentinel(u8, path, 0);
}

const lite_restore_options = capi.OpenOptions{ .storage_kind = capi.storage_kind_lite };

/// Tests that build a Handle on the stack register it to get a caller id,
/// then retire the id without freeing the Handle.
fn registerTestHandle(handle: *Handle) !*anyopaque {
    return handle_registry.register(handle);
}

fn unregisterTestHandle(ptr: *anyopaque) void {
    _, const id = handle_registry.beginClose(ptr) orelse return;
    handle_registry.finishClose(id);
}

fn cleanupTestDir(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn cleanupTestFile(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteFile(io_impl.io(), path) catch {};
}

fn testPathExists(path: []const u8) bool {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch return false;
    return true;
}

const JsonWritePair = struct {
    key_b64: []u8,
    value_b64: []u8,

    fn init(alloc: Allocator, write: db_mod.types.BatchWrite) !JsonWritePair {
        return .{
            .key_b64 = try dupBase64(alloc, write.key),
            .value_b64 = try dupBase64(alloc, write.value),
        };
    }

    pub fn deinit(self: *JsonWritePair, alloc: Allocator) void {
        alloc.free(self.key_b64);
        alloc.free(self.value_b64);
        self.* = undefined;
    }
};

fn liteLocalEmbeddingModelAvailable(alloc: Allocator) bool {
    const home_c = std.c.getenv("HOME") orelse return false;
    const home = std.mem.span(home_c);
    const owner_dir = std.fs.path.join(alloc, &.{ home, ".antfly", "inference", "models", "Qwen" }) catch return false;
    defer alloc.free(owner_dir);
    var dir = std.Io.Dir.cwd().openDir(std.testing.io, owner_dir, .{ .iterate = true }) catch return false;
    defer dir.close(std.testing.io);
    var it = dir.iterateAssumeFirstIteration();
    while (it.next(std.testing.io) catch return false) |entry| {
        if (std.mem.startsWith(u8, entry.name, "Qwen3-Embedding-0.6B-GGUF")) return true;
    }
    return false;
}

// Conditional on the `-Dcapi-inference=true` variant (skips on the default
// build, where capi_build_options.inference_enabled is false) and on the
// small local embedding model being present under
// ~/.antfly/inference/models, so this never requires a network call and
// never fails a machine that has not pulled the model.
test "storage-owner reverse callbacks preserve semantic error identity" {
    try std.testing.expectError(
        error.WouldBlock,
        StorageOwnerTransactionRecovery.callbackStatus(.would_block),
    );
    try std.testing.expectError(
        error.StorageBusy,
        StorageOwnerTransactionRecovery.callbackStatus(.busy),
    );
    try std.testing.expectError(
        error.ResourceBudgetExceeded,
        StorageOwnerTransactionRecovery.callbackStatus(.resource_budget_exceeded),
    );
    try std.testing.expectError(
        error.Canceled,
        StorageOwnerTransactionRecovery.callbackStatus(.canceled),
    );
    try std.testing.expectError(
        error.Cancelled,
        StorageOwnerTransactionRecovery.callbackStatus(.cancelled),
    );
}

test "capi lite AddIndexJSON registers the server's nested artifact-sourced enrichment shape" {
    // Reproduces the docsaf/dogfood chunk-artifact pattern -- a `chunk`
    // enrichment producing `document_chunks_v1`, then an embeddings index
    // consuming it via `"sources":[{"artifact":"document_chunk_dense_v1"}]`
    // with the producing `embedding` enrichment nested in the index's own
    // config -- driven entirely through `antfly_db_add_index_json`, the way
    // `go/pkg/docsaf/cmd/docsaf/main.go`'s `createHierarchyIndexes` and
    // `antfly.NewArtifactEmbeddingIndexConfig` build it. Before
    // `registerLiteIndexEnrichments` this silently dropped both nested
    // enrichment declarations (they are not valid `db.addIndex` fields), so
    // the physical dense_vector index referenced a `document_chunk_dense_v1`
    // artifact with no enrichment ever registered to produce it.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-nested-enrichments");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    // Producer index: a full_text index over the chunk artifact (docsaf's
    // `document_text`), with the `chunk` enrichment nested in its config.
    const chunk_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":64}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = chunk_index_json,
        .len = chunk_index_json.len,
    }));

    var enrichments_after_chunk: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(handle, &enrichments_after_chunk));
    defer freeRawBuffer(enrichments_after_chunk.ptr, enrichments_after_chunk.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments_after_chunk.ptr.?[0..enrichments_after_chunk.len], "\"document_chunks_v1\"") != null);

    // Consumer index: the exact two-stage `sources` form docsaf's
    // `NewArtifactEmbeddingIndexConfig` builds, with the `embedding`
    // enrichment nested in this index's own config and its
    // `source_artifact_name` pointing at the chunk artifact above.
    const vector_index_json =
        \\{"name":"document_vectors","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"sources\":[{\"artifact\":\"document_chunk_dense_v1\"}],\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"test-embed\",\"api_url\":\"http://127.0.0.1:1\"},\"distance_metric\":\"cosine\",\"enrichments\":[{\"name\":\"document_chunk_dense_v1\",\"kind\":\"embedding\",\"field\":\"text\",\"source_artifact_name\":\"document_chunks_v1\",\"expected_dims\":3}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = vector_index_json,
        .len = vector_index_json.len,
    }));

    var enrichments_after_vectors: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(handle, &enrichments_after_vectors));
    defer freeRawBuffer(enrichments_after_vectors.ptr, enrichments_after_vectors.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments_after_vectors.ptr.?[0..enrichments_after_vectors.len], "\"document_chunk_dense_v1\"") != null);

    var indexes: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_indexes_json(handle, &indexes));
    defer freeRawBuffer(indexes.ptr, indexes.len);
    const indexes_json = indexes.ptr.?[0..indexes.len];
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\"document_vectors\"") != null);
    // The physical config carries the translated artifact source reference
    // (config_json is itself JSON-encoded as a string, so its embedded quotes
    // are backslash-escaped here rather than literal).
    try std.testing.expect(std.mem.indexOf(u8, indexes_json, "\\\"sources\\\":[{\\\"artifact\\\":\\\"document_chunk_dense_v1\\\"") != null);
}

test "capi lite AddIndexJSON restores the enrichment catalog when admission rejects the index" {
    // Reviewer-reported P2: registerLiteIndexEnrichments durably upserted
    // every nested enrichment BEFORE db.addIndex validated the index, so
    // re-adding an existing index with a changed chunk_size updated the
    // active enrichment and then failed with IndexAlreadyExists — the caller
    // saw an error while the configuration had silently changed. AddIndex is
    // now all-or-nothing: the pre-call enrichment catalog is restored on any
    // admission failure.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-addindex-rollback");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const original_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":64}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = original_index_json,
        .len = original_index_json.len,
    }));

    // Same index name, changed chunk geometry: admission must reject it AND
    // the active chunk enrichment must keep chunk_size 64.
    const changed_index_json =
        \\{"name":"document_text_chunks","kind":"full_text","config_json":"{\"chunk_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"field\":\"body\",\"chunk_size\":128}]}"}
    ;
    try std.testing.expect(antfly_db_add_index_json(handle, .{
        .ptr = changed_index_json,
        .len = changed_index_json.len,
    }) != .ok);

    {
        const enrichments = try asHandle(handle).?.db.listEnrichments(alloc);
        defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
        try std.testing.expectEqual(@as(usize, 1), enrichments.len);
        try std.testing.expectEqualStrings("document_chunks_v1", enrichments[0].name);
        try std.testing.expectEqual(@as(u32, 64), enrichments[0].chunk_size);
    }
}

test "capi lite AddIndexJSON registers a graph config's nested resolvers" {
    // The server registers entity resolvers from the whole table's indexes
    // JSON (metadata_table_provisioner.ensureResolvers); a native Lite
    // handle admits one index at a time, so registerLiteIndexResolvers
    // harvests the graph config's own "resolvers" array — the shape
    // examples/dogfood's knowledgeGraphIndexJSON declares. Re-adding the
    // same index must be idempotent (upsertResolver observes an unchanged
    // config), matching the enrichment path's contract.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-graph-resolvers");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const graph_index_json =
        \\{"name":"knowledge","kind":"graph","config_json":"{\"source\":{\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\",\"mention_edge_type\":\"mentions\"},\"artifact\":{\"name\":\"relations_v1\",\"kind\":\"asset\",\"source\":{\"type\":\"field\",\"value\":\"body\"},\"content_type\":\"application/json\"},\"resolvers\":[{\"name\":\"entities\",\"table\":\"entities\",\"source_artifact\":\"relations_v1\",\"resolution_artifact\":\"entities_resolution_v1\",\"key_template\":\"{{ lower _entity.label }}/{{ slug _entity.text }}\",\"config_generation\":1}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = graph_index_json,
        .len = graph_index_json.len,
    }));

    {
        const resolvers = try asHandle(handle).?.db.listResolvers(alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(alloc);
            alloc.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 1), resolvers.len);
        try std.testing.expectEqualStrings("entities", resolvers[0].name);
        try std.testing.expectEqualStrings("relations_v1", resolvers[0].source_artifact);
        try std.testing.expectEqualStrings("entities_resolution_v1", resolvers[0].resolution_artifact);
        try std.testing.expectEqual(@as(u64, 1), resolvers[0].config_generation);
    }

    // Re-adding the identical index (whatever its own admission outcome)
    // must not duplicate or corrupt the resolver catalog: an unchanged
    // config upserts as a no-op.
    _ = antfly_db_add_index_json(handle, .{
        .ptr = graph_index_json,
        .len = graph_index_json.len,
    });
    {
        const resolvers = try asHandle(handle).?.db.listResolvers(alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(alloc);
            alloc.free(resolvers);
        }
        try std.testing.expectEqual(@as(usize, 1), resolvers.len);
    }
}

test "capi lite AddIndexJSON surfaces an unresolvable source_artifact_name as invalid_argument, not internal" {
    // Regression test for the ANTFLY_INTERNAL reported against every
    // `sources`-based dense_vector config: the enrichment catalog's own
    // upstream-reference validation (an `embedding` enrichment naming a
    // `source_artifact_name` with no matching `chunk` enrichment) is a
    // caller config mistake, not a server fault, and must map to
    // ANTFLY_INVALID_ARGUMENT (see `capi/types.zig`'s `mapError`).
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-unresolved-artifact-source");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const vector_index_json =
        \\{"name":"document_vectors","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"sources\":[{\"artifact\":\"document_chunk_dense_v1\"}],\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"test-embed\",\"api_url\":\"http://127.0.0.1:1\"},\"enrichments\":[{\"name\":\"document_chunk_dense_v1\",\"kind\":\"embedding\",\"field\":\"text\",\"source_artifact_name\":\"missing_chunks_v1\",\"expected_dims\":3}]}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_add_index_json(handle, .{
        .ptr = vector_index_json,
        .len = vector_index_json.len,
    }));
}

// Server table provisioning never calls `db.addIndex` with a caller's raw
// dense/sparse config: `metadata_table_provisioner.extractIndexConfigJsonForKind`
// first runs it through `managed_embedder.translateEmbeddingsIndexConfigJson`,
// which is what attaches the internal `"generator"` section that
// `index_manager`'s `hasGeneratedEnrichmentTargets`/`appendGeneratedEnrichments`
// need to ever schedule a document's field for embedding. A native Lite
// handle calling `db.addIndex` directly skipped that step entirely, so even
// after `refreshLiteManagedEmbeddingRuntime` wires up a working embedder,
// the physical index never asks it for anything: `parseDenseConfig` only
// looks at `field`/`dims`/`metric`/`embedding_name`/`external`, and nothing
// else marks the index as awaiting generated content. Runs the same
// translation here so what gets stored via `db.addIndex` matches what the
// server would have stored. Falls back to the untranslated config on any
// translation error (for example a managed, non-external index with
// neither `embedder` nor `chunker` configured) so a Lite caller that never
// relied on this feature keeps today's permissive, pass-through behavior;
// `refreshLiteManagedEmbeddingRuntime` is likewise a no-op for such an
// index.
// True for a plain embedder-only embeddings config -- no `field`/`template`
// of its own, and none of the other shapes that mean something different
// (an artifact-backed consumer, an external/caller-supplied index, or one
// still carrying its own chunker) -- where defaulting `field` to
// `"embedding"` is unambiguous. Keeps `litePhysicalIndexConfigJson` from
// defaulting a config whose author meant something other than "index the
// stored `embedding` field".
test "capi lite merged indexes JSON discovers a standalone asset extractor and chunk enrichment with no owning index" {
    // Regression test for db.zig:1091 (pre-fix): `liteMergedIndexesJsonAlloc`
    // only read `handle.db.listIndexes`, so a `kind:"asset"` extractor or a
    // `kind:"chunk"` enrichment registered directly through
    // `antfly_db_add_enrichment_json` -- with no index nesting the same
    // declaration in its own config (see `registerLiteIndexEnrichments`) --
    // was accepted into the catalog but invisible to
    // `local_write.indexesJsonNeedsAssetProducer`/`indexesJsonHasGeneratedEnrichment`.
    // `refreshLiteManagedEmbeddingRuntime` always resolved an empty producer
    // set for it, so `ManagedDbEnrichmentSet.enabled()` stayed false and the
    // enrichment runtime never serviced that name's pending work at all.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-standalone-enrichment-discovery");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    // Standalone `chunk` enrichment: no index anywhere nests this declaration.
    const chunk_enrichment_json =
        \\{"name":"standalone_chunks_v1","kind":"chunk","field":"body","chunk_size":64,"chunk_overlap":8}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = chunk_enrichment_json,
        .len = chunk_enrichment_json.len,
    }));

    // Standalone `asset` enrichment with a model-backed (non-"copy") extractor
    // producer: also nested nowhere.
    const asset_enrichment_json =
        \\{"name":"standalone_extract_v1","kind":"asset","field":"body","producer_json":"{\"type\":\"extractor\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-extract\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = asset_enrichment_json,
        .len = asset_enrichment_json.len,
    }));

    const owned_handle = asHandle(handle).?;
    const merged_json = try liteMergedIndexesJsonAlloc(owned_handle);
    defer std.heap.c_allocator.free(merged_json);

    try std.testing.expect(std.mem.indexOf(u8, merged_json, "$enrichment:chunk:standalone_chunks_v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged_json, "$enrichment:asset:standalone_extract_v1") != null);

    // Before the fix these both returned false: neither scanner ever saw a
    // "kind":"asset"/"chunk" object anywhere in the merged JSON.
    try std.testing.expect(try local_write.indexesJsonHasGeneratedEnrichment(alloc, merged_json));
    try std.testing.expect(try local_write.indexesJsonNeedsAssetProducer(alloc, merged_json));
}

const EnrichmentDrainStats = struct {
    enrichment: struct {
        error_count: u64,
        fatal_error_count: u64,
        stalled: bool,
        target_sequence: u64,
        applied_sequence: u64,
    },
};

fn enrichmentDrainStats(handle: ?*anyopaque) !EnrichmentDrainStats {
    // Maintenance may replace and retire the resident Lite runtime. Inspect
    // the current runtime through a guarded public call rather than retaining
    // an internal Handle/DB pointer across mutations or run-until-idle.
    var output: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(handle, &output));
    defer freeRawBuffer(output.ptr, output.len);
    var parsed = try std.json.parseFromSlice(EnrichmentDrainStats, std.testing.allocator, output.ptr.?[0..output.len], .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return parsed.value;
}

test "capi lite run until idle drains a standalone chunk enrichment with no owning index" {
    // End-to-end reproduction of the same gap: before the fix, a standalone
    // `kind:"chunk"` catalog enrichment left `generated=false` in
    // `local_write.createManagedDbEnrichments`'s scan of the merged JSON, so
    // `ManagedDbEnrichmentSet.enabled()` (dense/sparse/asset_runtime all null,
    // `generated` false) stayed false and `refreshLiteManagedEmbeddingRuntime`
    // never created an enrichment runtime at all. A document's pending chunk
    // work for that name was accepted (the catalog entry validates and
    // stores) but nothing ever serviced it, so `antfly_db_run_until_idle`
    // would return `.stalled` (`error.RunUntilIdleNoProgress`) instead of
    // draining. A fixed-size, non-semantic chunker (`chunk_size`/
    // `chunk_overlap`, no `chunker_json`) needs no embedder or extractor
    // provider at all (`chunker_mod.chunkText` in enrichment_runtime.zig), so
    // this reproduces and proves the fix end to end without any local model.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-standalone-chunk-drain");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);

    const enrichment_json =
        \\{"name":"standalone_chunks_v1","kind":"chunk","field":"body","chunk_size":16,"chunk_overlap":4}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = enrichment_json,
        .len = enrichment_json.len,
    }));

    const batch_json = "{\"inserts\":{\"doc:capi-standalone-chunk\":{\"body\":\"antfly lite chunks this document body text into overlapping windows for later retrieval\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));

    const drained = try enrichmentDrainStats(handle);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.error_count);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.fatal_error_count);
    try std.testing.expect(!drained.enrichment.stalled);
    try std.testing.expectEqual(drained.enrichment.target_sequence, drained.enrichment.applied_sequence);
    try std.testing.expect(drained.enrichment.target_sequence > 0);
}

test "capi system write cursor sees pending catalog rows and preserves abort" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("system-write-cursor");
    defer test_tmp.cleanup();
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "catalog");
    defer alloc.free(path);
    var backend = try lite_backend.Handle.create(alloc, path, false);
    defer backend.deinit();
    const store = try backend.runtimeStoreForNamespace("system/metadata");
    {
        var seed = try store.beginWrite();
        errdefer seed.abort();
        try seed.put("catalog:a", "old");
        try seed.commit();
    }
    {
        var txn = SystemWriteTxnHandle{ .alloc = alloc, .txn = try store.beginWrite() };
        defer txn.txn.abort();
        try txn.txn.delete("catalog:a");
        try txn.txn.put("catalog:b", "pending");
        try txn.txn.put("catalog:c", "last");
        var cursor: ?*anyopaque = null;
        try std.testing.expectEqual(kernel_owner_abi.Status.invalid_argument, storageSystemWriteOpenCursor(null, &cursor));
        try std.testing.expect(cursor == null);
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemWriteOpenCursor(&txn, &cursor));
        defer storageSystemCursorClose(cursor);
        var entry: kernel_owner_abi.SystemEntryResult = .{};
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .at_or_after, .fromSlice("catalog:"), &entry));
        try std.testing.expectEqual(@as(u8, 1), entry.present);
        try std.testing.expectEqualStrings("catalog:b", entry.key.slice());
        try std.testing.expectEqualStrings("pending", entry.value.slice());
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .next, .{}, &entry));
        try std.testing.expectEqualStrings("catalog:c", entry.key.slice());
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageSystemCursorMove(cursor, .previous, .{}, &entry));
        try std.testing.expectEqualStrings("catalog:b", entry.key.slice());
    }
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("old", try read.get("catalog:a"));
    try std.testing.expectError(error.NotFound, read.get("catalog:b"));
}

test "storage owner open rejects prior ABI before reading expanded request fields" {
    var owner: ?*anyopaque = @ptrFromInt(1);
    const old_request: kernel_owner_abi.OpenRequest = .{
        .version = 68,
        .path = .{ .ptr = @ptrFromInt(1), .len = 1 },
        .owner_catalog_deferred_out = @ptrFromInt(1),
    };
    try std.testing.expectEqual(kernel_owner_abi.Status.invalid_abi, storageOwnerOpen(&old_request, &owner));
    try std.testing.expect(owner == null);
}

test "storage HA seed boundary preserves status and exact failure identity" {
    var response: kernel_owner_abi.OwnedBytes = .{};
    var failure: kernel_owner_abi.FailureIdentity = .{};
    const status = storageHotStandbySeedActivateJson(&.{
        .version = 0,
        .operation = .activate,
        .request_json = .fromSlice("{}"),
    }, &response, &failure);
    try std.testing.expectEqual(kernel_owner_abi.Status.invalid_abi, status);
    try std.testing.expectEqual(status, failure.status);
    try std.testing.expectEqual(kernel_owner_abi.FailureBoundary.storage_owner, failure.boundary);
    try std.testing.expectEqual(@backingInt(kernel_owner_abi.HASeedOperation.activate), failure.operation);
    try std.testing.expectEqualStrings("InvalidAbiVersion", failure.errorName());
    try kernel_error_identity.validateFailureEnvelope(status, &failure, kernel_owner_abi.abi_version);
    try std.testing.expectEqual(@as(u64, 0), response.len);
}

test "storage owner runtime status does not wait behind apply writer" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("storage-owner-runtime-status-busy");
    defer test_tmp.cleanup();
    const path = try tempTestPath(alloc, test_tmp.path(), "db");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
        .storage_owner_table_name = @constCast("docs"),
        .storage_owner_group_id = 7,
    };
    defer handle.db.close();
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);

    handle.db.core.lockApplyExclusive();
    defer handle.db.core.unlockApplyExclusive();
    var response: kernel_owner_abi.OwnedBytes = .{};
    try std.testing.expectEqual(
        kernel_owner_abi.Status.busy,
        storageOwnerRuntimeStatusJson(
            handle_id,
            &.{ .table_name = .fromSlice("docs") },
            &response,
        ),
    );
    try std.testing.expectEqual(@as(u64, 0), response.len);
}

test "lite status marks index inventory unavailable during apply contention" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("lite-index-status-busy");
    defer test_tmp.cleanup();
    const path = try tempTestPath(alloc, test_tmp.path(), "db");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);

    var handle = Handle{ .alloc = std.heap.c_allocator, .db = try db_mod.DB.open(alloc, path, .{}) };
    defer handle.db.close();
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);

    var status: capi.Buffer = .{};
    {
        handle.db.core.lockApplyExclusive();
        defer handle.db.core.unlockApplyExclusive();
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle_id, &status));
        defer antfly_buffer_free(&status);
        try std.testing.expect(std.mem.indexOf(u8, status.ptr.?[0..status.len], "\"indexes_available\":false") != null);
        var stats: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(handle_id, &stats));
        defer antfly_buffer_free(&stats);
        try std.testing.expect(std.mem.indexOf(u8, stats.ptr.?[0..stats.len], "\"indexes_available\":false") != null);
    }

    status = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle_id, &status));
    try std.testing.expect(std.mem.indexOf(u8, status.ptr.?[0..status.len], "\"indexes_available\":true") != null);
    antfly_buffer_free(&status);
}

test "storage owner runtime status distinguishes absent and busy source vectors" {
    const alloc = std.testing.allocator;
    var test_tmp = try TestDirectory.init("storage-owner-source-status-busy");
    defer test_tmp.cleanup();
    for ([_]bool{ false, true }) |with_source| {
        const path = try tempTestPath(alloc, test_tmp.path(), if (with_source) "with-source" else "without-source");
        defer alloc.free(path);
        defer cleanupTestDir(path);
        var handle = Handle{
            .alloc = alloc,
            .db = try db_mod.DB.open(alloc, path, .{
                .table_storage = .{ .dense_embeddings = if (with_source) .vector_store else .primary_lsm },
                .start_index_workers = false,
                .start_optional_runtimes = false,
                .ttl_cleanup = .{ .enabled = false },
            }),
            .storage_owner_table_name = @constCast("docs"),
            .storage_owner_group_id = 7,
        };
        defer handle.db.close();
        const handle_id = try registerTestHandle(&handle);
        defer unregisterTestHandle(handle_id);
        handle.db.backend_runtime.durable_jobs.drainOwner(handle.db.repair_cleanup_owner_id);
        var response: kernel_owner_abi.OwnedBytes = .{};
        if (handle.db.local_execution.source_vectors.load(.acquire)) |source| {
            // Holding the mutex on this thread makes both the missing-field
            // bug and any blocking-lock replacement deterministic.
            while (!source.mutex.tryLock()) antfly.platform_time.yieldBriefly();
            defer source.mutex.unlock();
            try std.testing.expectEqual(kernel_owner_abi.Status.busy, storageOwnerRuntimeStatusJson(
                handle_id,
                &.{ .table_name = .fromSlice("docs") },
                &response,
            ));
            try std.testing.expectEqual(@as(u64, 0), response.len);
            try std.testing.expect(response.ptr == null);
        } else try std.testing.expect(!with_source);
        try std.testing.expectEqual(kernel_owner_abi.Status.ok, storageOwnerRuntimeStatusJson(
            handle_id,
            &.{ .table_name = .fromSlice("docs") },
            &response,
        ));
        defer alloc.free(response.ptr.?[0..@intCast(response.len)]);
        const Status = struct { source_vectors: ?struct { retained_payloads: u64 } = null };
        var parsed = try std.json.parseFromSlice(Status, alloc, response.ptr.?[0..@intCast(response.len)], .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqual(with_source, parsed.value.source_vectors != null);
    }
}

test "capi zero buffer helper wipes bytes before free" {
    var bytes = [_]u8{ 0xaa, 0xbb, 0xcc, 0xdd };
    wipeBufferBytes(.{ .ptr = &bytes, .len = bytes.len });
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0 }, &bytes);
    wipeBufferBytes(.{});

    var empty: capi.Buffer = .{};
    antfly_buffer_free_zero(&empty);
    try std.testing.expect(empty.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}

test "capi SQL reserved receipt preserves every known mutation outcome without allocation" {
    inline for (std.meta.tags(antfly.capi_dependencies.sql_catalog.MutationOutcome)) |outcome| {
        var reserved: ?[]u8 = try std.heap.c_allocator.alloc(u8, 512);
        defer if (reserved) |buffer| std.heap.c_allocator.free(buffer);
        const receipt = embeddedSqlCommitReceipt(&reserved, .{ .command_tag = "DELETE", .rows_affected = std.math.maxInt(u64), .mutation_outcome = outcome });
        defer freeRawBuffer(receipt.ptr, receipt.len);
        try std.testing.expect(reserved == null);
        const decoded = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, receipt.ptr.?[0..receipt.len], .{ .parse_numbers = false });
        defer decoded.deinit();
        try std.testing.expectEqualStrings(@tagName(outcome), decoded.value.object.get("mutation_outcome").?.string);
        try std.testing.expectEqualStrings("18446744073709551615", decoded.value.object.get("rows_affected").?.number_string);
        try std.testing.expect(!decoded.value.object.get("error").?.object.get("retryable").?.bool);
    }
}

test "capi SQL unknown commit receipt retains native transaction identity without allocation" {
    var reserved: ?[]u8 = try std.heap.c_allocator.alloc(u8, 512);
    defer if (reserved) |buffer| std.heap.c_allocator.free(buffer);
    const receipt = embeddedSqlUnknownReceipt(&reserved, @splat(0xab));
    defer freeRawBuffer(receipt.ptr, receipt.len);
    try std.testing.expect(reserved == null);
    const decoded = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, receipt.ptr.?[0..receipt.len], .{});
    defer decoded.deinit();
    try std.testing.expectEqualStrings("abababababababababababababababab", decoded.value.object.get("transaction_id").?.string);
    try std.testing.expectEqualStrings("40003", decoded.value.object.get("error").?.object.get("code").?.string);
    try std.testing.expect(!decoded.value.object.get("error").?.object.get("retryable").?.bool);
}

test "c api bool text parser treats filter clauses as required" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"bool":{"must":[{"term":{"field":"body","term":"invoice"}}],"filter":[{"term":{"field":"tenant","term":"acme"}}]}}
    , .{});
    const query = try parseTextQueryJson(alloc, parsed.value);

    try std.testing.expect(query == .bool_query);
    try std.testing.expectEqual(@as(usize, 2), query.bool_query.must.len);
    try std.testing.expect(query.bool_query.must[0] == .term);
    try std.testing.expectEqualStrings("tenant", query.bool_query.must[0].term.field);
    try std.testing.expectEqualStrings("acme", query.bool_query.must[0].term.term);
    try std.testing.expect(query.bool_query.must[1] == .term);
    try std.testing.expectEqualStrings("body", query.bool_query.must[1].term.field);
    try std.testing.expectEqualStrings("invoice", query.bool_query.must[1].term.term);
}

test "capi get edges json does not double free a non-empty edge slice" {
    // Regression test for graphFreeEdges double-freeing the edges slice
    // GraphIndex.freeEdges already frees (antfly_db_get_edges_json's only
    // caller). std.testing.allocator (a GeneralPurposeAllocator) detects a
    // double free immediately, so this test would have failed loudly before
    // the fix -- the bug otherwise only corrupted the libc heap used by
    // production builds, manifesting later as an unrelated SIGABRT with no
    // panic message. An empty-result query (before any edges exist) freed a
    // zero-length slice, which many allocators no-op, so it never caught this.
    // Exercise the edge cleanup helper with the testing allocator as well as
    // the public C ABI, which uses the C allocator for its handle and results.
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-get-edges-double-free");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const index_config = "{\"name\":\"gr_edges_v1\",\"kind\":\"graph\",\"config_json\":\"{}\"}";
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle_ptr, .{
        .ptr = index_config.ptr,
        .len = index_config.len,
    }));

    // Before any edges exist, getEdges returns an empty slice: freeing it
    // twice never crashed, which is exactly why this bug went unnoticed.
    var empty_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_edges_json(handle_ptr, .{
        .ptr = "gr_edges_v1",
        .len = "gr_edges_v1".len,
    }, .{ .ptr = "doc:edge-source", .len = "doc:edge-source".len }, .{}, 2, &empty_out));
    freeRawBuffer(empty_out.ptr, empty_out.len);

    const source_doc =
        \\{"title":"source","_edges":{"gr_edges_v1":{"links":[{"target":"doc:edge-target","weight":1.0}]}}}
    ;
    const batch_json = "{\"inserts\":{\"doc:edge-source\":" ++ source_doc ++ ",\"doc:edge-target\":{\"title\":\"target\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle_ptr, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle_ptr));

    // Now getEdges returns one real edge. Freeing that non-empty slice twice
    // is a real heap corruption that std.testing.allocator catches.
    {
        const edges = try asHandle(handle_ptr).?.db.getEdges(alloc, "gr_edges_v1", "doc:edge-source", "", .both);
        defer graphFreeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }

    // Read the same non-empty result through the public C ABI.
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_edges_json(handle_ptr, .{
        .ptr = "gr_edges_v1",
        .len = "gr_edges_v1".len,
    }, .{ .ptr = "doc:edge-source", .len = "doc:edge-source".len }, .{}, 2, &out));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(out.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "edge_type") != null);
}

test "capi transaction lifecycle" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const txn_id: [16]u8 = .{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_begin_transaction_with_id(handle_ptr, null, 1_000, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_write_transaction(handle_ptr, null, null, 0, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_resolve_intents(handle_ptr, null, @backingInt(transactions_mod.TxnStatus.committed), 2_000));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_transaction_status(handle_ptr, &txn_id, null));
    var reset_status: u8 = 99;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_transaction_status(handle_ptr, null, &reset_status));
    try std.testing.expectEqual(@as(u8, 0), reset_status);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_commit_version(handle_ptr, &txn_id, null));
    var reset_commit_version: u64 = 99;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_get_commit_version(handle_ptr, null, &reset_commit_version));
    try std.testing.expectEqual(@as(u64, 0), reset_commit_version);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_begin_transaction_with_id(handle_ptr, &txn_id, 1_000, null, 0));

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi", .len = "doc:capi".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_write_transaction(handle_ptr, &txn_id, &writes, writes.len, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_resolve_intents(handle_ptr, &txn_id, @backingInt(transactions_mod.TxnStatus.committed), 2_000));

    var status: u8 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_transaction_status(handle_ptr, &txn_id, &status));
    try std.testing.expectEqual(@as(u8, @backingInt(transactions_mod.TxnStatus.committed)), status);

    var commit_version: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_commit_version(handle_ptr, &txn_id, &commit_version));
    try std.testing.expectEqual(@as(u64, 2_000), commit_version);
}

test "Lite raw rows fail closed during row-policy owner transition" {
    var test_tmp = try TestDirectory.init("capi-row-policy-gate");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "lite-policy-gate");
    defer alloc.free(path);
    cleanupTestDir(path);
    defer cleanupTestDir(path);
    var handle_ptr: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);

    {
        const guard = enterHandle(handle_ptr, .exclusive) orelse return error.TestUnexpectedResult;
        defer guard.leave();
        try guard.handle.db.local_execution.row_policy_gate.beginPreparing(.disabled);
    }

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_batch_json(handle_ptr, .fromSlice("{\"inserts\":{\"a\":{\"x\":1}}}"), &out));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_lookup_json(handle_ptr, .fromSlice("a"), &out));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_get_raw(handle_ptr, .fromSlice("a"), &out));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_scan_json(handle_ptr, .fromSlice("{}"), &out));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"SELECT 1\"}"), &out));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"CREATE TABLE blocked (id INT)\"}"), &out));
    var timestamp: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_get_timestamp(handle_ptr, .fromSlice("a"), &timestamp));
    try std.testing.expect(out.ptr == null);
}

test "capi SQL document mutations preserve undeclared fields and typed null semantics" {
    var directory = try TestDirectory.init("capi-sql-document");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "document");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"},"j":{"default":{"annotated":true}}},"additionalProperties":true}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    var inserted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .fromSlice("{\"inserts\":{\"a\":{\"n\":9007199254740993,\"extra\":true}}}"), &inserted));
    defer freeRawBuffer(inserted.ptr, inserted.len);
    var response: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"SELECT n FROM \\\"default\\\" WHERE _id='a'\"}"), &response));
    defer freeRawBuffer(response.ptr, response.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.ptr.?[0..response.len], .{});
    defer parsed.deinit();
    const rows = parsed.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("9007199254740993", rows[0].array.items[0].string);
    for ([_][]const u8{
        "UPDATE \"default\" SET n=n+1, j='null' WHERE _id='a' RETURNING n,j",
        "INSERT INTO \"default\" (_id,n,j) VALUES ('b',2,NULL) RETURNING n,j",
        "INSERT INTO \"default\" (_id,n) VALUES ('c',2) RETURNING j",
        "UPDATE \"default\" SET j=NULL WHERE _id='a' RETURNING j",
    }) |statement| {
        const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
        defer alloc.free(body);
        var updated: capi.Buffer = .{};
        const status = antfly_db_sql_json(handle, .fromSlice(body), &updated);
        defer freeRawBuffer(updated.ptr, updated.len);
        if (status != .ok) std.debug.print("document mutation error: {s}\n", .{updated.ptr.?[0..updated.len]});
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
    }
    var stored: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle, .fromSlice("a"), &stored));
    defer freeRawBuffer(stored.ptr, stored.len);
    const document = try std.json.parseFromSlice(std.json.Value, alloc, stored.ptr.?[0..stored.len], .{});
    defer document.deinit();
    try std.testing.expectEqual(@as(i64, 9007199254740994), document.value.object.get("n").?.integer);
    try std.testing.expect(document.value.object.get("extra").?.bool);
    try std.testing.expect(!document.value.object.contains("j"));
    // JSON Schema defaults are annotations, not SQL column defaults. Both
    // omitted JSON and explicit SQL NULL stay absent under native semantics.
    for ([_][]const u8{ "b", "c" }) |key| {
        var row: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle, .fromSlice(key), &row));
        defer freeRawBuffer(row.ptr, row.len);
        const value = try std.json.parseFromSlice(std.json.Value, alloc, row.ptr.?[0..row.len], .{});
        defer value.deinit();
        try std.testing.expect(!value.value.object.contains("j"));
    }
    var deleted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"DELETE FROM \\\"default\\\" WHERE _id='a' RETURNING n\"}"), &deleted));
    defer freeRawBuffer(deleted.ptr, deleted.len);
}

test "capi SQL document validation rejects an entire mutation before publication" {
    var directory = try TestDirectory.init("capi-sql-document-validation");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "document");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(
        \\{"version":1,"enforce_types":true,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","minimum":0}},"required":["n"],"additionalProperties":true}}}}
    )));
    var response: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"INSERT INTO \\\"default\\\" (_id,n) VALUES ('valid',1),('invalid',-1) RETURNING n\"}"), &response);
    defer freeRawBuffer(response.ptr, response.len);
    try std.testing.expect(status != .ok);
    var count: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"SELECT COUNT(*) FROM \\\"default\\\"\"}"), &count));
    defer freeRawBuffer(count.ptr, count.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, count.ptr.?[0..count.len], .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("0", parsed.value.object.get("rows").?.array.items[0].array.items[0].string);
}

test "capi SQL mutations fence exact primary bytes with unchanged TTL and schema epochs" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "document", "relational" }) |mode| {
        var directory = try TestDirectory.init("capi-sql-digest-fence");
        defer directory.cleanup();
        var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer database.close();
        const schema = try std.fmt.allocPrint(alloc,
            \\{{"version":1,"storage_mode":"{s}","default_type":"row","ttl":{{"duration":"1s","field":"expires"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"n":{{"type":"integer"}},"expires":{{"type":"datetime"}}}},"additionalProperties":false}}}}}}}}
        , .{mode});
        defer alloc.free(schema);
        try database.setSchemaJson(alloc, schema);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":1,\"expires\":\"2090-01-01T00:00:00Z\"}" }} });
        var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "items" };
        const backend = adapter.backend();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const table = try backend.vtable.resolve(backend.ptr, arena.allocator(), .{ .table = "items" }, .read_write);
        const cursor = (try backend.vtable.open_scan.?(backend.ptr, alloc, table, .{ .fields = &.{"n"}, .include_primary_digest = true, .primary_key = "a", .limit = 1 })).?;
        defer cursor.close(cursor.ptr);
        const page = try cursor.next(cursor.ptr, alloc, 1);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        const before = page.rows[0];
        try std.testing.expect(before.expected_content_digest != null);
        try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"n\":2,\"expires\":\"2090-01-01T00:00:00Z\"}" }} });
        try std.testing.expectEqual(before.version, try database.getTimestamp(alloc, "a"));
        try std.testing.expectError(error.SqlWriteConflict, backend.vtable.mutate(backend.ptr, arena.allocator(), alloc, table, &.{.{ .key = "a", .expected_version = before.version, .expected_content_digest = before.expected_content_digest, .row = null }}));
        try std.testing.expectError(error.PreparedGenerationChanged, database.batch(.{ .schema_version = 2, .deletes = &.{"a"} }));
        const txn = try database.beginTransactionWithId(@splat(97), 1);
        try std.testing.expectError(error.PreparedGenerationChanged, database.writeTransaction(txn, .{ .schema_version = 2, .deletes = &.{"a"} }));
        try database.abortTransaction(txn, 2);
        var retained = (try database.lookup(alloc, "a", .{})).?;
        defer retained.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, retained.json, "\"n\":2") != null);
    }
}

test "capi SQL local integrity coordinator enforces unique arbitration and self FK actions" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-integrity");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 501, .shard_id = 502 } });
    defer database.close();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"parent_fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"],"on_delete":"cascade","on_update":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer","nullable":true}},"additionalProperties":false}}}}
    ;
    try database.setSchemaJson(alloc, schema);
    var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("antfly_local_sources").capi_sql;
    for ([_]struct { statement: []const u8, count: u64 }{
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('p',1,NULL),('c',2,1)", .count = 2 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('skipped',1,NULL) ON CONFLICT (id) DO NOTHING RETURNING id", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('skipped',1,NULL) ON CONFLICT ON CONSTRAINT pk DO NOTHING RETURNING id", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('skipped',1,NULL),('also_skipped',1,NULL) ON CONFLICT DO NOTHING RETURNING id", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,id,parent) VALUES ('new',1,NULL) ON CONFLICT ON CONSTRAINT pk DO UPDATE SET id=3 RETURNING id", .count = 1 },
    }) |case| {
        var compiled = try sql.compiler.compile(alloc, case.statement, .{});
        defer compiled.deinit();
        var result = try sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.output.rows_affected);
    }
    var child = (try database.lookup(alloc, "c", .{})).?;
    defer child.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, child.json, "\"parent\":3") != null);
    var removed = try sql.compiler.compile(alloc, "DELETE FROM rows WHERE _id='p'", .{});
    defer removed.deinit();
    var outcome = try sql.runtime.execute(alloc, adapter.backend(), &removed, &.{}, .{});
    defer outcome.deinit();
    try std.testing.expect(try database.lookup(alloc, "c", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "p", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "new", .{}) == null);
    var orphan = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id,parent) VALUES ('orphan',4,999)", .{});
    defer orphan.deinit();
    try std.testing.expectError(error.ForeignKeyParentMissing, sql.runtime.execute(alloc, adapter.backend(), &orphan, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "orphan", .{}) == null);
    // An absent secondary claim is a commit predicate, not permission to
    // overwrite whichever owner appears after conflict resolution.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const backend = adapter.backend();
    const table = try backend.vtable.resolve(backend.ptr, arena.allocator(), .{ .table = "rows" }, .read_write);
    const row = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"id\":4,\"parent\":null}", .{});
    var mutation: antfly.capi_dependencies.sql_catalog.Mutation = .{ .key = "racer", .row = row, .expected_version = 0 };
    const owners = try backend.vtable.resolve_conflict_owners.?(backend.ptr, arena.allocator(), alloc, table, .{ .constraint_name = "pk" }, &.{mutation});
    try std.testing.expect(owners[0].key == null);
    mutation.conflict_guard = owners[0].guard;
    var winner = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id,parent) VALUES ('winner',4,NULL)", .{});
    defer winner.deinit();
    var won = try sql.runtime.execute(alloc, backend, &winner, &.{}, .{});
    defer won.deinit();
    try std.testing.expectError(error.PreparedReadSetChanged, backend.vtable.mutate(backend.ptr, arena.allocator(), alloc, table, &.{mutation}));
    try std.testing.expect(try database.lookup(alloc, "racer", .{}) == null);
}

test "capi SQL named composite conflicts select one immediate generation and survive reopen" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-named-composite");
    defer directory.cleanup();
    const sql = @import("antfly_local_sources").capi_sql;
    for (0..2) |phase| {
        var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 701, .shard_id = 702 } });
        defer database.close();
        if (phase == 0) try database.setSchemaJson(alloc,
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"Selected Pair","columns":["tenant","id"]},{"name":"equivalent_deferred","columns":["id","tenant"],"deferrable":true},{"name":"n_key","columns":["n"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"tenant":{"type":"integer"},"n":{"type":"integer"}},"additionalProperties":false}}}}
        );
        var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
        if (phase == 0) {
            var seed = try sql.compiler.compile(alloc, "INSERT INTO rows(_id,tenant,id,n) VALUES('original',7,1,10),('other',8,2,20)", .{});
            defer seed.deinit();
            var seeded = try sql.runtime.execute(alloc, adapter.backend(), &seed, &.{}, .{});
            defer seeded.deinit();
        }
        const statement = if (phase == 0)
            "INSERT INTO rows(_id,tenant,id,n) VALUES('proposed',7,1,20) ON CONFLICT ON CONSTRAINT \"Selected Pair\" DO UPDATE SET n=rows.n+excluded.n RETURNING _id,tenant,id,n"
        else
            "INSERT INTO rows(_id,tenant,id,n) VALUES('proposed',7,1,5) ON CONFLICT ON CONSTRAINT \"Selected Pair\" DO UPDATE SET n=rows.n+excluded.n RETURNING _id,tenant,id,n";
        var compiled = try sql.compiler.compile(alloc, statement, .{});
        defer compiled.deinit();
        var result = try sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
        try std.testing.expectEqualStrings("original", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("7", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings("1", result.output.rows[0][2].string);
        try std.testing.expectEqualStrings(if (phase == 0) "30" else "35", result.output.rows[0][3].string);
        try std.testing.expect(try database.lookup(alloc, "proposed", .{}) == null);
        var original = (try database.lookup(alloc, "original", .{})).?;
        defer original.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, original.json, if (phase == 0) "\"n\":30" else "\"n\":35") != null);
        var rejected = try sql.compiler.compile(alloc, "INSERT INTO rows(_id,tenant,id,n) VALUES('invalid',9,3,20) ON CONFLICT ON CONSTRAINT \"Selected Pair\" DO NOTHING", .{});
        defer rejected.deinit();
        try std.testing.expectError(error.UniqueConstraintViolation, sql.runtime.execute(alloc, adapter.backend(), &rejected, &.{}, .{}));
        try std.testing.expect(try database.lookup(alloc, "invalid", .{}) == null);
        var deferred = try sql.compiler.compile(alloc, "INSERT INTO rows(_id,tenant,id,n) VALUES('deferred',9,3,40) ON CONFLICT ON CONSTRAINT equivalent_deferred DO NOTHING", .{});
        defer deferred.deinit();
        try std.testing.expectError(error.DeferrableConflictArbiter, sql.runtime.execute(alloc, adapter.backend(), &deferred, &.{}, .{}));
        try std.testing.expect(try database.lookup(alloc, "deferred", .{}) == null);
    }
}

test "capi SQL native expression partial unique claims reject collisions and arbitrate targetless inserts" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-expression-unique");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 601, .shard_id = 602 } });
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"email_key","keys":[{"expression":{"op":"lower_ascii","args":[{"op":"column","column":"email"}]},"result_type":"string"}],"where":[{"column":"active","op":"eq","value":true}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"active":{"type":"boolean"}},"additionalProperties":false}}}}
    );
    var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("antfly_local_sources").capi_sql;
    for ([_]struct { statement: []const u8, count: u64 }{
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('a','Alice',TRUE)", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip','ALICE',TRUE) ON CONFLICT DO NOTHING", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('b','ALICE',FALSE) ON CONFLICT DO NOTHING", .count = 1 },
        .{ .statement = "UPDATE rows SET active=FALSE WHERE _id='a'", .count = 1 },
        .{ .statement = "UPDATE rows SET active=TRUE WHERE _id='b'", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip2','alice',TRUE),('c','Carol',TRUE),('skip3','CAROL',TRUE) ON CONFLICT DO NOTHING", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('skip4','ALICE',TRUE) ON CONFLICT (lower(email)) WHERE active=TRUE DO NOTHING", .count = 0 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('replace','ALICE',TRUE) ON CONFLICT ((lower(email))) WHERE active=TRUE DO UPDATE SET email='Bob'", .count = 1 },
        .{ .statement = "INSERT INTO rows (_id,email,active) VALUES ('d','Alice',TRUE) ON CONFLICT (lower(email)) WHERE active=TRUE DO NOTHING", .count = 1 },
    }) |case| {
        var compiled = try sql.compiler.compile(alloc, case.statement, .{});
        defer compiled.deinit();
        var result = try sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.output.rows_affected);
    }
    var collision = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,email,active) VALUES ('collision','alice',TRUE)", .{});
    defer collision.deinit();
    try std.testing.expectError(error.UniqueConstraintViolation, sql.runtime.execute(alloc, adapter.backend(), &collision, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "collision", .{}) == null);
    try std.testing.expect(try database.lookup(alloc, "skip3", .{}) == null);
    for ([_][]const u8{
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (upper(email)) WHERE active=TRUE DO NOTHING",
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (lower(email)) DO NOTHING",
        "INSERT INTO rows (_id,email,active) VALUES ('wrong','ALICE',TRUE) ON CONFLICT (lower(email)) WHERE active=FALSE DO NOTHING",
    }) |statement| {
        var wrong = try sql.compiler.compile(alloc, statement, .{});
        defer wrong.deinit();
        try std.testing.expectError(error.ConflictArbiterNotFound, sql.runtime.execute(alloc, adapter.backend(), &wrong, &.{}, .{}));
    }
}

test "capi SQL local integrity refuses partial ownership instead of inventing coverage" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-partial-integrity");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 503, .shard_id = 504 } });
    defer database.close();
    try database.updateRange(.{ .start = "a", .end = "z" });
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
    const sql = @import("antfly_local_sources").capi_sql;
    var compiled = try sql.compiler.compile(alloc, "INSERT INTO rows (_id,id) VALUES ('k',1)", .{});
    defer compiled.deinit();
    try std.testing.expectError(error.UnsupportedSqlExecution, sql.runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{}));
    try std.testing.expect(try database.lookup(alloc, "k", .{}) == null);
}

fn sqlFkRecoveryStatement(handle: ?*anyopaque, statement: []const u8) !void {
    const alloc = std.testing.allocator;
    const request = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(request);
    var response: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice(request), &response);
    defer freeRawBuffer(response.ptr, response.len);
    if (status != .ok) std.debug.print("FK recovery SQL failure {s}: {s}\n", .{ statement, if (response.ptr) |ptr| ptr[0..response.len] else "" });
    try std.testing.expectEqual(capi.ErrorCode.ok, status);
}

test "capi SQL FK publication recovers lost owner replies at every phase" {
    const alloc = std.testing.allocator;
    const fk = @import("antfly_local_sources").capi_sql_fk;
    defer fk.interruptAfterPhaseForTest(null);
    for (0..4) |operation| for (1..6) |phase| {
        std.debug.print("FK recovery operation={d} phase={d}\n", .{ operation, phase });
        var directory = try TestDirectory.init("capi-sql-fk-recovery");
        defer directory.cleanup();
        const path = try tempTestAflitePath(alloc, directory.path(), "database");
        defer alloc.free(path);
        defer cleanupTestFile(path);
        var options: capi.OpenOptions = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
        options.storage_kind = capi.storage_kind_lite;
        var handle: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
        defer if (handle != null) antfly_db_close(handle);
        try sqlFkRecoveryStatement(handle, "CREATE TABLE parents(n BIGINT PRIMARY KEY)");
        try sqlFkRecoveryStatement(handle, "INSERT INTO parents(_id,n) VALUES ('p',1)");
        if (operation != 0) {
            try sqlFkRecoveryStatement(handle, if (operation == 1) "CREATE TABLE children(n BIGINT)" else "CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))");
            try sqlFkRecoveryStatement(handle, "INSERT INTO children(_id,n) VALUES ('c',1)");
        }
        fk.interruptAfterPhaseForTest(@intCast(phase));
        try sqlFkRecoveryStatement(handle, switch (operation) {
            0 => "CREATE TABLE children(n BIGINT,CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n))",
            1 => "ALTER TABLE children ADD CONSTRAINT fk FOREIGN KEY(n) REFERENCES parents(n)",
            2 => "ALTER TABLE children DROP CONSTRAINT fk",
            3 => "DROP TABLE children",
            else => unreachable,
        });
        antfly_db_close(handle);
        handle = null;
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &options, &handle));
        if (operation == 0 or operation == 1) {
            try sqlFkRecoveryStatement(handle, "INSERT INTO children(_id,n) VALUES ('c2',1)");
            try sqlFkRecoveryStatement(handle, "DROP TABLE children");
        }
        try sqlFkRecoveryStatement(handle, "DELETE FROM parents");
    };
}

test "capi SQL RETURNING uses native defaults generated values and versioned preimages" {
    var directory = try TestDirectory.init("capi-sql-returning");
    defer directory.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, directory.path(), "returning");
    defer alloc.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","column_defaults":[{"column":"b","expression":{"op":"literal","type":"integer","value":"2"}}],"generated_columns":[{"column":"total","expression":{"op":"add","args":[{"op":"column","column":"a"},{"op":"column","column":"b"}]}}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"},"total":{"type":"integer"}},"required":["a","b","total"],"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    for ([_]struct { statement: []const u8, expected: []const u8 }{
        .{ .statement = "INSERT INTO \"default\" (_id,a) VALUES ('a',3) RETURNING total", .expected = "5" },
        .{ .statement = "INSERT INTO \"default\" (_id,a) VALUES ('a',1) ON CONFLICT (_id) DO UPDATE SET a=excluded.a+\"default\".a RETURNING total", .expected = "6" },
        .{ .statement = "INSERT INTO \"default\" (_id,a) VALUES ('a',99) ON CONFLICT (_id) DO NOTHING RETURNING total", .expected = "empty" },
        .{ .statement = "INSERT INTO \"default\" (_id,a) VALUES ('a',99) ON CONFLICT DO NOTHING RETURNING total", .expected = "empty" },
        .{ .statement = "UPDATE \"default\" SET a=4 WHERE _id='a' RETURNING \"default\".total", .expected = "6" },
        .{ .statement = "DELETE FROM \"default\" WHERE _id='a' RETURNING total", .expected = "6" },
    }) |case| {
        const request = try std.json.Stringify.valueAlloc(alloc, .{ .statement = case.statement }, .{});
        defer alloc.free(request);
        var response: capi.Buffer = .{};
        const status = antfly_db_sql_json(handle, .fromSlice(request), &response);
        defer freeRawBuffer(response.ptr, response.len);
        if (status != .ok) std.debug.print("SQL RETURNING native failure: {s}\n", .{response.ptr.?[0..response.len]});
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.ptr.?[0..response.len], .{});
        defer parsed.deinit();
        const rows = parsed.value.object.get("rows").?.array.items;
        if (std.mem.eql(u8, case.expected, "empty")) {
            try std.testing.expectEqual(@as(usize, 0), rows.len);
            try std.testing.expectEqual(@as(i64, 0), parsed.value.object.get("rows_affected").?.integer);
            continue;
        }
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings(case.expected, rows[0].array.items[0].string);
    }
    var generated: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"INSERT INTO \\\"default\\\" (a) VALUES (3),(3) RETURNING _id,total\"}"), &generated);
    defer freeRawBuffer(generated.ptr, generated.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, status);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated.ptr.?[0..generated.len], .{});
    defer parsed.deinit();
    const generated_rows = parsed.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), generated_rows.len);
    const first = generated_rows[0].array.items[0].string;
    const second = generated_rows[1].array.items[0].string;
    try std.testing.expectEqual(@as(usize, 32), first.len);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expectEqualStrings("5", generated_rows[0].array.items[1].string);
}

test "capi SQL array mutations retain native cells and returning provenance" {
    var directory = try TestDirectory.init("capi-sql-array-mutations");
    defer directory.cleanup();
    const a = std.testing.allocator;
    const path = try tempTestPath(a, directory.path(), "sql");
    defer a.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"a":{"type":["sql_array","null"],"x-antfly-sql-type":"int64"},"j":{"type":"sql_array","x-antfly-sql-type":"jsonb"},"n":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    const Case = struct { sql: []const u8, parameters: []const std.json.Value = &.{}, lower: ?i64 = null, first: ?[]const u8 = null, whole_null: bool = false };
    for ([_]Case{
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) VALUES ('a','[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL],1) RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "UPDATE \"default\" SET n=2 WHERE _id='a' RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) SELECT 'b',a,j,n FROM \"default\" WHERE _id='a' RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "UPDATE \"default\" SET a='[3:4]={9223372036854775807,NULL}' WHERE _id='b' RETURNING a,j", .lower = 3, .first = "9223372036854775807" },
        .{ .sql = "DELETE FROM \"default\" WHERE _id='b' RETURNING a,j", .lower = 3, .first = "9223372036854775807" },
        .{ .sql = "UPDATE \"default\" SET a=ARRAY[1::smallint,NULL] WHERE _id='a' RETURNING a,j", .lower = 1, .first = "1" },
        .{ .sql = "UPDATE \"default\" SET a=$1 WHERE _id='a' RETURNING a,j", .parameters = &.{.{ .string = "[-1:1]={9007199254740993,NULL,2}" }}, .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) SELECT 'p','[-1:1]={9007199254740993,NULL,2}',ARRAY['null'::jsonb,NULL],1 RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) SELECT 'q',$1,ARRAY['null'::jsonb,NULL],1 RETURNING a,j", .parameters = &.{.{ .string = "[-1:1]={9007199254740993,NULL,2}" }}, .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) SELECT 'r',ARRAY[1::smallint,NULL],ARRAY['null'::jsonb,NULL],1 RETURNING a,j", .lower = 1, .first = "1" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) VALUES ('a',ARRAY[0],ARRAY[NULL::jsonb],9) ON CONFLICT (_id) DO UPDATE SET n=excluded.n RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,a,j,n) VALUES ('a','[3:4]={9223372036854775807,NULL}',ARRAY['null'::jsonb,NULL],9) ON CONFLICT (_id) DO UPDATE SET a=excluded.a,j=excluded.j WHERE \"default\".a='[-1:1]={9007199254740993,NULL,2}'::bigint[] RETURNING a,j", .lower = 3, .first = "9223372036854775807" },
        .{ .sql = "INSERT INTO \"default\" (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a='[-1:1]={9007199254740993,NULL,2}' RETURNING a,j", .lower = -1, .first = "9007199254740993" },
        .{ .sql = "INSERT INTO \"default\" (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=ARRAY[1::smallint,NULL] RETURNING a,j", .lower = 1, .first = "1" },
        .{ .sql = "INSERT INTO \"default\" (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=$1 RETURNING a,j", .parameters = &.{.{ .string = "[-1:1]={9007199254740993,NULL,2}" }}, .lower = -1, .first = "9007199254740993" },
        .{ .sql = "UPDATE \"default\" SET a=NULL WHERE _id='a' RETURNING a,j", .whole_null = true },
        .{ .sql = "SELECT a,j FROM \"default\" WHERE _id='a'", .whole_null = true },
    }) |case| {
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = case.sql, .parameters = case.parameters }, .{});
        defer a.free(request);
        var out: capi.Buffer = .{};
        defer freeRawBuffer(out.ptr, out.len);
        // Exercise the same C-ABI request implementation without erasing the
        // native error return trace behind the public diagnostic envelope.
        try executeEmbeddedSql(asHandle(handle).?, "default", request, &out);
        const response = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
        defer response.deinit();
        const rows = response.value.object.get("rows").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        const values = rows[0].array.items;
        if (case.whole_null) {
            try std.testing.expect(values[0] == .null);
        } else {
            try std.testing.expectEqual(case.lower.?, values[0].object.get("dimensions").?.array.items[0].object.get("lower_bound").?.integer);
            try std.testing.expectEqualStrings(case.first.?, values[0].object.get("values").?.array.items[0].string);
            try std.testing.expect(values[0].object.get("sql_nulls").?.array.items[1].bool);
        }
        const json = values[1].object;
        try std.testing.expect(json.get("values").?.array.items[0] == .null);
        try std.testing.expect(!json.get("sql_nulls").?.array.items[0].bool);
        try std.testing.expect(json.get("sql_nulls").?.array.items[1].bool);
    }
    for ([_]struct { sql: []const u8, state: []const u8, code: capi.ErrorCode = .invalid_argument }{
        .{ .sql = "UPDATE \"default\" SET a=ARRAY[] WHERE _id='a'", .state = "42P18" },
        .{ .sql = "UPDATE \"default\" SET a=ARRAY['1'] WHERE _id='a'", .state = "42804" },
        .{ .sql = "UPDATE \"default\" SET a='{1,2}'::text WHERE _id='a'", .state = "42804" },
        .{ .sql = "INSERT INTO \"default\" (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=(SELECT a FROM \"default\" WHERE _id='q') RETURNING a,j", .state = "0A000", .code = .unsupported },
        .{ .sql = "INSERT INTO \"default\" (_id,n) VALUES ('a',9) ON CONFLICT (_id) DO UPDATE SET a=coalesce((SELECT a FROM \"default\" WHERE _id='q'),excluded.a) RETURNING a,j", .state = "0A000", .code = .unsupported },
    }) |case| {
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = case.sql }, .{});
        defer a.free(request);
        var out: capi.Buffer = .{};
        defer freeRawBuffer(out.ptr, out.len);
        try std.testing.expectEqual(case.code, antfly_db_sql_json(handle, .fromSlice(request), &out));
        const parsed = try std.json.parseFromSlice(std.json.Value, a, out.ptr.?[0..out.len], .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(case.state, parsed.value.object.get("error").?.object.get("code").?.string);
    }
}

test "capi SQL relational search highlight values own captured text and match public JSON" {
    const a = std.testing.allocator;
    const sql = @import("antfly_local_sources").capi_sql;
    var source = std.heap.ArenaAllocator.init(a);
    var source_live = true;
    defer if (source_live) source.deinit();
    const input = source.allocator();
    const spans = try input.dupe(db_mod.types.HighlightSpan, &.{.{ .start = 1, .end = 3 }});
    const fragments = try input.alloc(db_mod.types.HighlightFragment, 2);
    fragments[0] = .{ .text = try input.dupe(u8, "éclair"), .offset = 7, .spans = spans };
    fragments[1] = .{ .text = try input.dupe(u8, "array"), .offset = 12, .item = 2, .spans = spans };
    const fields = try input.alloc(db_mod.types.HighlightedField, 1);
    fields[0] = .{ .field = try input.dupe(u8, "body"), .fragments = fragments };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const expected_value = try antfly.public_api.query.highlightsJsonValue(arena.allocator(), fields);
    const expected = try std.json.Stringify.valueAlloc(a, expected_value.?, .{});
    defer a.free(expected);
    const value = (try sql.highlightsValueLeaky(arena.allocator(), fields)).?;
    source.deinit();
    source_live = false;
    const actual = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(actual);
    try std.testing.expectEqualStrings(expected, actual);
    try std.testing.expect(value.object.get("body").?.array.items[0].object.get("item") == null);
}

test "capi SQL relational search preserves typed nulls and exact JSON numbers" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-search-typed");
    defer directory.cleanup();
    var database = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"},"payload":{"type":"json","nullable":true},"a":{"type":["sql_array","null"],"x-antfly-sql-type":"int64"},"n":{"type":["number","null"],"x-antfly-sql-type":"numeric"}},"additionalProperties":false}}}}
    );
    try database.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    const sql = @import("antfly_local_sources").capi_sql;
    var adapter = sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
    var inserted = try sql.compiler.compile(alloc,
        \\INSERT INTO rows(_id,body,payload,a,n) VALUES ('a','alpha',NULL,ARRAY[9007199254740993::bigint,NULL::bigint],1.2300::numeric),('b','alpha',CAST('null' AS JSONB),NULL,NULL),('c','alpha',CAST('{"amount":9007199254740993.0,"fraction":0.123456789012345678901}' AS JSONB),ARRAY[-7::bigint],9007199254740993.1200::numeric)
    , .{});
    defer inserted.deinit();
    var mutation = try sql.runtime.execute(alloc, adapter.backend(), &inserted, &.{}, .{});
    defer mutation.deinit();
    try database.runUntilIdle();
    var scanned = try sql.compiler.compile(alloc, "SELECT _id,payload,payload IS NULL,payload->>'amount',payload->>'fraction',a,cardinality(a),n FROM rows ORDER BY _id", .{});
    defer scanned.deinit();
    var baseline = try sql.runtime.execute(alloc, adapter.backend(), &scanned, &.{}, .{ .page_rows = 1 });
    defer baseline.deinit();
    var searched = try sql.compiler.compile(alloc, "SELECT _id,payload,payload IS NULL,payload->>'amount',payload->>'fraction',a,cardinality(a),n FROM antfly_search('rows','body:alpha') ORDER BY _id", .{});
    defer searched.deinit();
    for ([_]u32{ 1, 256 }) |page_rows| {
        var hits = try sql.runtime.execute(alloc, adapter.backend(), &searched, &.{}, .{ .page_rows = page_rows });
        defer hits.deinit();
        try std.testing.expectEqual(@as(usize, 3), hits.output.rows.len);
        const expected = try std.json.Stringify.valueAlloc(alloc, .{ .rows = baseline.output.rows, .nulls = baseline.output.sql_nulls }, .{});
        defer alloc.free(expected);
        const actual = try std.json.Stringify.valueAlloc(alloc, .{ .rows = hits.output.rows, .nulls = hits.output.sql_nulls }, .{});
        defer alloc.free(actual);
        try std.testing.expectEqualStrings(expected, actual);
        try std.testing.expect(hits.output.rows[0][2].bool);
        try std.testing.expect(!hits.output.rows[1][2].bool);
        try std.testing.expect(hits.output.rows[0][5] == .object);
        try std.testing.expectEqualStrings("2", hits.output.rows[0][6].string);
        try std.testing.expectEqualStrings("9007199254740993.1200", hits.output.rows[2][7].string);
        try std.testing.expectEqualStrings("9007199254740993", hits.output.rows[2][3].string);
        try std.testing.expectEqualStrings("0.123456789012345678901", hits.output.rows[2][4].string);
    }
    // A delivered ranked page owns its identities, layouts and values after
    // both the ranked cursor and its primary snapshot have been released.
    var region = std.heap.ArenaAllocator.init(alloc);
    defer region.deinit();
    const a = region.allocator();
    const backend = adapter.backend();
    const relation = searched.statement.select.source.?.table;
    const table = try backend.vtable.resolve(backend.ptr, a, relation.name, .read);
    const request: @import("antfly_local_sources").sql_catalog.Scan.Search = .{ .expression = relation.search.?.*, .request_text = "body:alpha" };
    const statement = try backend.vtable.open_statement.?(backend.ptr, alloc, &.{.{ .table = table, .request = .{ .fields = &.{ "a", "n", "payload" }, .search = &request, .limit = 1 } }});
    var live = true;
    defer if (live) statement.close(statement.ptr);
    const cursor = statement.cursors[0];
    const page = try cursor.next(cursor.ptr, alloc, 1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    const expected_id = try a.dupe(u8, page.rows[0].id);
    const before = try page.rows[0].cell("a");
    const expected_first: ?i64 = if (before.array) |array| array.flatCell(0).value.integer else null;
    statement.close(statement.ptr);
    live = false;
    try std.testing.expectEqualStrings(expected_id, page.rows[0].id);
    const after = try page.rows[0].cell("a");
    if (expected_first) |value| {
        try std.testing.expect(after.array.?.stored != null);
        try std.testing.expectEqual(value, after.array.?.flatCell(0).value.integer);
    } else try std.testing.expect(after.sql_null);
}

test "capi SQL uses native typed snapshots and atomic mutations" {
    var test_tmp = try TestDirectory.init("capi-sql");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "sql");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"j":{"type":"json"}},"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle_ptr, .fromSlice(schema_json)));
    const requests = [_][]const u8{
        \\{"statement":"INSERT INTO \"default\" (_id,id) VALUES ('a',$1)","parameters":["9007199254740993"]}
        ,
        \\{"statement":"SELECT id FROM \"default\" WHERE _id='a'"}
        ,
        \\{"statement":"UPDATE \"default\" SET id=id+1 WHERE _id='a'"}
        ,
        \\{"statement":"SELECT id FROM \"default\" WHERE _id='a'"}
        ,
    };
    for (requests, 0..) |request, index| {
        var out: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice(request), &out));
        defer freeRawBuffer(out.ptr, out.len);
        if (index == 1) try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "9007199254740993") != null);
        if (index == 3) try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "9007199254740994") != null);
    }
    var joined: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"SELECT l.id, r.id FROM \\\"default\\\" AS l JOIN \\\"default\\\" AS r ON l.id=r.id\"}"), &joined));
    defer freeRawBuffer(joined.ptr, joined.len);
    const joined_json = try std.json.parseFromSlice(std.json.Value, alloc, joined.ptr.?[0..joined.len], .{});
    defer joined_json.deinit();
    const joined_rows = joined_json.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), joined_rows.len);
    try std.testing.expectEqualStrings("9007199254740994", joined_rows[0].array.items[0].string);
    try std.testing.expectEqualStrings("9007199254740994", joined_rows[0].array.items[1].string);
    var inserted_json_null: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"INSERT INTO \\\"default\\\" (_id,id,j) VALUES ('b',2,CAST('null' AS json))\"}"), &inserted_json_null));
    defer freeRawBuffer(inserted_json_null.ptr, inserted_json_null.len);
    var null_projection: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"SELECT j FROM \\\"default\\\" ORDER BY _id\"}"), &null_projection));
    defer freeRawBuffer(null_projection.ptr, null_projection.len);
    const projected = try std.json.parseFromSlice(std.json.Value, alloc, null_projection.ptr.?[0..null_projection.len], .{});
    defer projected.deinit();
    const projected_rows = projected.value.object.get("rows").?.array.items;
    const projected_nulls = projected.value.object.get("sql_nulls").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), projected_rows.len);
    try std.testing.expect(projected_rows[0].array.items[0] == .null);
    try std.testing.expect(projected_rows[1].array.items[0] == .null);
    try std.testing.expect(projected_nulls[0].array.items[0].bool);
    try std.testing.expect(!projected_nulls[1].array.items[0].bool);
    var rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.unsupported, antfly_db_sql_json(handle_ptr, .fromSlice("{\"statement\":\"BEGIN\"}"), &rejected));
    defer freeRawBuffer(rejected.ptr, rejected.len);
    try std.testing.expect(std.mem.indexOf(u8, rejected.ptr.?[0..rejected.len], "0A000") != null);
}

test "capi batch and lookup json" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-batch-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const writes = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-batch", .len = "doc:capi-batch".len },
            .value = .{ .ptr = "{\"title\":\"ok\"}", .len = "{\"title\":\"ok\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(handle_ptr, &writes, writes.len, null, 0, 1_000, 0));

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle_ptr, .{
        .ptr = "doc:capi-batch",
        .len = "doc:capi-batch".len,
    }, &out));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"title\":\"ok\"") != null);

    const batch_json = "{\"inserts\":{\"doc:capi-batch-json\":{\"title\":\"json path\"}},\"sync_level\":\"write\"}";
    var batch_json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle_ptr, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_json_out));
    defer freeRawBuffer(batch_json_out.ptr, batch_json_out.len);
    try std.testing.expect(std.mem.indexOf(u8, batch_json_out.ptr.?[0..batch_json_out.len], "\"inserted\":1") != null);

    var json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(handle_ptr, .{
        .ptr = "doc:capi-batch-json",
        .len = "doc:capi-batch-json".len,
    }, &json_out));
    defer freeRawBuffer(json_out.ptr, json_out.len);
    try std.testing.expect(std.mem.indexOf(u8, json_out.ptr.?[0..json_out.len], "\"title\":\"json path\"") != null);

    var invalid_json_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch_json(handle_ptr, .{
        .ptr = "{".ptr,
        .len = 1,
    }, &invalid_json_out));
}

test "capi lite opens exports imports checks and vacuums aflite" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const plain_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-plain");
    defer alloc.free(plain_path);
    const invalid_lite_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-invalid");
    defer alloc.free(invalid_lite_path);
    const missing_readonly_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-missing-readonly");
    defer alloc.free(missing_readonly_path);
    const missing_status_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-missing-status");
    defer alloc.free(missing_status_path);
    const short_lite_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-short");
    defer alloc.free(short_lite_path);
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-src");
    defer alloc.free(src_path);
    const remote_inference_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-remote-inference");
    defer alloc.free(remote_inference_path);
    const local_inference_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-local-inference");
    defer alloc.free(local_inference_path);
    const dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-dst");
    defer alloc.free(dst_path);
    const bad_dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-bad-dst");
    defer alloc.free(bad_dst_path);
    const schema_dst_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-schema-dst");
    defer alloc.free(schema_dst_path);
    const snapshot_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-snapshot");
    defer alloc.free(snapshot_path);
    const snapshot_file_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-snapshot-file");
    defer alloc.free(snapshot_file_path);
    const pinned_snapshot_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-pinned-snapshot");
    defer alloc.free(pinned_snapshot_path);
    const restore_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore");
    defer alloc.free(restore_path);
    const restore_alias_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-alias");
    defer alloc.free(restore_alias_path);
    const restore_unknown_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-outcome-unknown");
    defer alloc.free(restore_unknown_path);
    const locked_restore_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-locked");
    defer alloc.free(locked_restore_path);
    const restore_malformed_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-restore-malformed");
    defer alloc.free(restore_malformed_path);
    const invalid_snapshot_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-snapshot-invalid");
    defer alloc.free(invalid_snapshot_path);
    const invalid_snapshot_file_path = try tempTestPath(alloc, test_tmp.path(), "capi-lite-snapshot-file-invalid");
    defer alloc.free(invalid_snapshot_file_path);
    cleanupTestDir(plain_path);
    cleanupTestFile(invalid_lite_path);
    cleanupTestFile(missing_readonly_path);
    cleanupTestFile(missing_status_path);
    cleanupTestFile(short_lite_path);
    cleanupTestFile(src_path);
    cleanupTestFile(remote_inference_path);
    cleanupTestFile(local_inference_path);
    cleanupTestFile(dst_path);
    cleanupTestFile(bad_dst_path);
    cleanupTestFile(schema_dst_path);
    cleanupTestFile(snapshot_path);
    cleanupTestFile(snapshot_file_path);
    cleanupTestFile(pinned_snapshot_path);
    cleanupTestFile(restore_path);
    cleanupTestFile(restore_alias_path);
    cleanupTestFile(restore_unknown_path);
    cleanupTestFile(locked_restore_path);
    cleanupTestFile(restore_malformed_path);
    cleanupTestFile(invalid_snapshot_path);
    cleanupTestFile(invalid_snapshot_file_path);
    defer cleanupTestDir(plain_path);
    defer cleanupTestFile(invalid_lite_path);
    defer cleanupTestFile(missing_readonly_path);
    defer cleanupTestFile(missing_status_path);
    defer cleanupTestFile(short_lite_path);
    defer cleanupTestFile(src_path);
    defer cleanupTestFile(remote_inference_path);
    defer cleanupTestFile(local_inference_path);
    defer cleanupTestFile(dst_path);
    defer cleanupTestFile(bad_dst_path);
    defer cleanupTestFile(schema_dst_path);
    defer cleanupTestFile(snapshot_path);
    defer cleanupTestFile(snapshot_file_path);
    defer cleanupTestFile(pinned_snapshot_path);
    defer cleanupTestFile(restore_path);
    defer cleanupTestFile(restore_alias_path);
    defer cleanupTestFile(restore_unknown_path);
    defer cleanupTestFile(locked_restore_path);
    defer cleanupTestFile(restore_malformed_path);
    defer cleanupTestFile(invalid_snapshot_path);
    defer cleanupTestFile(invalid_snapshot_file_path);

    try std.testing.expectEqual(public.abi_version, antfly_abi_version());
    try std.testing.expectEqualStrings("ANTFLY_OK", std.mem.span(antfly_error_code_name(@backingInt(capi.ErrorCode.ok))));
    try std.testing.expectEqualStrings("ANTFLY_INVALID_ARGUMENT", std.mem.span(antfly_error_code_name(@backingInt(capi.ErrorCode.invalid_argument))));
    try std.testing.expectEqualStrings("ANTFLY_OUTCOME_UNKNOWN", std.mem.span(antfly_error_code_name(@backingInt(capi.ErrorCode.outcome_unknown))));
    try std.testing.expectEqualStrings("ANTFLY_UNSUPPORTED", std.mem.span(antfly_error_code_name(@backingInt(capi.ErrorCode.unsupported))));
    try std.testing.expectEqualStrings("ANTFLY_UNKNOWN_ERROR", std.mem.span(antfly_error_code_name(12345)));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(antfly_error_code_description(@backingInt(capi.ErrorCode.busy))), "retry") != null);
    try std.testing.expectEqualStrings("unknown Antfly error code", std.mem.span(antfly_error_code_description(12345)));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.FileBusy));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.WriterLocked));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.SourceFileChanged));
    try std.testing.expectEqual(capi.ErrorCode.busy, capi.mapError(error.PortableRuntimeActivationPending));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, capi.mapError(error.FileLocksUnsupported));
    try std.testing.expectEqual(capi.ErrorCode.unsupported, capi.mapError(error.GenerationFileLocksUnsupported));
    try std.testing.expectEqual(capi.ErrorCode.not_found, capi.mapError(error.NotFound));
    try std.testing.expectEqual(capi.ErrorCode.txn_not_found, capi.mapError(error.TxnNotFound));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.TruncatedNativeHeader));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.UnsupportedNativeFormatVersion));
    try std.testing.expectEqual(capi.ErrorCode.outcome_unknown, capi.mapError(error.DurabilityOutcomeUnknown));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.InvalidBackupManifest));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, capi.mapError(error.BackupArtifactIntegrityMismatch));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(src_path, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_create(src_path, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(src_path, null, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_create_with_options(src_path, null, null));
    var null_path_sentinel: u8 = 0;
    var null_path_handle: ?*anyopaque = &null_path_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(null, &null_path_handle));
    try std.testing.expect(null_path_handle == null);

    var plain_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(plain_path, &plain_handle));
    defer antfly_db_close(plain_handle);
    var scratch: [1]u8 = .{0xaa};
    // Status, capabilities, backup, and drains work for directory storage
    // as well as .aflite files.
    var dir_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(plain_handle, &dir_caps));
    try std.testing.expect(std.mem.indexOf(u8, dir_caps.ptr.?[0..dir_caps.len], "\"threading\":\"serialized\"") != null);
    antfly_buffer_free(&dir_caps);
    var dir_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(plain_handle, &dir_status));
    try std.testing.expect(std.mem.indexOf(u8, dir_status.ptr.?[0..dir_status.len], "\"format\":\"directory\"") != null);
    antfly_buffer_free(&dir_status);
    var dir_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(plain_handle, &dir_backup));
    try std.testing.expect(dir_backup.len > 0);
    antfly_buffer_free(&dir_backup);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(plain_handle));
    var dir_idle: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(plain_handle, &dir_idle));
    antfly_buffer_free(&dir_idle);
    var dir_pending: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(plain_handle, &dir_pending));
    antfly_buffer_free(&dir_pending);
    // A null output buffer is still rejected.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(plain_handle, null));
    var invalid_lite_sentinel: u8 = 0;
    var invalid_lite_handle: ?*anyopaque = &invalid_lite_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open(invalid_lite_path, &invalid_lite_handle));
    try std.testing.expect(invalid_lite_handle == null);
    defer antfly_db_close(invalid_lite_handle);

    try std.testing.expect(!testPathExists(src_path));
    var missing_writer_sentinel: u8 = 0;
    var missing_writer_handle: ?*anyopaque = &missing_writer_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open(src_path, &missing_writer_handle));
    try std.testing.expect(missing_writer_handle == null);
    try std.testing.expect(!testPathExists(src_path));
    defer antfly_db_close(missing_writer_handle);

    try std.testing.expect(!testPathExists(missing_readonly_path));
    var missing_readonly_sentinel: u8 = 0;
    var missing_readonly_handle: ?*anyopaque = &missing_readonly_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open_readonly(missing_readonly_path, &missing_readonly_handle));
    try std.testing.expect(missing_readonly_handle == null);
    try std.testing.expect(!testPathExists(missing_readonly_path));
    defer antfly_db_close(missing_readonly_handle);

    try std.testing.expect(!testPathExists(missing_status_path));
    var missing_status_sentinel: u8 = 0;
    var missing_status_handle: ?*anyopaque = &missing_status_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.not_found, antfly_lite_open_status_only(missing_status_path, &missing_status_handle));
    try std.testing.expect(missing_status_handle == null);
    try std.testing.expect(!testPathExists(missing_status_path));
    defer antfly_db_close(missing_status_handle);

    {
        var short_file = try std.Io.Dir.cwd().createFile(std.testing.io, short_lite_path, .{});
        defer short_file.close(std.testing.io);
        try short_file.writePositionalAll(std.testing.io, "short native lite header", 0);
    }
    var short_lite_sentinel: u8 = 0;
    var short_lite_handle: ?*anyopaque = &short_lite_sentinel;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_open_readonly(short_lite_path, &short_lite_handle));
    try std.testing.expect(short_lite_handle == null);
    defer antfly_db_close(short_lite_handle);

    var short_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_file_json(short_lite_path, &short_check));
    defer freeRawBuffer(short_check.ptr, short_check.len);
    const short_check_json = short_check.ptr.?[0..short_check.len];
    try std.testing.expect(std.mem.indexOf(u8, short_check_json, "\"valid\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, short_check_json, "\"issue\":\"truncated_header\"") != null);

    var src_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src_handle));
    defer antfly_db_close(src_handle);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_capabilities_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_backup(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(src_path, null));
    var null_check_file_path: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(null, &null_check_file_path));
    try std.testing.expect(null_check_file_path.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_check_file_path.len);
    var invalid_check_file_path: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_check_file_json(plain_path, &invalid_check_file_path));
    try std.testing.expect(invalid_check_file_path.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_check_file_path.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, snapshot_path, false, null));
    var null_snapshot_dest: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, null, false, &null_snapshot_dest));
    try std.testing.expect(null_snapshot_dest.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_dest.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, null));
    var null_snapshot_file_src: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(null, snapshot_file_path, false, &null_snapshot_file_src));
    try std.testing.expect(null_snapshot_file_src.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_file_src.len);
    var null_snapshot_file_dest: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, null, false, &null_snapshot_file_dest));
    try std.testing.expect(null_snapshot_file_dest.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), null_snapshot_file_dest.len);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_compact_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_vacuum_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_run_until_idle_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_replay_generated_enrichments_json(src_handle, null));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_pending_work_stats_json(src_handle, null));

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(src_handle, &status));
    const status_json = status.ptr.?[0..status.len];
    const native_local_runtime_available = lite_backend.capabilitiesForProfile(.native).local_inference_runtime;
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"storage\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"format\":\"aflite\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"engine\":\"native_single_file\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"primary_layout\":\"native_document_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"replay_layout\":\"native_replay_lanes_in_document_catalog\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"native_index_catalog_pages\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_layout\":\"lsm") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"index_namespace\":\"__antfly_lite\"") != null);
    const format_version = blk: {
        // Enter before taking the artificial lock: entry recovers SQL state
        // and legitimately needs a read transaction. Storage status itself
        // must remain available without opening a competing external reader.
        const guard = public.enterHandle(src_handle, .read) orelse return error.InvalidHandle;
        defer guard.leave();
        try std.testing.expect(guard.entry_error == null);
        var held = try std.Io.Dir.cwd().openFile(std.testing.io, src_path, .{ .mode = .read_write, .lock = .exclusive });
        defer held.close(std.testing.io);
        break :blk guard.handle.owned_lite_backend.?.storageStatus().format_version.?;
    };
    const expected_format_version = try std.fmt.allocPrint(alloc, "\"format_version\":{d}", .{format_version});
    defer alloc.free(expected_format_version);
    try std.testing.expect(std.mem.indexOf(u8, status_json, expected_format_version) != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"page_size\":4096") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"active_checkpoint\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"stats\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"pending_work\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"local_runtime_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, if (native_local_runtime_available) "\"local_runtime_available\":true" else "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"capabilities\":") != null);
    antfly_buffer_free_zero(&status);
    try std.testing.expect(status.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), status.len);

    var remote_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_remote_provider_configured,
    };
    var remote_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(remote_inference_path, &remote_options, &remote_handle));
    defer antfly_db_close(remote_handle);
    var remote_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(remote_handle, &remote_status));
    defer freeRawBuffer(remote_status.ptr, remote_status.len);
    const remote_status_json = remote_status.ptr.?[0..remote_status.len];
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"mode\":\"remote_provider\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"remote_provider_configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, remote_status_json, "\"local_runtime_configured\":false") != null);

    var local_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var local_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(local_inference_path, &local_options, &local_handle));
    defer antfly_db_close(local_handle);
    var local_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(local_handle, &local_status));
    defer freeRawBuffer(local_status.ptr, local_status.len);
    const local_status_json = local_status.ptr.?[0..local_status.len];
    if (native_local_runtime_available) {
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"mode\":\"local_embedded\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"configured\":true") != null);
    } else {
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"mode\":\"caller_supplied_or_disabled\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"configured\":false") != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"remote_provider_configured\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"local_runtime_configured\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"local_runtime_available\":true" else "\"local_runtime_available\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, "\"capabilities\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"inference_mode\":\"local_embedded\"" else "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_status_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);

    var capabilities: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(src_handle, &capabilities));
    defer freeRawBuffer(capabilities.ptr, capabilities.len);
    const capabilities_json = capabilities.ptr.?[0..capabilities.len];
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"manual_maintenance\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"dense_vector_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"sparse_vector_search\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"no_inference_configured_ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"caller_supplied_artifacts\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"raft_replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_placement\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cross_node_joins\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"remote_shard_fanout\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"distributed_transaction_coordination\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, capabilities_json, "\"cluster_heartbeat_status_aggregation\":false") != null);

    var local_capabilities: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(local_handle, &local_capabilities));
    defer freeRawBuffer(local_capabilities.ptr, local_capabilities.len);
    const local_capabilities_json = local_capabilities.ptr.?[0..local_capabilities.len];
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"inference_mode\":\"local_embedded\"" else "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"available_inference_modes\":[\"caller_supplied_artifacts\",\"remote_provider\",\"local_embedded\",\"disabled_deferred\"]" else "\"available_inference_modes\":[\"caller_supplied_artifacts\",\"remote_provider\",\"disabled_deferred\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, local_capabilities_json, if (native_local_runtime_available) "\"local_inference_runtime\":true" else "\"local_inference_runtime\":false") != null);

    var pending: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(src_handle, &pending));
    defer freeRawBuffer(pending.ptr, pending.len);
    const pending_json = pending.ptr.?[0..pending.len];
    try std.testing.expect(std.mem.indexOf(u8, pending_json, "\"has_async_indexes\":") != null);

    var idle: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(src_handle, &idle));
    defer freeRawBuffer(idle.ptr, idle.len);
    const idle_json = idle.ptr.?[0..idle.len];
    try std.testing.expect(std.mem.indexOf(u8, idle_json, "\"derived_target_sequence\":") != null);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src_handle));

    var replayed: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_replay_generated_enrichments_json(src_handle, &replayed));
    defer freeRawBuffer(replayed.ptr, replayed.len);
    const replayed_json = replayed.ptr.?[0..replayed.len];
    try std.testing.expect(std.mem.indexOf(u8, replayed_json, "\"replayed\":") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_index(src_handle, .{
        .ptr = "missing-index",
        .len = "missing-index".len,
    }, null));
    var missing_index_deleted = true;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_delete_index(src_handle, .{
        .ptr = "missing-index",
        .len = "missing-index".len,
    }, &missing_index_deleted));
    try std.testing.expect(!missing_index_deleted);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(src_handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));

    var loaded_schema: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_schema_json(src_handle, &loaded_schema));
    defer freeRawBuffer(loaded_schema.ptr, loaded_schema.len);
    try std.testing.expectEqualStrings(schema_json, loaded_schema.ptr.?[0..loaded_schema.len]);

    const enrichment_json =
        \\{"name":"body_chunks_v1","kind":"chunk","field":"body","chunk_size":8,"chunk_overlap":2}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(src_handle, .{
        .ptr = enrichment_json,
        .len = enrichment_json.len,
    }));

    const scratch_enrichment_json =
        \\{"name":"scratch_chunks_v1","kind":"chunk","field":"scratch","chunk_size":4,"chunk_overlap":1}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(src_handle, .{
        .ptr = scratch_enrichment_json,
        .len = scratch_enrichment_json.len,
    }));

    var enrichments: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_enrichments_json(src_handle, &enrichments));
    defer freeRawBuffer(enrichments.ptr, enrichments.len);
    try std.testing.expect(std.mem.indexOf(u8, enrichments.ptr.?[0..enrichments.len], "\"body_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, enrichments.ptr.?[0..enrichments.len], "\"chunk_size\":8") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "chunk",
        .len = "chunk".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, null));
    var invalid_enrichment_deleted = true;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "unknown",
        .len = "unknown".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, &invalid_enrichment_deleted));
    try std.testing.expect(!invalid_enrichment_deleted);

    var deleted_enrichment = false;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_delete_enrichment(src_handle, .{
        .ptr = "chunk",
        .len = "chunk".len,
    }, .{
        .ptr = "scratch_chunks_v1",
        .len = "scratch_chunks_v1".len,
    }, &deleted_enrichment));
    try std.testing.expect(deleted_enrichment);

    const writes_a = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-lite", .len = "doc:capi-lite".len },
            .value = .{ .ptr = "{\"title\":\"first\"}", .len = "{\"title\":\"first\"}".len },
            .is_delete = false,
        },
        .{
            .key = .{ .ptr = "doc:gone", .len = "doc:gone".len },
            .value = .{ .ptr = "{\"title\":\"remove\"}", .len = "{\"title\":\"remove\"}".len },
            .is_delete = false,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &writes_a, writes_a.len, null, 0, 1_000, 0));

    const writes_b = [_]capi.WriteIntent{
        .{
            .key = .{ .ptr = "doc:capi-lite", .len = "doc:capi-lite".len },
            .value = .{ .ptr = "{\"title\":\"second\"}", .len = "{\"title\":\"second\"}".len },
            .is_delete = false,
        },
        .{
            .key = .{ .ptr = "doc:gone", .len = "doc:gone".len },
            .value = .{},
            .is_delete = true,
        },
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &writes_b, writes_b.len, null, 0, 2_000, 0));

    const lite_txn_id: [16]u8 = .{ 0x6c, 0x69, 0x74, 0x65, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_begin_transaction_with_id(src_handle, &lite_txn_id, 3_000, null, 0));
    const txn_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-lite-txn", .len = "doc:capi-lite-txn".len },
        .value = .{ .ptr = "{\"title\":\"transactional\"}", .len = "{\"title\":\"transactional\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_write_transaction(src_handle, &lite_txn_id, &txn_writes, txn_writes.len, null, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_resolve_intents(src_handle, &lite_txn_id, @backingInt(transactions_mod.TxnStatus.committed), 4_000));
    var lite_txn_status: u8 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_transaction_status(src_handle, &lite_txn_id, &lite_txn_status));
    try std.testing.expectEqual(@as(u8, @backingInt(transactions_mod.TxnStatus.committed)), lite_txn_status);
    var lite_txn_commit_version: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_commit_version(src_handle, &lite_txn_id, &lite_txn_commit_version));
    try std.testing.expectEqual(@as(u64, 4_000), lite_txn_commit_version);
    var lite_txn_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(src_handle, .{
        .ptr = "doc:capi-lite-txn",
        .len = "doc:capi-lite-txn".len,
    }, &lite_txn_lookup));
    defer freeRawBuffer(lite_txn_lookup.ptr, lite_txn_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, lite_txn_lookup.ptr.?[0..lite_txn_lookup.len], "\"transactional\"") != null);

    const pinned_seed_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-before\"}", .len = "{\"title\":\"pinned-before\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_seed_writes, pinned_seed_writes.len, null, 0, 4_100, 0));

    var concurrent_readonly_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(src_path, &concurrent_readonly_handle));
    defer antfly_db_close(concurrent_readonly_handle);
    try std.testing.expectEqual(db_mod.OpenOptions.OpenMode.query_readonly, asHandle(concurrent_readonly_handle).?.open_mode);
    var second_writer_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open(src_path, &second_writer_handle));
    defer antfly_db_close(second_writer_handle);
    var concurrent_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(concurrent_readonly_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &concurrent_lookup));
    defer freeRawBuffer(concurrent_lookup.ptr, concurrent_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, concurrent_lookup.ptr.?[0..concurrent_lookup.len], "\"second\"") != null);

    const pinned_advance_a = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-after-a\"}", .len = "{\"title\":\"pinned-after-a\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_advance_a, pinned_advance_a.len, null, 0, 4_200, 0));
    const pinned_advance_b = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-pinned", .len = "doc:capi-pinned".len },
        .value = .{ .ptr = "{\"title\":\"pinned-after-b\"}", .len = "{\"title\":\"pinned-after-b\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src_handle, &pinned_advance_b, pinned_advance_b.len, null, 0, 4_300, 0));

    var pinned_snapshot_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_json(concurrent_readonly_handle, pinned_snapshot_path, false, &pinned_snapshot_report));
    defer freeRawBuffer(pinned_snapshot_report.ptr, pinned_snapshot_report.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_report.ptr.?[0..pinned_snapshot_report.len], "\"tail_bytes\":") != null);

    var pinned_snapshot_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(pinned_snapshot_path, &pinned_snapshot_handle));
    defer antfly_db_close(pinned_snapshot_handle);
    var pinned_snapshot_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(pinned_snapshot_handle, &pinned_snapshot_check));
    defer freeRawBuffer(pinned_snapshot_check.ptr, pinned_snapshot_check.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_check.ptr.?[0..pinned_snapshot_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_check.ptr.?[0..pinned_snapshot_check.len], "\"tail_bytes\":0") != null);

    var pinned_snapshot_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(pinned_snapshot_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &pinned_snapshot_lookup));
    defer freeRawBuffer(pinned_snapshot_lookup.ptr, pinned_snapshot_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_snapshot_lookup.ptr.?[0..pinned_snapshot_lookup.len], "\"pinned-after-b\"") != null);

    var pinned_writer_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(src_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &pinned_writer_lookup));
    defer freeRawBuffer(pinned_writer_lookup.ptr, pinned_writer_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, pinned_writer_lookup.ptr.?[0..pinned_writer_lookup.len], "\"pinned-after-b\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(concurrent_readonly_handle, &writes_a, 1, null, 0, 4_500, 0));

    var concurrent_status_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_status_only(src_path, &concurrent_status_handle));
    defer antfly_db_close(concurrent_status_handle);
    try std.testing.expectEqual(db_mod.OpenOptions.OpenMode.status_only, asHandle(concurrent_status_handle).?.open_mode);
    var concurrent_status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(concurrent_status_handle, &concurrent_status));
    defer freeRawBuffer(concurrent_status.ptr, concurrent_status.len);
    try std.testing.expect(std.mem.indexOf(u8, concurrent_status.ptr.?[0..concurrent_status.len], "\"doc_count\":") != null);
    // Every fresh Lite database now carries the default full-text index,
    // whose maintenance for the writes above runs in the background. Bring
    // that work to idle before the vacuum and the physical-tail probes that
    // follow: the online vacuum waits its turn for the writer slot, but the
    // junk bytes appended below are only a stable tail if no later
    // checkpoint extends the file past them.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src_handle));
    var online_vacuum: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_vacuum_json(src_handle, &online_vacuum));
    defer freeRawBuffer(online_vacuum.ptr, online_vacuum.len);
    try std.testing.expect(online_vacuum.len > 0);
    var retired_reader_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(concurrent_readonly_handle, .{
        .ptr = "doc:capi-pinned",
        .len = "doc:capi-pinned".len,
    }, &retired_reader_lookup));
    defer freeRawBuffer(retired_reader_lookup.ptr, retired_reader_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, retired_reader_lookup.ptr.?[0..retired_reader_lookup.len], "\"pinned-after-b\"") != null);
    antfly_db_close(concurrent_status_handle);
    concurrent_status_handle = null;
    antfly_db_close(concurrent_readonly_handle);
    concurrent_readonly_handle = null;

    var check_before: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(src_handle, &check_before));
    defer freeRawBuffer(check_before.ptr, check_before.len);
    try std.testing.expect(std.mem.indexOf(u8, check_before.ptr.?[0..check_before.len], "\"valid\":true") != null);

    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        var file = try std.Io.Dir.cwd().openFile(io, src_path, .{ .mode = .read_write });
        defer file.close(io);
        const source_size = (try file.stat(io)).size;
        try file.writePositionalAll(io, "tail", source_size);
    }

    var invalid_snapshot_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_json(src_handle, invalid_snapshot_path, false, &invalid_snapshot_report));
    try std.testing.expect(invalid_snapshot_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_snapshot_report.len);

    var snapshot_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_json(src_handle, snapshot_path, false, &snapshot_report));
    defer freeRawBuffer(snapshot_report.ptr, snapshot_report.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_report.ptr.?[0..snapshot_report.len], "\"tail_bytes\":4") != null);

    var snapshot_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(snapshot_path, &snapshot_handle));
    defer antfly_db_close(snapshot_handle);

    var snapshot_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(snapshot_handle, &snapshot_check));
    defer freeRawBuffer(snapshot_check.ptr, snapshot_check.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_check.ptr.?[0..snapshot_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_check.ptr.?[0..snapshot_check.len], "\"tail_bytes\":0") != null);

    var snapshot_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(snapshot_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &snapshot_lookup));
    defer freeRawBuffer(snapshot_lookup.ptr, snapshot_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_lookup.ptr.?[0..snapshot_lookup.len], "\"second\"") != null);

    var invalid_snapshot_file_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, invalid_snapshot_file_path, false, &invalid_snapshot_file_report));
    try std.testing.expect(invalid_snapshot_file_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), invalid_snapshot_file_report.len);

    var snapshot_file_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, &snapshot_file_report));
    defer freeRawBuffer(snapshot_file_report.ptr, snapshot_file_report.len);
    if (std.mem.indexOf(u8, snapshot_file_report.ptr.?[0..snapshot_file_report.len], "\"tail_bytes\":4") == null)
        std.debug.print("stable snapshot file report: {s}\n", .{snapshot_file_report.ptr.?[0..snapshot_file_report.len]});
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_report.ptr.?[0..snapshot_file_report.len], "\"tail_bytes\":4") != null);
    var snapshot_file_existing_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_lite_copy_stable_snapshot_file_json(src_path, snapshot_file_path, false, &snapshot_file_existing_report));
    try std.testing.expect(snapshot_file_existing_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), snapshot_file_existing_report.len);

    var snapshot_file_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(snapshot_file_path, &snapshot_file_handle));
    defer antfly_db_close(snapshot_file_handle);
    var snapshot_file_check: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_check_json(snapshot_file_handle, &snapshot_file_check));
    defer freeRawBuffer(snapshot_file_check.ptr, snapshot_file_check.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_check.ptr.?[0..snapshot_file_check.len], "\"valid\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_check.ptr.?[0..snapshot_file_check.len], "\"tail_bytes\":0") != null);
    var snapshot_file_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(snapshot_file_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &snapshot_file_lookup));
    defer freeRawBuffer(snapshot_file_lookup.ptr, snapshot_file_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, snapshot_file_lookup.ptr.?[0..snapshot_file_lookup.len], "\"second\"") != null);

    var compacted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_compact_json(src_handle, &compacted));
    defer freeRawBuffer(compacted.ptr, compacted.len);
    try std.testing.expect(std.mem.indexOf(u8, compacted.ptr.?[0..compacted.len], "\"compacted\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, compacted.ptr.?[0..compacted.len], "\"vacuum\":") != null);

    var vacuumed: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_vacuum_json(src_handle, &vacuumed));
    defer freeRawBuffer(vacuumed.ptr, vacuumed.len);
    try std.testing.expect(std.mem.indexOf(u8, vacuumed.ptr.?[0..vacuumed.len], "\"reclaimed_bytes\":") != null);

    var backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src_handle, &backup));
    defer freeRawBuffer(backup.ptr, backup.len);
    try std.testing.expect(backup.len > 0);

    var exported_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src_handle, &exported_backup));
    defer freeRawBuffer(exported_backup.ptr, exported_backup.len);
    try std.testing.expect(exported_backup.len > 0);

    var dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(dst_path, &dst_handle));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 16,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 16,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(dst_handle, .{
        .ptr = null,
        .len = 0,
    }));

    var bad_dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(bad_dst_path, &bad_dst_handle));
    defer antfly_db_close(bad_dst_handle);

    const target_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-import-target", .len = "doc:capi-import-target".len },
        .value = .{ .ptr = "{\"title\":\"target survives bad capi import\"}", .len = "{\"title\":\"target survives bad capi import\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(bad_dst_handle, &target_writes, target_writes.len, null, 0, 5_000, 0));

    var malformed = std.ArrayList(u8).empty;
    defer malformed.deinit(alloc);
    try backup_codec.writeHeader(&malformed, alloc, .{
        .format_version = backup_codec.format_version,
        .flags = 0,
        .created_at_ns = 0,
        .backup_id = @as([16]u8, @splat(0)),
        .table_count = 1,
        .shard_count = 1,
    });
    const malformed_doc_payload = [_]u8{ 1, 0, 0, 0 };
    try backup_codec.writeBlock(&malformed, alloc, .document_batch, &malformed_doc_payload);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(bad_dst_handle, .{
        .ptr = malformed.items.ptr,
        .len = malformed.items.len,
    }));

    var target_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(bad_dst_handle, .{
        .ptr = "doc:capi-import-target",
        .len = "doc:capi-import-target".len,
    }, &target_lookup));
    defer freeRawBuffer(target_lookup.ptr, target_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, target_lookup.ptr.?[0..target_lookup.len], "\"target survives bad capi import\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(bad_dst_handle, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }));

    var target_after_valid_rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(bad_dst_handle, .{
        .ptr = "doc:capi-import-target",
        .len = "doc:capi-import-target".len,
    }, &target_after_valid_rejected));
    defer freeRawBuffer(target_after_valid_rejected.ptr, target_after_valid_rejected.len);
    try std.testing.expect(std.mem.indexOf(u8, target_after_valid_rejected.ptr.?[0..target_after_valid_rejected.len], "\"target survives bad capi import\"") != null);

    var schema_dst_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(schema_dst_path, &schema_dst_handle));
    defer antfly_db_close(schema_dst_handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(schema_dst_handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_import_backup(schema_dst_handle, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }));
    var schema_after_valid_rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_schema_json(schema_dst_handle, &schema_after_valid_rejected));
    defer freeRawBuffer(schema_after_valid_rejected.ptr, schema_after_valid_rejected.len);
    try std.testing.expectEqualStrings(schema_json, schema_after_valid_rejected.ptr.?[0..schema_after_valid_rejected.len]);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_import_backup(dst_handle, .{
        .ptr = exported_backup.ptr,
        .len = exported_backup.len,
    }));

    var lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(dst_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &lookup));
    defer freeRawBuffer(lookup.ptr, lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, lookup.ptr.?[0..lookup.len], "\"second\"") != null);

    antfly_db_close(dst_handle);
    dst_handle = null;

    var restore_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(restore_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &restore_report));
    defer freeRawBuffer(restore_report.ptr, restore_report.len);
    try std.testing.expect(std.mem.indexOf(u8, restore_report.ptr.?[0..restore_report.len], "\"format\":\"aflite\"") != null);

    lite_restore_staging.failNextPublishedFileDirectorySyncForTest();
    antfly.test_error_logs.expectErrorLogs(1);
    var unknown_report: capi.Buffer = .{ .ptr = @constCast("stale".ptr), .len = "stale".len };
    try std.testing.expectEqual(capi.ErrorCode.outcome_unknown, antfly_restore_backup_json(restore_unknown_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &unknown_report));
    try std.testing.expect(unknown_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), unknown_report.len);

    // Publication already happened, so the destination must be inspectable
    // and a blind retry must be rejected rather than replacing it again.
    var unknown_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_unknown_path, &unknown_handle));
    defer antfly_db_close(unknown_handle);
    var unknown_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(unknown_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &unknown_lookup));
    defer freeRawBuffer(unknown_lookup.ptr, unknown_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown_lookup.ptr.?[0..unknown_lookup.len], "\"second\"") != null);
    var retry_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_unknown_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &retry_report));

    var restored_file_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_path, &restored_file_handle));
    defer antfly_db_close(restored_file_handle);
    var restored_file_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored_file_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &restored_file_lookup));
    defer freeRawBuffer(restored_file_lookup.ptr, restored_file_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, restored_file_lookup.ptr.?[0..restored_file_lookup.len], "\"second\"") != null);

    var restore_alias_report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(restore_alias_path, &lite_restore_options, .{
        .ptr = exported_backup.ptr,
        .len = exported_backup.len,
    }, false, &restore_alias_report));
    defer freeRawBuffer(restore_alias_report.ptr, restore_alias_report.len);
    try std.testing.expect(std.mem.indexOf(u8, restore_alias_report.ptr.?[0..restore_alias_report.len], "\"format\":\"aflite\"") != null);

    var restored_alias_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(restore_alias_path, &restored_alias_handle));
    defer antfly_db_close(restored_alias_handle);
    var restored_alias_lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored_alias_handle, .{
        .ptr = "doc:capi-lite",
        .len = "doc:capi-lite".len,
    }, &restored_alias_lookup));
    defer freeRawBuffer(restored_alias_lookup.ptr, restored_alias_lookup.len);
    try std.testing.expect(std.mem.indexOf(u8, restored_alias_lookup.ptr.?[0..restored_alias_lookup.len], "\"second\"") != null);

    const locked_restore_tmp_path = try std.fmt.allocPrint(alloc, "{s}.restore-tmp.aflite", .{locked_restore_path});
    defer alloc.free(locked_restore_tmp_path);
    {
        var locked_restore = try antfly.lite.native.lockWriterPath(alloc, locked_restore_path);
        defer locked_restore.close();

        var locked_restore_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(locked_restore_path, &lite_restore_options, .{
            .ptr = backup.ptr,
            .len = backup.len,
        }, false, &locked_restore_report));
        try std.testing.expect(locked_restore_report.ptr == null);
        try std.testing.expectEqual(@as(usize, 0), locked_restore_report.len);
    }
    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        try std.testing.expect(!capiPathExists(io_impl.io(), locked_restore_path));
        try std.testing.expect(!capiPathExists(io_impl.io(), locked_restore_tmp_path));
    }

    var restore_existing: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_path, &lite_restore_options, .{
        .ptr = backup.ptr,
        .len = backup.len,
    }, false, &restore_existing));
    try std.testing.expect(restore_existing.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), restore_existing.len);

    var malformed_restore_report: capi.Buffer = .{ .ptr = scratch[0..].ptr, .len = scratch.len };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_restore_backup_json(restore_malformed_path, &lite_restore_options, .{
        .ptr = malformed.items.ptr,
        .len = malformed.items.len,
    }, false, &malformed_restore_report));
    try std.testing.expect(malformed_restore_report.ptr == null);
    try std.testing.expectEqual(@as(usize, 0), malformed_restore_report.len);
    {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        try std.testing.expect(!capiPathExists(io_impl.io(), restore_malformed_path));
    }

    var readonly_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_readonly(dst_path, &readonly_handle));
    defer antfly_db_close(readonly_handle);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(readonly_handle, &writes_a, 1, null, 0, 3_000, 0));
}

test "capi lite exposes hosted and status-only profiles" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-profiles");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var hosted_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create_hosted(path, &hosted_handle));
    defer if (hosted_handle != null) antfly_db_close(hosted_handle);

    var hosted_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(hosted_handle, &hosted_caps));
    defer freeRawBuffer(hosted_caps.ptr, hosted_caps.len);
    const hosted_caps_json = hosted_caps.ptr.?[0..hosted_caps.len];
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"hosted_profile\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"manual_maintenance\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"background_enrichment_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"ttl_cleanup_runtime\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, hosted_caps_json, "\"transaction_recovery_runtime\":false") != null);

    // Creating a Lite database already provisions the default full-text
    // index, matching the server's behavior on table create.
    var initial_indexes: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_indexes_json(hosted_handle, &initial_indexes));
    defer freeRawBuffer(initial_indexes.ptr, initial_indexes.len);
    try std.testing.expect(std.mem.indexOf(u8, initial_indexes.ptr.?[0..initial_indexes.len], "\"full_text_index_v0\"") != null);

    const index_json =
        \\{"name":"full_text_index_v0","kind":"full_text","config_json":"{}"}
    ;
    // Reopening clients replay identical inline definitions. These admissions
    // must succeed without duplicating the index; conflicting configs remain
    // errors and must preserve the installed catalog.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(hosted_handle, .{
        .ptr = index_json,
        .len = index_json.len,
    }));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(hosted_handle, .{
        .ptr = index_json,
        .len = index_json.len,
    }));
    const conflicting_index_json =
        \\{"name":"full_text_index_v0","kind":"full_text","config_json":"{\"field\":\"title\"}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_add_index_json(hosted_handle, .{
        .ptr = conflicting_index_json,
        .len = conflicting_index_json.len,
    }));
    var replayed_indexes: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_list_indexes_json(hosted_handle, &replayed_indexes));
    defer freeRawBuffer(replayed_indexes.ptr, replayed_indexes.len);
    try std.testing.expectEqualStrings(initial_indexes.ptr.?[0..initial_indexes.len], replayed_indexes.ptr.?[0..replayed_indexes.len]);

    const writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:capi-lite-profile", .len = "doc:capi-lite-profile".len },
        .value = .{ .ptr = "{\"title\":\"hosted\"}", .len = "{\"title\":\"hosted\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(hosted_handle, &writes, writes.len, null, 0, 1_000, 0));

    var pending_before: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_pending_work_stats_json(hosted_handle, &pending_before));
    defer freeRawBuffer(pending_before.ptr, pending_before.len);
    try std.testing.expect(std.mem.indexOf(u8, pending_before.ptr.?[0..pending_before.len], "\"has_async_indexes\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, pending_before.ptr.?[0..pending_before.len], "\"derived_target_sequence\":") != null);

    var idle_after: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle_json(hosted_handle, &idle_after));
    defer freeRawBuffer(idle_after.ptr, idle_after.len);
    try std.testing.expect(std.mem.indexOf(u8, idle_after.ptr.?[0..idle_after.len], "\"text_merge\"") != null);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(hosted_handle));

    const hosted_query =
        \\{"full_text_search":{"match":{"field":"title","text":"hosted"}},"limit":1}
    ;
    var hosted_search: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(hosted_handle, .{
        .ptr = hosted_query,
        .len = hosted_query.len,
    }, &hosted_search));
    defer freeRawBuffer(hosted_search.ptr, hosted_search.len);
    try std.testing.expect(std.mem.indexOf(u8, hosted_search.ptr.?[0..hosted_search.len], "\"doc:capi-lite-profile\"") != null);

    antfly_db_close(hosted_handle);
    hosted_handle = null;

    var status_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_open_status_only(path, &status_handle));
    defer antfly_db_close(status_handle);

    var status_caps: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_capabilities_json(status_handle, &status_caps));
    defer freeRawBuffer(status_caps.ptr, status_caps.len);
    const status_caps_json = status_caps.ptr.?[0..status_caps.len];
    try std.testing.expect(std.mem.indexOf(u8, status_caps_json, "\"hosted_profile\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_caps_json, "\"manual_maintenance\":false") != null);

    var stats: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(status_handle, &stats));
    defer freeRawBuffer(stats.ptr, stats.len);
    try std.testing.expect(std.mem.indexOf(u8, stats.ptr.?[0..stats.len], "\"doc_count\":") != null);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(status_handle, &writes, writes.len, null, 0, 2_000, 0));
}

test "capi directory restore coordinates with open handles and publishes atomically" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const io = handleLockIo();
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-dir-restore-src");
    defer alloc.free(src_path);
    const dest_path = try tempTestPath(alloc, test_tmp.path(), "capi-dir-restore-dest");
    defer alloc.free(dest_path);
    cleanupTestFile(src_path);
    defer cleanupTestFile(src_path);
    cleanupTestDir(dest_path);
    defer cleanupTestDir(dest_path);

    // Two backups with different contents.
    var src: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src));
    const first_writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:v1", .len = 6 }, .value = .{ .ptr = "{\"v\":1}", .len = 7 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &first_writes, 1, null, 0, 1, 0));
    var first_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &first_backup));
    defer antfly_buffer_free(&first_backup);
    const second_writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:v2", .len = 6 }, .value = .{ .ptr = "{\"v\":2}", .len = 7 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &second_writes, 1, null, 0, 2, 0));
    var second_backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &second_backup));
    defer antfly_buffer_free(&second_backup);
    antfly_db_close(src);

    const directory = capi.OpenOptions{ .storage_kind = capi.storage_kind_directory };
    var readonly = directory;
    readonly.open_mode = capi.open_mode_readonly;
    const first: capi.Slice = .{ .ptr = first_backup.ptr, .len = first_backup.len };
    const second: capi.Slice = .{ .ptr = second_backup.ptr, .len = second_backup.len };
    var report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, first, false, &report));
    antfly_buffer_free(&report);

    // An open read-only handle blocks replacement, and keeps working.
    var reader: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &reader));
    try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    var lookup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(reader, .{ .ptr = "doc:v1", .len = 6 }, &lookup));
    antfly_buffer_free(&lookup);
    antfly_db_close(reader);

    // A database opened outside libantfly holds the same generation lease.
    {
        var direct = try db_mod.DB.open(alloc, dest_path, .{});
        defer direct.close();
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    }

    // While a restore holds the generation transition, opens are refused.
    {
        var transition = try db_mod.generation_lifecycle.beginProcessExclusiveWithIo(dest_path, io);
        defer transition.deinit();
        var blocked: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.busy, antfly_db_open_with_options(dest_path, &directory, &blocked));
        try std.testing.expect(blocked == null);
    }

    // With no handles open, replacement publishes the new database.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, second, true, &report));
    antfly_buffer_free(&report);
    var restored: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &restored));
    defer antfly_db_close(restored);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(restored, .{ .ptr = "doc:v2", .len = 6 }, &lookup));
    antfly_buffer_free(&lookup);

    // No staging or displaced directory is left beside the destination.
    const parent = std.fs.path.dirname(dest_path).?;
    const prefix = try std.fmt.allocPrint(alloc, "{s}.restore-", .{std.fs.path.basename(dest_path)});
    defer alloc.free(prefix);
    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (std.mem.startsWith(u8, entry.name, prefix)) {
            std.debug.print("unexpected restore sibling left behind: {s}\n", .{entry.name});
            return error.TestUnexpectedResult;
        }
    }
}

test "capi directory restore publishes with derived work drained" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const src_path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-dir-restore-drain-src");
    defer alloc.free(src_path);
    const dest_path = try tempTestPath(alloc, test_tmp.path(), "capi-dir-restore-drain-dest");
    defer alloc.free(dest_path);
    cleanupTestFile(src_path);
    defer cleanupTestFile(src_path);
    cleanupTestDir(dest_path);
    defer cleanupTestDir(dest_path);

    var src: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(src_path, &src));
    const writes = [_]capi.WriteIntent{.{ .key = .{ .ptr = "doc:fox", .len = 7 }, .value = .{ .ptr = "{\"body\":\"the quick brown fox\"}", .len = 30 } }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(src, &writes, 1, null, 0, 1, 0));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(src));
    var backup: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_backup(src, &backup));
    defer antfly_buffer_free(&backup);
    antfly_db_close(src);

    const directory = capi.OpenOptions{ .storage_kind = capi.storage_kind_directory };
    var report: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_restore_backup_json(dest_path, &directory, .{ .ptr = backup.ptr, .len = backup.len }, false, &report));
    antfly_buffer_free(&report);

    // A read-only handle runs no derived work of its own, so the published
    // directory must already carry complete index results.
    var readonly = directory;
    readonly.open_mode = capi.open_mode_readonly;
    var restored: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dest_path, &readonly, &restored));
    defer antfly_db_close(restored);
    const request = "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"quick fox\"}},\"limit\":5}";
    var result: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(restored, .{ .ptr = request.ptr, .len = request.len }, &result));
    defer antfly_buffer_free(&result);
    const hits = result.ptr.?[0..result.len];
    if (std.mem.indexOf(u8, hits, "doc:fox") == null) {
        std.debug.print("restored directory search missed doc:fox: {s}\n", .{hits});
        return error.TestUnexpectedResult;
    }
}

test "capi handle ids are safe to use after close and across slot reuse" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path_a = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-id-a");
    defer alloc.free(path_a);
    const path_b = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-id-b");
    defer alloc.free(path_b);
    cleanupTestFile(path_a);
    defer cleanupTestFile(path_a);
    cleanupTestFile(path_b);
    defer cleanupTestFile(path_b);

    var a: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path_a, &a));
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(a, &out));
    antfly_buffer_free(&out);

    antfly_db_close(a);
    // Use after close and repeated close are defined: no access to freed memory.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(a, &out));
    antfly_db_close(a);

    // The next handle reuses the freed slot under a new generation, so the
    // stale id neither matches it nor closes it.
    var b: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path_b, &b));
    defer antfly_db_close(b);
    try std.testing.expect(a != b);
    try std.testing.expectEqual(handle_registry.decode(a).?.index, handle_registry.decode(b).?.index);
    if (HandleRegistry.reserve_address_space) {
        // Ids are addresses in the reservation, so bindings can keep them in
        // pointer-typed fields that a garbage collector inspects.
        const base = handle_registry.base.load(.acquire);
        try std.testing.expect(base >= 4096);
        try std.testing.expect(@intFromPtr(b) >= base);
        try std.testing.expectEqual(@as(usize, 0), @intFromPtr(b) % 8);
    }
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(a, &out));
    antfly_db_close(a);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(b, &out));
    antfly_buffer_free(&out);

    // Values that were never issued fail cleanly too.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(null, &out));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_status_json(@ptrFromInt(0x7fff_0000), &out));
}

test "capi handle registry retires a slot instead of wrapping its generation" {
    var first = Handle{ .alloc = std.testing.allocator, .db = undefined };
    var second = Handle{ .alloc = std.testing.allocator, .db = undefined };
    var third = Handle{ .alloc = std.testing.allocator, .db = undefined };

    // Put a slot at the front of the free list, then move it to the last
    // generation an id can encode.
    const first_id = try registerTestHandle(&first);
    const index = handle_registry.decode(first_id).?.index;
    unregisterTestHandle(first_id);
    const slot = handle_registry.slotFor(index).?;
    const last_generation = handle_registry.generationMask();
    slot.state.store(last_generation << 1, .release);

    const second_id = try registerTestHandle(&second);
    try std.testing.expectEqual(index, handle_registry.decode(second_id).?.index);
    try std.testing.expectEqual(last_generation, handle_registry.decode(second_id).?.generation);
    unregisterTestHandle(second_id);

    // Retired: still claimed, unusable by any id, and never handed out again.
    try std.testing.expectEqual(last_generation << 1 | 1, slot.state.load(.acquire));
    try std.testing.expect(asHandle(second_id) == null);
    try std.testing.expect(enterHandle(second_id, .read) == null);
    unregisterTestHandle(second_id);
    const third_id = try registerTestHandle(&third);
    defer unregisterTestHandle(third_id);
    try std.testing.expect(handle_registry.decode(third_id).?.index != index);
    const wrapped = handle_registry.encode(.{ .index = index, .generation = 0 });
    try std.testing.expect(enterHandle(wrapped, .read) == null);
}

test "capi concurrent calls and closes on one handle never touch freed memory" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-handle-close-race");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));

    const Worker = struct {
        fn call(id: ?*anyopaque, unexpected: *std.atomic.Value(u32)) void {
            while (true) {
                var out: capi.Buffer = .{};
                switch (antfly_db_status_json(id, &out)) {
                    .ok => antfly_buffer_free(&out),
                    .invalid_argument => return,
                    else => {
                        _ = unexpected.fetchAdd(1, .monotonic);
                        return;
                    },
                }
            }
        }
        fn close(id: ?*anyopaque) void {
            antfly_db_close(id);
        }
    };

    var unexpected = std.atomic.Value(u32).init(0);
    const spawn_config: std.Thread.SpawnConfig = .{ .stack_size = capi_min_thread_stack_size };
    var callers: [6]std.Thread = undefined;
    for (&callers) |*t| t.* = try std.Thread.spawn(spawn_config, Worker.call, .{ handle, &unexpected });
    std.Io.sleep(std.testing.io, .fromMilliseconds(20), .awake) catch {};
    const closer_a = try std.Thread.spawn(spawn_config, Worker.close, .{handle});
    const closer_b = try std.Thread.spawn(spawn_config, Worker.close, .{handle});
    closer_a.join();
    closer_b.join();
    for (callers) |t| t.join();
    try std.testing.expectEqual(@as(u32, 0), unexpected.load(.monotonic));
}

test "capi text and dense searches succeed while writes commit" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-search-during-writes");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_lite_create(path, &handle));
    defer antfly_db_close(handle);
    const dense_index =
        \\{"name":"dv_v1","kind":"dense_vector","config_json":"{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{ .ptr = dense_index, .len = dense_index.len }));

    const Writer = struct {
        fn run(id: ?*anyopaque, stop: *std.atomic.Value(bool), failures: *std.atomic.Value(u32)) void {
            var ts: u64 = 1;
            var key_buf: [32]u8 = undefined;
            while (!stop.load(.acquire)) : (ts += 1) {
                const key = std.fmt.bufPrint(&key_buf, "doc:{d}", .{ts}) catch unreachable;
                const value =
                    \\{"body":"searchable writes","_embeddings":{"dv_v1":[1.0,0.0]}}
                ;
                const writes = [_]capi.WriteIntent{.{
                    .key = .{ .ptr = key.ptr, .len = key.len },
                    .value = .{ .ptr = value, .len = value.len },
                    .is_delete = false,
                }};
                if (antfly_db_batch(id, &writes, writes.len, null, 0, ts, 0) != .ok) _ = failures.fetchAdd(1, .monotonic);
            }
        }
    };

    var stop = std.atomic.Value(bool).init(false);
    var write_failures = std.atomic.Value(u32).init(0);
    const writer = try std.Thread.spawn(.{ .stack_size = capi_min_thread_stack_size }, Writer.run, .{ handle, &stop, &write_failures });
    defer writer.join();
    defer stop.store(true, .release);

    // Every search stamps an identity generation that a concurrent commit
    // can invalidate; unpinned reads must restamp rather than fail.
    const vector = [_]f32{ 1.0, 0.0 };
    for (0..200) |_| {
        var text_result: capi.DenseSearchResult = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
            handle,
            .{},
            .{ .ptr = "body", .len = 4 },
            .{ .ptr = "searchable", .len = 10 },
            5,
            0,
            &text_result,
        ));
        antfly_dense_search_result_free(&text_result);
        var dense_result: capi.PackedDenseSearchResult = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
            handle,
            .{ .ptr = "dv_v1", .len = 5 },
            &vector,
            vector.len,
            1,
            1,
            0,
            &dense_result,
        ));
        antfly_packed_dense_search_result_free(&dense_result);
    }
    stop.store(true, .release);
    try std.testing.expectEqual(@as(u32, 0), write_failures.load(.monotonic));
}

test "capi lite open options validate and configure ttl cleanup" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-open-options");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    try std.testing.expectEqual(@as(u32, @intCast(@sizeOf(capi.OpenOptions))), antfly_open_options_size());
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_open_options_init(null));

    var generic_defaults = capi.OpenOptions{
        .abi_size = 0,
        .storage_kind = 99,
        .open_mode = 99,
        .profile = 99,
        .flags = std.math.maxInt(u32),
        .reserved0 = 1,
        .inference_host_budget_mb = 1,
        .busy_timeout_ms = 1,
        .reserved = @splat(1),
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&generic_defaults));
    try std.testing.expectEqual(@as(u32, @sizeOf(capi.OpenOptions)), generic_defaults.abi_size);
    try std.testing.expectEqual(capi.storage_kind_directory, generic_defaults.storage_kind);
    try std.testing.expectEqual(capi.open_mode_writer, generic_defaults.open_mode);
    try std.testing.expectEqual(capi.profile_native, generic_defaults.profile);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.flags);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.reserved0);
    try std.testing.expectEqual(@as(u32, 0), generic_defaults.inference_host_budget_mb);
    try std.testing.expectEqual(@as(u64, 0), generic_defaults.busy_timeout_ms);
    for (generic_defaults.reserved) |word| try std.testing.expectEqual(@as(u64, 0), word);

    // Lite opens use the same options with the Lite storage kind.
    var defaults = generic_defaults;
    defaults.storage_kind = capi.storage_kind_lite;

    var default_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    var prefix_lite_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @offsetOf(capi.OpenOptions, "flags"),
        .open_mode = capi.open_mode_readonly,
        .profile = capi.profile_native,
        .flags = std.math.maxInt(u32),
        .reserved = @splat(1),
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &defaults, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &prefix_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    var generic_lite_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &generic_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    generic_lite_options.open_mode = capi.open_mode_readonly;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &generic_lite_options, &default_handle));
    antfly_db_close(default_handle);
    default_handle = null;
    cleanupTestFile(path);

    const dir_path = try tempTestPath(alloc, test_tmp.path(), "capi-generic-directory-open");
    defer alloc.free(dir_path);
    cleanupTestDir(dir_path);
    defer cleanupTestDir(dir_path);
    var directory_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_create_with_options(dir_path, &generic_defaults, &directory_handle));
    try std.testing.expectEqual(@as(?*anyopaque, null), directory_handle);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dir_path, &generic_defaults, &directory_handle));
    const generic_writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:generic", .len = "doc:generic".len },
        .value = .{ .ptr = "{\"title\":\"generic\"}", .len = "{\"title\":\"generic\"}".len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(directory_handle, &generic_writes, generic_writes.len, null, 0, 1_000, 0));
    antfly_db_close(directory_handle);
    directory_handle = null;
    var generic_readonly = capi.OpenOptions{
        .storage_kind = capi.storage_kind_directory,
        .open_mode = capi.open_mode_readonly,
    };
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(dir_path, &generic_readonly, &directory_handle));
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_batch(directory_handle, &generic_writes, generic_writes.len, null, 0, 2_000, 0));
    antfly_db_close(directory_handle);
    directory_handle = null;

    var sentinel: u8 = 0;
    var invalid_handle: ?*anyopaque = &sentinel;
    var invalid_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .open_mode = 99,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &invalid_options, &invalid_handle));
    try std.testing.expect(invalid_handle == null);

    var hosted_ttl_handle: ?*anyopaque = &sentinel;
    var hosted_ttl_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .profile = capi.profile_hosted,
        .flags = capi.open_flag_ttl_cleanup,
        .ttl_cleanup_enabled = true,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &hosted_ttl_options, &hosted_ttl_handle));
    try std.testing.expect(hosted_ttl_handle == null);

    var hosted_generated_replay_handle: ?*anyopaque = &sentinel;
    var hosted_generated_replay_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .profile = capi.profile_hosted,
        .flags = capi.open_flag_generated_enrichment_replay,
    };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_open_with_options(path, &hosted_generated_replay_options, &hosted_generated_replay_handle));
    try std.testing.expect(hosted_generated_replay_handle == null);

    const owner_id = "capi-ttl-owner";
    var open_options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_no_sync | capi.open_flag_ttl_cleanup,
        .ttl_cleanup_enabled = true,
        .ttl_cleanup_lease_owned = true,
        .ttl_cleanup_batch_size = 8,
        .ttl_cleanup_owner_id = .{ .ptr = owner_id, .len = owner_id.len },
        .ttl_cleanup_lease_ttl_ms = 250,
        .ttl_cleanup_interval_ms = 10,
        .ttl_cleanup_grace_period_ns = 1,
    };

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &open_options, &handle));
    defer antfly_db_close(handle);

    const schema_json =
        \\{"default_type":"doc","ttl_duration_ns":1,"ttl_field":"expires_at","document_schemas":{"doc":{"schema":{"type":"object","properties":{"expires_at":{"type":"datetime"},"title":{"type":"text"}}}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .{
        .ptr = schema_json,
        .len = schema_json.len,
    }));

    const doc_json = "{\"title\":\"gone\",\"expires_at\":1}";
    const writes = [_]capi.WriteIntent{.{
        .key = .{ .ptr = "doc:expired", .len = "doc:expired".len },
        .value = .{ .ptr = doc_json, .len = doc_json.len },
        .is_delete = false,
    }};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch(handle, &writes, writes.len, null, 0, 1, 0));

    var stats: capi.Buffer = .{};
    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        freeRawBuffer(stats.ptr, stats.len);
        stats = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_stats_json(handle, &stats));
        const stats_json = stats.ptr.?[0..stats.len];
        if (std.mem.indexOf(u8, stats_json, "\"enabled\":true") != null and
            std.mem.indexOf(u8, stats_json, "\"lease_owned\":true") != null and
            std.mem.indexOf(u8, stats_json, "\"deleted_docs\":1") != null and
            std.mem.indexOf(u8, stats_json, "\"scanned_timestamps\":1") != null)
        {
            break;
        }
        antfly.platform_clock.Clock.real().sleepMs(10);
    }
    defer freeRawBuffer(stats.ptr, stats.len);
    try std.testing.expect(attempts < 200);
}

test "capi execute graph queries honors identity read generation" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-execute-graph-generation");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const handle = asHandle(handle_ptr).?;
    try handle.db.addIndex(.{
        .name = "gr_v1",
        .kind = .graph,
        .config_json = "{}",
    });
    try handle.db.batch(.{
        .writes = &.{
            .{ .key = "n:a", .value = "{\"title\":\"A\",\"_edges\":{\"gr_v1\":{\"links\":[{\"target\":\"n:b\"}]}}}" },
            .{ .key = "n:b", .value = "{\"title\":\"B\"}" },
        },
        .sync_level = .full_index,
    });

    const missing_generation_request =
        \\{"graph_queries":[{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{"result_ref":"seed"},"edge_types":["links"]}],"named_sets":[{"name":"seed","hit_ids_b64":["bjph"]}],"limit":10}
    ;
    var missing_generation_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = missing_generation_request.ptr, .len = missing_generation_request.len },
        &missing_generation_out,
    ));

    const current_generation = handle.db.core.nextDerivedSequence();
    const request = try std.fmt.allocPrint(alloc,
        \\{{"identity_read_generation":{d},"graph_queries":[{{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{{"result_ref":"seed"}},"edge_types":["links"]}}],"named_sets":[{{"name":"seed","hit_ids_b64":["bjph"]}}],"limit":10}}
    , .{current_generation});
    defer alloc.free(request);
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = request.ptr, .len = request.len },
        &out,
    ));
    defer freeRawBuffer(out.ptr, out.len);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "\"key_b64\":\"bjpi\"") != null);
    var parsed_out = try std.json.parseFromSlice(std.json.Value, alloc, out.ptr.?[0..out.len], .{});
    defer parsed_out.deinit();
    try std.testing.expectEqual(@as(i64, @intCast(current_generation)), parsed_out.value.array.items[0].object.get("identity_read_generation").?.integer);

    const stale_generation = handle.db.core.nextDerivedSequence() -| 1;
    const stale_request = try std.fmt.allocPrint(alloc,
        \\{{"identity_read_generation":{d},"graph_queries":[{{"name":"neighbors","type":"neighbors","index_name":"gr_v1","start_nodes":{{"result_ref":"seed"}},"edge_types":["links"]}}],"named_sets":[{{"name":"seed","hit_ids_b64":["bjph"]}}],"limit":10}}
    , .{stale_generation});
    defer alloc.free(stale_request);
    var stale_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_execute_graph_queries_json(
        handle_ptr,
        .{ .ptr = stale_request.ptr, .len = stale_request.len },
        &stale_out,
    ));
}

test "capi search rejects stale identity generation before readable lease hook" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-stale-generation-before-lease");
    defer alloc.free(path);

    cleanupTestDir(path);

    const Recorder = struct {
        count: usize = 0,

        fn callback(
            ctx: ?*anyopaque,
            _: u64,
            _: ?[*]const u8,
            _: usize,
        ) callconv(.c) capi.ErrorCode {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.count += 1;
            return .ok;
        }
    };

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
        }},
    });

    var recorder = Recorder{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        42,
        &recorder,
        &Recorder.callback,
    ));

    const stale_generation = handle.db.core.nextDerivedSequence() -| 1;
    const request = try std.fmt.allocPrint(alloc,
        \\{{"mode":"dense","index_name":"dv_v1","vector":[1,0],"k":1,"limit":1,"identity_read_generation":{d}}}
    , .{stale_generation});
    defer alloc.free(request);

    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_search_json(
        handle_id,
        .{ .ptr = request.ptr, .len = request.len },
        &out,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.count);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        0,
        null,
        null,
    ));
}

test "capi search json returns stamped identity generation" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-search-generation-response");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    // Evaluation must fail before either the Lite fallback or linked storage
    // path can return an unfiltered public result.
    const evaluated_query = "{\"full_text_search\":{\"match_all\":{}},\"evaluate\":{\"scope\":\"candidates\",\"candidate_count\":2,\"compute\":{\"x\":{\"literal\":1}},\"where\":{\"eq\":[{\"literal\":1},{\"literal\":0}]}}}";
    var rejected: capi.Buffer = .{};
    try std.testing.expectEqual(capi.mapError(error.UnsupportedQueryRequest), antfly_db_search_json(
        handle_id,
        .{ .ptr = evaluated_query.ptr, .len = evaluated_query.len },
        &rejected,
    ));
    try std.testing.expect(rejected.ptr == null);

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
        }},
    });

    const current_generation = handle.db.core.nextDerivedSequence();
    const search_req =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":1,\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
        handle_id,
        .{ .ptr = search_req.ptr, .len = search_req.len },
        &out,
    ));
    defer freeRawBuffer(out.ptr, out.len);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out.ptr.?[0..out.len], .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, @intCast(current_generation)), parsed.value.object.get("identity_read_generation").?.integer);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "doc_ordinal") == null);
    try std.testing.expect(std.mem.indexOf(u8, out.ptr.?[0..out.len], "ordinal") == null);

    var packed_result: capi.PackedDenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &packed_result,
    ));
    defer antfly_packed_dense_search_result_free(&packed_result);
    try std.testing.expectEqual(current_generation, packed_result.identity_read_generation);

    var text_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
        handle_id,
        .{ .ptr = "ft_v1".ptr, .len = "ft_v1".len },
        .{ .ptr = "title".ptr, .len = "title".len },
        .{ .ptr = "alpha".ptr, .len = "alpha".len },
        1,
        0,
        &text_result,
    ));
    defer antfly_dense_search_result_free(&text_result);
    try std.testing.expectEqual(current_generation, text_result.identity_read_generation);

    const hits_request =
        "{\"mode\":\"full_text\",\"index_name\":\"ft_v1\",\"text_query_type\":\"match\",\"field\":\"title\",\"text\":\"alpha\",\"limit\":1}";
    var hits_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_hits_json(
        handle_id,
        .{ .ptr = hits_request.ptr, .len = hits_request.len },
        &hits_result,
    ));
    defer antfly_dense_search_result_free(&hits_result);
    try std.testing.expectEqual(current_generation, hits_result.identity_read_generation);
}

test "capi aggregate hits rejects stale identity generation before aggregation materialization" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-aggregate-stale-generation");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\"}",
        }},
    });

    const current_generation = handle.db.core.nextDerivedSequence();
    const stale_generation = current_generation -| 1;
    try std.testing.expect(stale_generation != current_generation);

    const request_template =
        \\{{"identity_read_generation":{d},"hit_ids_b64":["ZG9jOmE="],"aggregations":[{{"name":"bad","type":"terms","field":"title","background_query_type":"bogus"}}]}}
    ;
    const current_request = try std.fmt.allocPrint(alloc, request_template, .{current_generation});
    defer alloc.free(current_request);
    var current_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = current_request.ptr, .len = current_request.len },
        &current_out,
    ));

    const missing_generation_request =
        \\{"hit_ids_b64":["ZG9jOmE="],"aggregations":[{"name":"bad","type":"terms","field":"title","background_query_type":"bogus"}]}
    ;
    var missing_generation_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = missing_generation_request.ptr, .len = missing_generation_request.len },
        &missing_generation_out,
    ));

    const stale_request = try std.fmt.allocPrint(alloc, request_template, .{stale_generation});
    defer alloc.free(stale_request);
    var stale_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_aggregate_hits_json(
        handle_id,
        .{ .ptr = stale_request.ptr, .len = stale_request.len },
        &stale_out,
    ));
    try std.testing.expect(std.mem.indexOf(u8, std.mem.span(capi.lastErrorDetail()), "IdentityReadGenerationChanged") != null);
}

test "capi request paths trigger readable lease hook" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-readable-lease");
    defer alloc.free(path);

    cleanupTestDir(path);

    const Recorder = struct {
        contexts: [9][32]u8 = @as([9][32]u8, @splat(@as([32]u8, @splat(0)))),
        context_lens: [9]usize = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        group_ids: [9]u64 = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0 },
        count: usize = 0,

        fn callback(
            ctx: ?*anyopaque,
            group_id: u64,
            request_ctx_ptr: ?[*]const u8,
            request_ctx_len: usize,
        ) callconv(.c) capi.ErrorCode {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (self.count >= self.contexts.len or request_ctx_len > self.contexts[self.count].len) return .internal;
            self.group_ids[self.count] = group_id;
            if (request_ctx_ptr != null and request_ctx_len > 0) {
                @memcpy(self.contexts[self.count][0..request_ctx_len], request_ctx_ptr.?[0..request_ctx_len]);
            }
            self.context_lens[self.count] = request_ctx_len;
            self.count += 1;
            return .ok;
        }
    };

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });
    try handle.db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"embedding\":[1,0],\"title\":\"alpha\"}",
            },
        },
    });
    const current_generation = handle.db.core.nextDerivedSequence();

    var recorder = Recorder{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_readable_lease_hook(
        handle_id,
        42,
        &recorder,
        &Recorder.callback,
    ));

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_json(
        handle_id,
        .{ .ptr = "doc:a".ptr, .len = "doc:a".len },
        &lookup_out,
    ));
    freeRawBuffer(lookup_out.ptr, lookup_out.len);

    const scan_req = "{\"from_key_b64\":\"\",\"to_key_b64\":\"\",\"include_documents\":false,\"limit\":10}";
    var scan_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_scan_json(
        handle_id,
        .{ .ptr = scan_req.ptr, .len = scan_req.len },
        &scan_out,
    ));
    freeRawBuffer(scan_out.ptr, scan_out.len);

    const search_req =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":1,\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var search_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
        handle_id,
        .{ .ptr = search_req.ptr, .len = search_req.len },
        &search_out,
    ));
    freeRawBuffer(search_out.ptr, search_out.len);

    var packed_result: capi.PackedDenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &packed_result,
    ));
    antfly_packed_dense_search_result_free(&packed_result);

    var dense_profile: capi.DenseSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_profile(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        (&[_]f32{ 1.0, 0.0 }).ptr,
        2,
        1,
        1,
        0,
        &dense_profile,
    ));

    const dense_wire_req = [_]u8{
        0x54, 0x46, 0x4E, 0x44,
        0x01, 0x00, 0x01, 0x00,
        0x05, 0x00, 0x02, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        'd',  'v',  '_',  'v',
        '1',  0x00, 0x00, 0x80,
        0x3f, 0x00, 0x00, 0x00,
        0x00,
    };
    var dense_wire_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire(
        handle_id,
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_out,
    ));
    try std.testing.expectEqual(@as(?u64, current_generation), try search_wire.denseResponseIdentityReadGeneration(dense_wire_out.ptr.?[0..dense_wire_out.len]));
    freeRawBuffer(dense_wire_out.ptr, dense_wire_out.len);

    var dense_wire_profile_out: capi.Buffer = .{};
    var dense_wire_profile: capi.DenseWireSearchProfile = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_dense_wire_profile(
        handle_id,
        .{ .ptr = &dense_wire_req, .len = dense_wire_req.len },
        &dense_wire_profile_out,
        &dense_wire_profile,
    ));
    try std.testing.expectEqual(@as(?u64, current_generation), try search_wire.denseResponseIdentityReadGeneration(dense_wire_profile_out.ptr.?[0..dense_wire_profile_out.len]));
    freeRawBuffer(dense_wire_profile_out.ptr, dense_wire_profile_out.len);

    var text_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_text_match(
        handle_id,
        .{ .ptr = "dv_v1".ptr, .len = "dv_v1".len },
        .{ .ptr = "title".ptr, .len = "title".len },
        .{ .ptr = "alpha".ptr, .len = "alpha".len },
        1,
        0,
        &text_result,
    ));
    antfly_dense_search_result_free(&text_result);

    const hits_req =
        "{\"mode\":\"full_text\",\"index_name\":\"dv_v1\",\"text_query_type\":\"match\",\"field\":\"title\",\"text\":\"alpha\",\"limit\":1,\"offset\":0,\"include_stored\":false}";
    var hits_result: capi.DenseSearchResult = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_hits_json(
        handle_id,
        .{ .ptr = hits_req.ptr, .len = hits_req.len },
        &hits_result,
    ));
    antfly_dense_search_result_free(&hits_result);

    try std.testing.expectEqual(@as(usize, 9), recorder.count);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[0]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[1]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[2]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[3]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[4]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[5]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[6]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[7]);
    try std.testing.expectEqual(@as(u64, 42), recorder.group_ids[8]);
    try std.testing.expectEqualStrings("enrichment:lookup:read_index", recorder.contexts[0][0..recorder.context_lens[0]]);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.contexts[1][0..recorder.context_lens[1]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[2][0..recorder.context_lens[2]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[3][0..recorder.context_lens[3]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[4][0..recorder.context_lens[4]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[5][0..recorder.context_lens[5]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[6][0..recorder.context_lens[6]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[7][0..recorder.context_lens[7]]);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.contexts[8][0..recorder.context_lens[8]]);
}

test "capi relational expression errors preserve public status semantics" {
    // Keep the public C error mapping aligned without importing schema source
    // across the standalone C ABI module's directory boundary.
    const expression_errors = antfly.capi_dependencies.relational_expression_errors;
    inline for (@typeInfo(expression_errors.Error).error_set.error_names.?) |field| {
        const err = @field(expression_errors.Error, field);
        const expected: capi.ErrorCode = if (err == error.SqlFeatureNotSupported) .unsupported else if (expression_errors.isInvalidInput(err)) .invalid_argument else .intent_conflict;
        try std.testing.expectEqual(expected, capi.mapError(err));
    }
}

test "capi artifact decode and lookup json" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-artifact-test");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    cleanupTestDir(path);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer cleanupTestDir(path);
    defer antfly_db_close(handle_ptr);

    const handle = asHandle(handle_ptr).?;
    var artifact_ref = db_mod.types.ArtifactRef{
        .document_id = try handle.alloc.dupe(u8, "doc:a"),
        .name = try handle.alloc.dupe(u8, "body_chunks_v1"),
        .kind = .chunk,
        .chunk_id = 0,
    };
    defer artifact_ref.deinit(handle.alloc);
    const internal_key = try db_mod.artifact_ids.internalKeyForArtifactRefAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(internal_key);
    try handle.db.core.store.put(
        internal_key,
        "{\"body\":\"abcdefgh\",\"_artifact_name\":\"body_chunks_v1\",\"_chunk_id\":0,\"_artifact_unit_fingerprint\":\"private\"}",
    );

    const artifact_id = try db_mod.artifact_ids.artifactPublicIdAlloc(handle.alloc, artifact_ref);
    defer handle.alloc.free(artifact_id);
    const artifact_id_b64 = try dupBase64(handle.alloc, artifact_id);
    defer handle.alloc.free(artifact_id_b64);

    var decode_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_decode_artifact_id_json(.{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &decode_out));
    defer freeRawBuffer(decode_out.ptr, decode_out.len);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, decode_out.ptr.?[0..decode_out.len], "\"name\":\"body_chunks_v1\"") != null);

    var lookup_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_lookup_artifact_json(handle_ptr, .{
        .ptr = artifact_id_b64.ptr,
        .len = artifact_id_b64.len,
    }, &lookup_out));
    defer freeRawBuffer(lookup_out.ptr, lookup_out.len);
    var lookup_json = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        lookup_out.ptr.?[0..lookup_out.len],
        .{},
    );
    defer lookup_json.deinit();
    try std.testing.expect(lookup_json.value.object.get("artifact_ref") != null);
    const value_b64 = lookup_json.value.object.get("value_b64") orelse return error.TestUnexpectedResult;
    if (value_b64 != .string) return error.TestUnexpectedResult;
    const public_value = try decodeBase64Alloc(alloc, value_b64.string);
    defer alloc.free(public_value);
    var public_json = try std.json.parseFromSlice(std.json.Value, alloc, public_value, .{});
    defer public_json.deinit();
    if (public_json.value != .object) return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), public_json.value.object.count());
    try std.testing.expectEqualStrings("abcdefgh", public_json.value.object.get("body").?.string);
    try std.testing.expectEqualStrings("body_chunks_v1", public_json.value.object.get("_artifact_name").?.string);
    try std.testing.expectEqual(@as(i64, 0), public_json.value.object.get("_chunk_id").?.integer);
    try std.testing.expect(public_json.value.object.get("_artifact_unit_fingerprint") == null);
}

test "capi dense search profile breakdown" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestPath(alloc, test_tmp.path(), "capi-dense-profile");
    defer alloc.free(path);

    cleanupTestDir(path);

    var handle = Handle{
        .alloc = alloc,
        .db = try db_mod.DB.open(alloc, path, .{}),
    };
    const handle_id = try registerTestHandle(&handle);
    defer unregisterTestHandle(handle_id);
    defer {
        handle.db.close();
        cleanupTestDir(path);
    }

    try handle.db.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\"}",
    });

    const writes = try alloc.alloc(db_mod.types.BatchWrite, 2048);
    defer {
        for (writes) |write| alloc.free(write.value);
        alloc.free(writes);
    }
    for (writes, 0..) |*write, i| {
        const x: f32 = if (i % 2 == 0) 1 else 0;
        const y: f32 = if (i % 2 == 0) 0 else 1;
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"embedding\":[{d},{d}],\"title\":\"doc-{d}\"}}", .{ x, y, i }),
        };
    }
    defer for (writes) |write| alloc.free(write.key);

    try handle.db.batch(.{ .writes = writes });

    const req: db_mod.types.SearchRequest = .{
        .index_name = "dv_v1",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0 },
            .k = 10,
        } },
        .limit = 10,
        .include_stored = false,
    };

    const dense_entry = handle.db.core.index_manager.denseIndex("dv_v1").?;

    const reps: usize = 20;

    var hbc_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicNowNs();
        var result = try dense_entry.index.search(&.{ 1.0, 0.0 }, 10);
        defer result.deinit();
        hbc_total_ns += monotonicNowNs() - start;
    }

    var db_total_ns: u64 = 0;
    for (0..reps) |_| {
        const start = monotonicNowNs();
        var result = try handle.db.search(alloc, req);
        defer result.deinit();
        db_total_ns += monotonicNowNs() - start;
    }

    const request_json =
        "{\"mode\":\"dense\",\"index_name\":\"dv_v1\",\"vector\":[1,0],\"k\":10,\"limit\":10,\"offset\":0,\"include_stored\":false}";
    var capi_total_ns: u64 = 0;
    for (0..reps) |_| {
        var out: capi.Buffer = .{};
        const start = monotonicNowNs();
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_search_json(
            handle_id,
            .{ .ptr = request_json.ptr, .len = request_json.len },
            &out,
        ));
        capi_total_ns += monotonicNowNs() - start;
        freeRawBuffer(out.ptr, out.len);
    }

    std.debug.print(
        "dense_profile reps={d} hbc_avg_ns={d} db_avg_ns={d} capi_avg_ns={d}\n",
        .{
            reps,
            @divTrunc(hbc_total_ns, reps),
            @divTrunc(db_total_ns, reps),
            @divTrunc(capi_total_ns, reps),
        },
    );

    var final_result = try handle.db.search(alloc, req);
    defer final_result.deinit();
    try std.testing.expectEqual(@as(u32, 10), final_result.total_hits);
}

// This always runs (on every build, including the default) and only asserts
// on the build-capability signal, never on whether construction succeeded:
// `local_inference_runtime` and `inference_mode: "local_embedded"` must
// report true/present only when the loaded build both advertises and links
// the local inference runtime.
test "capi lite local-runtime-configured flag reports local_embedded only when the build links inference" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-variant-caps");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];

    // capi_build_options.inference_enabled is only true for the isolated
    // `-Dcapi-inference=true` storage_kernel unit and for unit tests
    // themselves; it is never true for the default build.
    if (capi_build_options.inference_enabled and lite_backend.capabilitiesForProfile(.native).local_inference_runtime) {
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference_mode\":\"local_embedded\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"local_inference_runtime\":true") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference != null);
    } else {
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"inference_mode\":\"caller_supplied_or_disabled\"") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference == null);
    }
}

// Confirms `EmbeddedInferenceNodeOptions` plumbing end to end: an explicit
// process-memory budget override passed through `antfly_open_options`
// is what the embedded node actually resolves and reports back in
// `antfly_db_status_json`'s "inference" object, rather than the previous
// hardcoded zero-bytes/"automatic" policy that gave every Lite handle no way
// to distinguish "host-detected" from "unset" (see
// `inference_provider.createEmbeddedInferenceNode` and
// `LiteResolvedOpenOptions.inference`). Runs on every build (including the
// default, where the local runtime never actually starts) and only asserts
// the reported budgets on the build-capability signal, matching the sibling
// local-runtime test above.
test "capi lite explicit resource budget overrides are reported in status" {
    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-budget-status");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
        .inference_host_budget_mb = 256,
        .inference_backend_budget_mb = 128,
        .inference_process_memory_budget_mb = 512,
        .inference_combined_budget_mb = 384,
        .inference_kv_budget_mb = 64,
        .inference_scratch_budget_mb = 32,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];

    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"host_budget_mb\":256") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"backend_budget_mb\":128") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_budget_mb\":512") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"combined_budget_mb\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"kv_budget_mb\":64") != null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"scratch_budget_mb\":32") != null);

    if (capi_build_options.inference_enabled and lite_backend.capabilitiesForProfile(.native).local_inference_runtime) {
        // The node actually started against the explicit override: the
        // resolved envelope is exactly the requested 512 MiB (clamped by
        // `resolveEffectiveDetailed` only when the detected host is
        // smaller, which a 512 MiB request never exceeds on any test
        // runner), and its provenance is "explicit", not "automatic".
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_bytes\":536870912") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_source\":\"explicit\"") != null);
        const owned_handle = asHandle(handle).?;
        try std.testing.expect(owned_handle.lite_inference != null);
    } else {
        // No local runtime started, so the resolution never ran; status
        // still echoes the caller's requested override values (asserted
        // above) but the resolved fields stay at their zero-value/automatic
        // defaults.
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_bytes\":0") != null);
        try std.testing.expect(std.mem.indexOf(u8, status_json, "\"process_memory_limit_source\":\"automatic\"") != null);
    }
}

// A Lite handle opened with the local-runtime flag but no explicit budget
// overrides must not fall back to the previous zero-bytes/automatic
// generation-budget policy: that policy could not admit even one
// boundary-architecture extraction window (fastino/gliner2.5-base-v1's
// admission estimate exceeds it regardless of request size -- see
// GLINER25.md's "Memory budget" section and this task's
// gliner25-longdoc-handoff.md), so every such call failed with
// error.MemoryBudgetExceeded through the embedded/in-process worker path
// even though `antfly inference run` succeeded (only because an operator
// supplied `--host-budget-mb`/`--backend-budget-mb`/`--combined-budget-mb`/
// `--kv-budget-mb`/`--scratch-budget-mb` by hand). Confirms the embedded
// node instead defaults each lane to
// `inference_provider.default_{host,backend,combined,kv,scratch}_budget_mb`
// (clamped to the host-detected envelope), and logs the resolved values for
// the machine running this test.
test "capi lite defaults embedded generation budgets when no override is given" {
    if (!capi_build_options.inference_enabled) return error.SkipZigTest;
    if (!lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const alloc = std.testing.allocator;
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-inference-budget-defaults");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);

    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    const status_json = status.ptr.?[0..status.len];
    std.debug.print("capi lite default embedded generation budgets on this machine: {s}\n", .{status_json});

    // None of these lanes may resolve to 0 (the previous automatic/unbounded
    // policy that admitted no boundary-model window). host/backend/scratch
    // default to 16384 MiB, combined to 32768, kv to 4096, each clamped to
    // the host-detected envelope; a real dev/CI machine has well over 4 GiB,
    // so all five stay at their un-clamped defaults on any realistic runner.
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"host_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"backend_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"combined_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"kv_budget_mb\":0") == null);
    try std.testing.expect(std.mem.indexOf(u8, status_json, "\"scratch_budget_mb\":0") == null);
}

test "capi lite drains an antfly embedder with no api_url through the embedded inference provider" {
    if (!capi_build_options.inference_enabled) return error.SkipZigTest;
    if (!lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    if (!liteLocalEmbeddingModelAvailable(alloc)) return error.SkipZigTest;

    var test_tmp = try TestDirectory.init("capi");
    defer test_tmp.cleanup();
    const path = try tempTestAflitePath(alloc, test_tmp.path(), "capi-lite-local-embedding-drain");
    defer alloc.free(path);
    cleanupTestFile(path);
    defer cleanupTestFile(path);

    var options = capi.OpenOptions{
        .storage_kind = capi.storage_kind_lite,
        .abi_size = @sizeOf(capi.OpenOptions),
        .flags = capi.open_flag_local_runtime_configured,
    };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owned_handle = asHandle(handle).?;
    try std.testing.expect(owned_handle.lite_inference != null);

    const index_json =
        \\{"name":"body_embedding","kind":"dense_vector","config_json":"{\"type\":\"embeddings\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .{
        .ptr = index_json.ptr,
        .len = index_json.len,
    }));

    const enrichment_json =
        \\{"name":"body_embedder","kind":"embedding","field":"body","vector_space":"body_embedding","producer_json":"{\"type\":\"embedder\",\"config\":{\"provider\":\"antfly\",\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\"}}"}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_enrichment_json(handle, .{
        .ptr = enrichment_json.ptr,
        .len = enrichment_json.len,
    }));

    const batch_json = "{\"inserts\":{\"doc:capi-local-embedding\":{\"body\":\"antfly lite embeds documents locally\"}},\"sync_level\":\"write\"}";
    var batch_out: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .{
        .ptr = batch_json.ptr,
        .len = batch_json.len,
    }, &batch_out));
    defer freeRawBuffer(batch_out.ptr, batch_out.len);

    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));

    const drained = try enrichmentDrainStats(handle);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.error_count);
    try std.testing.expectEqual(@as(u64, 0), drained.enrichment.fatal_error_count);
    try std.testing.expect(!drained.enrichment.stalled);
    try std.testing.expectEqual(drained.enrichment.target_sequence, drained.enrichment.applied_sequence);
}

test "storage owner runtime status bulk recovery bridge preserves identities capability and debt" {
    const Capture = struct {
        calls: usize = 0,
        result: kernel_owner_abi.Status = .ok,
        fn acknowledge(ptr: ?*anyopaque, txn: *const kernel_owner_abi.TxnId, owner: kernel_owner_abi.BorrowedBytes, items: ?[*]const kernel_owner_abi.BorrowedBytes, len: usize) callconv(.c) kernel_owner_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            std.testing.expectEqual(@as([16]u8, @splat(5)), txn.bytes) catch return .internal;
            std.testing.expectEqualStrings("owner", owner.slice()) catch return .internal;
            std.testing.expectEqual(@as(usize, 2), len) catch return .internal;
            std.testing.expectEqualStrings("first", items.?[0].slice()) catch return .internal;
            std.testing.expectEqualStrings("second", items.?[1].slice()) catch return .internal;
            return self.result;
        }
    };
    var capture: Capture = .{};
    var bridge: StorageOwnerTransactionRecovery = undefined;
    var owner_id = [_]u8{ 'o', 'w', 'n', 'e', 'r' };
    bridge.owner_id = &owner_id;
    bridge.config = .{ .callback_ctx = &capture, .replicated_metadata = 1, .acknowledge_participants_fn = Capture.acknowledge };
    const config = bridge.dbConfig();
    try std.testing.expect(config.factory != null);
    try StorageOwnerTransactionRecovery.acknowledgeParticipants(&bridge, @splat(5), "owner", &.{ "first", "second" });
    for ([_]anyerror{ error.UnsupportedOperation, error.UnsupportedRaftBatchProtocolVersion, error.RaftBatchWriteOutcomeUnknown }) |err| {
        capture.result = kernel_error_identity.statusFromError(err);
        try std.testing.expectError(err, StorageOwnerTransactionRecovery.acknowledgeParticipants(&bridge, @splat(5), "owner", &.{ "first", "second" }));
    }
    try std.testing.expectError(error.InvalidParticipant, StorageOwnerTransactionRecovery.acknowledgeParticipants(&bridge, @splat(5), "owner", &.{}));
    try std.testing.expectEqual(@as(usize, 4), capture.calls);
    bridge.config.acknowledge_participants_fn = null;
    try std.testing.expectError(error.UnsupportedOperation, StorageOwnerTransactionRecovery.acknowledgeParticipants(&bridge, @splat(5), "owner", &.{ "first", "second" }));
}

// These callbacks are installed only by the private server owner. Public Lite
// handles share the registry and destruction ordering without importing server
// context, recovery, or coordination implementations.

test "capi fact relationships preserve identities and filter before ranking" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-facts");
    defer directory.cleanup();
    const path = try tempTestPath(alloc, directory.path(), "facts");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    const handle = asHandle(handle_ptr).?;
    try handle.db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{}" });
    try handle.db.batch(.{
        .writes = &.{
            .{ .key = "a", .value = "{}" },            .{ .key = "b", .value = "{}" },
            .{ .key = "fact:one", .value = "{}" },     .{ .key = "fact:two", .value = "{}" },
            .{ .key = "fact:expired", .value = "{}" },
        },
        .graph_writes = &.{
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "fact:one", .weight = 1, .metadata_json = "{\"value\":1.0000000000000001}" },
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "two", .owner_document = "fact:two", .weight = 2, .metadata_json = "{}" },
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "expired", .owner_document = "fact:expired", .weight = 0.1, .metadata_json = "{\"invalid_at\":\"2020-01-01T00:00:00Z\"}" },
        },
        .sync_level = .full_index,
    });
    var edge_output: capi.Buffer = .{};
    defer freeRawBuffer(edge_output.ptr, edge_output.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_get_edges_json(handle_ptr, .{ .ptr = "facts", .len = 5 }, .{ .ptr = "a", .len = 1 }, .{ .ptr = "R", .len = 1 }, 0, &edge_output));
    const edge_bytes = edge_output.ptr.?[0..edge_output.len];
    try std.testing.expect(std.mem.indexOf(u8, edge_bytes, "\"edge_id_b64\":\"b25l\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edge_bytes, "\"owner_document_b64\":\"ZmFjdDpvbmU=\"") != null);
    const requests = [_][]const u8{
        \\{"index_name":"facts","start_key_b64":"YQ==","max_depth":1,"deduplicate_nodes":false,"include_paths":true,"edge_filter":{"valid_at":"2022-01-01T00:00:00Z"}}
        ,
        \\{"index_name":"facts","source_b64":"YQ==","target_b64":"Yg==","weight_mode":"min_weight","edge_filter":{"valid_at":"2022-01-01T00:00:00Z"}}
        ,
        \\{"index_name":"facts","source_b64":"YQ==","target_b64":"Yg==","weight_mode":"min_weight","k":3,"edge_filter":{"valid_at":"2022-01-01T00:00:00Z"}}
        ,
        \\{"graph_queries":[{"name":"walk","type":"traverse","index_name":"facts","start_nodes":{"keys":["YQ=="]},"deduplicate":false,"include_paths":true,"max_depth":1,"edge_filter":{"valid_at":"2022-01-01T00:00:00Z"}}],"named_sets":[]}
        ,
    };
    for (requests, 0..) |request, i| {
        var output: capi.Buffer = .{};
        defer freeRawBuffer(output.ptr, output.len);
        const slice = capi.Slice{ .ptr = request.ptr, .len = request.len };
        const status = switch (i) {
            0 => antfly_db_traverse_edges_json(handle_ptr, slice, &output),
            1 => antfly_db_find_shortest_path_json(handle_ptr, slice, &output),
            2 => antfly_db_find_k_shortest_paths_json(handle_ptr, slice, &output),
            else => antfly_db_execute_graph_queries_json(handle_ptr, slice, &output),
        };
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
        const bytes = output.ptr.?[0..output.len];
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"edge_id_b64\":\"b25l\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"owner_document_b64\":\"ZmFjdDpvbmU=\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "ZXhwaXJlZA==") == null);
        if (i != 1) try std.testing.expect(std.mem.indexOf(u8, bytes, "\"edge_id_b64\":\"dHdv\"") != null);
    }
    const decimal_requests = [_][]const u8{
        \\{"index_name":"facts","start_key_b64":"YQ==","max_depth":1,"include_paths":true,"edge_filter":{"properties":[{"field":"/metadata/value","op":"eq","value":1.0000000000000001}]}}
        ,
        \\{"index_name":"facts","source_b64":"YQ==","target_b64":"Yg==","edge_filter":{"properties":[{"field":"/metadata/value","op":"eq","value":1.0000000000000001}]}}
        ,
        \\{"index_name":"facts","source_b64":"YQ==","target_b64":"Yg==","k":2,"edge_filter":{"properties":[{"field":"/metadata/value","op":"eq","value":1.0000000000000001}]}}
        ,
        \\{"graph_queries":[{"name":"walk","type":"traverse","index_name":"facts","start_nodes":{"keys":["YQ=="]},"include_paths":true,"max_depth":1,"edge_filter":{"properties":[{"field":"/metadata/value","op":"eq","value":1.0000000000000001}]}}],"named_sets":[]}
        ,
    };
    for (decimal_requests, 0..) |request, i| {
        var output: capi.Buffer = .{};
        defer freeRawBuffer(output.ptr, output.len);
        const slice = capi.Slice{ .ptr = request.ptr, .len = request.len };
        const status = switch (i) {
            0 => antfly_db_traverse_edges_json(handle_ptr, slice, &output),
            1 => antfly_db_find_shortest_path_json(handle_ptr, slice, &output),
            2 => antfly_db_find_k_shortest_paths_json(handle_ptr, slice, &output),
            else => antfly_db_execute_graph_queries_json(handle_ptr, slice, &output),
        };
        try std.testing.expectEqual(capi.ErrorCode.ok, status);
        const bytes = output.ptr.?[0..output.len];
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"edge_id_b64\":\"b25l\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"edge_id_b64\":\"dHdv\"") == null);
    }
    for ([_][]const u8{
        "{\"index_name\":\"facts\",\"start_key_b64\":\"YQ==\",\"edge_filter\":true}",
        "{\"index_name\":\"facts\",\"start_key_b64\":\"YQ==\",\"edge_filter\":[]}",
        "{\"index_name\":\"facts\",\"start_key_b64\":\"YQ==\",\"edge_filter\":42}",
    }) |malformed| {
        var rejected: capi.Buffer = .{};
        try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_traverse_edges_json(handle_ptr, .{ .ptr = malformed.ptr, .len = malformed.len }, &rejected));
    }
    const bad = "{\"index_name\":\"facts\",\"start_key_b64\":\"YQ==\",\"edge_filter\":{\"known_at\":\"bad\"}}";
    var output: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_db_traverse_edges_json(handle_ptr, .{ .ptr = bad.ptr, .len = bad.len }, &output));
}

test "capi fact path serialization and parsing release partial allocations" {
    const alloc = std.testing.allocator;
    const edge = paths_mod.PathEdge{ .source = "a", .target = "b", .edge_type = "R", .edge_id = "one\xff", .owner_document = "fact\x00", .weight = 1, .metadata = "{\"tenant\":\"g\"}", .traversal_direction = .out };
    const Case = struct {
        fn serialize(a: Allocator, value: paths_mod.PathEdge) !void {
            var encoded = try JsonPathEdge.init(a, value);
            defer encoded.deinit(a);
            const decoded_id = try decodeBase64Alloc(a, encoded.edge_id_b64.?);
            defer a.free(decoded_id);
            try std.testing.expectEqualStrings(value.edge_id, decoded_id);
            try std.testing.expectEqualStrings(value.metadata, encoded.metadata_json);
        }
        fn parse(a: Allocator) !void {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"properties\":[{\"field\":\"/metadata/tenant\",\"op\":\"eq\",\"value\":\"g\"}]}", .{});
            defer parsed.deinit();
            var query = try parseGraphQueryRequestOwned(a, .{ .name = "walk", .type = "traverse", .index_name = "facts", .start_nodes = .{ .keys = &.{"YQ=="} }, .edge_types = &.{"R"}, .edge_filter = parsed.value });
            defer deinitOwnedGraphQuery(a, &query);
            try std.testing.expect(query.params.edge_filter.active());
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Case.serialize, .{edge});
    try std.testing.checkAllAllocationFailures(alloc, Case.parse, .{});
}

test "capi fact algebraic paths retain provenance and respect frontier limits" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-fact-algebraic");
    defer directory.cleanup();
    const path = try tempTestPath(alloc, directory.path(), "facts");
    defer alloc.free(path);
    var handle_ptr: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle_ptr));
    defer antfly_db_close(handle_ptr);
    const handle = asHandle(handle_ptr).?;
    try handle.db.addIndex(.{ .name = "facts", .kind = .graph, .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}" });
    try handle.db.batch(.{
        .writes = &.{ .{ .key = "a", .value = "{}" }, .{ .key = "b", .value = "{}" }, .{ .key = "c", .value = "{}" }, .{ .key = "d", .value = "{}" }, .{ .key = "fact:one", .value = "{}" } },
        .graph_writes = &.{
            .{ .index_name = "facts", .source = "a", .target = "b", .edge_type = "R", .edge_id = "one", .owner_document = "fact:one", .weight = 2, .metadata_json = "{\"fact\":1}" },
            .{ .index_name = "facts", .source = "a", .target = "c", .edge_type = "R", .edge_id = "two" },
            .{ .index_name = "facts", .source = "b", .target = "d", .edge_type = "R", .edge_id = "three" },
        },
        .sync_level = .full_index,
    });
    for ([_]usize{ (paths_mod.PathFindOptions{}).max_intermediate_states, 8 }) |frontier_limit| {
        for ([_]graph_mod.EdgeDirection{ .out, .in, .both }) |direction| {
            const source: []const u8 = if (direction == .in) "b" else "a";
            const target: []const u8 = if (direction == .in) "a" else "b";
            const result = (try handle.db.findShortestPathWithOptions(alloc, "facts", source, target, .{ .direction = direction, .weight_mode = .min_hops, .max_depth = 2, .max_intermediate_states = frontier_limit })).?;
            defer paths_mod.freePath(alloc, result);
            try std.testing.expectEqualStrings("one", result.edges[0].edge_id);
            try std.testing.expectEqualStrings("fact:one", result.edges[0].owner_document);
            try std.testing.expectEqualStrings("{\"fact\":1}", result.edges[0].metadata);
            try std.testing.expectEqual(if (direction == .in) graph_mod.EdgeDirection.in else graph_mod.EdgeDirection.out, result.edges[0].traversal_direction.?);
        }
    }
    const queries = [_][]const u8{
        \\{"graph_queries":[{"name":"walk","type":"traverse","index_name":"facts","start_nodes":{"keys":["YQ=="]},"include_paths":true,"max_depth":1}],"named_sets":[]}
        ,
        \\{"graph_queries":[{"name":"path","type":"shortest_path","index_name":"facts","start_nodes":{"keys":["YQ=="]},"target_nodes":{"keys":["Yg=="]},"weight_mode":"min_hops","max_depth":2}],"named_sets":[]}
        ,
    };
    for (queries) |query| {
        var output: capi.Buffer = .{};
        defer freeRawBuffer(output.ptr, output.len);
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_execute_graph_queries_json(handle_ptr, .{ .ptr = query.ptr, .len = query.len }, &output));
        const bytes = output.ptr.?[0..output.len];
        try std.testing.expect(std.mem.indexOf(u8, bytes, "fact:one") != null or std.mem.indexOf(u8, bytes, "ZmFjdDpvbmU=") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\\\"fact\\\":1") != null);
        try std.testing.expect(std.mem.indexOf(u8, bytes, "\"traversal_direction\":0") != null);
    }
    try std.testing.expectError(error.GraphWorkBudgetExceeded, handle.db.findKShortestPathsWithOptions(alloc, "facts", "a", "d", 1, .{ .weight_mode = .min_hops, .max_depth = 2, .max_intermediate_states = 1 }));
    try std.testing.expectError(error.GraphWorkBudgetExceeded, handle.db.findShortestPathWithOptions(alloc, "facts", "a", "d", .{ .weight_mode = .min_hops, .max_depth = 2, .max_intermediate_states = 1 }));
}

test "capi fact edge cleanup releases the owned array exactly once" {
    const alloc = std.testing.allocator;
    const edges = try alloc.alloc(graph_mod.Edge, 1);
    edges[0] = .{ .source = "", .target = "", .edge_type = "", .weight = 1, .created_at = 0, .updated_at = 0, .metadata = "" };
    graphFreeEdges(alloc, edges);
}

test "capi SQL DROP INDEX retains catalog lookup and explicit table ownership" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-drop-index-merge");
    defer directory.cleanup();
    const path = try tempTestPath(a, directory.path(), "sql");
    defer a.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open(path, &handle));
    defer antfly_db_close(handle);
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_set_schema_json(handle, .fromSlice(schema)));
    for ([_][]const u8{
        "CREATE INDEX merge_idx ON \"default\" (id)",
        "DROP INDEX merge_idx",
        "CREATE INDEX merge_idx ON \"default\" (id)",
        "DROP INDEX merge_idx ON \"default\"",
        "DROP INDEX IF EXISTS missing_merge_idx",
    }, 0..) |sql, index| {
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = sql }, .{});
        defer a.free(request);
        var out: capi.Buffer = .{};
        defer freeRawBuffer(out.ptr, out.len);
        try executeEmbeddedSql(asHandle(handle).?, "default", request, &out);
        const bytes = (try asHandle(handle).?.db.getSchemaJson(a)).?;
        defer a.free(bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer parsed.deinit();
        const indexes = parsed.value.object.get("relational_indexes").?.array.items;
        try std.testing.expectEqual(@as(usize, if (index == 0 or index == 2) 1 else 0), indexes.len);
    }
}

fn sqlRecoveryScans(handle: ?*anyopaque) !struct { ddl: u64, commit: u64 } {
    const guard = public.enterHandle(handle, .exclusive) orelse return error.InvalidArgument;
    defer guard.leave();
    try std.testing.expect(guard.entry_error == null);
    try std.testing.expect(!guard.handle.sql_ddl_recovery_needed);
    try std.testing.expect(!guard.handle.sql_commit_recovery_needed);
    return .{ .ddl = guard.handle.sql_ddl_recovery_scans, .commit = guard.handle.sql_commit_recovery_scans };
}

test "capi SQL detached maintenance publishes through native storage admission" {
    const alloc = std.testing.allocator;
    const embedder = @import("antfly_local_sources").storage_db_enrichment_embedder;
    var deterministic: embedder.DeterministicDenseEmbedder = .{};
    var directory = try TestDirectory.init("capi-sql-maintenance-publication");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const connection = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try guard.handle.db.addIndex(.{
            .name = "body_dense",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"publication_policy\":\"progressive\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense\"}}",
        });
        try guard.handle.db.reconfigureEnrichmentRuntimePaused(.{ .dense_embedder = .{
            .ptr = &deterministic,
            .dense_embed_fn = embedder.DeterministicDenseEmbedder.embedDense,
            .dense_embed_with_context_fn = struct {
                fn embed(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, text: []const u8, dims: u32, context: @import("antfly_inference_execution_context").RequestContext) ![]f32 {
                    try context.check();
                    return embedder.DeterministicDenseEmbedder.embedDense(ptr, a, name, text, dims);
                }
            }.embed,
            .foreground_bounded = true,
        } });
        try guard.handle.db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\"}" }}, .sync_level = .write });
        connection.maintenance_pending = true;
    }
    var idle = false;
    for (0..64) |_| {
        if (!try connection.maintenanceTurnForTest()) {
            idle = true;
            break;
        }
    }
    try std.testing.expect(idle);
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    const key = try @import("antfly_local_sources").storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "body_dense");
    defer alloc.free(key);
    const artifact = (try guard.handle.db.core.getStoreValue(alloc, key)) orelse return error.TestExpectedArtifact;
    defer alloc.free(artifact);
    const debt = try guard.handle.db.listDerivedReplayDebt(alloc);
    defer {
        for (debt) |*item| item.deinit(alloc);
        alloc.free(debt);
    }
    for (debt) |item| try std.testing.expect(!item.catch_up_required);
}

test "capi SQL background embedding releases the file gate and fences stale results" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |source_changed| {
        const embedder = @import("antfly_local_sources").storage_db_enrichment_embedder;
        const Gate = struct {
            started: std.Io.Event = .unset,
            allowed: std.Io.Event = .unset,
            deterministic: embedder.DeterministicDenseEmbedder = .{},
            calls: std.atomic.Value(usize) = .init(0),

            fn computationKey(_: *anyopaque, name: []const u8, text: []const u8, dims: u32) !?[32]u8 {
                var hash = std.crypto.hash.Blake3.init(.{});
                hash.update(name);
                hash.update(std.mem.asBytes(&dims));
                hash.update(text);
                var digest: [32]u8 = undefined;
                hash.final(&digest);
                return digest;
            }

            fn embed(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, text: []const u8, dims: u32) ![]f32 {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const io = shared.handleLockIo();
                _ = self.calls.fetchAdd(1, .monotonic);
                self.started.set(io);
                try self.allowed.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
                return embedder.DeterministicDenseEmbedder.embedDense(&self.deterministic, a, name, text, dims);
            }

            fn embedWithContext(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, text: []const u8, dims: u32, context: @import("antfly_inference_execution_context").RequestContext) ![]f32 {
                try context.check();
                return embed(ptr, a, name, text, dims);
            }

            fn interface(self: *@This()) embedder.DenseEmbedder {
                return .{ .ptr = self, .dense_embed_fn = embed, .computation_key_fn = computationKey, .dense_embed_with_context_fn = embedWithContext, .foreground_bounded = true };
            }
        };
        var gate: Gate = .{};
        defer gate.allowed.set(shared.handleLockIo());
        var directory = try TestDirectory.init("capi-sql-detached-maintenance");
        defer directory.cleanup();
        const path = try tempTestAflitePath(alloc, directory.path(), "database");
        defer alloc.free(path);
        defer cleanupTestFile(path);
        var options: capi.OpenOptions = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
        options.storage_kind = capi.storage_kind_lite;
        options.profile = capi.profile_hosted;
        var handle: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
        defer antfly_db_close(handle);
        const connection = blk: {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            break :blk guard.connection.?.owner;
        };
        var captured_runtime: std.meta.Child(@FieldType(db_mod.DB, "enrichment_runtime")) = undefined;
        var captured_database: *db_mod.DB = undefined;
        {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            try guard.handle.db.setSchemaJson(alloc,
                \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"}},"additionalProperties":true}}}}
            );
            try guard.handle.db.addIndex(.{
                .name = "body_dense",
                .kind = .dense_vector,
                .config_json = "{\"field\":\"embedding\",\"dims\":3,\"publication_policy\":\"progressive\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense\"}}",
            });
            try guard.handle.db.reconfigureEnrichmentRuntimePaused(.{ .dense_embedder = gate.interface() });
            captured_runtime = guard.handle.db.enrichment_runtime.?;
            captured_database = &guard.handle.db;
            try guard.handle.db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"original\"}" }}, .sync_level = .write });
            connection.maintenance_pending = true;
        }
        const Background = struct {
            fn run(c: @TypeOf(connection)) void {
                _ = c.maintenanceTurnForTest() catch {};
            }
        };
        const background = try std.Thread.spawn(.{}, Background.run, .{connection});
        var background_joined = false;
        defer if (!background_joined) {
            gate.allowed.set(shared.handleLockIo());
            background.join();
        };
        try gate.started.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        const Foreground = struct {
            handle: ?*anyopaque,
            source_changed: bool,
            done: std.Io.Event = .unset,
            stop: std.atomic.Value(bool) = .init(false),
            writes: std.atomic.Value(usize) = .init(0),
            status: capi.ErrorCode = .internal,
            fn run(self: *@This()) void {
                var response: capi.Buffer = .{};
                self.status = antfly_db_sql_json(self.handle, .fromSlice(if (self.source_changed) "{\"statement\":\"UPDATE \\\"default\\\" SET body='foreground' WHERE _id='doc'\"}" else "{\"statement\":\"INSERT INTO \\\"default\\\" (_id,body) VALUES ('new','foreground')\"}"), &response);
                freeRawBuffer(response.ptr, response.len);
                self.done.set(shared.handleLockIo());
                while (!self.source_changed and !self.stop.load(.acquire) and self.status == .ok) {
                    const n = self.writes.fetchAdd(1, .monotonic);
                    const request = std.fmt.allocPrint(std.heap.c_allocator, "{{\"statement\":\"UPDATE \\\"default\\\" SET body='foreground{d}' WHERE _id='new'\"}}", .{n}) catch break;
                    defer std.heap.c_allocator.free(request);
                    var output: capi.Buffer = .{};
                    self.status = antfly_db_sql_json(self.handle, .fromSlice(request), &output);
                    freeRawBuffer(output.ptr, output.len);
                }
            }
        };
        var foreground: Foreground = .{ .handle = handle, .source_changed = source_changed };
        const writer = try std.Thread.spawn(.{}, Foreground.run, .{&foreground});
        var writer_joined = false;
        defer if (!writer_joined) {
            foreground.stop.store(true, .release);
            gate.allowed.set(shared.handleLockIo());
            writer.join();
        };
        const completed_while_embedding = if (foreground.done.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } })) |_| true else |_| false;
        if (completed_while_embedding) {
            // The common staging boundary must preserve document identity as
            // well as its bytes. Exercise it directly while inference is parked.
            const port = captured_runtime.generated_publication.?;
            const staging = try port.acquire_store(port.ptr, null, 0);
            defer staging.release(staging.ptr);
            const artifact = try @import("antfly_local_sources").storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "body_dense");
            defer alloc.free(artifact);
            const before = connection.publication_stats.generated_refused;
            if (source_changed) {
                try std.testing.expectError(error.EnrichmentWaitCanceled, staging.guard_key(staging.ptr, artifact));
                try std.testing.expectEqual(before + 1, connection.publication_stats.generated_refused);
                // Multiple guards in one refused store lease count once.
                try std.testing.expectError(error.EnrichmentWaitCanceled, staging.guard_key(staging.ptr, artifact));
                try std.testing.expectEqual(before + 1, connection.publication_stats.generated_refused);
            } else {
                try staging.guard_key(staging.ptr, artifact);
                try std.testing.expectEqual(before, connection.publication_stats.generated_refused);
            }
        }
        if (source_changed) {
            // Projection-only effects do not appear in changed_doc_keys, but
            // they still must reject a source replaced since capture.
            try std.testing.expectError(error.EnrichmentWaitCanceled, captured_database.derivedAsyncAppendDerivedBatchRecord(.{
                .documents = &.{.{ .key = "doc", .action = .preserve_base_document }},
            }));
            const guard = public.enterHandle(handle, .read).?;
            defer guard.leave();
            try std.testing.expectEqual(@as(u64, 1), connection.publication_stats.replay_refused);
        }
        {
            const port = captured_runtime.generated_publication.?;
            // A missing lease rejects both the staging and final commit ports.
            try std.testing.expectError(error.EnrichmentLeaseFenceLost, port.acquire_store(port.ptr, .{ .lease_key = "__antfly_test_missing_lease", .owner_id = "review", .epoch = 1 }, 0));
            try std.testing.expectError(error.EnrichmentWaitCanceled, port.commit(port.ptr, .{}, &.{}, &.{}, &.{}, .{ .lease_key = "__antfly_test_missing_lease", .owner_id = "review", .epoch = 1 }));
            const guard = public.enterHandle(handle, .read).?;
            defer guard.leave();
            const expected_refusals: u64 = if (source_changed) 3 else 2;
            try std.testing.expectEqual(expected_refusals, connection.publication_stats.generated_refused);
            try std.testing.expect(connection.publication_stats.refused >= expected_refusals);
        }
        gate.allowed.set(shared.handleLockIo());
        background.join();
        foreground.stop.store(true, .release);
        writer.join();
        writer_joined = true;
        background_joined = true;
        try std.testing.expect(completed_while_embedding);
        try std.testing.expectEqual(capi.ErrorCode.ok, foreground.status);
        // Publication uses the current owner and validates only its actual inputs.
        {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            try std.testing.expect(guard.entry_error == null);
            const result = (try guard.handle.db.lookup(alloc, if (source_changed) "doc" else "new", .{})).?;
            defer alloc.free(result.json);
            try std.testing.expect(std.mem.indexOf(u8, result.json, "foreground") != null);
            try std.testing.expectEqual(@as(usize, 0), connection.retired.items.len);
            const artifact_key = try @import("antfly_local_sources").storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "body_dense");
            defer alloc.free(artifact_key);
            const completed = try guard.handle.db.core.getStoreValue(alloc, artifact_key);
            defer if (completed) |bytes| alloc.free(bytes);
            // A completed result survives unrelated commits, while an updated
            // source fails its live transactional digest/incarnation guard.
            try std.testing.expect((completed == null) == source_changed);
            try guard.handle.db.reconfigureEnrichmentRuntimePaused(.{ .dense_embedder = gate.interface() });
            connection.maintenance_pending = true;
        }
        try std.testing.expectEqual(@as(usize, 1), gate.calls.load(.monotonic));
        var idle = false;
        for (0..64) |_| {
            if (!try connection.maintenanceTurnForTest()) {
                idle = true;
                break;
            }
        }
        try std.testing.expect(idle);
        // One original computation and one new document: retry reused the first
        // result, then the normal source/generation guards published it safely.
        try std.testing.expectEqual(@as(usize, 2), gate.calls.load(.monotonic));
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const key = try @import("antfly_local_sources").storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "body_dense");
        defer alloc.free(key);
        const artifact = (try guard.handle.db.core.getStoreValue(alloc, key)) orelse return error.TestExpectedArtifact;
        defer alloc.free(artifact);
    }
}

// Force the same replacement path as a captured maintenance generation, without
// relying on a background timer or hundreds of writes to win a scheduling race.
fn sqlRefreshCleanGeneration(handle: ?*anyopaque) !void {
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    try std.testing.expect(guard.entry_error == null);
    try std.testing.expect(!guard.handle.sql_ddl_recovery_needed);
    try std.testing.expect(!guard.handle.sql_commit_recovery_needed);
    const connection = guard.connection.?;
    const owner = connection.owner;
    try std.testing.expect(owner.maintenance_snapshot == null);
    const previous = guard.handle;
    previous.maintenance_references += 1;
    defer previous.maintenance_references -= 1;
    owner.maintenance_snapshot = previous;
    defer owner.maintenance_snapshot = null;
    connection.detach();
    const replacement = try owner.refresh();
    try std.testing.expect(replacement != previous);
    try std.testing.expect(!replacement.sql_ddl_recovery_needed);
    try std.testing.expect(!replacement.sql_commit_recovery_needed);
    try std.testing.expectEqual(@as(usize, 0), replacement.embedded_tables.count());
    try std.testing.expect(!replacement.embedded_tables_discovered);
}

fn sqlSessionStatement(handle: ?*anyopaque, session_id: u64, statement: []const u8) !void {
    const a = std.testing.allocator;
    const request = try std.json.Stringify.valueAlloc(a, .{ .statement = statement, .session_id = session_id }, .{});
    defer a.free(request);
    var response: capi.Buffer = .{};
    const status = antfly_db_sql_json(handle, .fromSlice(request), &response);
    defer freeRawBuffer(response.ptr, response.len);
    if (status != .ok) std.debug.print("session SQL failure {s}: {s}\n", .{ statement, if (response.ptr) |ptr| ptr[0..response.len] else "" });
    try std.testing.expectEqual(capi.ErrorCode.ok, status);
}

test "capi SQL refreshed generations retain complete FK catalogs and unqualified indexes" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-refreshed-integrity");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    try sqlFkRecoveryStatement(handle, "CREATE TABLE parent(id TEXT PRIMARY KEY)");
    try sqlFkRecoveryStatement(handle, "CREATE TABLE child(id TEXT PRIMARY KEY, parent_id TEXT NOT NULL REFERENCES parent(id) ON DELETE CASCADE)");
    try sqlFkRecoveryStatement(handle, "INSERT INTO parent(id) VALUES ('p')");
    var session: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, struct {
        extern fn antfly_db_sql_session_open(?*anyopaque, *u64) capi.ErrorCode;
    }.antfly_db_sql_session_open(handle, &session));
    try sqlSessionStatement(handle, session, "BEGIN ISOLATION LEVEL READ COMMITTED");
    try sqlSessionStatement(handle, session, "INSERT INTO child(id,parent_id) VALUES ('c','p')");
    try sqlSessionStatement(handle, session, "DELETE FROM child WHERE id IN (SELECT id FROM child WHERE parent_id='p' ORDER BY id DESC OFFSET 8)");
    try sqlRefreshCleanGeneration(handle);
    try sqlSessionStatement(handle, session, "COMMIT");
    // Discovery is cached per generation, independently of durable recovery.
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try std.testing.expect(guard.handle.embedded_tables_discovered);
        try std.testing.expectEqual(@as(usize, 2), guard.handle.embedded_tables.count());
        try std.testing.expectEqual(@as(u64, 0), guard.handle.sql_ddl_recovery_scans);
    }
    try sqlRefreshCleanGeneration(handle);
    // Ordinary reads remain lazy and must not probe unrelated tables.
    try sqlSessionStatement(handle, session, "SELECT id FROM child");
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try std.testing.expectEqual(@as(usize, 1), guard.handle.embedded_tables.count());
        try std.testing.expectEqual(@as(u64, 0), guard.handle.sql_ddl_recovery_scans);
    }
    // A transient namespace-open failure must not poison a hosted handle's
    // discovery cache. Hosted handles have no background worker to retry it.
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const root = guard.handle;
        const child = &root.embedded_tables.get("child").?.db;
        var view = child.core.acquireSchemaView().?;
        defer view.release();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .handle = root, .db = child, .table_name = "child" };
        // Preserve diagnostic capacity so only the parent's open fails.
        try root.embedded_table_errors.ensureTotalCapacity(root.alloc, 2);
        const original_alloc = root.alloc;
        {
            var failing = std.testing.FailingAllocator.init(original_alloc, .{ .fail_index = 0 });
            root.alloc = failing.allocator();
            defer root.alloc = original_alloc;
            try std.testing.expectError(error.IntegrityCatalogUnavailable, adapter.localCatalog(arena.allocator(), view.version()));
            try std.testing.expectEqual(@as(usize, 1), root.embedded_table_errors.count());
        }
        const recovered = try adapter.localCatalog(arena.allocator(), view.version());
        try std.testing.expectEqual(@as(usize, 2), recovered.table.len);
        try std.testing.expectEqual(@as(usize, 0), root.embedded_table_errors.count());
        try std.testing.expect(root.embedded_tables_discovered);
        try std.testing.expectEqual(@as(u64, 0), root.sql_ddl_recovery_scans);
    }
    try sqlRefreshCleanGeneration(handle);
    // Inbound FK owners must be discovered even before the child is referenced
    // in this generation. The cascade must be staged, then committed.
    try sqlSessionStatement(handle, session, "BEGIN ISOLATION LEVEL READ COMMITTED");
    try sqlSessionStatement(handle, session, "DELETE FROM parent WHERE id='p'");
    try sqlSessionStatement(handle, session, "COMMIT");
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const child = &guard.handle.embedded_tables.get("child").?.db;
        var result = try child.scan(a, "", "", .{ .limit = 10, .include_documents = true });
        defer result.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), result.documents.len);
    }
    try sqlFkRecoveryStatement(handle, "CREATE INDEX child_parent_idx ON child(parent_id)");
    try sqlRefreshCleanGeneration(handle);
    try sqlFkRecoveryStatement(handle, "DROP INDEX child_parent_idx");
    try sqlRefreshCleanGeneration(handle);
    const ReadApi = struct {
        extern fn antfly_db_sql_describe_json(?*anyopaque, capi.Slice, *capi.Buffer) capi.ErrorCode;
        extern fn antfly_db_sql_open_cursor_json(?*anyopaque, capi.Slice, *u64, *capi.Buffer) capi.ErrorCode;
        extern fn antfly_db_sql_close_cursor(?*anyopaque, u64) capi.ErrorCode;
    };
    const read_request = capi.Slice.fromSlice("{\"statement\":\"SELECT id FROM parent\"}");
    var description: capi.Buffer = .{};
    defer freeRawBuffer(description.ptr, description.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, ReadApi.antfly_db_sql_describe_json(handle, read_request, &description));
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try std.testing.expectEqual(@as(usize, 1), guard.handle.embedded_tables.count());
    }
    var cursor: u64 = 0;
    var response: capi.Buffer = .{};
    defer freeRawBuffer(response.ptr, response.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, ReadApi.antfly_db_sql_open_cursor_json(handle, read_request, &cursor, &response));
    defer _ = ReadApi.antfly_db_sql_close_cursor(handle, cursor);
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try std.testing.expectEqual(@as(usize, 1), guard.handle.embedded_tables.count());
    }
}

test "capi SQL recovery stays clean across row writes reads and cursors" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-recovery-cache");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    for (0..8) |i| {
        const statement = try std.fmt.allocPrint(alloc, "CREATE TABLE t{d}(n BIGINT PRIMARY KEY)", .{i});
        defer alloc.free(statement);
        try sqlFkRecoveryStatement(handle, statement);
    }
    const before = try sqlRecoveryScans(handle);
    for (0..8) |i| {
        const statement = try std.fmt.allocPrint(alloc, "INSERT INTO t0(_id,n) VALUES ('k{d}',{d})", .{ i, i });
        defer alloc.free(statement);
        try sqlFkRecoveryStatement(handle, statement);
        try sqlFkRecoveryStatement(handle, "SELECT n FROM t0");
    }
    const CursorApi = struct {
        extern fn antfly_db_sql_open_cursor_json(?*anyopaque, capi.Slice, *u64, *capi.Buffer) capi.ErrorCode;
        extern fn antfly_db_sql_close_cursor(?*anyopaque, u64) capi.ErrorCode;
    };
    var cursor: u64 = 0;
    var response: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, CursorApi.antfly_db_sql_open_cursor_json(handle, .fromSlice("{\"statement\":\"SELECT n FROM t0\"}"), &cursor, &response));
    defer freeRawBuffer(response.ptr, response.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, CursorApi.antfly_db_sql_close_cursor(handle, cursor));
    const after = try sqlRecoveryScans(handle);
    try std.testing.expectEqual(before.ddl, after.ddl);
    // One post-decision resolution per INSERT, no pre-statement record probes.
    try std.testing.expectEqual(before.commit + 8, after.commit);
    try sqlFkRecoveryStatement(handle, "ALTER TABLE t0 ADD CONSTRAINT positive CHECK(n>=0)");
    const changed = try sqlRecoveryScans(handle);
    try std.testing.expectEqual(after.ddl + 1, changed.ddl);

    var other: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &options, &other));
    defer antfly_db_close(other);
    try sqlFkRecoveryStatement(other, "CREATE TABLE external_table(n BIGINT PRIMARY KEY)");
    try sqlFkRecoveryStatement(handle, "INSERT INTO external_table(_id,n) VALUES ('external',1)");
    const refreshed = try sqlRecoveryScans(handle);
    // Compatible connections share an owner; their DDL dirties the same state.
    try std.testing.expect(refreshed.ddl > changed.ddl);
}

test "capi SQL reclamation assessment and copy release the foreground gate" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const Store = @import("antfly_local_sources").storage_lite_docstore.Store;
    for ([_]bool{ false, true }) |independent| {
        for ([_]Store.MaintenancePhase{ .assessment, .copy }) |phase| {
            const Gate = struct {
                phase: Store.MaintenancePhase,
                started: std.Io.Event = .unset,
                allowed: std.Io.Event = .unset,
                fn notify(ptr: *anyopaque, current: Store.MaintenancePhase) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    if (current != self.phase) return;
                    self.started.set(shared.handleLockIo());
                    try self.allowed.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } });
                }
            };
            var gate: Gate = .{ .phase = phase };
            defer gate.allowed.set(shared.handleLockIo());
            var directory = try TestDirectory.init("capi-sql-detached-reclamation");
            defer directory.cleanup();
            const path = try tempTestAflitePath(alloc, directory.path(), "database");
            defer alloc.free(path);
            defer cleanupTestFile(path);
            var options: capi.OpenOptions = .{};
            try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
            options.storage_kind = capi.storage_kind_lite;
            options.profile = if (independent) capi.profile_native else capi.profile_hosted;
            var handle: ?*anyopaque = null;
            try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
            defer antfly_db_close(handle);
            if (independent) {
                const connection = blk: {
                    const guard = public.enterHandle(handle, .exclusive).?;
                    defer guard.leave();
                    break :blk guard.connection.?.owner;
                };
                connection.stopping.store(true, .release);
                connection.wake.set(shared.handleLockIo());
                connection.worker.?.join();
                connection.worker = null;
                connection.stopping.store(false, .release);
            }
            const connection = blk: {
                const guard = public.enterHandle(handle, .exclusive).?;
                defer guard.leave();
                const store = guard.handle.owned_lite_backend.?.native_docstore.?;
                store.maintenance_policy.options.enabled = false;
                const value = try alloc.alloc(u8, 512 * 1024);
                defer alloc.free(value);
                @memset(value, 'x');
                var write = try store.beginWrite();
                try write.put("large", value);
                try write.commit();
                write = try store.beginWrite();
                try write.delete("large");
                try write.commit();
                store.maintenance_policy.options.enabled = true;
                store.maintenance_policy.options.assessment_bytes = 4096;
                store.maintenance_policy.options.minimum_reclaim_bytes = 4096;
                store.maintenance_policy.next_assessment_size = 0;
                store.test_maintenance_hook = .{ .ptr = &gate, .notify = Gate.notify };
                break :blk guard.connection.?.owner;
            };
            const Background = struct {
                fn run(c: @TypeOf(connection)) void {
                    _ = c.maintenanceTurnForTest() catch {};
                }
            };
            const before_capture = connection.current.?;
            const background = try std.Thread.spawn(.{}, Background.run, .{connection});
            var joined = false;
            defer if (!joined) {
                gate.allowed.set(shared.handleLockIo());
                background.join();
            };
            try gate.started.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
            // Capture after any short orphan/retirement publication preceding
            // the parked scan. Computation itself must not retire this runtime.
            const foreground_root = if (independent) blk: {
                const guard = public.enterHandle(handle, .exclusive).?;
                defer guard.leave();
                if (guard.handle != before_capture) {
                    try std.testing.expectEqual(@as(u64, 0), guard.handle.sql_commit_recovery_scans);
                    try std.testing.expectEqual(@as(u64, 0), guard.handle.sql_ddl_recovery_scans);
                }
                break :blk guard.handle;
            } else connection.current.?;
            const Foreground = struct {
                handle: ?*anyopaque,
                done: std.Io.Event = .unset,
                status: capi.ErrorCode = .internal,
                fn run(self: *@This()) void {
                    var response: capi.Buffer = .{};
                    self.status = antfly_db_batch_json(self.handle, .fromSlice("{\"inserts\":{\"new\":{\"body\":\"foreground\"}},\"sync_level\":\"write\"}"), &response);
                    freeRawBuffer(response.ptr, response.len);
                    self.done.set(shared.handleLockIo());
                }
            };
            var foreground: Foreground = .{ .handle = handle };
            const writer = try std.Thread.spawn(.{}, Foreground.run, .{&foreground});
            const completed = if (foreground.done.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } })) |_| true else |_| false;
            if (independent and completed) try std.testing.expectEqual(foreground_root, connection.current.?);
            gate.allowed.set(shared.handleLockIo());
            writer.join();
            background.join();
            joined = true;
            try std.testing.expect(completed);
            try std.testing.expectEqual(capi.ErrorCode.ok, foreground.status);
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            const result = (try guard.handle.db.lookup(alloc, "new", .{})).?;
            defer alloc.free(result.json);
            try std.testing.expect(std.mem.indexOf(u8, result.json, "foreground") != null);
            try std.testing.expectEqual(@as(usize, 0), connection.retired.items.len);
        }
    }
}

test "capi SQL transaction staging retains owned rows within its memory budget" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-staging-memory");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    try sqlFkRecoveryStatement(handle, "CREATE TABLE staged (k TEXT PRIMARY KEY, body TEXT)");
    var session_id: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, struct {
        extern "c" fn antfly_db_sql_session_open(?*anyopaque, *u64) capi.ErrorCode;
    }.antfly_db_sql_session_open(handle, &session_id));
    const begin = try std.fmt.allocPrint(alloc, "{{\"statement\":\"BEGIN\",\"session_id\":{d}}}", .{session_id});
    defer alloc.free(begin);
    var output: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(begin), &output));
    freeRawBuffer(output.ptr, output.len);
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        guard.handle.sql_sessions.get(session_id).?.budget.limit = 5 * 1024 * 1024;
    }
    const body = try alloc.alloc(u8, 32 * 1024);
    defer alloc.free(body);
    @memset(body, '"');
    const encoded = try std.json.Stringify.valueAlloc(alloc, body, .{});
    defer alloc.free(encoded);
    for (0..64) |i| {
        const request = try std.fmt.allocPrint(alloc, "{{\"statement\":\"INSERT INTO staged (k,body) VALUES ($1,$2)\",\"session_id\":{d},\"parameters\":[\"{d}\",{s}]}}", .{ session_id, i, encoded });
        defer alloc.free(request);
        output = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(request), &output));
        freeRawBuffer(output.ptr, output.len);
    }
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const session = guard.handle.sql_sessions.get(session_id).?;
        try std.testing.expect(session.budget.peak <= session.budget.limit);
        try std.testing.expectEqual(@as(usize, 64), session.entries.items.len);
        const image = try std.json.parseFromSlice(std.json.Value, alloc, session.entries.items[0].encoded_row.?, .{});
        defer image.deinit();
        try std.testing.expectEqualStrings("0", image.value.object.get("k").?.string);
        try std.testing.expectEqualStrings(body, image.value.object.get("body").?.string);
    }
    const commit = try std.fmt.allocPrint(alloc, "{{\"statement\":\"COMMIT\",\"session_id\":{d}}}", .{session_id});
    defer alloc.free(commit);
    output = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(commit), &output));
    freeRawBuffer(output.ptr, output.len);
}

test "capi SQL batched conflict validation reclaims prior row scratch" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-conflict-scratch");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    try sqlFkRecoveryStatement(handle, "CREATE TABLE batched (a TEXT, b TEXT, c TEXT, d BIGINT, e BIGINT, f TEXT, payload JSON, g BIGINT, h BIGINT, i BIGINT, PRIMARY KEY(a,b))");
    var session_id: u64 = 0;
    try std.testing.expectEqual(capi.ErrorCode.ok, struct {
        extern "c" fn antfly_db_sql_session_open(?*anyopaque, *u64) capi.ErrorCode;
    }.antfly_db_sql_session_open(handle, &session_id));
    const payload = try alloc.alloc(u8, 4096);
    defer alloc.free(payload);
    @memset(payload, 'x');
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const begin = try std.fmt.allocPrint(a, "{{\"statement\":\"BEGIN\",\"session_id\":{d}}}", .{session_id});
    var output: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(begin), &output));
    freeRawBuffer(output.ptr, output.len);
    for (0..5) |batch| {
        var statement: std.ArrayList(u8) = .empty;
        try statement.appendSlice(a, "INSERT INTO batched (a,b,c,d,e,f,payload,g,h,i) VALUES ");
        var parameters: std.ArrayList(std.json.Value) = .empty;
        for (0..64) |row| {
            if (row != 0) try statement.appendSlice(a, ",");
            try statement.appendSlice(a, "(");
            for (0..10) |column| {
                if (column != 0) try statement.appendSlice(a, ",");
                try statement.appendSlice(a, try std.fmt.allocPrint(a, "${d}", .{row * 10 + column + 1}));
            }
            try statement.appendSlice(a, ")");
            var json: std.json.ObjectMap = .empty;
            try json.put(a, "body", .{ .string = payload });
            try json.put(a, "kind", .{ .string = "tool_output" });
            try parameters.appendSlice(a, &.{ .{ .string = "thread" }, .{ .string = try std.fmt.allocPrint(a, "{d}", .{batch * 64 + row}) }, .null, .{ .integer = @intCast(row) }, .{ .integer = @intCast(row) }, .{ .string = "message" }, .{ .object = json }, .null, .null, .null });
        }
        try statement.appendSlice(a, " ON CONFLICT(a,b) DO UPDATE SET payload=excluded.payload");
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = statement.items, .parameters = parameters.items, .session_id = session_id }, .{});
        output = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(request), &output));
        freeRawBuffer(output.ptr, output.len);
    }
    // Repeated writes preserve the first native fence and latest postimage;
    // rolling back a deletion must restore that image for reads and commit.
    for ([_][]const u8{
        "UPDATE batched SET c='first' WHERE a='thread' AND b='0'",
        "UPDATE batched SET c='second' WHERE a='thread' AND b='0'",
        "SAVEPOINT before_delete",
        "DELETE FROM batched WHERE a='thread' AND b='0'",
        "ROLLBACK TO SAVEPOINT before_delete",
        "RELEASE SAVEPOINT before_delete",
    }) |statement| {
        const request = try std.json.Stringify.valueAlloc(a, .{ .statement = statement, .session_id = session_id }, .{});
        output = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(request), &output));
        freeRawBuffer(output.ptr, output.len);
    }
    const select = try std.json.Stringify.valueAlloc(a, .{ .statement = "SELECT c FROM batched WHERE a='thread' AND b='0'", .session_id = session_id }, .{});
    output = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(select), &output));
    {
        defer freeRawBuffer(output.ptr, output.len);
        const result = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
        defer result.deinit();
        try std.testing.expectEqualStrings("second", result.value.object.get("rows").?.array.items[0].array.items[0].string);
    }
    const commit = try std.fmt.allocPrint(a, "{{\"statement\":\"COMMIT\",\"session_id\":{d}}}", .{session_id});
    output = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice(commit), &output));
    freeRawBuffer(output.ptr, output.len);
    output = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_sql_json(handle, .fromSlice("{\"statement\":\"SELECT count(*) FROM batched\"}"), &output));
    defer freeRawBuffer(output.ptr, output.len);
    const result = try std.json.parseFromSlice(std.json.Value, alloc, output.ptr.?[0..output.len], .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("320", result.value.object.get("rows").?.array.items[0].array.items[0].string);
}

test "capi SQL index publication accepts unrelated primary commits and fences changed inputs" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    for ([_]enum { bookkeeping, append, conflicting_index }{ .bookkeeping, .append, .conflicting_index }) |mode| {
        const index_changed = mode == .conflicting_index;
        const Gate = struct {
            entered: std.atomic.Value(bool) = .init(false),
            started: std.Io.Event = .unset,
            allowed: std.Io.Event = .unset,
            publication_error: ?anyerror = null,
            fn notify(ptr: *anyopaque, root: *shared.Handle) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.entered.swap(true, .acq_rel)) return;
                _ = root.db.core.nextDerivedAppendSequence(); // Prime the detached allocator.
                self.started.set(shared.handleLockIo());
                self.allowed.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } }) catch |err| {
                    self.publication_error = err;
                    return;
                };
                // Lease/progress transactions must also survive a moved primary
                // checkpoint before the first immutable segment is published.
                var txn = root.db.core.store.beginWriteTxn() catch |err| {
                    self.publication_error = err;
                    return;
                };
                var committed = false;
                defer if (!committed) txn.abort();
                txn.put("__antfly_test_detached_progress", "progress") catch |err| {
                    self.publication_error = err;
                    return;
                };
                txn.commit() catch |err| {
                    self.publication_error = err;
                    return;
                };
                committed = true;
                // Deliver already prepared immutable bytes through the same
                // storage port used by full-text segments, after foreground
                // commits have moved the primary checkpoint.
                root.db.core.index_manager.text_lsm_storage.?.writeFileAbsolute("__antfly_lite/indexes/body_text/prepared.bin", "prepared index bytes") catch |err| {
                    std.debug.print("detached index bytes failed: {s}\n", .{@errorName(err)});
                    self.publication_error = err;
                    return;
                };
                // Reserving on the detached cache used to fail after the
                // foreground commits. Both generic and resolver producers must
                // allocate through the current coordinator instead.
                const sequence = root.db.derivedAsyncAppendDerivedBatchRecord(.{}) catch |err| {
                    std.debug.print("detached generic append failed: {s}\n", .{@errorName(err)});
                    self.publication_error = err;
                    return;
                };
                const runtime = root.db.resolution_runtime.?;
                const resolved = runtime.write_fn(runtime.write_ctx, .{ .inputs = .{}, .batch = .{}, .artifact_writes = &.{.{ .key = "__antfly_test_resolver_artifact", .value = "resolver" }}, .target_hints = &.{} }) catch |err| {
                    std.debug.print("detached resolver append failed: {s}\n", .{@errorName(err)});
                    self.publication_error = err;
                    return;
                };
                if (resolved <= sequence or root.db.core.nextDerivedSequence() < resolved) {
                    self.publication_error = error.TestExpectedIncreasingSequence;
                    return;
                }
            }
        };
        var gate: Gate = .{};
        defer gate.allowed.set(shared.handleLockIo());
        var directory = try TestDirectory.init("capi-sql-text-publication-progress");
        defer directory.cleanup();
        const path = try tempTestAflitePath(alloc, directory.path(), "database");
        defer alloc.free(path);
        defer cleanupTestFile(path);
        var options: capi.OpenOptions = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
        options.storage_kind = capi.storage_kind_lite;
        options.profile = capi.profile_native;
        var handle: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
        defer antfly_db_close(handle);
        // Exercise production's independent capture runtime. Stop its automatic
        // worker so the test controls the exact capture/publication interleaving.
        const connection = blk: {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            break :blk guard.connection.?.owner;
        };
        connection.stopping.store(true, .release);
        connection.wake.set(shared.handleLockIo());
        connection.worker.?.join();
        connection.worker = null;
        connection.stopping.store(false, .release);
        const owner = blk: {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            try guard.handle.db.setSchemaJson(alloc,
                \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"}},"additionalProperties":true}}}}
            );
            try guard.handle.db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
            try guard.handle.db.batch(.{ .writes = &.{.{ .key = "original", .value = "{\"body\":\"original needle\"}" }}, .sync_level = .write });
            const maintenance_owner = guard.connection.?.owner;
            maintenance_owner.maintenance_pending = true;
            maintenance_owner.test_after_capture = .{ .ptr = &gate, .notify = Gate.notify };
            break :blk maintenance_owner;
        };
        const Background = struct {
            fn run(o: @TypeOf(owner)) void {
                _ = o.maintenanceTurnForTest() catch {};
            }
        };
        const background = try std.Thread.spawn(.{}, Background.run, .{owner});
        var joined = false;
        defer if (!joined) {
            gate.allowed.set(shared.handleLockIo());
            background.join();
        };
        try gate.started.waitTimeout(shared.handleLockIo(), .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        for (0..8) |i| {
            if (mode != .bookkeeping) {
                const statement = try std.fmt.allocPrint(alloc, "INSERT INTO \"default\" (_id,body) VALUES ('other{d}','unrelated')", .{i});
                defer alloc.free(statement);
                try sqlFkRecoveryStatement(handle, statement);
            } else {
                // A primary-only bookkeeping commit moves the header and allocator,
                // while leaving every physical full-text input unchanged.
                const guard = public.enterHandle(handle, .exclusive).?;
                defer guard.leave();
                var txn = try guard.handle.db.core.store.beginWriteTxn();
                errdefer txn.abort();
                var bytes: [32]u8 = undefined;
                try txn.put("__antfly_test_bookkeeping", try std.fmt.bufPrint(&bytes, "{d}", .{i}));
                try txn.commit();
            }
        }
        if (index_changed) {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            // Native SQL may queue its index work. Publish an actual conflicting
            // target here so rejection proves the old bytes cannot overwrite a
            // newer owner's immutable artifact.
            try guard.handle.db.core.index_manager.text_lsm_storage.?.writeFileAbsolute("__antfly_lite/indexes/body_text/prepared.bin", "foreground index bytes");
        }
        gate.allowed.set(shared.handleLockIo());
        background.join();
        joined = true;
        owner.test_after_capture = null;
        if (index_changed) {
            try std.testing.expectEqual(error.EnrichmentWaitCanceled, gate.publication_error orelse return error.TestExpectedCancellation);
        } else if (gate.publication_error) |err| return err;

        if (index_changed) {
            // The target now belongs to a newer owner. The captured publication
            // must yield and retry, preserving the foreground artifact.
            if (!owner.maintenance_pending) return error.TestIndexChangeNotPending;
        }
        if (!index_changed) {
            // Prove progress in the captured turn, before any quiet-time drain.
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            const progress = (try guard.handle.db.core.getStoreValue(alloc, "__antfly_test_detached_progress")) orelse return error.TestExpectedProgress;
            defer alloc.free(progress);
            try std.testing.expectEqualStrings("progress", progress);
            try std.testing.expect(owner.publication_stats.rebased > 0);
            const resolved = (try guard.handle.db.core.getStoreValue(alloc, "__antfly_test_resolver_artifact")) orelse return error.TestExpectedResolverArtifact;
            defer alloc.free(resolved);
            try std.testing.expectEqualStrings("resolver", resolved);
            try std.testing.expect(owner.publication_stats.replay_attempts >= 2);
            try std.testing.expectEqual(@as(u64, 0), owner.publication_stats.replay_refused);
        }
        for (0..64) |_| {
            if (!try owner.maintenanceTurnForTest()) break;
        }
        // Unrelated primary commits permit publication in the captured turn. After
        // index conflicts, a fresh turn must index the original without losing the
        // foreground rows or their searchable terms.
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const storage = guard.handle.db.core.index_manager.text_lsm_storage.?;
        const published = try storage.readFileAlloc(alloc, "__antfly_lite/indexes/body_text/prepared.bin", 1024);
        defer alloc.free(published);
        try std.testing.expectEqualStrings(if (index_changed) "foreground index bytes" else "prepared index bytes", published);
        var result = try guard.handle.db.search(alloc, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "original needle" } }, .limit = 10 });
        defer result.deinit();
        try std.testing.expect(result.hits.len != 0);
        try std.testing.expectEqualStrings("original", result.hits[0].id);
        if (mode != .bookkeeping) {
            var unrelated = try guard.handle.db.search(alloc, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "unrelated" } }, .limit = 10 });
            defer unrelated.deinit();
            try std.testing.expectEqual(@as(usize, 8), unrelated.hits.len);
        }
    }
}

test "capi SQL resolver input read sets fence negative reads and range phantoms" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-resolver-read-set");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    try guard.handle.db.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
    const runtime = guard.handle.db.resolution_runtime.?;
    try @import("antfly_local_sources").storage_db_resolution_runtime.testReplayReadTracking(runtime.store_handle.store);
}

test "capi SQL resolver publication fences captured inputs and resolver generations" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const resolution = @import("antfly_local_sources").storage_db_resolution_runtime;
    const k = @import("antfly_local_sources").storage_internal_keys;
    const Mode = enum { unchanged, unrelated, source, sibling, override, candidate, catalog, mixed, mixed_conflict, repeated };
    for ([_]Mode{ .unchanged, .unrelated, .source, .sibling, .override, .candidate, .catalog, .mixed, .mixed_conflict, .repeated }) |mode| {
        errdefer std.debug.print("resolver publication fence mode={s}\n", .{@tagName(mode)});
        var directory = try TestDirectory.init("capi-sql-resolver-input-fence");
        defer directory.cleanup();
        const path = try tempTestAflitePath(alloc, directory.path(), "database");
        defer alloc.free(path);
        defer cleanupTestFile(path);
        var options: capi.OpenOptions = .{};
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
        options.storage_kind = capi.storage_kind_lite;
        options.profile = capi.profile_native;
        var handle: ?*anyopaque = null;
        try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
        defer antfly_db_close(handle);
        const owner = blk: {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            break :blk guard.connection.?.owner;
        };
        owner.stopping.store(true, .release);
        owner.wake.set(shared.handleLockIo());
        owner.worker.?.join();
        owner.worker = null;
        owner.stopping.store(false, .release);
        const source = try k.artifactNamedPrefixAlloc(alloc, "doc", "asset", "mentions");
        defer alloc.free(source);
        const out = try k.resolutionArtifactKeyAlloc(alloc, "doc", "resolution");
        defer alloc.free(out);
        {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            _ = try guard.handle.db.core.upsertResolver(.{ .name = "review", .table = "default", .source_artifact = "mentions", .resolution_artifact = "resolution", .key_template = "{{ slug _entity.text }}", .config_generation = 1, .promote = false, .labels = &.{"Person"} });
            _ = try guard.handle.db.core.upsertResolver(.{ .name = "sibling", .table = "default", .source_artifact = "mentions", .resolution_artifact = "sibling", .key_template = "{{ slug _entity.text }}", .config_generation = 1, .promote = false, .labels = &.{"Org"} });
            try guard.handle.db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"unchanged\"}" }}, .sync_level = .write });
            var txn = try guard.handle.db.core.store.beginWriteTxn();
            errdefer txn.abort();
            try txn.put(source, "{\"entities\":[{\"id\":\"e0\",\"label\":\"Person\",\"text\":\"Old Name\"}]}");
            try txn.commit();
            if (mode == .mixed or mode == .mixed_conflict) {
                // A second source is already resolved, making the captured record
                // contain both changed and unchanged decisions.
                try guard.handle.db.batch(.{ .writes = &.{.{ .key = "doc2", .value = "{}" }}, .sync_level = .write });
                const second = try k.artifactNamedPrefixAlloc(alloc, "doc2", "asset", "mentions");
                defer alloc.free(second);
                var second_txn = try guard.handle.db.core.store.beginWriteTxn();
                errdefer second_txn.abort();
                try second_txn.put(second, "{\"entities\":[{\"id\":\"e0\",\"label\":\"Person\",\"text\":\"Old Name\"}]}");
                try second_txn.commit();
                const runtime = guard.handle.db.resolution_runtime.?;
                var borrowed = runtime.store_handle.store;
                var adapter: resolution.DbArtifactStore(@TypeOf(borrowed)) = .{ .store = &borrowed, .index_manager = guard.handle.db.core.index_manager };
                const configs = try guard.handle.db.core.listResolvers(alloc);
                defer {
                    for (configs) |*cfg| cfg.deinit(alloc);
                    alloc.free(configs);
                }
                try resolution.processRecordKeys(alloc, configs, adapter.artifactStore(), null, &.{second}, runtime.write_ctx, runtime.write_fn, null, null);
            }
        }
        const Probe = struct {
            handle: ?*anyopaque,
            mode: Mode,
            source: []const u8,
            out: []const u8,
            rejection: ?anyerror = null,
            failure: ?anyerror = null,
            fn notify(ptr: *anyopaque, root: *shared.Handle) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.run(root) catch |err| {
                    self.failure = err;
                };
            }
            fn run(self: *@This(), root: *shared.Handle) !void {
                {
                    const guard = public.enterHandle(self.handle, .exclusive).?;
                    defer guard.leave();
                    if (self.mode == .catalog) {
                        _ = try guard.handle.db.core.upsertResolver(.{ .name = "review", .table = "default", .source_artifact = "mentions", .resolution_artifact = "resolution", .key_template = "changed/{{ slug _entity.text }}", .config_generation = 2, .promote = false, .labels = &.{"Person"} });
                    } else if (self.mode == .candidate) {
                        try guard.handle.db.batch(.{ .writes = &.{.{ .key = "old_name", .value = "{\"name\":\"Canonical\"}" }}, .sync_level = .write });
                    } else if (self.mode != .unchanged and self.mode != .mixed and self.mode != .repeated) {
                        const a = std.heap.c_allocator;
                        const key = switch (self.mode) {
                            .source => try a.dupe(u8, self.source),
                            .mixed_conflict => try k.artifactNamedPrefixAlloc(a, "doc2", "asset", "mentions"),
                            .sibling => try k.resolutionArtifactKeyAlloc(a, "doc", "sibling"),
                            .override => try resolution.reviewOverrideArtifactKeyAlloc(a, "doc", "mentions", "resolution"),
                            else => try a.dupe(u8, "unrelated"),
                        };
                        defer a.free(key);
                        var txn = try guard.handle.db.core.store.beginWriteTxn();
                        errdefer txn.abort();
                        try txn.put(key, "{\"entities\":[]}");
                        try txn.commit();
                    }
                }
                const runtime = root.db.resolution_runtime.?;
                var borrowed = runtime.store_handle.store;
                var adapter: resolution.DbArtifactStore(@TypeOf(borrowed)) = .{ .store = &borrowed, .index_manager = root.db.core.index_manager };
                const a = std.heap.c_allocator;
                const resolvers = try root.db.core.listResolvers(a);
                defer {
                    for (resolvers) |*cfg| cfg.deinit(a);
                    a.free(resolvers);
                }
                const second = try k.artifactNamedPrefixAlloc(a, "doc2", "asset", "mentions");
                defer a.free(second);
                const changed = if (self.mode == .mixed or self.mode == .mixed_conflict) &[_][]const u8{ self.source, second } else &[_][]const u8{self.source};
                const replay_before = root.db.core.nextDerivedSequence();
                resolution.processRecordKeys(a, resolvers, adapter.artifactStore(), null, changed, runtime.write_ctx, runtime.write_fn, null, null) catch |err| {
                    if (err != error.EnrichmentWaitCanceled and err != error.EnrichmentSourceChanged) return err;
                    self.rejection = err;
                };
                if (self.mode == .repeated) {
                    // Refreshing the detached checkpoint after the first
                    // publication makes its output the second record's input.
                    // Both records must finish in this same captured turn.
                    try resolution.processRecordKeys(a, resolvers, adapter.artifactStore(), null, changed, runtime.write_ctx, runtime.write_fn, null, null);
                }
                // Inspect before the ordinary maintenance pass can retry from
                // a new snapshot and publish a fresh, valid decision.
                const guard = public.enterHandle(self.handle, .read).?;
                defer guard.leave();
                if (self.mode == .mixed) {
                    try std.testing.expectEqual(replay_before + 2, root.db.core.nextDerivedSequence());
                    try std.testing.expectEqual(@as(u64, 1), guard.connection.?.owner.publication_stats.replay_attempts);
                    try std.testing.expectEqual(@as(u64, 0), guard.connection.?.owner.publication_stats.replay_refused);
                }
                const value = try guard.handle.db.core.getStoreValue(a, self.out);
                defer if (value) |bytes| a.free(bytes);
                if (self.rejection != null) try std.testing.expect(value == null) else try std.testing.expect(value != null);
            }
        };
        var probe: Probe = .{ .handle = handle, .mode = mode, .source = source, .out = out };
        owner.test_after_capture = .{ .ptr = &probe, .notify = Probe.notify };
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurnForTest();
        owner.test_after_capture = null;
        if (probe.failure) |err| return err;
        try std.testing.expectEqual(mode != .unchanged and mode != .unrelated and mode != .mixed and mode != .repeated, probe.rejection != null);
    }
}

test "capi SQL maintenance advances every captured full text prefix during steady appends" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-steady-append-progress");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owner = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    owner.stopping.store(true, .release);
    owner.wake.set(shared.handleLockIo());
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try guard.handle.db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
        try guard.handle.db.batch(.{ .writes = &.{.{ .key = "original", .value = "{\"body\":\"needle\"}" }}, .sync_level = .write });
    }
    const Append = struct {
        handle: ?*anyopaque,
        count: usize = 0,
        target: u64 = 0,
        failure: ?anyerror = null,
        fn notify(ptr: *anyopaque, root: *shared.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.target = root.db.core.nextDerivedSequence();
            self.append() catch |err| {
                self.failure = err;
            };
        }
        fn append(self: *@This()) !void {
            const guard = public.enterHandle(self.handle, .exclusive).?;
            defer guard.leave();
            var key: [64]u8 = undefined;
            const name = try std.fmt.bufPrint(&key, "append{d}", .{self.count});
            try guard.handle.db.batch(.{ .writes = &.{.{ .key = name, .value = "{\"body\":\"continuous\"}" }}, .sync_level = .write });
            self.count += 1;
        }
    };
    var append: Append = .{ .handle = handle };
    owner.test_after_capture = .{ .ptr = &append, .notify = Append.notify };
    defer owner.test_after_capture = null;
    for (0..4) |_| {
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurnForTest();
        if (append.failure) |err| return err;
        // A foreground append moves the tip in every turn. Inspect durable
        // coverage before another maintenance turn or any search/idle drain.
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        const debt = try guard.handle.db.listDerivedReplayDebt(alloc);
        defer {
            for (debt) |*item| item.deinit(alloc);
            alloc.free(debt);
        }
        var found = false;
        for (debt) |item| if (std.mem.eql(u8, item.index_name, "body_text")) {
            found = true;
            try std.testing.expect(item.applied_sequence >= append.target);
        };
        try std.testing.expect(found);
        const json = try public.pendingWorkStatsJson(guard.handle);
        defer freeRawBuffer(json.ptr, json.len);
        const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json.ptr.?[0..json.len], .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.value.object.get("index_replay_debt").?.array.items.len != 0);
        try std.testing.expect(parsed.value.object.get("background_publications").?.object.get("rebased").?.integer > 0);
    }
    try std.testing.expectEqual(@as(usize, 4), append.count);
}

test "capi SQL missing namespace forces coordinated catalog validation" {
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-missing-namespace-validation");
    defer directory.cleanup();
    const path = try tempTestAflitePath(alloc, directory.path(), "database");
    defer alloc.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .handle = guard.handle, .db = &guard.handle.db, .table_name = "default" };
    try std.testing.expect(!try adapter.localRequiresCoordination());
    try guard.handle.embedded_table_errors.put(guard.handle.alloc, "broken", "InvalidSchema");
    try std.testing.expect(try adapter.localRequiresCoordination());
    try std.testing.expectError(error.IntegrityCatalogUnavailable, adapter.localCatalog(alloc, 1));
}

test "capi SQL maintenance capture queues after bounded foreground contention" {
    try public.testMaintenanceCaptureAdmission();
}

test "capi SQL detached renewal releases the mutex and rejects changed tenure" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Runtime = std.meta.Child(std.meta.Child(@FieldType(db_mod.DB, "enrichment_runtime")));
    try Runtime.testDetachedLeaseRenewal();
}

test "capi SQL runtime refresh fences serving inputs while reusing lease and shadow bookkeeping" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = shared.handleLockIo();
    var directory = try TestDirectory.init("capi-runtime-reuse");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owner = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    owner.stopping.store(true, .release);
    owner.wake.set(io);
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    owner.gate.mutex.lockUncancelable(io);
    defer owner.gate.mutex.unlock(io);
    var path_lease = try lite_backend.native.lockWriterPathWithIo(a, io, path);
    defer path_lease.close();
    for ([_]enum { lease_add, lease_delete, shadow, serving, metadata, primary, retired_checkpoint }{
        .lease_add, .lease_delete, .shadow, .serving, .metadata, .primary, .retired_checkpoint,
    }) |change| {
        const before = owner.current.?;
        {
            var writer = try lite_backend.native.NativeFile.openWithIo(a, io, path, .{ .externally_locked = true });
            defer writer.close();
            switch (change) {
                .lease_add => try writer.putCatalogRecord(lite_backend.native.NativeFile.artifact_lease_key_prefix ++ "refresh-probe", "lease"),
                .lease_delete => try writer.putCatalogBatch(&.{.{ .key = lite_backend.native.NativeFile.artifact_lease_key_prefix ++ "refresh-probe", .is_delete = true }}),
                .shadow, .serving => {
                    const indexes = try before.db.core.listIndexes(a);
                    defer {
                        for (indexes) |*index| index.deinit(a);
                        a.free(indexes);
                    }
                    const key = try std.fmt.allocPrint(a, "{s}/indexes/{s}/refresh-probe.bin", .{ before.db.core.index_manager.base_path, if (change == .shadow) "unpublished-shadow" else indexes[0].name });
                    defer a.free(key);
                    try writer.putIndexCatalogRecord(key, "physical input");
                },
                .metadata => try writer.putCatalogRecord("runtime-probe", "changed"),
                .primary => try writer.putDocument("runtime-probe", "{}"),
                .retired_checkpoint => {
                    try writer.putCatalogRecord(lite_backend.native.NativeFile.artifact_lease_key_prefix ++ "evict-a", "lease");
                    try writer.putCatalogRecord(lite_backend.native.NativeFile.artifact_lease_key_prefix ++ "evict-b", "lease");
                },
            }
        }
        const after = try owner.refresh();
        if (change == .lease_add or change == .lease_delete or change == .shadow)
            try std.testing.expectEqual(before, after)
        else
            try std.testing.expect(before != after);
    }
}

test "capi native replacement wait observes its background owner without rebuilding" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("native-replacement-observer");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owner = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    owner.stopping.store(true, .release);
    owner.wake.set(shared.handleLockIo());
    owner.worker.?.join();
    // Simulate a paused native builder while preserving its ownership. Only
    // explicit scheduler turns below may mutate its reservation.
    defer owner.worker = null;
    owner.stopping.store(false, .release);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .fromSlice(
        \\{"name":"old","type":"graph","edge_types":[{"name":"R","field":"links"}]}
    )));
    var inserted: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_batch_json(handle, .fromSlice(
        \\{"inserts":{"source":{"links":["target"]}}}
    ), &inserted));
    freeRawBuffer(inserted.ptr, inserted.len);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_run_until_idle(handle));
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .fromSlice(
        \\{"name":"new","type":"graph","replaces":"old","edge_types":[{"name":"R","field":"links"}]}
    )));
    const Reservation = struct {
        fn bytes(allocator: std.mem.Allocator, id: ?*anyopaque) ![]u8 {
            const guard = public.enterHandle(id, .read).?;
            defer guard.leave();
            const jobs = try guard.handle.database().graphReplacementStatusAlloc(allocator);
            defer jobs.deinit();
            try std.testing.expectEqual(@as(usize, 1), jobs.value.len);
            return std.json.Stringify.valueAlloc(allocator, jobs.value, .{});
        }
    };
    const before = try Reservation.bytes(a, handle);
    defer a.free(before);
    try std.testing.expectEqual(capi.ErrorCode.busy, antfly_db_add_index_json(handle, .fromSlice(
        \\{"name":"new","type":"graph","replaces":"old","wait":{"timeout_ms":1000},"edge_types":[{"name":"R","field":"links"}]}
    )));
    const after = try Reservation.bytes(a, handle);
    defer a.free(after);
    try std.testing.expectEqualStrings(before, after);
    for (0..32) |_| if (!try owner.maintenanceTurnForTest()) break;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .fromSlice(
        \\{"name":"new","type":"graph","replaces":"old","wait":{"timeout_ms":0},"edge_types":[{"name":"R","field":"links"}]}
    )));
}

test "capi native resolver maintenance retries transient inference and survives reopen" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const producer = @import("antfly_local_sources").storage_db_enrichment_asset_producer;
    const Fixture = struct {
        failures: usize = 0,
        successful: usize = 0,
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const producer.Request) !@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .data_uri, .fixed_bytes = 16 << 20, .allocator_limit_bytes = 16 << 20, .max_result_bytes_per_item = 1 << 20, .max_result_bytes = 1 << 20 };
        }
        fn produce(ptr: *anyopaque, a: std.mem.Allocator, _: producer.Request) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Admission pressure and an open provider circuit must preserve
            // durable replay debt, not require another foreground write.
            if (self.failures < 2) {
                self.failures += 1;
                return if (self.failures == 1) error.ResourceTemporarilyUnavailable else error.InferenceCircuitOpen;
            }
            self.successful += 1;
            return a.dupe(u8, "{\"entities\":[{\"id\":\"a\",\"text\":\"Antfly\",\"label\":\"technology\"}],\"relations\":[]}");
        }
        fn produceBounded(ptr: *anyopaque, a: std.mem.Allocator, request: producer.Request, context: producer.InvocationContext) ![]u8 {
            try context.check();
            return produce(ptr, a, request);
        }
        fn notify(ptr: *anyopaque, root: *shared.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!root.db_live) return;
            const runtime = root.db.enrichment_runtime orelse return;
            runtime.config.asset_producer = .{ .ptr = self, .vtable = &.{ .produce = produce, .produce_with_context = produceBounded, .foreground_bounded = true, .invocation_memory_for_requests = memory } };
            runtime.config.inline_retry_max_attempts = 1;
        }
        fn pause(handle: ?*anyopaque) !std.meta.Child(@FieldType(shared.Handle, "maintenance_owner")) {
            const owner = blk: {
                const guard = public.enterHandle(handle, .exclusive).?;
                defer guard.leave();
                break :blk guard.connection.?.owner;
            };
            owner.stopping.store(true, .release);
            owner.wake.set(shared.handleLockIo());
            owner.worker.?.join();
            owner.worker = null;
            owner.stopping.store(false, .release);
            return owner;
        }
    };
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("native-resolver-retry");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    options.flags |= capi.open_flag_generated_enrichment_replay;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer if (handle != null) antfly_db_close(handle);
    var owner = try Fixture.pause(handle);
    var fixture: Fixture = .{};
    owner.test_after_capture = .{ .ptr = &fixture, .notify = Fixture.notify };
    try std.testing.expectEqual(capi.ErrorCode.ok, struct {
        extern fn antfly_db_create_table_json(?*anyopaque, capi.Slice, capi.Slice) capi.ErrorCode;
    }.antfly_db_create_table_json(handle, .fromSlice("concepts"), .fromSlice("{}")));
    const config =
        \\{"source":{"artifact":"extraction","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"about"},"artifact":{"name":"extraction","kind":"asset","source":{"type":"field","value":"text"},"content_type":"application/json","producer_json":{"type":"generator","config":{"provider":"mock"}}},"edge_types":[{"name":"about"}],"resolvers":[{"name":"concepts","table":"concepts","source_artifact":"extraction","resolution_artifact":"resolution","key_template":"concept/{{ uslug _entity.text }}"}]}
    ;
    const index = try std.json.Stringify.valueAlloc(a, .{ .name = "kg", .kind = "graph", .config_json = config }, .{});
    defer a.free(index);
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_add_index_json(handle, .fromSlice(index)));
    for (0..30) |i| {
        {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            const key = try std.fmt.allocPrint(a, "d{d}", .{i});
            defer a.free(key);
            try guard.handle.db.batch(.{ .writes = &.{.{ .key = key, .value = "{\"text\":\"Antfly\"}" }}, .sync_level = .write });
            owner.maintenance_pending = true;
        }
        _ = try owner.maintenanceTurnForTest();
    }
    try std.testing.expect(fixture.failures > 0);
    antfly_db_close(handle);
    handle = null;
    // Reopen with unfinished work and no new mutation or foreground drain.
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &options, &handle));
    owner = try Fixture.pause(handle);
    owner.test_after_capture = .{ .ptr = &fixture, .notify = Fixture.notify };
    for (0..160) |_| {
        if (!try owner.maintenanceTurnForTest()) break;
        try shared.handleLockIo().sleep(.fromMilliseconds(25), .awake);
    }
    try std.testing.expectEqual(@as(usize, 2), fixture.failures);
    try std.testing.expect(fixture.successful >= 30);
    const guard = public.enterHandle(handle, .read).?;
    defer guard.leave();
    const stats = guard.handle.db.pendingWorkStats();
    try std.testing.expect(stats.enrichment.applied_sequence >= stats.enrichment.target_sequence);
    try std.testing.expect(!stats.resolution.catch_up_required);
    if (stats.promotion.catch_up_required) std.debug.print("resolver reopen stats={f} publications={f}\n", .{ std.json.fmt(stats, .{}), std.json.fmt(owner.publication_stats, .{}) });
    try std.testing.expect(!stats.promotion.catch_up_required);
    try std.testing.expectEqual(@as(?[]const u8, null), owner.publication_stats.last_maintenance_error);
}

test "capi SQL idle native maintenance avoids runtime construction and resumes work" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-idle-maintenance");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    options.flags |= capi.open_flag_generated_enrichment_replay;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owner = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    owner.stopping.store(true, .release);
    owner.wake.set(shared.handleLockIo());
    owner.worker.?.join();
    // Keep notification behavior enabled while driving this joined worker
    // deterministically. Clear it before close, which must not join twice.
    defer owner.worker = null;
    owner.stopping.store(false, .release);
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try guard.handle.db.addIndex(.{ .name = "idle_graph", .kind = .graph, .config_json =
            \\{"source":{"artifact":"extraction","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"about"},"artifact":{"name":"extraction","kind":"asset","source":{"type":"field","value":"payload"},"content_type":"application/json"},"edge_types":[{"name":"about"}]}
        });
        _ = try guard.handle.db.upsertResolverWithResultOptions(.{
            .name = "concepts",
            .table = "default",
            .source_artifact = "extraction",
            .resolution_artifact = "resolution",
            .key_template = "entity/{{ uslug _entity.text }}",
            .promote = false,
            .inline_managed = true,
        }, .{ .drain_backfill = false });
        const document = try std.json.Stringify.valueAlloc(a, .{ .payload = "{\"entities\":[{\"id\":\"a\",\"text\":\"Antfly\",\"label\":\"technology\",\"confidence\":0.9}],\"relations\":[]}" }, .{});
        defer a.free(document);
        try guard.handle.db.batch(.{ .writes = &.{.{ .key = "seed", .value = document }}, .sync_level = .write });
        owner.maintenance_pending = true;
    }
    // Finish any initial reclamation. Later idle polls must reuse the physical
    // store and must neither open a DB runtime nor refresh the foreground DB.
    for (0..64) |_| if (!try owner.maintenanceTurnForTest()) break;
    owner.wake.reset();
    const opens = owner.test_runtime_snapshot_opens;
    const settled_replay_sequence = owner.current.?.db.core.nextDerivedSequence();
    const settled_sequence = owner.maintenance_snapshot.?.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence;
    for (0..8) |_| {
        const current = owner.current.?;
        const snapshot = owner.maintenance_snapshot.?;
        // Real page retirement may advance the file tip. Reuse is required
        // when the checkpoint is stable, rather than across real GC writes.
        var probe = try lite_backend.native.NativeFile.openWithIo(a, shared.handleLockIo(), path, .{ .read_only = true, .internal_reader = true });
        const same_tip = std.meta.eql(probe.header, current.owned_lite_backend.?.native_docstore.?.file.header) and
            std.meta.eql(probe.header, snapshot.owned_lite_backend.?.native_docstore.?.file.header);
        probe.close();
        _ = try owner.maintenanceTurnForTest();
        if (same_tip) {
            try std.testing.expectEqual(current, owner.current.?);
            try std.testing.expectEqual(snapshot, owner.maintenance_snapshot.?);
        }
        try std.testing.expect(!owner.maintenance_snapshot.?.db_live);
    }
    try std.testing.expect(!owner.wake.isSet());
    try std.testing.expectEqual(settled_replay_sequence, owner.current.?.db.core.nextDerivedSequence());
    try std.testing.expectEqual(opens, owner.test_runtime_snapshot_opens);
    try std.testing.expectEqual(settled_sequence, owner.maintenance_snapshot.?.owned_lite_backend.?.native_docstore.?.file.activeCheckpoint().commit_sequence);
    // Enrichment/index work still upgrades the storage-only snapshot and is
    // replayed through the ordinary independent native maintenance runtime.
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        try guard.handle.db.setSchemaJson(a,
            \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"body":{"type":"string"}},"additionalProperties":true}}}}
        );
        try guard.handle.db.addIndex(.{ .name = "body_text", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
        try guard.handle.db.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"indexed after idle\"}" }}, .sync_level = .write });
        owner.maintenance_pending = true;
    }
    for (0..64) |_| if (!try owner.maintenanceTurnForTest()) break;
    try std.testing.expect(owner.test_runtime_snapshot_opens > opens);
    const after_work = owner.test_runtime_snapshot_opens;
    for (0..8) |_| _ = try owner.maintenanceTurnForTest();
    try std.testing.expectEqual(after_work, owner.test_runtime_snapshot_opens);
    {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        var result = try guard.handle.db.search(a, .{ .index_name = "body_text", .full_text = .{ .match = .{ .field = "body", .text = "indexed after idle" } }, .limit = 10 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.hits.len);
        try std.testing.expectEqualStrings("doc", result.hits[0].id);
    }
    // A different writer's catalog change during an idle turn must reopen the
    // serving runtime. It must never be adopted as our lease/allocator cleanup.
    const External = struct {
        path: []const u8,
        failure: ?anyerror = null,
        fn publish(self: *@This()) !void {
            var lease = try lite_backend.native.lockWriterPathWithIo(a, shared.handleLockIo(), self.path);
            defer lease.close();
            const other = try public.openLiteHandleAllocWithRuntime(a, self.path, .{
                .externally_locked = true,
                .profile = .hosted,
                .open_mode = .writer_no_replay,
            }, false, shared.handleLockIo(), null);
            other.lease_snapshot = true;
            defer shared.closeHandle(other);
            try other.db.core.store.put("\x00antfly/test/external-catalog", "external");
        }
        fn notify(ptr: *anyopaque, _: *shared.Handle) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.publish() catch |err| {
                self.failure = err;
            };
        }
    };
    try std.testing.expect(!owner.maintenance_pending);
    var external = External{ .path = path };
    owner.test_after_capture = .{ .ptr = &external, .notify = External.notify };
    defer owner.test_after_capture = null;
    const before_external = owner.current.?;
    _ = try owner.maintenanceTurnForTest();
    owner.test_after_capture = null;
    if (external.failure) |err| return err;
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    try std.testing.expect(guard.handle != before_external);
    const external_value = try guard.handle.db.core.store.get(a, "\x00antfly/test/external-catalog");
    defer a.free(external_value);
    try std.testing.expectEqualStrings("external", external_value);
}

test "capi lite inference service survives foreground and maintenance snapshots" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    if (!capi_build_options.inference_enabled or !lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-shared-inference");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_native;
    options.flags |= capi.open_flag_local_runtime_configured;
    const models_path = try std.fs.path.join(a, &.{ directory.path(), "models" });
    defer a.free(models_path);
    const caller_models_path = try a.dupe(u8, models_path);
    defer a.free(caller_models_path);
    options.models_dir = capi.Slice.fromSlice(caller_models_path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const owner = blk: {
        const guard = public.enterHandle(handle, .exclusive).?;
        defer guard.leave();
        break :blk guard.connection.?.owner;
    };
    // The public options buffer belongs to the caller and may disappear as
    // soon as open returns. Refresh and compatible opens must use owned bytes.
    @memset(caller_models_path, 0xDD);
    try std.testing.expectEqualStrings(models_path, owner.options.inference.models_dir.?);
    const second_models_path = try a.dupe(u8, models_path);
    defer a.free(second_models_path);
    options.models_dir = capi.Slice.fromSlice(second_models_path);
    var second: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_open_with_options(path, &options, &second));
    {
        defer antfly_db_close(second);
        const guard = public.enterHandle(second, .exclusive).?;
        defer guard.leave();
        try std.testing.expectEqual(owner, guard.connection.?.owner);
    }
    owner.stopping.store(true, .release);
    owner.wake.set(shared.handleLockIo());
    owner.worker.?.join();
    owner.worker = null;
    owner.stopping.store(false, .release);
    try sqlFkRecoveryStatement(handle, "CREATE TABLE docs(id TEXT PRIMARY KEY, body TEXT)");
    const service = owner.current.?.lite_inference.?;
    // Retiring a storage generation must not quiesce another generation's
    // provider calls. Hold one admitted borrower across every refresh/close.
    var provider_borrow = try service.lifetime.acquire();
    defer provider_borrow.deinit();
    for (0..4) |i| {
        const statement = try std.fmt.allocPrint(a, "INSERT INTO docs(id,body) VALUES ('{d}','body')", .{i});
        defer a.free(statement);
        try sqlFkRecoveryStatement(handle, statement);
        {
            const guard = public.enterHandle(handle, .exclusive).?;
            defer guard.leave();
            const previous = guard.handle;
            previous.maintenance_references += 1;
            defer previous.maintenance_references -= 1;
            guard.connection.?.detach();
            const refreshed = try owner.refresh();
            try std.testing.expect(refreshed != previous);
            try std.testing.expectEqual(service, refreshed.lite_inference.?);
        }
        owner.collectRetired();
        owner.maintenance_pending = true;
        _ = try owner.maintenanceTurnForTest();
        try std.testing.expectEqual(service, owner.maintenance_snapshot.?.lite_inference.?);
        try std.testing.expectEqual(service, owner.current.?.lite_inference.?);
        try std.testing.expectEqual(@as(usize, 2), service.references.load(.acquire));
    }
    var status: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_status_json(handle, &status));
    defer freeRawBuffer(status.ptr, status.len);
    try std.testing.expect(std.mem.indexOf(u8, status.ptr.?[0..status.len], "\"memory_admission\"") != null);
}

test "capi lite inference producer retry is visible in graph replacement status" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-producer-retry-status");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    var options: capi.OpenOptions = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_open_options_init(&options));
    options.storage_kind = capi.storage_kind_lite;
    options.profile = capi.profile_hosted;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_db_create_with_options(path, &options, &handle));
    defer antfly_db_close(handle);
    const guard = public.enterHandle(handle, .exclusive).?;
    defer guard.leave();
    const database = guard.handle.database();
    try database.reconfigureEnrichmentRuntimePaused(.{ .enable_without_producers = true });
    const runtime = database.enrichment_runtime.?;
    runtime.applied_sequence = 1;
    runtime.last_error_code = db_mod.types.RuntimeErrorName.init("ResourceTemporarilyUnavailable");
    runtime.retrying = true;
    runtime.next_retry_at_ms = 12345;
    for ([_]bool{ true, false }) |complete| {
        try database.core.index_manager.saveGraphReplacementBuild(database.core.store, .{
            .name = "replacement",
            .generation = 1,
            .old = "old",
            .producer_replay_complete = complete,
            .producer_replay_target = 10,
        });
        const json = try public.pendingWorkStatsJson(guard.handle);
        defer freeRawBuffer(json.ptr, json.len);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, json.ptr.?[0..json.len], .{});
        defer parsed.deinit();
        const row = parsed.value.object.get("replacements").?.array.items[0].object;
        try std.testing.expectEqualStrings(if (complete) "waiting_for_producer_enrichment" else "replaying_producer_sources", row.get("blocked_reason").?.string);
        try std.testing.expectEqualStrings("ResourceTemporarilyUnavailable", row.get("upstream_last_error").?.string);
        try std.testing.expect(row.get("upstream_retrying").?.bool);
        try std.testing.expectEqual(@as(i64, 12345), row.get("upstream_next_retry_at_ms").?.integer);
        try std.testing.expect(row.get("last_error").? == .null);
        try std.testing.expectEqual(@as(i64, 0), row.get("retry_count").?.integer);
    }
    try database.core.index_manager.saveGraphReplacementBuild(database.core.store, .{
        .name = "replacement",
        .generation = 1,
        .old = "old",
        .producer_replay_complete = true,
        .producer_replay_target = 10,
    });
    runtime.applied_sequence = 10;
    const json = try public.pendingWorkStatsJson(guard.handle);
    defer freeRawBuffer(json.ptr, json.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, json.ptr.?[0..json.len], .{});
    defer parsed.deinit();
    const row = parsed.value.object.get("replacements").?.array.items[0].object;
    try std.testing.expect(row.get("blocked_reason").? == .null);
    try std.testing.expect(row.get("upstream_last_error").? == .null);
}

test "capi SQL visibility waits for physical graph indexes and normalized vector text filters" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |quarantined_default| {
        for ([_]enum { graph_alias, vector_filter, vector_exclusion, structured_filter, structured_exclusion }{ .graph_alias, .vector_filter, .vector_exclusion, .structured_filter, .structured_exclusion }) |mode| {
            var directory = try TestDirectory.init("capi-search-visibility");
            defer directory.cleanup();
            if (quarantined_default) {
                {
                    var setup = try db_mod.DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_independent_maintenance = false });
                    defer setup.close();
                    try setup.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
                    try setup.addIndex(.{ .name = "custom_text", .kind = .full_text, .config_json = "{}" });
                    try setup.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"stale\"}" }}, .sync_level = .full_index });
                }
                // A persisted, unavailable default remains in the catalog. Only
                // custom_text can actually serve text queries after reopen.
                const broken = try std.fmt.allocPrint(a, "{s}/indexes/full_text_index_v0", .{directory.path()});
                defer a.free(broken);
                try std.Io.Dir.cwd().deleteTree(std.testing.io, broken);
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = broken, .data = "broken index root" });
            }
            var database = try db_mod.DB.open(a, directory.path(), .{ .executor = .{ .backend = .manual }, .open_mode = if (quarantined_default) .writer_no_replay else .writer, .start_optional_runtimes = false, .start_independent_maintenance = false });
            defer database.close();
            if (!quarantined_default) try database.addIndex(.{ .name = "custom_text", .kind = .full_text, .config_json = "{}" });
            if (quarantined_default) {
                const indexes = try database.listIndexes(a);
                defer db_mod.types.freeIndexConfigs(a, indexes);
                try std.testing.expectEqual(@as(usize, 2), indexes.len);
                const selected = (try database.resolvedFilterTextIndexNameAlloc(a, null, null)).?;
                defer a.free(selected);
                try std.testing.expectEqualStrings("custom_text", selected);
                try std.testing.expectError(error.IndexUnavailable, database.resolvedFilterTextIndexNameAlloc(a, "full_text_index_v0", null));
            }
            try database.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2}" });
            try database.addIndex(.{ .name = "knowledge", .kind = .graph, .config_json = "{\"edge_types\":[{\"name\":\"R\",\"field\":\"links\"}]}" });
            try database.batch(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"fresh\",\"embedding\":[1,0],\"links\":[\"target\"]}" }}, .sync_level = .write });
            const target = database.core.nextDerivedSequence();
            // A ready vector worker must not mask the independent text debt.
            try database.runDerivedUntilTargets(target, &.{"semantic"});
            var adapter = @import("antfly_local_sources").capi_sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
            var request: db_mod.types.SearchRequest = .{};
            const graph = [_]db_mod.types.NamedGraphQuery{.{ .name = "neighbors", .query = .{
                .query_type = .traverse,
                .index_name = "knowledge",
                .start_nodes = .{ .keys = &.{"doc"} },
            } }};
            if (mode == .graph_alias) {
                request.graph_queries = &graph;
            } else {
                request.index_name = "semantic";
                request.dense = .{ .vector = &.{ 1, 0 }, .k = 10 };
                // This is the normalized vector request: full_text is absent.
                switch (mode) {
                    .vector_filter => request.filter_text = .{ .match = .{ .field = "body", .text = "fresh" } },
                    .vector_exclusion => request.exclusion_text = .{ .match = .{ .field = "body", .text = "fresh" } },
                    .structured_filter => request.filter_query_json = "{\"term\":{\"path\":\"body\",\"value\":\"fresh\"}}",
                    .structured_exclusion => request.exclusion_query_json = "{\"term\":{\"path\":\"body\",\"value\":\"fresh\"}}",
                    .graph_alias => unreachable,
                }
            }
            const expected = if (mode == .graph_alias) "knowledge" else "custom_text";
            {
                const debt = try database.listDerivedReplayDebt(a);
                defer {
                    for (debt) |*item| item.deinit(a);
                    a.free(debt);
                }
                var lagging = false;
                for (debt) |item| if (std.mem.eql(u8, expected, item.index_name)) {
                    lagging = item.applied_sequence < target;
                };
                try std.testing.expect(lagging);
            }
            const clock = database.backend_runtime.monotonicClock();
            _ = try adapter.waitSearchIndexes(a, request, clock.nowRealtimeNs() + 5 * std.time.ns_per_s);
            const debt = try database.listDerivedReplayDebt(a);
            defer {
                for (debt) |*item| item.deinit(a);
                a.free(debt);
            }
            var ready = false;
            for (debt) |item| if (std.mem.eql(u8, expected, item.index_name)) {
                ready = item.applied_sequence >= target;
            };
            try std.testing.expect(ready);
        }
    }
}

test "capi lite inference failed open drains the configured generation before releasing its service" {
    if (!capi_build_options.inference_enabled or !lite_backend.capabilitiesForProfile(.native).local_inference_runtime) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-inference-open-unwind");
    defer directory.cleanup();
    const path = try tempTestAflitePath(a, directory.path(), "database");
    defer a.free(path);
    defer cleanupTestFile(path);
    const options: public.LiteResolvedOpenOptions = .{
        .externally_locked = true,
        .generated_enrichment_replay = true,
        .inference = .{ .local_runtime_configured = true },
    };
    const root = try public.openLiteHandleAllocWithRuntime(a, path, options, true, shared.handleLockIo(), null);
    defer shared.closeHandle(root);
    const service = root.lite_inference.?;
    try root.db.core.store.put("\x00antfly/embedded/tables/v1", "{");
    const references = service.references.load(.acquire);
    // Catalog discovery fails after managed producers have been reconfigured
    // on the new DB copy and the service reference has been retained.
    try std.testing.expectError(error.UnexpectedEndOfInput, public.openLiteHandleAllocWithSharedInference(a, path, options, false, shared.handleLockIo(), null, root));
    try std.testing.expectEqual(references, service.references.load(.acquire));
    try std.testing.expect(service.lifetime.isAccepting());
    var borrower = try service.lifetime.acquire();
    borrower.deinit();
    // Exercise final-service destruction through the same failed-open path.
    try std.testing.expectError(error.UnexpectedEndOfInput, public.openLiteHandleAllocWithRuntime(a, path, options, false, shared.handleLockIo(), null));
}

test "capi SQL document search projects native cells without copying unselected bodies" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("capi-sql-document-search");
    defer directory.cleanup();
    var database = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer database.close();
    try database.setSchemaJson(a,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"string"},"payload":{"type":"object","properties":{"amount":{"type":"integer"}}},"ignored":{"type":"string"}},"additionalProperties":false}}}}
    );
    try database.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
    const ignored = try a.alloc(u8, 1024 * 1024);
    defer a.free(ignored);
    @memset(ignored, 'x');
    const document = try std.fmt.allocPrint(a, "{{\"body\":\"alpha\",\"payload\":{{\"amount\":9007199254740993}},\"ignored\":\"{s}\"}}", .{ignored});
    defer a.free(document);
    try database.batch(.{ .writes = &.{.{ .key = "a", .value = document }} });
    try database.runUntilIdle();
    const sql = @import("antfly_local_sources").capi_sql;
    var adapter = sql.Adapter(antfly){ .db = &database, .table_name = "rows" };
    var searched = try sql.compiler.compile(a, "SELECT _id,body,payload->>'amount',score FROM antfly_search('rows','body:alpha')", .{});
    defer searched.deinit();
    var hits = try sql.runtime.execute(a, adapter.backend(), &searched, &.{}, .{ .page_rows = 1 });
    defer hits.deinit();
    try std.testing.expectEqual(@as(usize, 1), hits.output.rows.len);
    try std.testing.expectEqualStrings("a", hits.output.rows[0][0].string);
    try std.testing.expectEqualStrings("alpha", hits.output.rows[0][1].string);
    try std.testing.expectEqualStrings("9007199254740993", hits.output.rows[0][2].string);
    try std.testing.expect(!hits.output.sql_nulls.?[0][3]);
    var native_json = try sql.compiler.compile(a, "SELECT _id,payload FROM antfly_search('rows','body:alpha')", .{});
    defer native_json.deinit();
    const stream = (try @import("antfly_local_sources").sql_read_stream.Stream.open(a, adapter.backend(), &native_json, &.{}, .{ .page_rows = 1 })).?;
    defer stream.close();
    var native_page = try stream.nextBatch(1);
    defer native_page.deinit();
    try std.testing.expectEqualStrings("a", (try native_page.values.cell(native_page.arena.allocator(), 0, 0)).value.string);
    try std.testing.expectEqualStrings("{\"amount\":9007199254740993}", (try native_page.values.encodedJson(0, 1)).?);
    var metadata = try sql.compiler.compile(a, "SELECT _id,score FROM antfly_search('rows','body:alpha')", .{});
    defer metadata.deinit();
    var metadata_hits = try sql.runtime.execute(a, adapter.backend(), &metadata, &.{}, .{});
    defer metadata_hits.deinit();
    try std.testing.expectEqualStrings("a", metadata_hits.output.rows[0][0].string);
    try std.testing.expect(!metadata_hits.output.sql_nulls.?[0][1]);
    var count = try sql.compiler.compile(a, "SELECT count(*) FROM antfly_search('rows','body:alpha')", .{});
    defer count.deinit();
    var counted = try sql.runtime.execute(a, adapter.backend(), &count, &.{}, .{});
    defer counted.deinit();
    try std.testing.expectEqualStrings("1", counted.output.rows[0][0].string);
}

test "capi SQL document ranked hydration pins old primary bytes and retires each scope" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator, is_lite: bool) !void {
            var directory = try TestDirectory.init("capi-sql-document-ranked-cut");
            defer directory.cleanup();
            try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
            const path = try std.fmt.allocPrint(a, "{s}/primary.aflite", .{directory.path()});
            defer a.free(path);
            var lite_runtime: ?db_mod.background_runtime.BackendRuntimeHandle = if (is_lite) try .initManualWithOwnedIo(a) else null;
            defer if (lite_runtime) |*runtime| runtime.deinit();
            const handle = if (is_lite) try public.openLiteHandleAllocWithRuntime(a, path, .{ .profile = .hosted, .open_mode = .writer_no_replay, .no_sync = true }, true, shared.handleLockIo(), lite_runtime.?.runtime) else null;
            defer if (handle) |owner| shared.closeHandle(owner);
            var lsm: db_mod.DB = if (is_lite) undefined else try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
            defer if (!is_lite) lsm.close();
            const database = if (handle) |owner| &owner.db else &lsm;
            try database.setSchemaJson(a,
                \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"string"},"ignored":{"type":"string"}},"additionalProperties":false}}}}
            );
            try database.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{\"field\":\"body\"}" });
            const ignored = try a.alloc(u8, 1024 * 1024);
            defer a.free(ignored);
            var random = std.Random.DefaultPrng.init(0x9561165);
            random.random().bytes(ignored);
            for (ignored) |*byte| byte.* = 'a' + byte.* % 26;
            const document = try std.fmt.allocPrint(a, "{{\"ignored\":\"{s}\",\"body\":\"alpha\"}}", .{ignored});
            defer a.free(document);
            const second = try std.fmt.allocPrint(a, "{{\"ignored\":\"{s}\",\"body\":\"beta\"}}", .{ignored});
            defer a.free(second);
            try database.batch(.{ .writes = &.{ .{ .key = "a", .value = document }, .{ .key = "b", .value = second }, .{ .key = "legacy", .value = document } } });
            var small_arena = std.heap.ArenaAllocator.init(a);
            defer small_arena.deinit();
            const small_a = small_arena.allocator();
            var small_ids: [64][]const u8 = undefined;
            var small_writes: [64]db_mod.types.BatchWrite = undefined;
            for (&small_ids, &small_writes, 0..) |*id, *write, index| {
                id.* = try std.fmt.allocPrint(small_a, "s-{d:0>3}", .{index});
                write.* = .{ .key = id.*, .value = "{\"body\":\"tiny\",\"ignored\":\"small\"}" };
            }
            try database.batch(.{ .writes = &small_writes });
            try database.runUntilIdle();
            // Exercise production cursor batching as well as the reader below.
            // Seventeen-row pages cross several proof/body window boundaries.
            const sql = @import("antfly_local_sources").capi_sql;
            var adapter = sql.Adapter(antfly){ .db = database, .table_name = "rows" };
            var statement = try sql.compiler.compile(a, "SELECT _id,body FROM antfly_search('rows','body:tiny',64) ORDER BY _id", .{});
            defer statement.deinit();
            var output = try sql.runtime.execute(a, adapter.backend(), &statement, &.{}, .{ .page_rows = 17 });
            defer output.deinit();
            try std.testing.expectEqual(@as(usize, 64), output.output.rows.len);
            for (output.output.rows, small_ids) |row, id| {
                try std.testing.expectEqualStrings(id, row[0].string);
                try std.testing.expectEqualStrings("tiny", row[1].string);
            }
            const revisions = @import("antfly_local_sources").storage_document_mutation_revision;
            const projection_keys = @import("antfly_local_sources").storage_document_projection;
            const legacy_key = try revisions.keyForDocumentAlloc(a, "legacy");
            try database.core.store.delete(&projection_keys.keyForRevision(legacy_key));
            var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = a, .limit = 8 * 1024 * 1024 };
            var lease = try database.beginQueryReadLease();
            var held = true;
            defer if (held) lease.release();
            var reader = try lease.documentRows(budget.allocator(), 1);
            defer reader.deinit();
            const file = if (handle) |owner| &owner.owned_lite_backend.?.native_docstore.?.read_generation.?.file else null;
            if (file) |native_file| native_file.page_cache_enabled.store(false, .monotonic);
            var ranked = (try lease.search(a, .{ .full_text = .{ .match_all = {} }, .limit = 2, .include_stored = false })).result;
            defer ranked.deinit();
            try std.testing.expectEqual(@as(usize, 2), ranked.hits.len);
            for (ranked.hits) |hit| try std.testing.expect(hit.stored_data == null and hit.source_value == null);
            lease.release();
            held = false;
            try database.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"body\":\"changed\",\"ignored\":\"small\"}" }}, .deletes = &.{"b"} });
            try database.runUntilIdle();
            // Both projections use the original captured cut, after update/delete.
            const projected_before = if (file) |native_file| native_file.test_backing_read_bytes.load(.monotonic) else 0;
            const projected = try reader.lookupProjectedBatch(&.{ "a", "b" }, &.{"body"}, 256 * 1024);
            try std.testing.expectEqual(@as(usize, if (is_lite) 2 else 1), projected.consumed);
            if (is_lite) try std.testing.expectEqualStrings("\"beta\"", projected.values[1].fields[0].value);
            try std.testing.expectEqualStrings("body", projected.values[0].fields[0].name);
            try std.testing.expectEqualStrings("\"alpha\"", projected.values[0].fields[0].value);
            const projected_reads = if (file) |native_file| native_file.test_backing_read_bytes.load(.monotonic) - projected_before else 0;
            std.debug.print("projected hydration lite={any}: scoped_peak={d} backing_read_bytes={d} source_bytes={d}\n", .{ is_lite, budget.peak, projected_reads, document.len });
            if (is_lite) {
                try std.testing.expect(budget.peak < 256 * 1024);
                try std.testing.expect(projected_reads < document.len / 4);
            }
            reader.releasePage();
            const repeated_projection = try reader.lookupProjectedBatch(&.{ "b", "a", "b" }, &.{"body"}, 256 * 1024);
            try std.testing.expectEqualStrings("\"beta\"", repeated_projection.values[0].fields[0].value);
            if (is_lite) {
                try std.testing.expectEqual(@as(usize, 3), repeated_projection.consumed);
                try std.testing.expectEqualStrings("\"alpha\"", repeated_projection.values[1].fields[0].value);
                try std.testing.expectEqual(repeated_projection.values[0].fields.ptr, repeated_projection.values[2].fields.ptr);
            }
            reader.releasePage();
            const bounded_projection = try reader.lookupProjectedBatch(&.{ "b", "a" }, &.{"body"}, 8);
            try std.testing.expectEqual(@as(usize, 1), bounded_projection.consumed);
            const metadata_owner = reader.directory_scope.?.ptr;
            const cached_directory = reader.directories[1].?.ptr;
            budget.limit = 0;
            try std.testing.expectError(error.OutOfMemory, reader.lookupProjectedBatch(&.{"a"}, &.{"body"}, 8));
            budget.limit = 8 * 1024 * 1024;
            const continued_projection = try reader.lookupProjectedBatch(&.{"a"}, &.{"body"}, 8);
            try std.testing.expectEqual(metadata_owner, reader.directory_scope.?.ptr);
            try std.testing.expectEqual(cached_directory, reader.directories[1].?.ptr);

            try std.testing.expectEqualStrings("\"alpha\"", continued_projection.values[0].fields[0].value);
            reader.releasePage();
            const absent_projection = try reader.lookupProjectedBatch(&.{ "b", "a", "b" }, &.{"not-present"}, 8);
            try std.testing.expectEqual(@as(usize, 3), absent_projection.consumed);
            for (absent_projection.values) |source| try std.testing.expectEqual(@as(usize, 0), source.fields.len);
            reader.releasePage();
            const deleted_projection = try reader.lookupProjectedBatch(&.{"b"}, &.{"body"}, 256 * 1024);
            try std.testing.expectEqualStrings("\"beta\"", deleted_projection.values[0].fields[0].value);
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            const legacy = try reader.lookupProjectedBatch(&.{"legacy"}, &.{"body"}, 256 * 1024);
            try std.testing.expectEqualSlices(u8, document, legacy.values[0].bytes);
            reader.releasePage();
            for (0..32) |index| {
                const raw = try reader.lookup(if (index % 2 == 0) "a" else "b");
                try std.testing.expectEqualSlices(u8, if (index % 2 == 0) document else second, raw);
            }
            // The primary generation remains pinned, but body/decompression owners
            // retire on every lookup rather than accumulating across the result set.
            try std.testing.expect(budget.peak < 8 * 1024 * 1024);
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            try std.testing.expectEqualSlices(u8, second, try reader.lookup("b"));
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            budget.limit = 0;
            try std.testing.expectError(error.OutOfMemory, reader.lookupBatch(&.{ "b", "a", "b" }, 4 * 1024 * 1024));
            budget.limit = 8 * 1024 * 1024;
            // Retry the failed metadata preparation on the same pinned owner.
            const batch = try reader.lookupBatch(&.{ "b", "a", "b" }, 4 * 1024 * 1024);
            try std.testing.expectEqual(@as(usize, 3), batch.consumed);
            try std.testing.expectEqualSlices(u8, second, batch.values[0].?);
            try std.testing.expectEqualSlices(u8, document, batch.values[1].?);
            try std.testing.expectEqualSlices(u8, second, batch.values[2].?);
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            const isolated = try reader.lookupBatch(&.{ "a", "b" }, 1);
            try std.testing.expectEqual(@as(usize, 1), isolated.consumed);
            try std.testing.expectEqualSlices(u8, document, isolated.values[0].?);
            // Consume the cached suffix with a different byte target. A caller
            // can supply more IDs than remain in the current proof window.
            const suffix = try reader.lookupBatch(&.{ "b", "s-000" }, 256 * 1024);
            try std.testing.expectEqual(@as(usize, 1), suffix.consumed);
            try std.testing.expectEqualSlices(u8, second, suffix.values[0].?);
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            try std.testing.expectError(error.InvalidBatch, reader.lookupBatch(&.{ "a", "b" }, 0));
            reader.releasePage();
            const point_before = if (file) |native_file| native_file.test_page_reads.load(.monotonic) else 0;
            for (small_ids) |id| try std.testing.expectEqualStrings("{\"body\":\"tiny\",\"ignored\":\"small\"}", try reader.lookup(id));
            const point_reads = if (file) |native_file| native_file.test_page_reads.load(.monotonic) - point_before else 0;
            reader.releasePage();
            const batch_before = if (file) |native_file| native_file.test_page_reads.load(.monotonic) else 0;
            const small = try reader.lookupBatch(&small_ids, 256 * 1024);
            try std.testing.expectEqual(@as(usize, 64), small.consumed);
            for (small.values) |raw| try std.testing.expectEqualStrings("{\"body\":\"tiny\",\"ignored\":\"small\"}", raw.?);
            const batch_reads = if (file) |native_file| native_file.test_page_reads.load(.monotonic) - batch_before else 0;
            if (file != null) std.debug.print("document hydration 64 small keys: point pages {d}, bounded batch pages incl proofs {d}\n", .{ point_reads, batch_reads });
            reader.releasePage();
            try std.testing.expectEqual(@as(usize, 0), budget.live);
            const legacy_revision = blk: {
                var txn = try database.core.store.beginReadTxn();
                defer txn.abort();
                break :blk try revisions.load(&txn, legacy_key);
            };
            try std.testing.expect(database.document_authority_backfill_requested.load(.acquire));
            // Manual owners drive the independent authority worker explicitly;
            // runUntilIdle drains replay/index work, not this cache migration.
            var maintenance_pages: usize = 0;
            while (database.document_authority_backfill_active.load(.acquire) or database.document_authority_backfill_requested.load(.acquire)) : (maintenance_pages += 1) {
                try std.testing.expect(maintenance_pages < 256);
                _ = try database.advanceDocumentAuthorityBackfillPage();
            }
            {
                var txn = try database.core.store.beginReadTxn();
                defer txn.abort();
                try std.testing.expectEqual(legacy_revision, try revisions.load(&txn, legacy_key));
            }
            var migrated_lease = try database.beginQueryReadLease();
            defer migrated_lease.release();
            var migrated = try migrated_lease.documentRows(budget.allocator(), 1);
            defer migrated.deinit();
            const migrated_value = try migrated.lookupProjectedBatch(&.{"legacy"}, &.{"body"}, 256 * 1024);
            try std.testing.expectEqualStrings("\"alpha\"", migrated_value.values[0].fields[0].value);
            std.debug.print("document ranked hydration lite={any}: scoped peak {d} bytes across snapshot and bounded batch reads\n", .{ is_lite, budget.peak });
        }
    };
    try Fixture.run(std.testing.allocator, false);
    try Fixture.run(std.testing.allocator, true);
}
