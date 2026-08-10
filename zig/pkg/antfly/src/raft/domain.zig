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

//! Raft production surface without simulation harnesses.

const metadata = @import("../metadata/domain.zig");

pub const catalog = @import("catalog.zig");
pub const db_enrichment_executor = @import("db_enrichment_executor.zig");
pub const db_enrichment_runtime_factory = @import("db_enrichment_runtime_factory.zig");
pub const enrichment_runtime = @import("enrichment_runtime.zig");
pub const feature_db_reads = @import("feature_db_reads.zig");
pub const feature_reads = @import("feature_reads.zig");
pub const host = @import("host.zig");
pub const hosted_shard_ops = @import("hosted_shard_ops.zig");
pub const leader_runtime = @import("leader_runtime.zig");
pub const managed_host = @import("managed_host.zig");
pub const metadata_apply = @import("metadata_apply.zig");
pub const metadata_view = @import("metadata_view.zig");
pub const peer_resolver = @import("peer_resolver.zig");
pub const read_gate = @import("read_gate.zig");
pub const reconciler = @import("reconciler.zig");
pub const runtime_loop = @import("runtime_loop.zig");
pub const service = @import("service.zig");
pub const shard_ops = @import("shard_ops.zig");
pub const state_machine = @import("state_machine/mod.zig");
pub const storage = @import("storage/mod.zig");
pub const transition_runtime = @import("transition_runtime.zig");
pub const transition_service = @import("transition_service.zig");
pub const transport = @import("transport/mod.zig");

pub const AppliedMetadataChange = metadata_apply.AppliedMetadataChange;
pub const CallbackReadableLeaseRequester = read_gate.CallbackReadableLeaseRequester;
pub const DbEnrichmentExecutor = db_enrichment_executor.DbEnrichmentExecutor;
pub const DbEnrichmentExecutorConfig = db_enrichment_executor.DbEnrichmentExecutorConfig;
pub const EnrichmentExecutor = enrichment_runtime.EnrichmentExecutor;
pub const EnrichmentReadGate = read_gate.EnrichmentReadGate;
pub const EnrichmentReadKind = read_gate.EnrichmentReadKind;
pub const EventedEnrichmentExecutor = enrichment_runtime.EventedExecutor;
pub const ExecutorBackend = enrichment_runtime.ExecutorBackend;
pub const FeatureDBReads = feature_db_reads.FeatureDBReads;
pub const FeatureReads = feature_reads.FeatureReads;
pub const FileReplicaCatalog = storage.FileReplicaCatalog;
pub const GroupDbPathResolver = db_enrichment_runtime_factory.GroupDbPathResolver;
pub const GroupRuntimeFactory = db_enrichment_executor.GroupRuntimeFactory;
pub const GroupRuntimeHandle = db_enrichment_executor.GroupRuntimeHandle;
pub const Host = host.Host;
pub const HostConfig = host.HostConfig;
pub const HostDeps = host.HostDeps;
pub const HostMetrics = host.HostMetrics;
pub const HostedReplicaStatus = host.HostedReplicaStatus;
pub const HostedShardDbAdapter = hosted_shard_ops.HostedShardDbAdapter;
pub const HostedShardOperationAdapter = hosted_shard_ops.HostedShardOperationAdapter;
pub const HttpHost = host.HttpHost;
pub const HttpHostConfig = host.HttpHostConfig;
pub const HttpHostDeps = host.HttpHostDeps;
pub const LeaderEnrichmentRuntime = enrichment_runtime.LeaderEnrichmentRuntime;
pub const LeaderObserver = leader_runtime.LeaderObserver;
pub const LeadershipEvent = leader_runtime.LeadershipEvent;
pub const LeadershipEventKind = leader_runtime.LeadershipEventKind;
pub const LeadershipTracker = leader_runtime.LeadershipTracker;
pub const LeaseGatedLeaderEnrichmentRuntime = enrichment_runtime.LeaseGatedLeaderEnrichmentRuntime;
pub const LeaseReadState = enrichment_runtime.LeaseReadState;
pub const ManagedHost = managed_host.ManagedHost;
pub const ManagedHostConfig = managed_host.ManagedHostConfig;
pub const ManagedHostDeps = managed_host.ManagedHostDeps;
pub const ManagedHostRuntime = runtime_loop.ManagedHostRuntime;
pub const ManagedHostService = service.ManagedHostService;
pub const ManagedHttpHost = managed_host.ManagedHttpHost;
pub const ManagedHttpHostConfig = managed_host.ManagedHttpHostConfig;
pub const ManagedHttpHostDeps = managed_host.ManagedHttpHostDeps;
pub const ManagedHttpHostRuntime = runtime_loop.ManagedHttpHostRuntime;
pub const ManagedHttpHostService = service.ManagedHttpHostService;
pub const ManagedProgressDriver = runtime_loop.ManagedProgressDriver;
pub const ManagedServiceConfig = service.ManagedServiceConfig;
pub const ManagedServiceDeps = service.ManagedServiceDeps;
pub const ManagedServiceMetrics = service.ManagedServiceMetrics;
pub const ManagedSyncResult = managed_host.ManagedSyncResult;
pub const MemoryPeerResolver = peer_resolver.MemoryPeerResolver;
pub const MemoryPlacementProvider = reconciler.MemoryPlacementProvider;
pub const MemoryReplicaCatalog = storage.MemoryReplicaCatalog;
pub const MemoryUpdateSource = runtime_loop.MemoryUpdateSource;
pub const MergeCoordinatorRuntime = transition_runtime.MergeCoordinatorRuntime;
pub const MergeExecutionState = metadata.MergeExecutionState;
pub const MergeExecutionStateTag = metadata.MergeExecutionStateTag;
pub const MergeRuntime = transition_runtime.MergeRuntime;
pub const MetadataApplier = metadata_apply.MetadataApplier;
pub const MetadataPlacementState = reconciler.MetadataPlacementState;
pub const MetadataPlacementUpdate = reconciler.MetadataPlacementUpdate;
pub const MetadataUpdate = metadata_view.MetadataUpdate;
pub const MetadataUpdateSink = runtime_loop.MetadataUpdateSink;
pub const MetadataUpdateSource = runtime_loop.MetadataUpdateSource;
pub const MetadataView = metadata_view.MetadataView;
pub const MultiplexedTransitionRuntime = transition_runtime.MultiplexedTransitionRuntime;
pub const OpenDbRuntimeFactory = db_enrichment_runtime_factory.OpenDbRuntimeFactory;
pub const OpenDbRuntimeFactoryConfig = db_enrichment_runtime_factory.OpenDbRuntimeFactoryConfig;
pub const PeerEndpoint = peer_resolver.PeerEndpoint;
pub const PeerResolver = peer_resolver.PeerResolver;
pub const PlacementIntent = reconciler.PlacementIntent;
pub const PlacementProvider = reconciler.PlacementProvider;
pub const ProgressSource = runtime_loop.ProgressSource;
pub const ReadConsistency = read_gate.ReadConsistency;
pub const ReadStateObserver = state_machine.ReadStateObserver;
pub const ReadableLeaseRequester = read_gate.ReadableLeaseRequester;
pub const ReconcileResult = reconciler.ReconcileResult;
pub const Reconciler = reconciler.Reconciler;
pub const ReplicaCatalog = storage.ReplicaCatalog;
pub const ReplicaDescriptorFactory = host.ReplicaDescriptorFactory;
pub const ReplicaRecord = storage.ReplicaRecord;
pub const ReplicaStateBackend = host.ReplicaStateBackend;
pub const RuntimeCadence = runtime_loop.RuntimeCadence;
pub const RuntimeHooks = host.RuntimeHooks;
pub const RuntimeLoopConfig = runtime_loop.RuntimeLoopConfig;
pub const RuntimeStepResult = runtime_loop.RuntimeStepResult;
pub const ShardOperationAdapter = shard_ops.ShardOperationAdapter;
pub const SimulatedEnrichmentExecutor = enrichment_runtime.SimulatedExecutor;
pub const SplitCoordinatorRuntime = transition_runtime.SplitCoordinatorRuntime;
pub const SplitExecutionState = metadata.SplitExecutionState;
pub const SplitExecutionStateTag = metadata.SplitExecutionStateTag;
pub const SplitRuntime = transition_runtime.SplitRuntime;
pub const ThreadedEnrichmentExecutor = enrichment_runtime.ThreadedExecutor;
pub const TransitionRuntime = transition_runtime.TransitionRuntime;
pub const TransitionService = transition_service.TransitionService;
pub const TransitionServiceMetrics = transition_service.TransitionServiceMetrics;
pub const TransitionServiceStepResult = transition_service.TransitionStepResult;
pub const default_http_listener_max_connection_threads = host.default_http_listener_max_connection_threads;
pub const httpListenerConfig = host.httpListenerConfig;
pub const placementMayLeadMembershipTransition = reconciler.placementMayLeadMembershipTransition;
pub const stableRandomSeed = host.stableRandomSeed;
