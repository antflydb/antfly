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

pub const algebraic_manifest = @import("algebraic_manifest.zig");
pub const algebraic_publish = @import("algebraic_publish.zig");
pub const builder = @import("builder.zig");
pub const compactor = @import("compactor.zig");
pub const coordinator = @import("coordinator.zig");
pub const external_source_manifest = @import("external_source_manifest.zig");
pub const external_source_plan_resolver = @import("external_source_plan_resolver.zig");
pub const external_source_plan_resolver_api = @import("external_source_plan_resolver_api.zig");
pub const external_source_publish = @import("external_source_publish.zig");
pub const external_source_publication = @import("external_source_publication.zig");
pub const impact_planner = @import("impact_planner.zig");
pub const lake_gc = @import("lake_gc.zig");
pub const lake_promotion = @import("lake_promotion.zig");
pub const lake_rebuild = @import("lake_rebuild.zig");
pub const publication_plan = @import("publication_plan.zig");
pub const retention = @import("retention.zig");
pub const row_fragment_manifest = @import("row_fragment_manifest.zig");
pub const row_fragment_publish = @import("row_fragment_publish.zig");
pub const row_fragments = @import("row_fragments.zig");
pub const vector_index = @import("vector_index.zig");

pub const BuildResult = builder.BuildResult;
pub const Builder = builder.Builder;
pub const AlgebraicManifestPlan = algebraic_manifest.Plan;
pub const AlgebraicPublishedArtifact = algebraic_manifest.PublishedArtifact;
pub const planAlgebraicManifestAlloc = algebraic_manifest.planAlloc;
pub const AlgebraicPublishOptions = algebraic_publish.PublishOptions;
pub const AlgebraicExpressionPublishOptions = algebraic_publish.ExpressionPublishOptions;
pub const AlgebraicPublishResult = algebraic_publish.PublishResult;
pub const publishAlgebraicGroupByAggregateAlloc = algebraic_publish.publishGroupByAggregateAlloc;
pub const publishAlgebraicExpressionFoldsAlloc = algebraic_publish.publishExpressionFoldsAlloc;
pub const Compactor = compactor.Compactor;
pub const CompactionResult = compactor.CompactionResult;
pub const BackgroundPublisher = coordinator.BackgroundPublisher;
pub const ArtifactImpactPlan = impact_planner.ArtifactImpactPlan;
pub const ArtifactFamily = impact_planner.ArtifactFamily;
pub const ExternalSourceManifestPlan = external_source_manifest.Plan;
pub const ExternalSourceAttachedArtifacts = external_source_manifest.AttachedArtifacts;
pub const ExternalSourcePublishedArtifact = external_source_manifest.PublishedArtifact;
pub const planExternalSourceManifestAlloc = external_source_manifest.planAlloc;
pub const planExternalSourceManifestFromBindingAndInventoryAlloc = external_source_manifest.planFromBindingAndInventoryAlloc;
pub const attachExternalSourceArtifactsAlloc = external_source_manifest.attachArtifactsAlloc;
pub const ExternalSourcePublishOptions = external_source_publish.PublishOptions;
pub const ExternalSourcePublishResult = external_source_publish.PublishResult;
pub const publishExternalSourceInventoryAlloc = external_source_publish.publishInventoryAlloc;
pub const ExternalSourceOpenedObjectStoreResolver = external_source_plan_resolver.OpenedObjectStoreResolver;
pub const ExternalSourceRemoteUriObjectStoreResolver = external_source_plan_resolver.RemoteUriObjectStoreResolver;
pub const ExternalSourcePublicationPlanResolver = external_source_plan_resolver.Resolver;
pub const ExternalSourcePublicationPlanResolverOptions = external_source_plan_resolver.ResolverOptions;
pub const attachExternalSourcePlanToOwnedManifestAlloc = external_source_publication.attachPlanToOwnedManifestAlloc;
pub const ImpactPlanInput = impact_planner.PlanInput;
pub const planArtifactImpactAlloc = impact_planner.planAlloc;
pub const LakeGcSnapshot = lake_gc.Snapshot;
pub const LakeGcCandidate = lake_gc.Candidate;
pub const LakeGcPlan = lake_gc.Plan;
pub const planLakeGcAlloc = lake_gc.planAlloc;
pub const LakePromotionThresholds = lake_promotion.Thresholds;
pub const LakePromotionObservation = lake_promotion.Observation;
pub const LakePromotionRecommendation = lake_promotion.Recommendation;
pub const LakePromotionRecommendationKind = lake_promotion.RecommendationKind;
pub const recommendLakePromotion = lake_promotion.recommend;
pub const LakeRebuildAction = lake_rebuild.Action;
pub const LakeRebuildDesiredArtifact = lake_rebuild.DesiredArtifact;
pub const LakeRebuildPublishedArtifact = lake_rebuild.PublishedArtifact;
pub const LakeRebuildDecision = lake_rebuild.Decision;
pub const LakeRebuildPlan = lake_rebuild.Plan;
pub const planLakeRebuildAlloc = lake_rebuild.planAlloc;
pub const TablePublicationPlan = publication_plan.TablePublicationPlan;
pub const MetadataRepublishReasons = publication_plan.MetadataRepublishReasons;
pub const ArtifactAction = publication_plan.ArtifactAction;
pub const PublicationArtifactActions = publication_plan.ArtifactActions;
pub const DerivedOutputAction = publication_plan.DerivedOutputAction;
pub const PublicationDerivedOutputActions = publication_plan.DerivedOutputActions;
pub const TableDefinitionSnapshot = publication_plan.TableDefinitionSnapshot;
pub const PublishRunStats = coordinator.PublishRunStats;
pub const Pruner = retention.Pruner;
pub const PruneResult = retention.PruneResult;
pub const RowFragmentManifestPlan = row_fragment_manifest.Plan;
pub const RowFragmentPublishedArtifact = row_fragment_manifest.PublishedArtifact;
pub const planRowFragmentManifestAlloc = row_fragment_manifest.planAlloc;
pub const RowFragmentPublishOptions = row_fragment_publish.PublishOptions;
pub const RowFragmentPublishResult = row_fragment_publish.PublishResult;
pub const publishRowFragmentBatchAlloc = row_fragment_publish.publishBatchAlloc;
pub const RowFragmentBuildOptions = row_fragments.BuildOptions;
pub const buildRowFragmentFromBatchAlloc = row_fragments.buildFragmentFromBatchAlloc;
pub const encodeRowFragmentFromBatchAlloc = row_fragments.encodeFragmentFromBatchAlloc;
pub const buildRowFragmentStatsFromBatchAlloc = row_fragments.buildFragmentStatsFromBatchAlloc;
pub const encodeRowFragmentStatsFromBatchAlloc = row_fragments.encodeFragmentStatsFromBatchAlloc;

test "serverless build module compiles" {
    _ = algebraic_manifest;
    _ = algebraic_publish;
    _ = builder;
    _ = compactor;
    _ = coordinator;
    _ = external_source_manifest;
    _ = external_source_plan_resolver;
    _ = external_source_plan_resolver_api;
    _ = external_source_publish;
    _ = external_source_publication;
    _ = impact_planner;
    _ = lake_gc;
    _ = lake_promotion;
    _ = lake_rebuild;
    _ = publication_plan;
    _ = retention;
    _ = row_fragment_manifest;
    _ = row_fragment_publish;
    _ = row_fragments;
    _ = vector_index;
    _ = BuildResult;
    _ = Builder;
    _ = AlgebraicManifestPlan;
    _ = AlgebraicPublishedArtifact;
    _ = planAlgebraicManifestAlloc;
    _ = AlgebraicPublishOptions;
    _ = AlgebraicExpressionPublishOptions;
    _ = AlgebraicPublishResult;
    _ = publishAlgebraicGroupByAggregateAlloc;
    _ = publishAlgebraicExpressionFoldsAlloc;
    _ = Compactor;
    _ = CompactionResult;
    _ = BackgroundPublisher;
    _ = ArtifactImpactPlan;
    _ = ArtifactFamily;
    _ = ExternalSourceManifestPlan;
    _ = ExternalSourceAttachedArtifacts;
    _ = ExternalSourcePublishedArtifact;
    _ = planExternalSourceManifestAlloc;
    _ = planExternalSourceManifestFromBindingAndInventoryAlloc;
    _ = attachExternalSourceArtifactsAlloc;
    _ = ExternalSourcePublishOptions;
    _ = ExternalSourcePublishResult;
    _ = publishExternalSourceInventoryAlloc;
    _ = attachExternalSourcePlanToOwnedManifestAlloc;
    _ = ImpactPlanInput;
    _ = planArtifactImpactAlloc;
    _ = LakeGcSnapshot;
    _ = LakeGcCandidate;
    _ = LakeGcPlan;
    _ = planLakeGcAlloc;
    _ = LakePromotionThresholds;
    _ = LakePromotionObservation;
    _ = LakePromotionRecommendation;
    _ = LakePromotionRecommendationKind;
    _ = recommendLakePromotion;
    _ = LakeRebuildAction;
    _ = LakeRebuildDesiredArtifact;
    _ = LakeRebuildPublishedArtifact;
    _ = LakeRebuildDecision;
    _ = LakeRebuildPlan;
    _ = planLakeRebuildAlloc;
    _ = TablePublicationPlan;
    _ = MetadataRepublishReasons;
    _ = ArtifactAction;
    _ = PublicationArtifactActions;
    _ = DerivedOutputAction;
    _ = PublicationDerivedOutputActions;
    _ = TableDefinitionSnapshot;
    _ = PublishRunStats;
    _ = Pruner;
    _ = PruneResult;
    _ = RowFragmentManifestPlan;
    _ = RowFragmentPublishedArtifact;
    _ = planRowFragmentManifestAlloc;
    _ = RowFragmentPublishOptions;
    _ = RowFragmentPublishResult;
    _ = publishRowFragmentBatchAlloc;
    _ = RowFragmentBuildOptions;
    _ = buildRowFragmentFromBatchAlloc;
    _ = encodeRowFragmentFromBatchAlloc;
    _ = buildRowFragmentStatsFromBatchAlloc;
    _ = encodeRowFragmentStatsFromBatchAlloc;
}
