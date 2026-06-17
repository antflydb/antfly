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

pub const builder = @import("builder.zig");
pub const compactor = @import("compactor.zig");
pub const coordinator = @import("coordinator.zig");
pub const impact_planner = @import("impact_planner.zig");
pub const lake_promotion = @import("lake_promotion.zig");
pub const publication_plan = @import("publication_plan.zig");
pub const retention = @import("retention.zig");
pub const row_fragments = @import("row_fragments.zig");
pub const vector_index = @import("vector_index.zig");

pub const BuildResult = builder.BuildResult;
pub const Builder = builder.Builder;
pub const Compactor = compactor.Compactor;
pub const CompactionResult = compactor.CompactionResult;
pub const BackgroundPublisher = coordinator.BackgroundPublisher;
pub const ArtifactImpactPlan = impact_planner.ArtifactImpactPlan;
pub const ArtifactFamily = impact_planner.ArtifactFamily;
pub const ImpactPlanInput = impact_planner.PlanInput;
pub const planArtifactImpactAlloc = impact_planner.planAlloc;
pub const LakePromotionThresholds = lake_promotion.Thresholds;
pub const LakePromotionObservation = lake_promotion.Observation;
pub const LakePromotionRecommendation = lake_promotion.Recommendation;
pub const LakePromotionRecommendationKind = lake_promotion.RecommendationKind;
pub const recommendLakePromotion = lake_promotion.recommend;
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
pub const RowFragmentBuildOptions = row_fragments.BuildOptions;
pub const buildRowFragmentFromBatchAlloc = row_fragments.buildFragmentFromBatchAlloc;
pub const encodeRowFragmentFromBatchAlloc = row_fragments.encodeFragmentFromBatchAlloc;

test "serverless build module compiles" {
    _ = builder;
    _ = compactor;
    _ = coordinator;
    _ = impact_planner;
    _ = lake_promotion;
    _ = publication_plan;
    _ = retention;
    _ = row_fragments;
    _ = vector_index;
    _ = BuildResult;
    _ = Builder;
    _ = Compactor;
    _ = CompactionResult;
    _ = BackgroundPublisher;
    _ = ArtifactImpactPlan;
    _ = ArtifactFamily;
    _ = ImpactPlanInput;
    _ = planArtifactImpactAlloc;
    _ = LakePromotionThresholds;
    _ = LakePromotionObservation;
    _ = LakePromotionRecommendation;
    _ = LakePromotionRecommendationKind;
    _ = recommendLakePromotion;
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
    _ = RowFragmentBuildOptions;
    _ = buildRowFragmentFromBatchAlloc;
    _ = encodeRowFragmentFromBatchAlloc;
}
