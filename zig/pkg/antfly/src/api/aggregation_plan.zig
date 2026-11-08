// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Compatibility surface for the storage-free shared aggregation planner.
const implementation = @import("antfly_local_sources").api_aggregation_plan;
pub const default_aggregation_full_result_budget = implementation.default_aggregation_full_result_budget;
pub const aggregationFullResultBudgetFromRaw = implementation.aggregationFullResultBudgetFromRaw;
pub const aggregationFullResultBudget = implementation.aggregationFullResultBudget;
pub const identityGenerationForAggregationFullResultRerun = implementation.identityGenerationForAggregationFullResultRerun;
pub const aggregationCanUseCurrentResult = implementation.aggregationCanUseCurrentResult;
pub const aggregationFullResultLimit = implementation.aggregationFullResultLimit;
pub const requireCompleteAggregationFullResult = implementation.requireCompleteAggregationFullResult;
pub const aggregationFullResultRequest = implementation.aggregationFullResultRequest;
pub const distributedAggregationFullResultRequest = implementation.distributedAggregationFullResultRequest;
pub const aggregationFullResultRequestAtGeneration = implementation.aggregationFullResultRequestAtGeneration;
pub const aggregationCollectionRequest = implementation.aggregationCollectionRequest;
pub const requestVectorComponentCount = implementation.requestVectorComponentCount;
pub const requestHasAggregationTextDomain = implementation.requestHasAggregationTextDomain;
pub const AggregationDomainPlan = implementation.AggregationDomainPlan;
pub const AggregationDomainDecision = implementation.AggregationDomainDecision;
pub const aggregationDomainPlan = implementation.aggregationDomainPlan;
pub const SearchCapability = implementation.SearchCapability;
pub const collectAggregationFullResult = implementation.collectAggregationFullResult;
pub const collectFullResult = implementation.collectFullResult;
pub const collectAggregationDomain = implementation.collectAggregationDomain;
pub const collectDomain = implementation.collectDomain;
pub const mergeAggregationDomainResults = implementation.mergeAggregationDomainResults;
pub const canConsiderAlgebraicAggregations = implementation.canConsiderAlgebraicAggregations;
