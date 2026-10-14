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

#pragma once
// A single bounded workspace per serialized decoder runtime. No request CTs
// are retained: the current hidden input is copied before each execution.
typedef struct { uint32_t hidden, vocab, clusters, topk, lm_type, centroid_type; } TermiteMtpHeadParams;
@interface TermiteMetalMtpHeadWorkspace : NSObject {
@public
    id<MTLComputePipelineState> pipelines[4];
    id<MTLBuffer> input, centroids, cluster_ids, scores, tokens, result, params;
    id<MTLBuffer> lm_weight, centroid_weight, ordering;
    NSUInteger lm_offset, centroid_offset, ordering_offset;
    id<MTLIndirectCommandBuffer> commands;
    TermiteMtpHeadParams geometry;
}
@end
