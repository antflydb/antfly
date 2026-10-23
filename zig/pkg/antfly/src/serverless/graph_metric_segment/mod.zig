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

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const Score = types.Score;
pub const Segment = types.Segment;
pub const MaterializationState = types.MaterializationState;
pub const RejectionReason = types.RejectionReason;
pub const freeSegment = types.freeSegment;
pub const artifactNameAlloc = types.artifactNameAlloc;
pub const parseArtifactName = types.parseArtifactName;
pub const encodeAlloc = codec.encodeAlloc;
pub const encodeAllocWithCancellation = codec.encodeAllocWithCancellation;
pub const encodeAllocWithCancellationAndLimit = codec.encodeAllocWithCancellationAndLimit;
pub const encodedSize = codec.encodedSize;
pub const encodedSizeWithCancellation = codec.encodedSizeWithCancellation;
pub const decodeAlloc = codec.decodeAlloc;
pub const decodeAllocWithLimits = codec.decodeAllocWithLimits;
pub const decodeAllocWithCancellation = codec.decodeAllocWithCancellation;
pub const decodeAllocWithLimitsAndCancellation = codec.decodeAllocWithLimitsAndCancellation;
pub const decodeHeader = codec.decodeHeader;
pub const decodeControl = codec.decodeControl;
pub const decodeRoutingIndexAlloc = codec.decodeRoutingIndexAlloc;
pub const decodeRoutingIndexWithCancellationAlloc = codec.decodeRoutingIndexWithCancellationAlloc;
pub const decodeRoutingIndexForVersionWithCancellationAlloc = codec.decodeRoutingIndexForVersionWithCancellationAlloc;
pub const artifactIntegrity = codec.artifactIntegrity;
pub const routingFooterLenFromTrailer = codec.routingFooterLenFromTrailer;
pub const decodeScoreBlockWithCancellation = codec.decodeScoreBlockWithCancellation;
pub const scoreFromBlockWithCancellation = codec.scoreFromBlockWithCancellation;
pub const wire_version = codec.wire_version;
pub const headerProbeLen = codec.headerProbeLen;
pub const controlProbeLen = codec.controlProbeLen;
pub const routing_trailer_len = codec.routing_trailer_len;
pub const score_block_entries = codec.score_block_entries;

test "serverless graph metric segment module compiles" {
    _ = types;
    _ = codec;
}
