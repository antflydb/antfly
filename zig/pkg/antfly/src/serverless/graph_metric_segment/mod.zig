// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const Score = types.Score;
pub const Segment = types.Segment;
pub const MaterializationState = types.MaterializationState;
pub const RejectionReason = types.RejectionReason;
pub const freeSegment = types.freeSegment;
pub const artifactNameAlloc = types.artifactNameAlloc;
pub const encodeAlloc = codec.encodeAlloc;
pub const encodedSize = codec.encodedSize;
pub const decodeAlloc = codec.decodeAlloc;
pub const decodeAllocWithLimits = codec.decodeAllocWithLimits;
pub const decodeHeader = codec.decodeHeader;
pub const wire_version = codec.wire_version;
pub const headerProbeLen = codec.headerProbeLen;

test "serverless graph metric segment module compiles" {
    _ = types;
    _ = codec;
}
