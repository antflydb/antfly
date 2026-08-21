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

pub const types = @import("types.zig");
pub const codec = @import("codec.zig");
pub const builder = @import("builder.zig");
pub const reader = @import("reader.zig");

pub const SourceKind = types.SourceKind;
pub const AggregateOp = types.AggregateOp;
pub const AggregateValue = types.AggregateValue;
pub const SourceRef = types.SourceRef;
pub const GroupFold = types.GroupFold;
pub const GroupByAggregate = types.GroupByAggregate;
pub const ExpressionFold = types.ExpressionFold;
pub const ExpressionMaterialization = types.ExpressionMaterialization;
pub const Segment = types.Segment;
pub const BuildOptions = builder.BuildOptions;
pub const ExpressionSpec = builder.ExpressionSpec;
pub const ExpressionBuildOptions = builder.ExpressionBuildOptions;
pub const Reader = reader.Reader;
pub const ExpressionReader = reader.ExpressionReader;
pub const DecodeLimits = codec.DecodeLimits;
pub const freeSegment = types.freeSegment;
pub const freeExpressionMaterialization = types.freeExpressionMaterialization;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const decodeAllocWithLimits = codec.decodeAllocWithLimits;
pub const encodeExpressionAlloc = codec.encodeExpressionAlloc;
pub const decodeExpressionAlloc = codec.decodeExpressionAlloc;
pub const decodeExpressionAllocWithLimits = codec.decodeExpressionAllocWithLimits;
pub const buildGroupByAggregateAlloc = builder.buildGroupByAggregateAlloc;
pub const encodeGroupByAggregateAlloc = builder.encodeGroupByAggregateAlloc;
pub const buildExpressionFoldsAlloc = builder.buildExpressionFoldsAlloc;
pub const encodeExpressionFoldsAlloc = builder.encodeExpressionFoldsAlloc;

test "serverless algebraic segment module compiles" {
    _ = types;
    _ = codec;
    _ = builder;
    _ = reader;
    _ = Segment;
    _ = ExpressionMaterialization;
    _ = BuildOptions;
    _ = ExpressionSpec;
    _ = ExpressionBuildOptions;
    _ = Reader;
    _ = ExpressionReader;
    _ = DecodeLimits;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = decodeAllocWithLimits;
    _ = encodeExpressionAlloc;
    _ = decodeExpressionAlloc;
    _ = decodeExpressionAllocWithLimits;
    _ = buildGroupByAggregateAlloc;
    _ = encodeGroupByAggregateAlloc;
    _ = buildExpressionFoldsAlloc;
    _ = encodeExpressionFoldsAlloc;
}
