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
pub const Segment = types.Segment;
pub const BuildOptions = builder.BuildOptions;
pub const Reader = reader.Reader;
pub const freeSegment = types.freeSegment;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const buildGroupByAggregateAlloc = builder.buildGroupByAggregateAlloc;
pub const encodeGroupByAggregateAlloc = builder.encodeGroupByAggregateAlloc;

test "serverless algebraic segment module compiles" {
    _ = types;
    _ = codec;
    _ = builder;
    _ = reader;
    _ = Segment;
    _ = BuildOptions;
    _ = Reader;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = buildGroupByAggregateAlloc;
    _ = encodeGroupByAggregateAlloc;
}
