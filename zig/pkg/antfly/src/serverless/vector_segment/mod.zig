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

pub const Cluster = types.Cluster;
pub const Entry = types.Entry;
pub const Segment = types.Segment;
pub const freeSegment = types.freeSegment;
pub const encodeAlloc = codec.encodeAlloc;
pub const decodeAlloc = codec.decodeAlloc;
pub const Header = codec.Header;
pub const header_len = codec.header_len;
pub const clusterRecordLen = codec.clusterRecordLen;
pub const decodeHeader = codec.decodeHeader;
pub const decodeClusterTableAlloc = codec.decodeClusterTableAlloc;
pub const encodeExactEntriesAlloc = codec.encodeExactEntriesAlloc;
pub const decodeExactEntriesAlloc = codec.decodeExactEntriesAlloc;

test "serverless vector segment module compiles" {
    _ = types;
    _ = codec;
    _ = Cluster;
    _ = Entry;
    _ = Segment;
    _ = freeSegment;
    _ = Header;
    _ = header_len;
    _ = clusterRecordLen;
    _ = encodeAlloc;
    _ = decodeAlloc;
    _ = decodeHeader;
    _ = decodeClusterTableAlloc;
    _ = encodeExactEntriesAlloc;
    _ = decodeExactEntriesAlloc;
}
