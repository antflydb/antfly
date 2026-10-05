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

pub const std = @import("std");
pub const metadata_incarnation = @import("incarnation.zig");
pub const MetadataClusterIncarnation = metadata_incarnation.MetadataClusterIncarnation;
pub const CatalogMutationStamp = struct {
    metadata_group_id: u64,
    metadata_incarnation: MetadataClusterIncarnation,
    term: u64,
    index: u64,

    pub fn eql(lhs: CatalogMutationStamp, rhs: CatalogMutationStamp) bool {
        return lhs.metadata_group_id == rhs.metadata_group_id and
            std.mem.eql(u8, &lhs.metadata_incarnation, &rhs.metadata_incarnation) and
            lhs.term == rhs.term and lhs.index == rhs.index;
    }
};
