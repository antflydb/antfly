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
