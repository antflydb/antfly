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

//! Deterministic storage-owner open contract carried by replicated apply.
//! This module intentionally contains no DB, catalog, or runtime imports.

pub const Identity = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,

    pub fn eql(left: Identity, right: Identity) bool {
        return left.table_id == right.table_id and
            left.shard_id == right.shard_id and
            left.range_id == right.range_id;
    }
};

pub const Descriptor = struct {
    lsm_root_generation: u64,
    identity: Identity,
    schema_json: []const u8 = "",
    indexes_json: []const u8 = "",
};
