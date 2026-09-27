// Copyright 2026 Antfly, Inc.
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

//! Data-only identity contract shared by storage ownership and distributed
//! control. Physical ordinal and visibility state remains in `doc_identity.zig`.

pub const Namespace = struct {
    table_id: u64 = 0,
    shard_id: u64 = 0,
    range_id: u64 = 0,

    pub fn eql(self: Namespace, other: Namespace) bool {
        return self.table_id == other.table_id and
            self.shard_id == other.shard_id and
            self.range_id == other.range_id;
    }
};

pub const default_namespace = Namespace{};
