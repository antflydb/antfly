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

const docstore = @import("docstore.zig");
const shard = @import("shard.zig");

pub const platform_time = @import("../platform/time.zig");
pub const DocStore = docstore.DocStore;
pub const DocStoreOptions = docstore.DocStoreOptions;
pub const KVPair = docstore.KVPair;
pub const OwnedKVPair = docstore.OwnedKVPair;
pub const ByteRange = docstore.ByteRange;
pub const ShardManager = shard.ShardManager;
