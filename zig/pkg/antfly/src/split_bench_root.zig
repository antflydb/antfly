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

const root = @import("storage/split_bench_root.zig");

pub const DocStore = root.DocStore;
pub const DocStoreOptions = root.DocStoreOptions;
pub const KVPair = root.KVPair;
pub const OwnedKVPair = root.OwnedKVPair;
pub const ByteRange = root.ByteRange;
pub const ShardManager = root.ShardManager;
pub const platform_time = @import("antfly_platform").time;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
