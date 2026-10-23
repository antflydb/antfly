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

const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
const native_snapshot = @import("raft/storage/native_snapshot.zig");
const file_snapshot_store = @import("raft/storage/file_snapshot_store.zig");
const snapshot_payload_store = @import("raft/storage/snapshot_payload_store.zig");
const snapshot_metadata = @import("raft/storage/snapshot_metadata.zig");
const replica_state = @import("raft/storage/replica_state.zig");
const wal_replica_state = @import("raft/storage/wal_replica_state.zig");

test "raft snapshot storage tests are reachable" {
    std.testing.refAllDecls(file_snapshot_store);
    std.testing.refAllDecls(native_snapshot);
    std.testing.refAllDecls(snapshot_payload_store);
    std.testing.refAllDecls(snapshot_metadata);
    std.testing.refAllDecls(replica_state);
    std.testing.refAllDecls(wal_replica_state);
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
