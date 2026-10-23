// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
