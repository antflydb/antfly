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
const runtime = @import("data/runtime.zig");
const raft_batch = @import("data/raft_batch.zig");
const runtime_status = @import("antfly_local_sources").api_runtime_status;
const indexes = @import("api/indexes.zig");
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const private_provisioning = @import("data/private_provisioning.zig");

// The auth storage adapter deliberately receives storage through an injected
// module to avoid a production import cycle. Focused runtime tests expose the
// same narrow surface as root.zig.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = runtime;
    _ = raft_batch;
    _ = runtime_status;
    _ = indexes;
    _ = table_writes;
    _ = private_provisioning;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_control.zig");

// Match the distributed runtime's inline lake owner. This test root owns
// the coordinator's source implementation without adding physical storage
// implementations to the control catalog shared by native consumers.
pub const lake_execution_backend = @import("api/lake_execution_build_impl.zig");

pub const consumer_tests_only = true;

pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

test "data runtime commit requirements activate prepared effects before source admission" {
    const protocol = @import("common/data_raft_protocol.zig");
    const direct: @import("antfly_local_sources").storage_db_selected_root.db.types.BatchRequest = .{ .commit_requires = .direct_enrichments };
    try std.testing.expectEqual(protocol.batch_prepared_enrichments_protocol_version, runtime.DataServer.requiredRaftBatchProtocolVersion(direct));
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidBatchRequest, raft_batch.encode(a, "docs", direct));
    var prepared = direct;
    prepared.prepared_enrichments_json = "{}";
    const encoded = try raft_batch.encode(a, "docs", prepared);
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "prepared-enrichments-batch-v1") != null);
    try std.testing.expectEqual(protocol.batch_prepared_enrichments_protocol_version, runtime.DataServer.requiredRaftBatchProtocolVersion(prepared));
}
