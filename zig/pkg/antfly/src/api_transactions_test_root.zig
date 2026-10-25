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

const distributed_txn = @import("api/distributed_txn.zig");
const distributed_entity_sink = @import("api/distributed_entity_sink.zig");
const internal_group_operations = @import("api/internal_group_operations.zig");
const restore_catalog = @import("api/restore_catalog.zig");
const transactions = @import("api/transactions.zig");

test {
    _ = distributed_txn;
    _ = @import("antfly_local_sources").api_relational_integrity;
    _ = @import("antfly_local_sources").api_relational_integrity_wire;
    _ = @import("api/relational_integrity_errors.zig");
    _ = @import("antfly_local_sources").api_relational_integrity_commit;
    _ = @import("api/relational_session_statement.zig");
    _ = @import("api/relational_witness_ddl.zig");
    _ = @import("api/relational_rewrite_admission.zig");
    _ = @import("api/online_merge_io.zig");
    _ = @import("api/relational_ttl.zig");
    _ = @import("api/relational_activation_worker.zig");
    _ = @import("api/relational_retirement_worker.zig");
    _ = @import("metadata/storage/raft_apply_store.zig");
    _ = @import("antfly_local_sources").api_batch;
    _ = distributed_entity_sink;
    _ = internal_group_operations;
    _ = restore_catalog;
    _ = transactions;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
