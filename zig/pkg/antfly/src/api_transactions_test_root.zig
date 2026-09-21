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

const distributed_txn = @import("api/distributed_txn.zig");
const distributed_entity_sink = @import("api/distributed_entity_sink.zig");
const internal_group_operations = @import("api/internal_group_operations.zig");
const restore_catalog = @import("api/restore_catalog.zig");
const transactions = @import("api/transactions.zig");

test {
    _ = distributed_txn;
    _ = @import("api/relational_integrity.zig");
    _ = @import("api/relational_integrity_wire.zig");
    _ = @import("api/relational_integrity_errors.zig");
    _ = @import("api/relational_integrity_commit.zig");
    _ = @import("api/relational_session_statement.zig");
    _ = @import("api/relational_witness_ddl.zig");
    _ = @import("api/relational_rewrite_admission.zig");
    _ = @import("api/online_merge_io.zig");
    _ = @import("api/relational_ttl.zig");
    _ = @import("api/relational_activation_worker.zig");
    _ = @import("api/relational_retirement_worker.zig");
    _ = @import("metadata/storage/raft_apply_store.zig");
    _ = @import("api/batch.zig");
    _ = distributed_entity_sink;
    _ = internal_group_operations;
    _ = restore_catalog;
    _ = transactions;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");
