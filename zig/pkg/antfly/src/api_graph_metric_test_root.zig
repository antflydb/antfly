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

//! Focused discovery root for graph-metric public contracts and distributed
//! fan-in. This keeps the fail-closed matrix independently runnable without
//! coupling it to the monolithic storage test artifact.

const query = @import("antfly_local_sources").api_query;
pub const antfly_sources = @import("source_owner_physical.zig");
const distributed_graph = @import("api/distributed_graph.zig");
const openapi_contract = @import("api/openapi_contract.zig");
const indexes = @import("api/indexes.zig");
const public_table_http = @import("api/public_table_http.zig");
const graph_exec = @import("antfly_local_sources").storage_db_query_graph_exec;
const metadata_status_codec = @import("metadata/storage/raft_apply_store.zig");

// Storage adapters resolve these declarations through their discovery root.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = query;
    _ = distributed_graph;
    _ = openapi_contract;
    _ = indexes;
    _ = public_table_http;
    _ = graph_exec;
    _ = metadata_status_codec;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
