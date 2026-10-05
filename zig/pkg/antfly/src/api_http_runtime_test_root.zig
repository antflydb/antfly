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

//! Focused discovery root for API HTTP runtime and linked-boundary contracts.
//! Keeping these tests out of the monolithic library root prevents transport
//! changes from forcing code generation for every storage and search test.

const http_server = @import("api/http_server.zig");
const httpx_handler = @import("api/httpx_handler.zig");
const indexes = @import("api/indexes.zig");
const kernel_abi = @import("api/kernel_abi.zig");
const kernel_bridge = @import("api/kernel_bridge.zig");
const kernel_exports = @import("api/kernel_exports.zig");
const openapi_contract = @import("api/openapi_contract.zig");
const runtime_http_abi = @import("antfly_runtime_abi").http_abi;
const runtime_http_bridge = @import("antfly_runtime_abi").http_bridge;
const table_contract = @import("api/table_contract.zig");
const table_read_source = @import("antfly_local_sources").api_table_read_source;

// Some API storage adapters deliberately resolve these declarations through
// the discovery root to avoid production import cycles.
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = @import("api/sql_connection_record.zig");
    _ = @import("api/sql_connections.zig");
    _ = @import("api/kernel_owner_source.zig");
    _ = @import("api/sql_pgwire.zig");
    _ = @import("api/table_catalog.zig");
    _ = @import("api/table_reads.zig");
    _ = @import("api/tables.zig");
    _ = @import("antfly_local_sources").storage_db_resolution_runtime;
    _ = @import("api/distributed_candidate_source.zig");
    _ = @import("api/distributed_entity_sink.zig");
    _ = @import("api/http_client.zig");
    _ = @import("api/distributed_join.zig");
    _ = @import("api/distributed_graph.zig");
    _ = @import("antfly_inference_query_embedding_cache");
    _ = @import("api/agent_tools.zig");
    _ = @import("antfly_local_sources").generating_mod;
    _ = @import("api/query_builder_agent.zig");
    _ = @import("api/retrieval_agent.zig");
    _ = http_server;
    _ = httpx_handler;
    _ = indexes;
    _ = kernel_abi;
    _ = kernel_bridge;
    _ = kernel_exports;
    _ = openapi_contract;
    _ = runtime_http_abi;
    _ = runtime_http_bridge;
    _ = table_contract;
    _ = table_read_source;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

test "system catalog routing and transport discovery" {
    _ = @import("api/table_reads.zig").consumer_tests;
    _ = @import("api/table_catalog.zig");
    _ = @import("api/distributed_candidate_source.zig");
    _ = @import("api/distributed_entity_sink.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("api/lake_sql_cursor.zig");
}

test {
    _ = @import("api/lake_sql_integration_test.zig");
}
