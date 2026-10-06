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

// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Test-only server fixtures used by local implementation tests.
pub const api_agent_tools = @import("api/agent_tools.zig");
pub const api_distributed_graph = @import("api/distributed_graph.zig");
pub const api_relational_index_status = @import("api/relational_index_status.zig");
pub const api_relational_row_merge = @import("api/relational_row_merge.zig");
pub const api_table_router = @import("api/table_router.zig");
pub const api_tables = @import("api/tables.zig");
pub const common_http_std_http_executor = @import("common/http/std_http_executor.zig");
pub const common_secret_projection_test_support = @import("common/secret_projection_test_support.zig");
pub const data_storage_db_split_handoff = @import("data/storage/db_split_handoff.zig");
pub const metadata_table_manager = @import("metadata/table_manager.zig");
pub const raft_transport_http_common = @import("raft/transport/http_common.zig");
pub const raft_transport_http_driver = @import("raft/transport/http_driver.zig");
pub const raft_transport_http_server = @import("raft/transport/http_server.zig");
pub const raft_transport_http_snapshot = @import("raft/transport/http_snapshot.zig");
pub const raft_transport_routes = @import("raft/transport/routes.zig");
pub const raft_transport_std_http_listener = @import("raft/transport/std_http_listener.zig");
pub const storage_artifact_publication_dispatch = @import("storage/artifact_publication_dispatch.zig");
pub const storage_hot_standby_db_commit = @import("storage/hot_standby/db_commit.zig");
pub const storage_hot_standby_effects = @import("storage/hot_standby/effects.zig");
pub const storage_hot_standby_primary = @import("storage/hot_standby/primary.zig");
pub const storage_hot_standby_public_gate_state = @import("storage/hot_standby/public_gate_state.zig");
pub const storage_hot_standby_write_gate = @import("storage/hot_standby/write_gate.zig");
pub const storage_lite_restore_staging = @import("antfly_local_sources").storage_lite_restore_staging;
pub const storage_relational_read_set = @import("storage/relational_read_set.zig");
pub const storage_retained_read_registry = @import("storage/retained_read_registry.zig");
pub const storage_server_db_adapter = @import("storage/server_db_adapter.zig");
pub const storage_vector_migration_offline = @import("storage/vector_migration_offline.zig");
