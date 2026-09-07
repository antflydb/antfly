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

const query = @import("api/query.zig");
const distributed_graph = @import("api/distributed_graph.zig");
const openapi_contract = @import("api/openapi_contract.zig");
const indexes = @import("api/indexes.zig");
const public_table_http = @import("api/public_table_http.zig");
const graph_exec = @import("storage/db/query/graph_exec.zig");

// Storage adapters resolve these declarations through their discovery root.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = query;
    _ = distributed_graph;
    _ = openapi_contract;
    _ = indexes;
    _ = public_table_http;
    _ = graph_exec;
}
