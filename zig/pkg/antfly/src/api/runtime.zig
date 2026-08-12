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

//! Production API surface shared by runtime entry points.
//!
//! Keep test harnesses and whole-module compile assertions in `mod.zig`; a
//! production runtime importing this facade must not pull `e2e.zig` or another
//! command runtime into its potential dependency graph.

pub const cluster = @import("cluster.zig");
pub const batch = @import("batch.zig");
pub const backups = @import("backups.zig");
pub const linear_merge = @import("linear_merge.zig");
pub const query = @import("query.zig");
pub const query_contract = @import("query_contract.zig");
pub const cluster_api_http = @import("cluster_api_http.zig");
pub const retrieval_agent = @import("retrieval_agent.zig");
pub const public_table_http = @import("public_table_http.zig");
pub const public_embedding_query = @import("public_embedding_query.zig");
pub const public_graph_query = @import("public_graph_query.zig");
pub const public_search_request = @import("public_search_request.zig");
pub const public_query_string = @import("public_query_string.zig");
pub const public_text_query = @import("public_text_query.zig");
pub const query_builder_agent = @import("query_builder_agent.zig");
pub const distributed_txn = @import("distributed_txn.zig");
pub const transactions = @import("transactions.zig");
pub const table_catalog = @import("table_catalog.zig");
pub const table_router = @import("table_router.zig");
pub const tables = @import("tables.zig");
pub const table_contract = @import("table_contract.zig");
pub const indexes = @import("indexes.zig");
pub const http_routes = @import("http_routes.zig");
pub const provisioned_storage = @import("provisioned_storage.zig");
pub const table_reads = @import("table_reads.zig");
pub const table_writes = @import("table_writes.zig");
pub const distributed_candidate_source = @import("distributed_candidate_source.zig");
pub const distributed_entity_sink = @import("distributed_entity_sink.zig");
pub const distributed_join = @import("distributed_join.zig");
pub const distributed_graph = @import("distributed_graph.zig");
pub const artifact_reprocess_jobs = @import("artifact_reprocess_jobs.zig");
pub const repair_jobs = @import("repair_jobs.zig");
pub const restore_jobs = @import("restore_jobs.zig");
pub const internal_group_operations = @import("internal_group_operations.zig");
pub const internal_query_operations = @import("internal_query_operations.zig");
pub const internal_transition_wire = @import("internal_transition_wire.zig");
pub const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
pub const http_server = @import("http_server.zig");
pub const kernel_bridge = @import("kernel_bridge.zig");
pub const kernel_abi = @import("kernel_abi.zig");
pub const http_client = @import("http_client.zig");
pub const httpx_handler = @import("httpx_handler.zig");
pub const connections = @import("connections.zig");

pub const ClusterHealth = cluster.ClusterHealth;
pub const ClusterStatus = cluster.ClusterStatus;
pub const clusterStatusFromMetadata = cluster.fromMetadataStatus;
pub const BatchResult = batch.BatchResult;
pub const QueryResponse = query.QueryResponse;
pub const TableReadSource = table_reads.TableReadSource;
pub const BoundTableReadSource = table_reads.BoundTableReadSource;
pub const ProvisionedGroupStorage = provisioned_storage.ProvisionedGroupStorage;
pub const ProvisionedTableReadCache = table_reads.ProvisionedTableReadCache;
pub const ProvisionedTableReadSource = table_reads.ProvisionedTableReadSource;
pub const GroupVisibleRootGenerationSource = table_reads.GroupVisibleRootGenerationSource;
pub const HAReadGate = table_reads.HAReadGate;
pub const backend_current_root_generation = table_reads.backend_current_root_generation;
pub const HostedProvisionedTableReadSource = table_reads.HostedProvisionedTableReadSource;
pub const DistributedCandidateSource = distributed_candidate_source.DistributedCandidateSource;
pub const DistributedEntitySink = distributed_entity_sink.DistributedEntitySink;
pub const TableWriteSource = table_writes.TableWriteSource;
pub const BoundTableWriteSource = table_writes.BoundTableWriteSource;
pub const ProvisionedTableWriteCache = table_writes.ProvisionedTableWriteCache;
pub const ProvisionedTableWriteSource = table_writes.ProvisionedTableWriteSource;
pub const HostedProvisionedTableWriteSource = table_writes.HostedProvisionedTableWriteSource;
pub const HostedGroupRouter = table_router.HostedGroupRouter;
pub const ApiHttpServer = kernel_bridge.ApiHttpServer;
pub const ApiHttpClient = http_client.ApiHttpClient;
