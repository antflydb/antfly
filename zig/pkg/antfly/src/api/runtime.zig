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

const linked_runtime_options = @import("linked_runtime_options");

pub const batch = @import("batch.zig");
pub const backups = @import("backups.zig");
pub const distributed_candidate_source = @import("distributed_candidate_source.zig");
pub const distributed_entity_sink = @import("distributed_entity_sink.zig");
pub const distributed_graph = @import("distributed_graph.zig");
pub const http_routes = @import("http_routes.zig");
pub const http_internal_group_write_routes = @import("http_internal_group_write_routes.zig");
pub const http_server = @import("http_server.zig");
pub const http_client = @import("http_client.zig");
pub const httpx_handler = @import("httpx_handler.zig");
pub const indexes = @import("indexes.zig");
pub const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
pub const kernel_bridge = @import("kernel_bridge.zig");
pub const provisioned_storage = @import("provisioned_storage.zig");
pub const public_runtime = @import("public_runtime.zig");
pub const table_catalog = @import("../metadata/catalog/routing.zig");
pub const table_contract = @import("table_contract.zig");
pub const table_reads = @import("table_reads.zig");
pub const table_writes = @import("table_writes.zig");
pub const transactions = @import("transactions.zig");

pub const ApiHttpServer = if (linked_runtime_options.enabled) kernel_bridge.ApiHttpServer else http_server.ApiHttpServer;
pub const ApiHttpClient = http_client.ApiHttpClient;
pub const BatchResult = batch.BatchResult;
pub const BoundTableReadSource = table_reads.BoundTableReadSource;
pub const BoundTableWriteSource = table_writes.BoundTableWriteSource;
pub const DistributedCandidateSource = distributed_candidate_source.DistributedCandidateSource;
pub const DistributedEntitySink = distributed_entity_sink.DistributedEntitySink;
pub const GroupVisibleRootGenerationSource = table_reads.GroupVisibleRootGenerationSource;
pub const HAReadGate = table_reads.HAReadGate;
pub const HostedProvisionedTableReadSource = table_reads.HostedProvisionedTableReadSource;
pub const HostedProvisionedTableWriteSource = table_writes.HostedProvisionedTableWriteSource;
pub const ProvisionedGroupStorage = provisioned_storage.ProvisionedGroupStorage;
pub const ProvisionedTableReadCache = table_reads.ProvisionedTableReadCache;
pub const ProvisionedTableReadSource = table_reads.ProvisionedTableReadSource;
pub const ProvisionedTableWriteCache = table_writes.ProvisionedTableWriteCache;
pub const ProvisionedTableWriteSource = table_writes.ProvisionedTableWriteSource;
pub const TableReadSource = table_reads.TableReadSource;
pub const TableWriteSource = table_writes.TableWriteSource;
pub const backend_current_root_generation = table_reads.backend_current_root_generation;
