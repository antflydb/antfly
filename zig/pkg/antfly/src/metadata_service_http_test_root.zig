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

const service = @import("metadata/service.zig");
const catalog_projection_reader = @import("metadata/catalog_projection_reader.zig");
const admin_read_operations = @import("metadata/admin_read_operations.zig");
const admin_mutation_operations = @import("metadata/admin_mutation_operations.zig");
const extension_operations = @import("metadata/extension_operations.zig");
const node_operations = @import("metadata/node_operations.zig");
const table_operations = @import("metadata/table_operations.zig");
const http_client = @import("metadata/http_client.zig");
const http_routes = @import("metadata/http_routes.zig");
const http_server = @import("metadata/http_server.zig");

test {
    _ = service;
    _ = catalog_projection_reader;
    _ = admin_read_operations;
    _ = admin_mutation_operations;
    _ = extension_operations;
    _ = node_operations;
    _ = table_operations;
    _ = http_client;
    _ = http_routes;
    _ = http_server;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

test "system catalog bounded transfer and outbox discovery" {
    _ = @import("metadata/snapshot_transfer.zig");
    _ = @import("metadata/store_report_update.zig");
}
