// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
const runtime_http_abi = @import("runtime_http_abi.zig");
const runtime_http_bridge = @import("runtime_http_bridge.zig");
const table_contract = @import("api/table_contract.zig");

// Some API storage adapters deliberately resolve these declarations through
// the discovery root to avoid production import cycles.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
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
}
