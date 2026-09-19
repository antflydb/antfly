// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Explicit bounded discovery for native backing evidence and its transport.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = @import("api/completion_attestation_protocol.zig");
    _ = @import("api/completion_attestation_client.zig");
    _ = @import("api/httpx_handler.zig");
    _ = @import("data/runtime.zig");
}
