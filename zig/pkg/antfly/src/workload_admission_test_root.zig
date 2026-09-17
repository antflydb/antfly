// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = @import("common/request_admission.zig");
    _ = @import("common/workload_resources.zig");
    _ = @import("common/workload_scheduler.zig");
    _ = @import("common/workload_admission_vopr_test.zig");
    _ = @import("api/httpx_handler.zig");
    _ = @import("api/kernel_exports.zig");
    _ = @import("serverless/api/http_handler.zig");
}
