// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
pub const consumer_tests_only = true;
test {
    _ = @import("storage/hot_standby/operation_admission.zig");
    _ = @import("storage/hot_standby/http_operation.zig");
}
