// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Remote CLI contracts share the product CLI dependency boundary.
pub const antfly_sources = @import("source_owner_common.zig");
test {
    _ = @import("cmd/cli/mod.zig");
    _ = @import("cmd/cli/maintenance.zig");
}
