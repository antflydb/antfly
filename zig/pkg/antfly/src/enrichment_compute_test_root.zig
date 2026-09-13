// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

test {
    _ = @import("storage/enrichment_compute_test.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_common.zig");
