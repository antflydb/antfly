// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Native-only qualification: no server metadata/compiler imports.
test {
    _ = @import("storage/db/relational_index_candidate.zig");
    _ = @import("storage/db/relational_index_catalog.zig");
}
