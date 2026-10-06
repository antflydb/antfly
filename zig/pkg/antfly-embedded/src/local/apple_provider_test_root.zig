// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = @import("source_owner_storage.zig");

test {
    _ = @import("asset_producer_runtime.zig");
    _ = @import("generating/mod.zig");
    _ = @import("storage/db/enrichment/document_extraction.zig");
    _ = @import("storage/db/enrichment/enrichment_runtime.zig");
}
