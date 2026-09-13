// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the enrichment_compute runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

const enrichment_compute_exports = @import("storage/enrichment_compute_provider.zig");

comptime {
    exportInternal(&enrichment_compute_exports.extractStream, "antfly_enrichment_extract_stream");
    exportInternal(&enrichment_compute_exports.renderPdfPagePng, "antfly_enrichment_render_pdf_page_png");
    exportInternal(&enrichment_compute_exports.bufferDestroy, "antfly_enrichment_buffer_destroy");
}
