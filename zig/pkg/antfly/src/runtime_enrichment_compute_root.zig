// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
