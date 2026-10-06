// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Native local document/media compute exports, independent of server runtime.
pub const antfly_sources = struct {
    pub const physical_db = struct {};
    pub const selected_db = @import("storage/db/control_root.zig");
};
const provider = @import("storage/enrichment_compute_provider.zig");
comptime {
    @export(&provider.extractStream, .{ .name = "antfly_enrichment_extract_stream", .visibility = .hidden });
    @export(&provider.renderPdfPagePng, .{ .name = "antfly_enrichment_render_pdf_page_png", .visibility = .hidden });
    @export(&provider.bufferDestroy, .{ .name = "antfly_enrichment_buffer_destroy", .visibility = .hidden });
}
