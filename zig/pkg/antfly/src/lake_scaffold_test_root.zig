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

//! Focused compile/test root for the Antfly-owned lake-native scaffold.

pub const rowsource = @import("storage/rowsource/mod.zig");
pub const local_rowsource = @import("storage/rowsource/local.zig");
pub const external_rowsource = @import("storage/rowsource/external.zig");
pub const row_fragment = @import("serverless/row_fragment/mod.zig");
pub const row_fragment_stats = @import("serverless/row_fragment/stats.zig");
pub const row_fragment_build = @import("serverless/build/row_fragments.zig");
pub const row_fragment_manifest = @import("serverless/build/row_fragment_manifest.zig");
pub const row_fragment_publish = @import("serverless/build/row_fragment_publish.zig");
pub const external_source_manifest = @import("serverless/build/external_source_manifest.zig");
pub const algebraic_manifest = @import("serverless/build/algebraic_manifest.zig");
pub const algebraic_publish = @import("serverless/build/algebraic_publish.zig");
pub const lake_promotion = @import("serverless/build/lake_promotion.zig");
pub const algebraic_segment = @import("serverless/algebraic_segment/mod.zig");
pub const external_source = @import("serverless/external_source/mod.zig");
pub const lake_rows_query = @import("serverless/query/lake_rows.zig");
pub const lake_explain_query = @import("serverless/query/lake_explain.zig");
pub const sidecar_source_binding = @import("serverless/segment/source_binding.zig");
pub const manifest_artifact_ref = @import("serverless/manifest/artifact_ref.zig");
pub const manifest_base_source = @import("serverless/manifest/base_source.zig");
pub const manifest_compatibility = @import("serverless/manifest/compatibility.zig");

test {
    _ = rowsource;
    _ = local_rowsource;
    _ = external_rowsource;
    _ = row_fragment;
    _ = row_fragment_stats;
    _ = row_fragment_build;
    _ = row_fragment_manifest;
    _ = row_fragment_publish;
    _ = external_source_manifest;
    _ = algebraic_manifest;
    _ = algebraic_publish;
    _ = lake_promotion;
    _ = algebraic_segment;
    _ = external_source;
    _ = lake_rows_query;
    _ = lake_explain_query;
    _ = sidecar_source_binding;
    _ = manifest_artifact_ref;
    _ = manifest_base_source;
    _ = manifest_compatibility;
}
