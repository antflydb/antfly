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
pub const row_fragment_build = @import("serverless/build/row_fragments.zig");
pub const lake_promotion = @import("serverless/build/lake_promotion.zig");
pub const algebraic_segment = @import("serverless/algebraic_segment/mod.zig");
pub const external_source = @import("serverless/external_source/mod.zig");
pub const sidecar_source_binding = @import("serverless/segment/source_binding.zig");
pub const manifest_base_source = @import("serverless/manifest/base_source.zig");

test {
    _ = rowsource;
    _ = local_rowsource;
    _ = external_rowsource;
    _ = row_fragment;
    _ = row_fragment_build;
    _ = lake_promotion;
    _ = algebraic_segment;
    _ = external_source;
    _ = sidecar_source_binding;
    _ = manifest_base_source;
}
