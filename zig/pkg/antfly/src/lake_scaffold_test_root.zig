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
pub const row_fragment = @import("serverless/row_fragment/mod.zig");
pub const row_fragment_build = @import("serverless/build/row_fragments.zig");
pub const manifest_base_source = @import("serverless/manifest/base_source.zig");

test {
    _ = rowsource;
    _ = row_fragment;
    _ = row_fragment_build;
    _ = manifest_base_source;
}
