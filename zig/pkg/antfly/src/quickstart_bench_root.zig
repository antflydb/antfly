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

pub const inverted = @import("antfly_local_sources").section_inverted;
pub const scorer = @import("antfly_local_sources").search_scorer;
pub const analysis = @import("antfly_local_sources").search_analysis;
pub const roaring = @import("antfly_local_sources").encoding_roaring;
pub const platform_time = @import("antfly_platform").time;
pub const fst = @import("antfly_fst");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
