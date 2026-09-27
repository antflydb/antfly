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

//! Qualification executable; worker controls are never linked into Antfly.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const main = @import("testing/maintenance_process.zig").main;

// The linked API kernel and this physical fixture share one activity counter.
comptime {
    @export(&@import("storage/db/enrichment/enrichment_types.zig").interactiveActivity, .{ .name = "antfly_storage_interactive_activity" });
}
