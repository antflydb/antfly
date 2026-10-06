// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

const std = @import("std");
const restore_jobs = @import("api/restore_jobs.zig");
const restore_staging_driver = @import("api/restore_staging_driver.zig");

test {
    std.testing.refAllDecls(restore_jobs);
    std.testing.refAllDecls(restore_staging_driver);
    std.testing.refAllDecls(@import("api/relational_rewrite_driver.zig"));
    std.testing.refAllDecls(@import("api/restore_owner.zig"));
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
