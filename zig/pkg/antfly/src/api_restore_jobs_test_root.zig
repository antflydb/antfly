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

test {
    _ = @import("api/document_run_storage.zig");
    _ = @import("api/document_jobs.zig");
    _ = @import("antfly_readers").document_runs;
}
