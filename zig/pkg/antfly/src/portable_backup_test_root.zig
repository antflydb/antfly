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
const portable_backup = @import("antfly_local_sources").storage_portable_backup;
const backup_bundle = @import("antfly_local_sources").storage_backup_bundle;
const backup_bundle_io = @import("antfly_local_sources").storage_backup_bundle_io;
const backup_repository = @import("storage/backup_repository.zig");

test {
    std.testing.refAllDecls(portable_backup);
    std.testing.refAllDecls(backup_bundle);
    std.testing.refAllDecls(backup_bundle_io);
    std.testing.refAllDecls(backup_repository);
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
