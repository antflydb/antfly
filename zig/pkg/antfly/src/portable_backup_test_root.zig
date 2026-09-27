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

const std = @import("std");
const portable_backup = @import("storage/portable_backup.zig");
const backup_bundle = @import("storage/backup_bundle.zig");
const backup_bundle_io = @import("storage/backup_bundle_io.zig");
const backup_repository = @import("storage/backup_repository.zig");

test {
    std.testing.refAllDecls(portable_backup);
    std.testing.refAllDecls(backup_bundle);
    std.testing.refAllDecls(backup_bundle_io);
    std.testing.refAllDecls(backup_repository);
}
