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

//! File-oriented Lite commands share the engine, not the server facade.
pub const build_options = @import("build_options");
pub const common = struct {
    pub const fs_paths = @import("antfly_runtime_fs").fs_paths;
    pub const secret_record = @import("common/secret_record.zig");
};
pub const db = @import("storage/db/selected_root.zig").db;
pub const lite = @import("storage/lite/mod.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const public_api = struct {
    pub const batch = @import("api/batch.zig");
    pub const query = @import("api/query.zig");
    pub const backups = @import("api/local_backups.zig");
};
