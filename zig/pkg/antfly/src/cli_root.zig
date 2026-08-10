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

//! Focused facade for the installed Antfly CLI. Keep this list limited to the
//! namespaces referenced by main.zig, serverless_main.zig, and cmd/ so the
//! compiler does not have to load the entire public library root.

pub const build_options = @import("build_options");

pub const admin = @import("admin/mod.zig");
pub const common = @import("common/mod.zig");
pub const data = @import("data/mod.zig");
pub const metadata = @import("metadata/mod.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const serverless = @import("serverless/mod.zig");

pub const ha = @import("storage/ha/mod.zig");
pub const db = @import("storage/db/mod.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const backup_codec = @import("storage/backup_codec.zig");
pub const portable_backup = @import("storage/portable_backup.zig");
pub const platform_time = @import("antfly_platform").time;

// usermgr/storage_imports.zig depends back on these through antfly_root.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
