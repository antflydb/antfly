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

pub const replication_record = @import("replication_record.zig");
pub const replication_log = @import("replication_log.zig");
pub const slot_store = @import("slot_store.zig");
pub const standby = @import("standby.zig");
pub const primary = @import("primary.zig");
pub const session = @import("session.zig");
pub const backup_manifest = @import("backup_manifest.zig");
pub const bootstrap = @import("bootstrap.zig");
pub const status = @import("status.zig");
pub const replication_api = @import("replication_api.zig");
pub const fencing = @import("fencing.zig");

test {
    _ = replication_record;
    _ = replication_log;
    _ = slot_store;
    _ = standby;
    _ = primary;
    _ = session;
    _ = backup_manifest;
    _ = bootstrap;
    _ = status;
    _ = replication_api;
    _ = fencing;
}
