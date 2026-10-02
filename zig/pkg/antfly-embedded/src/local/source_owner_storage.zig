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

pub const physical_db = @import("storage/db/db.zig");
pub const selected_db = @import("storage/db/mod.zig");
pub const local_query = @import("storage/local_query.zig");
pub const local_write = @import("storage/local_write.zig");
pub const table_reads = struct {};
pub const table_writes = struct {};
pub const lite_serve = struct {};
