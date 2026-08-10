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

//! Internal dependency surface for the production metadata runtime.

pub const common = @import("../common/mod.zig");
pub const extensions = @import("../extensions/mod.zig");
pub const lsm_backend = @import("../storage/lsm_backend/mod.zig");
pub const metadata = @import("domain.zig");
pub const metadata_api = @import("api.zig");
pub const metadata_server = @import("server.zig");
pub const metadata_service = @import("service.zig");
pub const metadata_table_workflow = @import("table_workflow.zig");
pub const public_api = @import("../api/runtime.zig");
pub const raft = @import("../raft/domain.zig");
pub const readers = @import("antfly_readers");
pub const storage_backend_erased = @import("../storage/backend_erased.zig");
pub const synthesizing = @import("antfly_synthesizing");
pub const transcribing = @import("antfly_transcribing");
pub const usermgr = @import("../usermgr/mod.zig");
