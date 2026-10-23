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

//! Internal dependency surface for the production metadata runtime.

pub const common = @import("../common/mod.zig");
pub const extensions = @import("../extensions/mod.zig");
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;
pub const metadata = @import("domain.zig");
pub const metadata_api = @import("api.zig");
pub const metadata_server = @import("server.zig");
pub const metadata_service = @import("service.zig");
pub const public_api = @import("../api/runtime.zig");
pub const raft = @import("../raft/mod.zig");
pub const readers = @import("antfly_readers");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const synthesizing = @import("antfly_synthesizing");
pub const transcribing = @import("antfly_transcribing");
pub const usermgr = @import("../usermgr/mod.zig");
