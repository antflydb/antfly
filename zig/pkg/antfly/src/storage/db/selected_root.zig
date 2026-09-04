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

//! Selects the physical DB root for storage owners and the contract-only
//! root for linked control consumers. Control-facing code should import this
//! facade instead of naming `mod.zig` directly.

const storage_source_options = @import("storage_source_options");

pub const db = if (storage_source_options.control_only)
    @import("control_root.zig")
else
    @import("mod.zig");
