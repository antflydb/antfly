// Copyright 2026 Antfly, Inc.
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

//! Selects the physical DB root for storage owners and the contract-only
//! root for linked control consumers. Control-facing code should import this
//! facade instead of naming `mod.zig` directly.

const storage_source_options = @import("storage_source_options");

pub const db = if (storage_source_options.control_only)
    @import("control_root.zig")
else
    @import("antfly_source_root").antfly_sources.selected_db;
