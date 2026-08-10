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

//! Import-facing table read callback contract.
//!
//! Keep implementation barrels and source implementations in `table_reads.zig`
//! and `table_reads/`. This facade gives runtime ABI code a stable import that
//! does not directly depend on the broad table-read implementation barrel.

const core = @import("table_reads/core.zig");

pub const LsmStorageStats = core.LsmStorageStats;
pub const ObservedDynamicFieldCapabilitySet = core.ObservedDynamicFieldCapabilitySet;
pub const TableReadSource = core.TableReadSource;
