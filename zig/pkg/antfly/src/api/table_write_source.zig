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

//! Import-facing table write callback contract.
//!
//! Keep implementation barrels and source implementations in `table_writes.zig`
//! and `table_writes/`. This facade gives runtime ABI code a stable import that
//! does not directly depend on the broad table-write implementation barrel.

const core = @import("table_writes/core.zig");

pub const TableWriteSource = core.TableWriteSource;
