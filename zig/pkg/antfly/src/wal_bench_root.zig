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

pub const wal = @import("antfly_local_sources").storage_wal;

pub const CommitStats = wal.CommitStats;
pub const CommitBackend = wal.CommitBackend;
pub const WalOptions = wal.WalOptions;
pub const WalEntry = wal.WalEntry;
pub const BatchAppendResult = wal.BatchAppendResult;
pub const WalStats = wal.WalStats;
pub const FullStats = wal.FullStats;
pub const WAL = wal.WAL;
pub const platform_time = @import("antfly_platform").time;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
