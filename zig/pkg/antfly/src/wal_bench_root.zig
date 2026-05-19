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

pub const wal = @import("storage/wal.zig");

pub const CommitStats = wal.CommitStats;
pub const CommitBackend = wal.CommitBackend;
pub const WalOptions = wal.WalOptions;
pub const WalEntry = wal.WalEntry;
pub const BatchAppendResult = wal.BatchAppendResult;
pub const WalStats = wal.WalStats;
pub const FullStats = wal.FullStats;
pub const WAL = wal.WAL;
pub const platform_time = @import("platform/time.zig");
