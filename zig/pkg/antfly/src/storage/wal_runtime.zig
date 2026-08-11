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

//! Source selector for runtime durability owners. Control-only compilation
//! sees the opaque client; the storage archive and ordinary tests retain the
//! concrete WAL implementation.

const storage_source_options = @import("storage_source_options");
const native = @import("wal.zig");
const client = @import("kernel_wal_client.zig");

pub const WAL = if (storage_source_options.control_only) client.WAL else native.WAL;
pub const WalOptions = native.WalOptions;
pub const WalEntry = if (storage_source_options.control_only) client.WalEntry else native.WalEntry;
pub const WalStats = if (storage_source_options.control_only) client.WalStats else native.WalStats;
pub const BatchAppendResult = native.BatchAppendResult;
pub const CommitBackend = native.CommitBackend;
pub const StorageBackend = native.StorageBackend;

pub fn appendAt(wal: *WAL, expected_lsn: u64, data: []const u8) !u64 {
    if (storage_source_options.control_only) return wal.appendAt(expected_lsn, data);
    const original_next_lsn = wal.next_lsn;
    wal.next_lsn = expected_lsn;
    errdefer wal.next_lsn = original_next_lsn;
    return wal.append(data);
}
