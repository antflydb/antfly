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

const impl = @import("../lsm_backend.zig");

pub const state = @import("state.zig");
pub const repository = @import("repository.zig");
pub const runtime = @import("runtime.zig");
pub const compaction = @import("compaction.zig");
pub const compaction_scheduler = @import("compaction_scheduler.zig");
pub const recovery = @import("recovery.zig");
pub const storage_io = @import("storage_io.zig");
pub const background = @import("background.zig");
pub const cache = @import("cache.zig");
pub const wal = @import("wal.zig");
pub const Options = impl.Options;
pub const Backend = impl.Backend;
pub const BackendHandle = impl.BackendHandle;
pub const BackgroundExecutor = background.Executor;
pub const IoRuntime = impl.IoRuntime;
pub const Storage = impl.Storage;
pub const HostStorage = storage_io.HostStorage;
pub const MemoryStorage = storage_io.MemoryStorage;
pub const NativeStorageStats = impl.NativeStorageStats;
pub const Cache = impl.Cache;
pub const DefaultCacheSizeBytes = impl.DefaultCacheSizeBytes;
pub const TableEntry = impl.TableEntry;
pub const MutableSnapshotReason = impl.MutableSnapshotReason;
pub const ReaderPinKind = impl.ReaderPinKind;
pub const reader_pin_kind_count = impl.reader_pin_kind_count;
pub const readerPinKindName = impl.readerPinKindName;
pub const mutableSnapshotReasonName = impl.mutableSnapshotReasonName;

test "lsm backend module tests are reachable" {
    const std = @import("std");
    std.testing.refAllDecls(impl);
    std.testing.refAllDecls(cache);
    std.testing.refAllDecls(storage_io);
    std.testing.refAllDecls(wal);
    std.testing.refAllDecls(background);
    std.testing.refAllDecls(compaction_scheduler);
    std.testing.refAllDecls(@import("../sim_runtime.zig"));
}
