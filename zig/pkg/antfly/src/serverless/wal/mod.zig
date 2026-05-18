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

pub const types = @import("types.zig");
pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const remote_store = @import("remote_store.zig");

pub const Record = types.Record;
pub const freeRecords = types.freeRecords;
pub const WalStore = store.WalStore;
pub const FsStore = fs_store.FsStore;
pub const RemoteStore = remote_store.RemoteStore;

test "serverless wal module compiles" {
    _ = types;
    _ = store;
    _ = fs_store;
    _ = remote_store;
    _ = Record;
    _ = freeRecords;
    _ = WalStore;
    _ = FsStore;
    _ = RemoteStore;
}
