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

pub const types = @import("types.zig");
pub const store = @import("store.zig");
pub const fs_store = @import("fs_store.zig");
pub const remote_store = @import("remote_store.zig");

pub const Record = types.Record;
pub const freeRecords = types.freeRecords;
pub const WalStore = store.WalStore;
pub const FsStore = fs_store.FsStore;
pub const RemoteStore = remote_store.RemoteStore;
pub const IndexedObjectStore = @import("indexed_object_store.zig").IndexedObjectStore;
pub const collector = @import("collector.zig");
pub const ReadRange = types.ReadRange;

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
    _ = IndexedObjectStore;
    _ = collector;
}
