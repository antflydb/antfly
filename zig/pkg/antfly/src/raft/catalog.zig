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

const impl = @import("storage/catalog.zig");

pub const ReplicaBootstrapMode = impl.ReplicaBootstrapMode;
pub const BackupRestoreBootstrapRecord = impl.BackupRestoreBootstrapRecord;
pub const ReplicaBootstrapSource = impl.ReplicaBootstrapSource;
pub const SnapshotBootstrapRecord = impl.SnapshotBootstrapRecord;
pub const ReplicaRecord = impl.ReplicaRecord;
pub const ReplicaCatalog = impl.ReplicaCatalog;
pub const ReplicaCatalogToken = impl.ReplicaCatalogToken;
pub const PreparedReplicaCatalogBatch = impl.PreparedReplicaCatalogBatch;
pub const ReplicaCatalogSnapshot = impl.ReplicaCatalogSnapshot;
pub const MemoryReplicaCatalog = impl.MemoryReplicaCatalog;
pub const FileReplicaCatalog = impl.FileReplicaCatalog;
pub const freeReplicaRecords = impl.freeReplicaRecords;
pub const eqlReplicaRecord = impl.eqlReplicaRecord;
pub const freeRuntimeBootstrap = impl.freeRuntimeBootstrap;
pub const runtimeBootstrapFromRecord = impl.runtimeBootstrapFromRecord;
