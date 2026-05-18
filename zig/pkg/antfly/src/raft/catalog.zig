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

const impl = @import("storage/catalog.zig");

pub const ReplicaBootstrapMode = impl.ReplicaBootstrapMode;
pub const BackupRestoreBootstrapRecord = impl.BackupRestoreBootstrapRecord;
pub const ReplicaBootstrapSource = impl.ReplicaBootstrapSource;
pub const SnapshotBootstrapRecord = impl.SnapshotBootstrapRecord;
pub const ReplicaRecord = impl.ReplicaRecord;
pub const ReplicaCatalog = impl.ReplicaCatalog;
pub const MemoryReplicaCatalog = impl.MemoryReplicaCatalog;
pub const FileReplicaCatalog = impl.FileReplicaCatalog;
pub const freeReplicaRecords = impl.freeReplicaRecords;
pub const freeRuntimeBootstrap = impl.freeRuntimeBootstrap;
pub const runtimeBootstrapFromRecord = impl.runtimeBootstrapFromRecord;
