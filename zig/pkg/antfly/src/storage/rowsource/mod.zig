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

pub const types = @import("antfly_local_sources").storage_rowsource_types;
pub const local = @import("antfly_local_sources").storage_rowsource_local;
pub const external = @import("antfly_local_sources").storage_rowsource_external;

pub const SourceKind = types.SourceKind;
pub const NextBatchFn = types.NextBatchFn;
pub const DeinitFn = types.DeinitFn;
pub const Source = types.Source;
pub const SnapshotRef = types.SnapshotRef;
pub const ServerlessRowRef = types.ServerlessRowRef;
pub const ExternalRowRef = types.ExternalRowRef;
pub const RowRef = types.RowRef;
pub const ColumnKind = types.ColumnKind;
pub const ColumnValues = types.ColumnValues;
pub const NullBitmap = types.NullBitmap;
pub const ColumnVector = types.ColumnVector;
pub const ColumnBatch = types.ColumnBatch;
pub const LocalBatchSource = local.BatchSource;
pub const relationalStoreSource = local.relationalStoreSource;
pub const jsonMaterializedSource = local.jsonMaterializedSource;
pub const validateLocalBatch = local.validateLocalBatch;
pub const ExternalFormat = external.Format;
pub const ExternalBinding = external.Binding;
pub const ExternalFileRef = external.FileRef;
pub const ExternalBatchSource = external.BatchSource;
pub const makeExternalRowRef = external.makeRowRef;
pub const validateExternalBatch = external.validateExternalBatch;

test "rowsource module compiles" {
    _ = types;
    _ = local;
    _ = external;
    _ = Source;
    _ = SnapshotRef;
    _ = RowRef;
    _ = ColumnVector;
    _ = ColumnBatch;
    _ = LocalBatchSource;
    _ = ExternalBatchSource;
}
