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
pub const local = @import("local.zig");
pub const external = @import("external.zig");

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
