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

const metadata_api = @import("../api.zig");

pub const ForeignKeySchemaControllerStatus = metadata_api.ForeignKeySchemaControllerStatus;
pub const UniqueConstraintSchemaControllerStatus = metadata_api.UniqueConstraintSchemaControllerStatus;
pub const MetadataStatus = metadata_api.MetadataStatus;
pub const MetadataHead = metadata_api.MetadataHead;
pub const ReplicationSourceActionHint = metadata_api.ReplicationSourceActionHint;
pub const AdminSnapshot = metadata_api.AdminSnapshot;

pub const captureSnapshot = metadata_api.captureSnapshot;
pub const freeSnapshot = metadata_api.freeSnapshot;
pub const deriveReplicationSourceActionHints = metadata_api.deriveReplicationSourceActionHints;
pub const captureSplitObservations = metadata_api.captureSplitObservations;
pub const captureMergeObservations = metadata_api.captureMergeObservations;
