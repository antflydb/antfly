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

const hbc_adapter = @import("hbc_adapter.zig");
const vectorindex_hbc = @import("antfly_vectorindex").hbc;

pub const HBCConfig = hbc_adapter.HBCConfig;
pub const StorageBackend = hbc_adapter.StorageBackend;
pub const BulkBuildAlgo = hbc_adapter.BulkBuildAlgo;
pub const HBCIndex = hbc_adapter.HBCIndex;
pub const BatchInsertItem = hbc_adapter.BatchInsertItem;
pub const PreparedBulkBuildInput = hbc_adapter.PreparedBulkBuildInput;
pub const ProfiledSearchResults = hbc_adapter.ProfiledSearchResults;
pub const VectorId = hbc_adapter.VectorId;
pub const PostingId = hbc_adapter.PostingId;
pub const PostingView = hbc_adapter.PostingView;
pub const PostingState = hbc_adapter.PostingState;
pub const PostingMaintenanceOptions = hbc_adapter.PostingMaintenanceOptions;
pub const PostingMaintenanceResult = hbc_adapter.PostingMaintenanceResult;
pub const PostingBacklogStats = hbc_adapter.PostingBacklogStats;
pub const PostingStore = hbc_adapter.PostingStore;
pub const AssignmentMap = hbc_adapter.AssignmentMap;
pub const CentroidDirectory = hbc_adapter.CentroidDirectory;

pub const meta_key = vectorindex_hbc.meta_key;
pub const hbc_index_version = vectorindex_hbc.hbc_index_version;
pub const IndexMetadata = vectorindex_hbc.IndexMetadata;
pub const Suffix = vectorindex_hbc.Suffix;
pub const NodeHeader = vectorindex_hbc.NodeHeader;
pub const encodeNodeKey = vectorindex_hbc.encodeNodeKey;
pub const encodeVecKey = vectorindex_hbc.encodeVecKey;
pub const encodeVecLeafKey = vectorindex_hbc.encodeVecLeafKey;
pub const encodeVecMetaKey = vectorindex_hbc.encodeVecMetaKey;
pub const encodeQuantKey = vectorindex_hbc.encodeQuantKey;
