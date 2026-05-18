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

pub const types = @import("types.zig");
pub const bulk_build = @import("bulk_build.zig");
pub const kmeans = @import("kmeans.zig");
pub const search_results = @import("search_results.zig");
pub const search_types = @import("search_types.zig");
pub const search = @import("search.zig");
pub const search_runtime = @import("search_runtime.zig");
pub const store = @import("store.zig");
pub const posting = @import("posting.zig");
pub const hbc_runtime = @import("hbc_runtime.zig");
pub const hbc = @import("hbc.zig");
pub const hbc_index = @import("hbc_index.zig");
pub const spfresh_index = @import("spfresh_index.zig");
pub const hbc_transfer = @import("hbc_transfer.zig");
pub const hbc_debug = @import("hbc_debug.zig");

pub const HBCConfig = types.HBCConfig;
pub const CentroidDirectoryMode = types.HBCConfig.CentroidDirectoryMode;
pub const BulkBuildAlgo = types.BulkBuildAlgo;
pub const StorageBackend = types.StorageBackend;
pub const Node = types.Node;
pub const PriorityItem = types.PriorityItem;
pub const NodeSplitClass = types.NodeSplitClass;
pub const NodeSplitRange = types.NodeSplitRange;
pub const SplitPlanningStats = types.SplitPlanningStats;
pub const SplitReusePlan = types.SplitReusePlan;
pub const SplitRebuildWork = types.SplitRebuildWork;
pub const BulkBuildOptions = bulk_build.BulkBuildOptions;
pub const PreparedBulkBuildInput = bulk_build.PreparedBulkBuildInput;
pub const SearchResult = search_results.SearchResult;
pub const SearchResults = search_results.SearchResults;
pub const ApproxSearchResult = search_results.ApproxSearchResult;
pub const ApproxSearchResults = search_results.ApproxSearchResults;
pub const SearchRequest = search_types.SearchRequest;
pub const SearchProfile = search_types.SearchProfile;
pub const DebugHit = search_types.DebugHit;
pub const DebugPair = search_types.DebugPair;
pub const ProfiledSearchResults = search_types.ProfiledSearchResults;
pub const DebugLeafScore = search_types.DebugLeafScore;
pub const DebugNodeDistance = search_types.DebugNodeDistance;
pub const IndexStats = search_types.IndexStats;
pub const HBCDebugNode = search_types.HBCDebugNode;
pub const RequestFilterState = search_types.RequestFilterState;
pub const candidateLessThan = search_types.candidateLessThan;
pub const SearchScratch = search_runtime.SearchScratch;
pub const Namespace = store.Namespace;
pub const Entry = store.Entry;
pub const Cursor = store.Cursor;
pub const NamespaceReadTxn = store.NamespaceReadTxn;
pub const NamespaceWriteTxn = store.NamespaceWriteTxn;
pub const NamespaceBatch = store.NamespaceBatch;
pub const NamespaceStore = store.NamespaceStore;
pub const VectorId = posting.VectorId;
pub const PostingId = posting.PostingId;
pub const PostingView = posting.PostingView;
pub const PostingState = posting.PostingState;
pub const PostingMaintenanceOptions = posting.PostingMaintenanceOptions;
pub const PostingMaintenanceResult = posting.PostingMaintenanceResult;
pub const PostingBacklogStats = posting.PostingBacklogStats;
pub const PostingStore = posting.PostingStore;
pub const AssignmentMap = posting.AssignmentMap;
pub const CentroidDirectory = posting.CentroidDirectory;
pub const meta_key = hbc.meta_key;
pub const hbc_index_version = hbc.hbc_index_version;
pub const IndexMetadata = hbc.IndexMetadata;
pub const Suffix = hbc.Suffix;
pub const NodeHeader = hbc.NodeHeader;
pub const QuantizedSet = hbc_runtime.QuantizedSet;
pub const WriteProfile = hbc_runtime.WriteProfile;
pub const BatchInsertItem = hbc_runtime.BatchInsertItem;
pub const BatchInsertOptions = hbc_runtime.BatchInsertOptions;
pub const ScratchHandle = hbc_runtime.ScratchHandle;
pub const collectCompetitiveCandidatesAlloc = search.collectCompetitiveCandidatesAlloc;
pub const sortApproxResultsByVectorId = search.sortApproxResultsByVectorId;
pub const sortSearchResultsByDistance = search.sortSearchResultsByDistance;
pub const sortDebugLeafScores = search.sortDebugLeafScores;
pub const BeamSearchState = search.BeamSearchState;
pub const rerankFactor = search.rerankFactor;
pub const candidateCapacity = search.candidateCapacity;
pub const shouldStopBeamSearch = search.shouldStopBeamSearch;
pub const shouldBreakOnInternalCandidate = search.shouldBreakOnInternalCandidate;
pub const shouldSkipInternalCandidate = search.shouldSkipInternalCandidate;
pub const shouldSkipLeafCandidate = search.shouldSkipLeafCandidate;
pub const noteLeafExplored = search.noteLeafExplored;
pub const requestHasExtraFilters = search_runtime.requestHasExtraFilters;
pub const exactDistanceToStoredVector = search_runtime.exactDistanceToStoredVector;
pub const encodeNodeKey = hbc.encodeNodeKey;
pub const encodeVecKey = hbc.encodeVecKey;
pub const encodeVecLeafKey = hbc.encodeVecLeafKey;
pub const encodeVecMetaKey = hbc.encodeVecMetaKey;
pub const encodeQuantKey = hbc.encodeQuantKey;
