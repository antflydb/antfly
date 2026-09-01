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

const std = @import("std");

pub const types = @import("types.zig");
pub const bulk_build = @import("bulk_build.zig");
pub const kmeans = @import("kmeans.zig");
pub const search_results = @import("search_results.zig");
pub const search_types = @import("search_types.zig");
pub const search = @import("search.zig");
pub const search_runtime = @import("search_runtime.zig");
pub const store = @import("store.zig");
pub const posting = @import("posting.zig");
pub const checked_region = @import("checked_region.zig");
pub const posting_segment = @import("posting_segment.zig");
pub const centroid_directory = @import("centroid_directory.zig");
pub const quantized_directory = @import("quantized_directory.zig");
pub const posting_wal = @import("posting_wal.zig");
pub const hbc_vector_directory = @import("hbc_vector_directory.zig");
pub const vector_block = @import("vector_block.zig");
pub const vector_block_wal = @import("vector_block_wal.zig");
pub const vector_block_manifest = @import("vector_block_manifest.zig");
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
pub const CandidateCoverage = search_results.CandidateCoverage;
pub const ApproxSearchResult = search_results.ApproxSearchResult;
pub const ApproxSearchResults = search_results.ApproxSearchResults;
pub const SearchRequest = search_types.SearchRequest;
pub const CancellationToken = search_types.CancellationToken;
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
pub const PostingSegmentWriter = posting_segment.Writer;
pub const PostingSegmentReader = posting_segment.Reader;
pub const VerifiedPostingSegmentReader = posting_segment.VerifiedReader;
pub const PostingSegmentEntryKind = posting_segment.EntryKind;
pub const PostingWalWriter = posting_wal.Writer;
pub const PostingWalReplay = posting_wal.Replay;
pub const PostingWalRecordKind = posting_wal.RecordKind;
pub const PostingCheckpoint = posting_wal.Checkpoint;
pub const VectorBlockWriter = vector_block.Writer;
pub const VectorBlockReader = vector_block.Reader;
pub const VectorBlockWalWriter = vector_block_wal.Writer;
pub const VectorBlockWalReplay = vector_block_wal.Replay;
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

test "posting segment stores point and ordered delta values" {
    try posting_segment.testStoresPointAndOrderedDeltaValues();
}

test "posting segment rejects duplicate logical entries" {
    try posting_segment.testRejectsDuplicateLogicalEntries();
}

test "posting segment validates footer and version" {
    try posting_segment.testValidatesFooterAndVersion();
}

test "posting segment validates index and payload checksums" {
    try posting_segment.testValidatesIndexAndPayloadChecksums();
}

test "posting segment verified reader memoizes payload status" {
    try posting_segment.testVerifiedReaderMemoizesPayloadStatus();
}

test "posting wal replays committed batches in order" {
    try posting_wal.testCommittedBatchesReplayInOrder();
}

test "posting wal ignores uncommitted and partial tails" {
    try posting_wal.testIgnoresUncommittedAndPartialTails();
}

test "posting wal rejects checksum and ordering errors" {
    try posting_wal.testRejectsChecksumAndOrderingErrors();
}

test "posting checkpoint round trips with checksum" {
    try posting_wal.testCheckpointRoundTripAndChecksum();
}

test "posting segment checkpoint and wal tail compose" {
    try posting_wal.testSegmentCheckpointAndWalTailCompose();
}

test "cache-rejected unaligned batch vector reads keep stable per-id views" {
    const TestTxn = struct {
        vector_1: [12]u8 = undefined,
        vector_2: [12]u8 = undefined,

        fn unalignedOffset(storage: *const [12]u8) usize {
            return if ((@intFromPtr(storage) & (@alignOf(f32) - 1)) == 0) 1 else 0;
        }

        fn writeUnalignedVector(storage: *[12]u8, vector: []const f32) void {
            const offset = unalignedOffset(storage);
            for (vector, 0..) |value, i| {
                std.mem.writeInt(u32, storage[offset + i * 4 ..][0..4], @bitCast(value), .little);
            }
        }

        fn view(storage: *const [12]u8) []const u8 {
            const offset = unalignedOffset(storage);
            const bytes = storage[offset..][0 .. 2 * @sizeOf(f32)];
            std.debug.assert((@intFromPtr(bytes.ptr) & (@alignOf(f32) - 1)) != 0);
            return bytes;
        }

        pub fn getManySorted(self: *@This(), _: anytype, keys: []const []const u8, values: []?[]const u8) !void {
            for (keys, 0..) |key, i| {
                const vector_id = std.mem.readInt(u64, key[2..10], .big);
                values[i] = switch (vector_id) {
                    1 => view(&self.vector_1),
                    2 => view(&self.vector_2),
                    else => null,
                };
            }
        }
    };
    const TestIndex = struct {
        pub fn cacheVector(_: @This(), _: u64, vector: []const f32) ![]const f32 {
            // Models a governed cache declining admission and returning the
            // caller-owned view unchanged.
            return vector;
        }

        pub fn getVectorInto(_: @This(), _: anytype, _: u64, _: []f32) ![]const f32 {
            return error.UnexpectedScalarFallback;
        }
    };

    const index = TestIndex{};
    var txn: TestTxn = .{};
    TestTxn.writeUnalignedVector(&txn.vector_1, &.{ 1, 10 });
    TestTxn.writeUnalignedVector(&txn.vector_2, &.{ 2, 20 });
    var vector_views: [2][]const f32 = undefined;
    var lookups: [2]search_runtime.RerankLookup = undefined;
    var key_views: [2][]const u8 = undefined;
    var values: [2]?[]const u8 = .{ null, null };
    var scratch: [2]f32 = undefined;
    var batch_scratch: [4]f32 = undefined;

    try hbc_index.loadVectorIdsSortedWithScratchForTest(
        index,
        &txn,
        &.{ 2, 1 },
        vector_views[0..],
        lookups[0..],
        key_views[0..],
        values[0..],
        scratch[0..],
        batch_scratch[0..],
    );

    try std.testing.expectEqualSlices(f32, &.{ 2, 20 }, vector_views[0]);
    try std.testing.expectEqualSlices(f32, &.{ 1, 10 }, vector_views[1]);
    try std.testing.expect(@intFromPtr(vector_views[0].ptr) != @intFromPtr(vector_views[1].ptr));
}

test "search effort normalization owns exhaustive coverage semantics" {
    try std.testing.expect(search_types.normalizedSearchEffort(.{ .query = &.{}, .k = 1 }) == null);
    try std.testing.expectEqual(@as(f32, 0), search_types.normalizedSearchEffort(.{ .query = &.{}, .k = 1, .search_effort = -1 }).?);
    try std.testing.expectEqual(@as(f32, 1), search_types.normalizedSearchEffort(.{ .query = &.{}, .k = 1, .search_effort = 2 }).?);
    try std.testing.expect(!search_types.requiresExhaustiveCoverage(.{ .query = &.{}, .k = 1, .search_effort = 0.999 }));
    try std.testing.expect(search_types.requiresExhaustiveCoverage(.{ .query = &.{}, .k = 1, .search_effort = 1 }));
    try std.testing.expectEqual(search_types.CoveragePolicy.best_effort, search_types.coveragePolicy(.{ .query = &.{}, .k = 1, .search_effort = 0.999 }));
    try std.testing.expectEqual(search_types.CoveragePolicy.complete_snapshot, search_types.coveragePolicy(.{ .query = &.{}, .k = 1, .search_effort = 1 }));
}

test "candidate ANN ordering is independent of resolved subtree bounds" {
    const nearby = PriorityItem{
        .id = 1,
        .distance = 1,
        .error_bound = 0.25,
        .lower_bound = 100,
        .bound_resolved = true,
    };
    const broad_far = PriorityItem{
        .id = 2,
        .distance = 4,
        .error_bound = 0,
        .lower_bound = 0,
        .bound_resolved = true,
    };
    try std.testing.expectEqual(std.math.Order.lt, candidateLessThan({}, nearby, broad_far));
}

test "flat probe ANN ordering is independent of covering-radius bounds" {
    const nearby = spfresh_index.FlatCentroidProbe{
        .posting_id = 1,
        .distance = 1,
        .error_bound = 0.25,
        .member_lower_bound = 100,
        .bound_resolved = true,
    };
    const broad_far = spfresh_index.FlatCentroidProbe{
        .posting_id = 2,
        .distance = 4,
        .error_bound = 0,
        .member_lower_bound = 0,
        .bound_resolved = true,
    };
    try std.testing.expect(spfresh_index.flatProbeLessForTesting(nearby, broad_far));
    try std.testing.expect(!spfresh_index.flatProbeLessForTesting(broad_far, nearby));
}
