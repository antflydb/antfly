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

//! Compatibility exports for the shared Antfly ordered page engine.
const shared = @import("objectstore").ordered_tree;
pub const max_page_bytes = shared.max_page_bytes;
pub const target_page_bytes = shared.target_page_bytes;
pub const bulk_target_page_bytes = shared.bulk_target_page_bytes;
pub const max_key_bytes = shared.max_key_bytes;
pub const max_record_bytes = shared.max_record_bytes;
pub const max_height = shared.max_height;
pub const max_aggregate_bytes = shared.max_aggregate_bytes;
pub const max_pack_bytes = shared.max_pack_bytes;
pub const Pack = shared.Pack;
pub const Ref = shared.Ref;
pub const Store = shared.Store;
pub const ValuePrefix = shared.ValuePrefix;
pub const Write = shared.Write;
pub const max_write_batch = shared.max_write_batch;
pub const Cache = shared.Cache;
pub const Mutation = shared.Mutation;
pub const Entry = shared.Entry;
pub const DecodedEntry = shared.DecodedEntry;
pub const Page = shared.Page;
pub const rankOf = shared.rankOf;
pub const max_read_batch = shared.max_read_batch;
pub const max_lookup_batch = shared.max_lookup_batch;
pub const BatchEntry = shared.BatchEntry;
pub const readWorkspace = shared.readWorkspace;
pub const lookupMany = shared.lookupMany;
pub const seekMany = shared.seekMany;
pub const ranksOf = shared.ranksOf;
pub const readPage = shared.readPage;
pub const decodePage = shared.decodePage;
pub const joinDisjoint = shared.joinDisjoint;
pub const apply = shared.apply;
pub const applyPacked = shared.applyPacked;
pub const retainKnown = shared.retainKnown;
pub const buildSorted = shared.buildSorted;
pub const BuildOptions = shared.BuildOptions;
pub const buildSortedWithOptions = shared.buildSortedWithOptions;
pub const SortedBuilder = shared.SortedBuilder;
pub const mergeSorted = shared.mergeSorted;
pub const applySorted = shared.applySorted;
pub const applySortedWithOptions = shared.applySortedWithOptions;
pub const containsMany = shared.containsMany;
pub const BestFirst = shared.BestFirst;
pub const Equivalence = shared.Equivalence;
pub const equivalent = shared.equivalent;
pub const visitDifferences = shared.visitDifferences;
pub const Cursor = shared.Cursor;
pub const countRange = shared.countRange;
pub const Range = shared.Range;
pub const RangeCounts = shared.RangeCounts;
pub const countSortedRanges = shared.countSortedRanges;
pub const reduceRange = shared.reduceRange;
pub const visitRange = shared.visitRange;
pub const walkPage = shared.walkPage;
pub const walkPostOrder = shared.walkPostOrder;
pub const testing = shared.testing;

test {
    _ = shared;
}
