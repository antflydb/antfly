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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const user_namespace: u8 = 0x01;
pub const replay_namespace: u8 = 0x02;
pub const identity_namespace: u8 = 0x03;
pub const relational_column_index_namespace: u8 = 0x04;
pub const relational_column_index_by_doc_namespace: u8 = 0x05;
pub const relational_foreign_key_ref_namespace: u8 = 0x06;
pub const relational_unique_namespace: u8 = 0x07;
pub const relational_foreign_key_conflict_namespace: u8 = 0x08;
pub const relational_array_element_index_namespace: u8 = 0x09;
pub const relational_json_value_index_namespace: u8 = 0x0a;
pub const relational_array_value_index_namespace: u8 = 0x0b;
pub const relational_json_path_index_namespace: u8 = 0x0c;
pub const relational_temporal_unique_namespace: u8 = 0x0d;
pub const relational_ordered_tuple_index_namespace: u8 = 0x0e;
pub const relational_ordered_tuple_index_by_doc_namespace: u8 = 0x0f;
pub const relational_ordered_tuple_unique_conflict_namespace: u8 = 0x14;
pub const replay_all_kind: u8 = 0xfe;

pub const primary_kind: u8 = 0x10;
pub const ttl_kind: u8 = 0x11;
pub const relational_row_kind: u8 = 0x12;
pub const relational_column_kind: u8 = 0x13;
pub const artifact_kind: u8 = 0x20;
pub const chunk_record_kind: u8 = 0x30;
pub const derived_embedding_kind: u8 = 0x31;
pub const graph_edge_record_kind: u8 = 0x32;
pub const asset_state_kind: u8 = 0x33;
pub const graph_asset_state_kind: u8 = 0x34;
pub const document_unit_record_kind: u8 = 0x35;

pub const replay_key_len: usize = 1 + 1 + @sizeOf(u64);
pub const replay_meta_init_key = [_]u8{ replay_namespace, 0xff, 0x01 };
pub const replay_meta_next_sequence_key = [_]u8{ replay_namespace, 0xff, 0x02 };
pub const replay_meta_latest_sequence_kind: u8 = 0x03;
pub const ha_applied_lsn_key = [_]u8{ replay_namespace, 0xff, 0x04 };
pub const artifact_presence_key = [_]u8{ replay_namespace, 0xff, 0x20 };
pub const asset_artifact_source_index_kind: u8 = 0x21;
pub const document_child_range_outbox_kind: u8 = 0x22;
pub const relational_cte_spill_kind: u8 = 0x40;
pub const relational_aggregate_spill_kind: u8 = 0x41;
pub const identity_doc_to_ordinal_kind: u8 = 0x01;
pub const identity_ordinal_to_doc_kind: u8 = 0x02;
pub const identity_ordinal_state_kind: u8 = 0x03;
pub const identity_canonical_to_ordinal_kind: u8 = 0x04;
pub const identity_namespace_key = [_]u8{ identity_namespace, 0xff, 0x00 };
pub const identity_next_ordinal_key = [_]u8{ identity_namespace, 0xff, 0x01 };
pub const identity_visibility_summary_key = [_]u8{ identity_namespace, 0xff, 0x02 };

pub fn isInternalMetadataKey(key: []const u8) bool {
    if (key.len == 0) return false;
    return key[0] == replay_namespace or key[0] == identity_namespace;
}

pub fn isInternalUserKey(key: []const u8) bool {
    return key.len > 0 and key[0] == user_namespace;
}

pub fn isInternalPhysicalTableDataKey(key: []const u8) bool {
    return isInternalUserKey(key) or isRelationalPhysicalTableDataKey(key);
}

pub fn isRelationalPhysicalTableDataKey(key: []const u8) bool {
    return isRelationalRowKey(key) or
        isRelationalColumnKey(key) or
        isRelationalColumnIndexKey(key) or
        isRelationalArrayElementIndexKey(key) or
        isRelationalArrayValueIndexKey(key) or
        isRelationalJsonValueIndexKey(key) or
        isRelationalJsonPathIndexKey(key) or
        isRelationalColumnIndexByDocKey(key) or
        isRelationalForeignKeyRefKey(key) or
        isRelationalUniqueKey(key) or
        isRelationalTemporalUniqueKey(key) or
        isRelationalOrderedTupleIndexKey(key) or
        isRelationalOrderedTupleIndexByDocKey(key) or
        isRelationalOrderedTupleUniqueConflictKey(key) or
        isRelationalForeignKeyConflictKey(key);
}

pub fn encodedBodyLen(bytes: []const u8) usize {
    var extra: usize = 0;
    for (bytes) |b| {
        if (b == 0) extra += 1;
    }
    return bytes.len + extra;
}

pub fn encodedComponentLen(bytes: []const u8) usize {
    return encodedBodyLen(bytes) + 2;
}

pub fn encodeBody(out: []u8, bytes: []const u8) usize {
    var pos: usize = 0;
    for (bytes) |b| {
        if (b == 0) {
            out[pos] = 0;
            out[pos + 1] = 0xff;
            pos += 2;
        } else {
            out[pos] = b;
            pos += 1;
        }
    }
    return pos;
}

pub fn encodeComponent(out: []u8, bytes: []const u8) usize {
    const pos = encodeBody(out, bytes);
    out[pos] = 0;
    out[pos + 1] = 0;
    return pos + 2;
}

pub fn appendEncodedComponent(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    const start = list.items.len;
    try list.resize(alloc, start + encodedComponentLen(bytes));
    _ = encodeComponent(list.items[start..], bytes);
}

pub fn findComponentTerminator(key: []const u8, start: usize) ?usize {
    var i = start;
    while (i + 1 < key.len) : (i += 1) {
        if (key[i] != 0) continue;
        if (key[i + 1] == 0) return i;
        if (key[i + 1] == 0xff) {
            i += 1;
            continue;
        }
        return null;
    }
    return null;
}

pub fn decodeBodyAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var out = try alloc.alloc(u8, maxDecodedLen(body));
    errdefer alloc.free(out);

    var in_pos: usize = 0;
    var out_pos: usize = 0;
    while (in_pos < body.len) {
        const b = body[in_pos];
        if (b != 0) {
            out[out_pos] = b;
            in_pos += 1;
            out_pos += 1;
            continue;
        }

        if (in_pos + 1 >= body.len or body[in_pos + 1] != 0xff) return error.InvalidInternalUserKey;
        out[out_pos] = 0;
        in_pos += 2;
        out_pos += 1;
    }

    return try alloc.realloc(out, out_pos);
}

pub fn decodeBodyView(body: []const u8) !?[]const u8 {
    var i: usize = 0;
    while (i < body.len) : (i += 1) {
        if (body[i] != 0) continue;
        if (i + 1 >= body.len or body[i + 1] != 0xff) return error.InvalidInternalUserKey;
        return null;
    }
    return body;
}

fn maxDecodedLen(body: []const u8) usize {
    return body.len;
}

pub fn appendDocumentPrefix(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, doc_key: []const u8) !void {
    try list.append(alloc, user_namespace);
    try appendEncodedComponent(list, alloc, doc_key);
}

pub fn appendDocumentRangeLower(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, prefix: []const u8) !void {
    try list.append(alloc, user_namespace);
    const start = list.items.len;
    try list.resize(alloc, start + encodedBodyLen(prefix));
    _ = encodeBody(list.items[start..], prefix);
}

pub fn documentKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var key = try alloc.alloc(u8, 1 + encodedComponentLen(doc_key) + 1);
    key[0] = user_namespace;
    const pos = 1 + encodeComponent(key[1..], doc_key);
    key[pos] = primary_kind;
    return key;
}

pub fn ttlKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, ttl_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalCteSpillPrefixAlloc(alloc: Allocator, spill_id: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, replay_namespace);
    try list.append(alloc, relational_cte_spill_kind);
    try appendEncodedComponent(&list, alloc, spill_id);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalCteSpillRowKeyAlloc(alloc: Allocator, spill_id: []const u8, row_index: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, replay_namespace);
    try list.append(alloc, relational_cte_spill_kind);
    try appendEncodedComponent(&list, alloc, spill_id);
    const start = list.items.len;
    try list.resize(alloc, start + @sizeOf(u64));
    std.mem.writeInt(u64, list.items[start..][0..@sizeOf(u64)], row_index, .big);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalAggregateSpillPrefixAlloc(alloc: Allocator, spill_id: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, replay_namespace);
    try list.append(alloc, relational_aggregate_spill_kind);
    try appendEncodedComponent(&list, alloc, spill_id);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalAggregateDistinctSpillKeyAlloc(alloc: Allocator, spill_id: []const u8, distinct_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, replay_namespace);
    try list.append(alloc, relational_aggregate_spill_kind);
    try appendEncodedComponent(&list, alloc, spill_id);
    try appendEncodedComponent(&list, alloc, distinct_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalRowKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, relational_row_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnKeyAlloc(alloc: Allocator, doc_key: []const u8, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, relational_column_kind);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnIndexKeyAlloc(alloc: Allocator, column_path: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_column_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnIndexPrefixAlloc(alloc: Allocator, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_column_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayElementIndexKeyAlloc(alloc: Allocator, column_path: []const u8, element_key: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_element_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, element_key);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayElementIndexPrefixAlloc(alloc: Allocator, column_path: []const u8, element_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_element_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, element_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayElementIndexColumnPrefixAlloc(alloc: Allocator, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_element_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayValueIndexKeyAlloc(alloc: Allocator, column_path: []const u8, array_key: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, array_key);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayValueIndexPrefixAlloc(alloc: Allocator, column_path: []const u8, array_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, array_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalArrayValueIndexColumnPrefixAlloc(alloc: Allocator, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_array_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonValueIndexKeyAlloc(alloc: Allocator, column_path: []const u8, json_path: []const u8, value_key: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, json_path);
    try appendEncodedComponent(&list, alloc, value_key);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonValueIndexPrefixAlloc(alloc: Allocator, column_path: []const u8, json_path: []const u8, value_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, json_path);
    try appendEncodedComponent(&list, alloc, value_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonValueIndexColumnPrefixAlloc(alloc: Allocator, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_value_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonPathIndexKeyAlloc(alloc: Allocator, column_path: []const u8, json_path: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_path_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, json_path);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonPathIndexPrefixAlloc(alloc: Allocator, column_path: []const u8, json_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_path_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    try appendEncodedComponent(&list, alloc, json_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalJsonPathIndexColumnPrefixAlloc(alloc: Allocator, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_json_path_index_namespace);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnIndexByDocKeyAlloc(alloc: Allocator, doc_key: []const u8, column_path: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_column_index_by_doc_namespace);
    try appendEncodedComponent(&list, alloc, doc_key);
    try appendEncodedComponent(&list, alloc, column_path);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnIndexByDocRangeLowerAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_column_index_by_doc_namespace);
    const start = list.items.len;
    try list.resize(alloc, start + encodedBodyLen(doc_key));
    _ = encodeBody(list.items[start..], doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalColumnIndexByDocRangeUpperAlloc(alloc: Allocator, doc_key: []const u8) !?[]u8 {
    if (doc_key.len == 0) {
        return try alloc.dupe(u8, &[_]u8{relational_column_index_by_doc_namespace + 1});
    }
    const lower = try relationalColumnIndexByDocRangeLowerAlloc(alloc, doc_key);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn relationalOrderedTupleIndexKeyAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple: []const u8,
    doc_key: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_namespace);
    try appendEncodedComponent(&list, alloc, index_id);
    try appendEncodedComponent(&list, alloc, encoded_tuple);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexPrefixAlloc(alloc: Allocator, index_id: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_namespace);
    try appendEncodedComponent(&list, alloc, index_id);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexTuplePrefixAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_namespace);
    try appendEncodedComponent(&list, alloc, index_id);
    try appendEncodedComponent(&list, alloc, encoded_tuple);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexTupleRangeLowerAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple_prefix: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_namespace);
    try appendEncodedComponent(&list, alloc, index_id);
    const start = list.items.len;
    try list.resize(alloc, start + encodedBodyLen(encoded_tuple_prefix));
    _ = encodeBody(list.items[start..], encoded_tuple_prefix);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexTupleRangeUpperAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple_prefix: []const u8,
) !?[]u8 {
    if (encoded_tuple_prefix.len == 0) {
        const lower = try relationalOrderedTupleIndexPrefixAlloc(alloc, index_id);
        errdefer alloc.free(lower);
        const upper = try nextPrefixAlloc(alloc, lower);
        alloc.free(lower);
        return upper;
    }
    const lower = try relationalOrderedTupleIndexTupleRangeLowerAlloc(alloc, index_id, encoded_tuple_prefix);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn relationalOrderedTupleIndexTuplePrefixUpperAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple: []const u8,
) !?[]u8 {
    const lower = try relationalOrderedTupleIndexTuplePrefixAlloc(alloc, index_id, encoded_tuple);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn relationalOrderedTupleIndexByDocKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    index_id: []const u8,
    encoded_tuple: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_by_doc_namespace);
    try appendEncodedComponent(&list, alloc, doc_key);
    try appendEncodedComponent(&list, alloc, index_id);
    try appendEncodedComponent(&list, alloc, encoded_tuple);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleUniqueConflictKeyAlloc(
    alloc: Allocator,
    index_id: []const u8,
    encoded_tuple: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_unique_conflict_namespace);
    try appendEncodedComponent(&list, alloc, index_id);
    try appendEncodedComponent(&list, alloc, encoded_tuple);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_ordered_tuple_index_by_doc_namespace);
    const start = list.items.len;
    try list.resize(alloc, start + encodedBodyLen(doc_key));
    _ = encodeBody(list.items[start..], doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalOrderedTupleIndexByDocRangeUpperAlloc(alloc: Allocator, doc_key: []const u8) !?[]u8 {
    if (doc_key.len == 0) {
        return try alloc.dupe(u8, &[_]u8{relational_ordered_tuple_index_by_doc_namespace + 1});
    }
    const lower = try relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, doc_key);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn relationalForeignKeyRefKeyAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    child_table: []const u8,
    child_key: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendRelationalForeignKeyParentPrefix(&list, alloc, constraint_name, parent_table, parent_key);
    try appendEncodedComponent(&list, alloc, child_table);
    try appendEncodedComponent(&list, alloc, child_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalForeignKeyRefParentPrefixAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendRelationalForeignKeyParentPrefix(&list, alloc, constraint_name, parent_table, parent_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalForeignKeyRefParentPrefixUpperAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
) !?[]u8 {
    const lower = try relationalForeignKeyRefParentPrefixAlloc(alloc, constraint_name, parent_table, parent_key);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn relationalUniqueKeyAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_unique_namespace);
    try appendEncodedComponent(&list, alloc, constraint_name);
    try appendEncodedComponent(&list, alloc, encoded_value);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalTemporalUniqueKeyAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
    encoded_start: []const u8,
    encoded_end: []const u8,
    doc_key: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendRelationalTemporalUniquePrefix(&list, alloc, constraint_name, encoded_value);
    try appendEncodedComponent(&list, alloc, encoded_start);
    try appendEncodedComponent(&list, alloc, encoded_end);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalTemporalUniquePrefixAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendRelationalTemporalUniquePrefix(&list, alloc, constraint_name, encoded_value);
    return try list.toOwnedSlice(alloc);
}

pub fn relationalTemporalUniquePrefixUpperAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
) !?[]u8 {
    const lower = try relationalTemporalUniquePrefixAlloc(alloc, constraint_name, encoded_value);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

fn appendRelationalTemporalUniquePrefix(
    list: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    constraint_name: []const u8,
    encoded_value: []const u8,
) !void {
    try list.append(alloc, relational_temporal_unique_namespace);
    try appendEncodedComponent(list, alloc, constraint_name);
    try appendEncodedComponent(list, alloc, encoded_value);
}

pub fn relationalForeignKeyConflictKeyAlloc(
    alloc: Allocator,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.append(alloc, relational_foreign_key_conflict_namespace);
    try appendEncodedComponent(&list, alloc, constraint_name);
    try appendEncodedComponent(&list, alloc, parent_table);
    try appendEncodedComponent(&list, alloc, parent_key);
    return try list.toOwnedSlice(alloc);
}

fn appendRelationalForeignKeyParentPrefix(
    list: *std.ArrayListUnmanaged(u8),
    alloc: Allocator,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
) !void {
    try list.append(alloc, relational_foreign_key_ref_namespace);
    try appendEncodedComponent(list, alloc, constraint_name);
    try appendEncodedComponent(list, alloc, parent_table);
    try appendEncodedComponent(list, alloc, parent_key);
}

pub fn documentExactPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn documentRangeLowerAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentRangeLower(&list, alloc, prefix);
    return try list.toOwnedSlice(alloc);
}

pub fn documentRangeUpperAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
    const lower = try documentRangeLowerAlloc(alloc, prefix);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn artifactRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn assetStateRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, asset_state_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn graphAssetStateRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, graph_asset_state_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactTypePrefixAlloc(alloc: Allocator, doc_key: []const u8, artifact_type: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, artifact_type);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactNamedPrefixAlloc(alloc: Allocator, doc_key: []const u8, artifact_type: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, artifact_type);
    try appendEncodedComponent(&list, alloc, artifact_name);

    return try list.toOwnedSlice(alloc);
}

pub fn assetArtifactSourceIndexRootPrefixAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn assetArtifactSourceIndexPrefixAlloc(alloc: Allocator, source_artifact: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    try appendEncodedComponent(&list, alloc, source_artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn assetArtifactSourceIndexKeyAlloc(alloc: Allocator, source_artifact: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    try appendEncodedComponent(&list, alloc, source_artifact);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn documentChildRangeOutboxRootPrefixAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, document_child_range_outbox_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn documentChildRangeOutboxKeyAlloc(alloc: Allocator, sequence: u64, ordinal: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, document_child_range_outbox_kind });
    const sequence_be = std.mem.nativeToBig(u64, sequence);
    try list.appendSlice(alloc, std.mem.asBytes(&sequence_be));
    const ordinal_be = std.mem.nativeToBig(u32, ordinal);
    try list.appendSlice(alloc, std.mem.asBytes(&ordinal_be));
    return try list.toOwnedSlice(alloc);
}

pub fn chunkArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, chunk_id: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "chunk");
    try appendEncodedComponent(&list, alloc, artifact_name);

    try list.append(alloc, chunk_record_kind);
    const be = std.mem.nativeToBig(u32, chunk_id);
    try list.appendSlice(alloc, std.mem.asBytes(&be));

    return try list.toOwnedSlice(alloc);
}

pub fn documentUnitChunkArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, unit_id: []const u8, chunk_id: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "chunk");
    try appendEncodedComponent(&list, alloc, artifact_name);
    try list.append(alloc, document_unit_record_kind);
    try appendEncodedComponent(&list, alloc, unit_id);

    const be = std.mem.nativeToBig(u32, chunk_id);
    try list.append(alloc, chunk_record_kind);
    try list.appendSlice(alloc, std.mem.asBytes(&be));

    return try list.toOwnedSlice(alloc);
}

pub fn documentUnitArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, unit_id: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "asset");
    try appendEncodedComponent(&list, alloc, artifact_name);
    try list.append(alloc, document_unit_record_kind);
    try appendEncodedComponent(&list, alloc, unit_id);

    return try list.toOwnedSlice(alloc);
}

pub fn embeddingArtifactKeyForDocumentAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "embedding", artifact_name);
}

/// Resolution artifacts record the entity-resolution decisions for a source
/// document. They are stored like asset artifacts but under the "resolution"
/// artifact type so they stay distinct from extractor-produced asset artifacts:
/// [0x01][doc][0x00 0x00][0x20]["resolution"][0x00 0x00][name][0x00 0x00]
pub fn resolutionArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "resolution", artifact_name);
}

pub fn derivedEmbeddingArtifactKeyAlloc(alloc: Allocator, base_internal_key: []const u8, artifact_name: []const u8) ![]u8 {
    if (!isInternalUserKey(base_internal_key)) return error.InvalidInternalUserKey;

    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, base_internal_key);
    try list.append(alloc, derived_embedding_kind);
    const start = list.items.len;
    try list.resize(alloc, start + encodedComponentLen(artifact_name));
    _ = encodeComponent(list.items[start..], artifact_name);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedEmbeddingArtifactPrefixAlloc(alloc: Allocator, base_internal_key: []const u8, artifact_name: []const u8) ![]u8 {
    return derivedEmbeddingArtifactKeyAlloc(alloc, base_internal_key, artifact_name);
}

pub fn graphArtifactIndexPrefixAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "graph", index_name);
}

pub fn graphEdgeArtifactPrefixAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    index_name: []const u8,
    edge_type: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "graph");
    try appendEncodedComponent(&list, alloc, index_name);
    try list.append(alloc, graph_edge_record_kind);
    if (edge_type.len > 0) try appendEncodedComponent(&list, alloc, edge_type);

    return try list.toOwnedSlice(alloc);
}

pub fn graphEdgeArtifactKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    index_name: []const u8,
    edge_type: []const u8,
    target_doc_key: []const u8,
) ![]u8 {
    const total_len = 1 +
        encodedComponentLen(doc_key) +
        1 +
        encodedComponentLen("graph") +
        encodedComponentLen(index_name) +
        1 +
        encodedComponentLen(edge_type) +
        encodedComponentLen(target_doc_key);
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    var pos: usize = 0;
    out[pos] = user_namespace;
    pos += 1;
    pos += encodeComponent(out[pos..], doc_key);
    out[pos] = artifact_kind;
    pos += 1;
    pos += encodeComponent(out[pos..], "graph");
    pos += encodeComponent(out[pos..], index_name);
    out[pos] = graph_edge_record_kind;
    pos += 1;
    pos += encodeComponent(out[pos..], edge_type);
    pos += encodeComponent(out[pos..], target_doc_key);
    std.debug.assert(pos == out.len);
    return out;
}

pub fn derivedEmbeddingBaseKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isDerivedEmbeddingArtifactKey(key)) return null;

    const doc_term = findComponentTerminator(key, 1).?;
    var pos = doc_term + 2;
    if (key[pos] == artifact_kind) {
        pos += 1;

        const type_term = findComponentTerminator(key, pos).?;
        pos = type_term + 2;

        const name_term = findComponentTerminator(key, pos).?;
        pos = name_term + 2;

        pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return error.InvalidInternalUserKey;
    }

    if (key[pos] != derived_embedding_kind) return error.InvalidInternalUserKey;
    return try alloc.dupe(u8, key[0..pos]);
}

pub fn isPrimaryDocumentKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const term = findComponentTerminator(key, 1) orelse return false;
    return term + 3 == key.len and key[term + 2] == primary_kind;
}

pub fn isTtlKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const term = findComponentTerminator(key, 1) orelse return false;
    return term + 3 == key.len and key[term + 2] == ttl_kind;
}

pub fn isRelationalRowKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const term = findComponentTerminator(key, 1) orelse return false;
    return term + 3 == key.len and key[term + 2] == relational_row_kind;
}

pub fn isRelationalColumnKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != relational_column_kind) return false;
    pos += 1;
    const column_term = findComponentTerminator(key, pos) orelse return false;
    return column_term + 2 == key.len;
}

pub fn isRelationalColumnIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_column_index_namespace) return false;
    const column_term = findComponentTerminator(key, 1) orelse return false;
    const doc_start = column_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalArrayElementIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_array_element_index_namespace) return false;
    const column_term = findComponentTerminator(key, 1) orelse return false;
    const element_start = column_term + 2;
    const element_term = findComponentTerminator(key, element_start) orelse return false;
    const doc_start = element_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalArrayValueIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_array_value_index_namespace) return false;
    const column_term = findComponentTerminator(key, 1) orelse return false;
    const value_start = column_term + 2;
    const value_term = findComponentTerminator(key, value_start) orelse return false;
    const doc_start = value_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalJsonValueIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_json_value_index_namespace) return false;
    const column_term = findComponentTerminator(key, 1) orelse return false;
    const path_start = column_term + 2;
    const path_term = findComponentTerminator(key, path_start) orelse return false;
    const value_start = path_term + 2;
    const value_term = findComponentTerminator(key, value_start) orelse return false;
    const doc_start = value_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalJsonPathIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_json_path_index_namespace) return false;
    const column_term = findComponentTerminator(key, 1) orelse return false;
    const path_start = column_term + 2;
    const path_term = findComponentTerminator(key, path_start) orelse return false;
    const doc_start = path_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalColumnIndexByDocKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_column_index_by_doc_namespace) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    const column_start = doc_term + 2;
    const column_term = findComponentTerminator(key, column_start) orelse return false;
    return column_term + 2 == key.len;
}

pub fn isRelationalOrderedTupleIndexKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_ordered_tuple_index_namespace) return false;
    const index_term = findComponentTerminator(key, 1) orelse return false;
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start) orelse return false;
    const doc_start = tuple_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalOrderedTupleIndexByDocKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_ordered_tuple_index_by_doc_namespace) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    const index_start = doc_term + 2;
    const index_term = findComponentTerminator(key, index_start) orelse return false;
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start) orelse return false;
    return tuple_term + 2 == key.len;
}

pub fn isRelationalOrderedTupleUniqueConflictKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_ordered_tuple_unique_conflict_namespace) return false;
    const index_term = findComponentTerminator(key, 1) orelse return false;
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start) orelse return false;
    return tuple_term + 2 == key.len;
}

pub fn isRelationalForeignKeyRefKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_foreign_key_ref_namespace) return false;
    const constraint_term = findComponentTerminator(key, 1) orelse return false;
    const parent_table_start = constraint_term + 2;
    const parent_table_term = findComponentTerminator(key, parent_table_start) orelse return false;
    const parent_key_start = parent_table_term + 2;
    const parent_key_term = findComponentTerminator(key, parent_key_start) orelse return false;
    const child_table_start = parent_key_term + 2;
    const child_table_term = findComponentTerminator(key, child_table_start) orelse return false;
    const child_key_start = child_table_term + 2;
    const child_key_term = findComponentTerminator(key, child_key_start) orelse return false;
    return child_key_term + 2 == key.len;
}

pub fn isRelationalUniqueKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_unique_namespace) return false;
    const constraint_term = findComponentTerminator(key, 1) orelse return false;
    const value_start = constraint_term + 2;
    const value_term = findComponentTerminator(key, value_start) orelse return false;
    return value_term + 2 == key.len;
}

pub fn isRelationalTemporalUniqueKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_temporal_unique_namespace) return false;
    const constraint_term = findComponentTerminator(key, 1) orelse return false;
    const value_start = constraint_term + 2;
    const value_term = findComponentTerminator(key, value_start) orelse return false;
    const start_start = value_term + 2;
    const start_term = findComponentTerminator(key, start_start) orelse return false;
    const end_start = start_term + 2;
    const end_term = findComponentTerminator(key, end_start) orelse return false;
    const doc_start = end_term + 2;
    const doc_term = findComponentTerminator(key, doc_start) orelse return false;
    return doc_term + 2 == key.len;
}

pub fn isRelationalForeignKeyConflictKey(key: []const u8) bool {
    if (key.len == 0 or key[0] != relational_foreign_key_conflict_namespace) return false;
    const constraint_term = findComponentTerminator(key, 1) orelse return false;
    const parent_table_start = constraint_term + 2;
    const parent_table_term = findComponentTerminator(key, parent_table_start) orelse return false;
    const parent_key_start = parent_table_term + 2;
    const parent_key_term = findComponentTerminator(key, parent_key_start) orelse return false;
    return parent_key_term + 2 == key.len;
}

pub fn isStoredDocumentRowKey(key: []const u8) bool {
    return isPrimaryDocumentKey(key) or isRelationalRowKey(key);
}

pub fn decodePrimaryDocumentKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isPrimaryDocumentKey(key)) return null;
    const term = findComponentTerminator(key, 1).?;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub fn decodeRelationalRowKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isRelationalRowKey(key)) return null;
    const term = findComponentTerminator(key, 1).?;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub const RelationalColumnKey = struct {
    doc_key: []u8,
    column_path: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.column_path);
        self.* = undefined;
    }
};

pub const RelationalColumnIndexKey = struct {
    column_path: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column_path);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalArrayElementIndexKey = struct {
    column_path: []u8,
    element_key: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column_path);
        alloc.free(self.element_key);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalArrayValueIndexKey = struct {
    column_path: []u8,
    array_key: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column_path);
        alloc.free(self.array_key);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalJsonValueIndexKey = struct {
    column_path: []u8,
    json_path: []u8,
    value_key: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column_path);
        alloc.free(self.json_path);
        alloc.free(self.value_key);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalJsonPathIndexKey = struct {
    column_path: []u8,
    json_path: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.column_path);
        alloc.free(self.json_path);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalColumnIndexByDocKey = struct {
    doc_key: []u8,
    column_path: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.column_path);
        self.* = undefined;
    }
};

pub const RelationalOrderedTupleIndexKey = struct {
    index_id: []u8,
    encoded_tuple: []u8,
    doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_id);
        alloc.free(self.encoded_tuple);
        alloc.free(self.doc_key);
        self.* = undefined;
    }
};

pub const RelationalOrderedTupleIndexByDocKey = struct {
    doc_key: []u8,
    index_id: []u8,
    encoded_tuple: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.index_id);
        alloc.free(self.encoded_tuple);
        self.* = undefined;
    }
};

pub const RelationalForeignKeyRefKey = struct {
    constraint_name: []u8,
    parent_table: []u8,
    parent_key: []u8,
    child_table: []u8,
    child_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.constraint_name);
        alloc.free(self.parent_table);
        alloc.free(self.parent_key);
        alloc.free(self.child_table);
        alloc.free(self.child_key);
        self.* = undefined;
    }
};

pub fn decodeRelationalColumnKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalColumnKey {
    if (!isRelationalColumnKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);
    const column_start = doc_term + 3;
    const column_term = findComponentTerminator(key, column_start).?;
    const column_path = try decodeBodyAlloc(alloc, key[column_start..column_term]);
    return .{
        .doc_key = doc_key,
        .column_path = column_path,
    };
}

pub fn decodeRelationalColumnIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalColumnIndexKey {
    if (!isRelationalColumnIndexKey(key)) return null;
    const column_term = findComponentTerminator(key, 1).?;
    const column_path = try decodeBodyAlloc(alloc, key[1..column_term]);
    errdefer alloc.free(column_path);
    const doc_start = column_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .column_path = column_path,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalArrayElementIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalArrayElementIndexKey {
    if (!isRelationalArrayElementIndexKey(key)) return null;
    const column_term = findComponentTerminator(key, 1).?;
    const column_path = try decodeBodyAlloc(alloc, key[1..column_term]);
    errdefer alloc.free(column_path);
    const element_start = column_term + 2;
    const element_term = findComponentTerminator(key, element_start).?;
    const element_key = try decodeBodyAlloc(alloc, key[element_start..element_term]);
    errdefer alloc.free(element_key);
    const doc_start = element_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .column_path = column_path,
        .element_key = element_key,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalArrayValueIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalArrayValueIndexKey {
    if (!isRelationalArrayValueIndexKey(key)) return null;
    const column_term = findComponentTerminator(key, 1).?;
    const column_path = try decodeBodyAlloc(alloc, key[1..column_term]);
    errdefer alloc.free(column_path);
    const value_start = column_term + 2;
    const value_term = findComponentTerminator(key, value_start).?;
    const array_key = try decodeBodyAlloc(alloc, key[value_start..value_term]);
    errdefer alloc.free(array_key);
    const doc_start = value_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .column_path = column_path,
        .array_key = array_key,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalJsonValueIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalJsonValueIndexKey {
    if (!isRelationalJsonValueIndexKey(key)) return null;
    const column_term = findComponentTerminator(key, 1).?;
    const column_path = try decodeBodyAlloc(alloc, key[1..column_term]);
    errdefer alloc.free(column_path);
    const path_start = column_term + 2;
    const path_term = findComponentTerminator(key, path_start).?;
    const json_path = try decodeBodyAlloc(alloc, key[path_start..path_term]);
    errdefer alloc.free(json_path);
    const value_start = path_term + 2;
    const value_term = findComponentTerminator(key, value_start).?;
    const value_key = try decodeBodyAlloc(alloc, key[value_start..value_term]);
    errdefer alloc.free(value_key);
    const doc_start = value_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .column_path = column_path,
        .json_path = json_path,
        .value_key = value_key,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalJsonPathIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalJsonPathIndexKey {
    if (!isRelationalJsonPathIndexKey(key)) return null;
    const column_term = findComponentTerminator(key, 1).?;
    const column_path = try decodeBodyAlloc(alloc, key[1..column_term]);
    errdefer alloc.free(column_path);
    const path_start = column_term + 2;
    const path_term = findComponentTerminator(key, path_start).?;
    const json_path = try decodeBodyAlloc(alloc, key[path_start..path_term]);
    errdefer alloc.free(json_path);
    const doc_start = path_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .column_path = column_path,
        .json_path = json_path,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalColumnIndexByDocKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalColumnIndexByDocKey {
    if (!isRelationalColumnIndexByDocKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);
    const column_start = doc_term + 2;
    const column_term = findComponentTerminator(key, column_start).?;
    const column_path = try decodeBodyAlloc(alloc, key[column_start..column_term]);
    return .{
        .doc_key = doc_key,
        .column_path = column_path,
    };
}

pub fn decodeRelationalOrderedTupleIndexKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalOrderedTupleIndexKey {
    if (!isRelationalOrderedTupleIndexKey(key)) return null;
    const index_term = findComponentTerminator(key, 1).?;
    const index_id = try decodeBodyAlloc(alloc, key[1..index_term]);
    errdefer alloc.free(index_id);
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start).?;
    const encoded_tuple = try decodeBodyAlloc(alloc, key[tuple_start..tuple_term]);
    errdefer alloc.free(encoded_tuple);
    const doc_start = tuple_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    const doc_key = try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
    return .{
        .index_id = index_id,
        .encoded_tuple = encoded_tuple,
        .doc_key = doc_key,
    };
}

pub fn decodeRelationalOrderedTupleIndexDocKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isRelationalOrderedTupleIndexKey(key)) return null;
    const index_term = findComponentTerminator(key, 1).?;
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start).?;
    const doc_start = tuple_term + 2;
    const doc_term = findComponentTerminator(key, doc_start).?;
    return try decodeBodyAlloc(alloc, key[doc_start..doc_term]);
}

pub fn decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalOrderedTupleIndexByDocKey {
    if (!isRelationalOrderedTupleIndexByDocKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);
    const index_start = doc_term + 2;
    const index_term = findComponentTerminator(key, index_start).?;
    const index_id = try decodeBodyAlloc(alloc, key[index_start..index_term]);
    errdefer alloc.free(index_id);
    const tuple_start = index_term + 2;
    const tuple_term = findComponentTerminator(key, tuple_start).?;
    const encoded_tuple = try decodeBodyAlloc(alloc, key[tuple_start..tuple_term]);
    return .{
        .doc_key = doc_key,
        .index_id = index_id,
        .encoded_tuple = encoded_tuple,
    };
}

pub fn decodeRelationalForeignKeyRefKeyAlloc(alloc: Allocator, key: []const u8) !?RelationalForeignKeyRefKey {
    if (!isRelationalForeignKeyRefKey(key)) return null;
    const constraint_term = findComponentTerminator(key, 1).?;
    const constraint_name = try decodeBodyAlloc(alloc, key[1..constraint_term]);
    errdefer alloc.free(constraint_name);
    const parent_table_start = constraint_term + 2;
    const parent_table_term = findComponentTerminator(key, parent_table_start).?;
    const parent_table = try decodeBodyAlloc(alloc, key[parent_table_start..parent_table_term]);
    errdefer alloc.free(parent_table);
    const parent_key_start = parent_table_term + 2;
    const parent_key_term = findComponentTerminator(key, parent_key_start).?;
    const parent_key = try decodeBodyAlloc(alloc, key[parent_key_start..parent_key_term]);
    errdefer alloc.free(parent_key);
    const child_table_start = parent_key_term + 2;
    const child_table_term = findComponentTerminator(key, child_table_start).?;
    const child_table = try decodeBodyAlloc(alloc, key[child_table_start..child_table_term]);
    errdefer alloc.free(child_table);
    const child_key_start = child_table_term + 2;
    const child_key_term = findComponentTerminator(key, child_key_start).?;
    const child_key = try decodeBodyAlloc(alloc, key[child_key_start..child_key_term]);
    return .{
        .constraint_name = constraint_name,
        .parent_table = parent_table,
        .parent_key = parent_key,
        .child_table = child_table,
        .child_key = child_key,
    };
}

pub fn decodeStoredDocumentRowKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isStoredDocumentRowKey(key)) return null;
    const term = findComponentTerminator(key, 1).?;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub fn decodeDocumentComponentAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isInternalUserKey(key)) return null;
    const term = findComponentTerminator(key, 1) orelse return null;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub fn isChunkArtifactRecordKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "chunk")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    const name_term = findComponentTerminator(key, pos) orelse return false;
    pos = name_term + 2;

    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        pos = unit_term + 2;
    }

    return pos + 5 == key.len and key[pos] == chunk_record_kind;
}

pub fn matchesChunkArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "chunk")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        pos = unit_term + 2;
    }

    return pos + 5 == key.len and key[pos] == chunk_record_kind;
}

pub fn matchesEmbeddingArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "embedding")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    return findComponentTerminator(key, pos).? + 2 == key.len;
}

pub fn isDerivedEmbeddingArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;

    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len) return false;

    switch (key[pos]) {
        primary_kind, ttl_kind => return false,
        artifact_kind => {
            pos += 1;

            const type_term = findComponentTerminator(key, pos) orelse return false;
            pos = type_term + 2;

            const name_term = findComponentTerminator(key, pos) orelse return false;
            pos = name_term + 2;

            if (pos == key.len) return false;
            pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return false;
        },
        else => return false,
    }

    if (pos >= key.len or key[pos] != derived_embedding_kind) return false;
    pos += 1;

    const embedding_term = findComponentTerminator(key, pos) orelse return false;
    return embedding_term + 2 == key.len;
}

pub fn matchesDerivedEmbeddingArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;

    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len) return false;

    switch (key[pos]) {
        primary_kind, ttl_kind => return false,
        artifact_kind => {
            pos += 1;

            const type_term = findComponentTerminator(key, pos) orelse return false;
            pos = type_term + 2;

            const name_term = findComponentTerminator(key, pos) orelse return false;
            pos = name_term + 2;

            if (pos == key.len) return false;
            pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return false;
        },
        else => return false,
    }

    if (pos >= key.len or key[pos] != derived_embedding_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, artifact_name)) return false;
    return findComponentTerminator(key, pos).? + 2 == key.len;
}

fn skipDerivedEmbeddingBaseRecordSuffix(key: []const u8, pos: usize) ?usize {
    var cursor = pos;
    if (cursor < key.len and key[cursor] == document_unit_record_kind) {
        cursor += 1;
        const unit_term = findComponentTerminator(key, cursor) orelse return null;
        cursor = unit_term + 2;
    }
    if (cursor < key.len and key[cursor] == chunk_record_kind) {
        cursor += 1 + @sizeOf(u32);
    }
    if (cursor > key.len) return null;
    return cursor;
}

pub fn isGraphEdgeArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "graph")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    const index_term = findComponentTerminator(key, pos) orelse return false;
    pos = index_term + 2;

    if (pos >= key.len or key[pos] != graph_edge_record_kind) return false;
    pos += 1;

    const edge_type_term = findComponentTerminator(key, pos) orelse return false;
    pos = edge_type_term + 2;

    const target_term = findComponentTerminator(key, pos) orelse return false;
    return target_term + 2 == key.len;
}

pub fn componentEquals(key: []const u8, start: usize, raw: []const u8) bool {
    const term = findComponentTerminator(key, start) orelse return false;
    var in_pos = start;
    var raw_pos: usize = 0;
    while (in_pos < term) {
        if (raw_pos >= raw.len) return false;
        const b = key[in_pos];
        if (b != 0) {
            if (raw[raw_pos] != b) return false;
            in_pos += 1;
            raw_pos += 1;
            continue;
        }
        if (in_pos + 1 >= term or key[in_pos + 1] != 0xff) return false;
        if (raw[raw_pos] != 0) return false;
        in_pos += 2;
        raw_pos += 1;
    }
    return raw_pos == raw.len;
}

/// Returns true if key is an embedding artifact: [0x01][doc][0x00 0x00][0x20]["embedding"][0x00 0x00][name][0x00 0x00]
pub fn isEmbeddingArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    // Check artifact type is "embedding"
    if (!componentEquals(key, pos, "embedding")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    // Must have exactly one more component (the artifact name)
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

pub fn isAssetArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "asset")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

pub fn matchesAssetArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "asset")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (pos == key.len) return true;
    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        return unit_term + 2 == key.len;
    }
    return false;
}

/// Returns true if key is a summary artifact: [0x01][doc][0x00 0x00][0x20]["summary"][0x00 0x00][name][0x00 0x00]
pub fn isSummaryArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "summary")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

/// Returns true if key is a resolution artifact: [0x01][doc][0x00 0x00][0x20]["resolution"][0x00 0x00][name][0x00 0x00]
pub fn isResolutionArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "resolution")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

/// Parse a resolution artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not a resolution artifact key.
pub fn parseResolutionArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isResolutionArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

/// Parse an asset artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not an asset artifact key.
pub fn parseAssetArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isAssetArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

/// Parse an embedding artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not an embedding artifact key.
pub fn parseEmbeddingArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isEmbeddingArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    // Skip [0x00 0x00][artifact_kind][encoded("embedding")][0x00 0x00]
    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    // Decode artifact name
    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

pub fn parseEmbeddingArtifactKeyView(key: []const u8) !?struct { doc_key: []const u8, artifact_name: []const u8 } {
    if (!isInternalUserKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1) orelse return null;
    const doc_key = (try decodeBodyView(key[1..doc_term])) orelse return null;

    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return null;
    pos += 1;

    if (!componentEquals(key, pos, "embedding")) return null;
    const type_term = findComponentTerminator(key, pos) orelse return null;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos) orelse return null;
    if (name_term + 2 != key.len) return null;
    const artifact_name = (try decodeBodyView(key[pos..name_term])) orelse return null;

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

pub fn parseGraphEdgeArtifactKeyAlloc(
    alloc: Allocator,
    key: []const u8,
) !?struct { doc_key: []u8, index_name: []u8, edge_type: []u8, target_doc_key: []u8 } {
    if (!isGraphEdgeArtifactKey(key)) return null;

    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1;
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const index_term = findComponentTerminator(key, pos).?;
    const index_name = try decodeBodyAlloc(alloc, key[pos..index_term]);
    errdefer alloc.free(index_name);
    pos = index_term + 2;

    if (key[pos] != graph_edge_record_kind) return error.InvalidInternalUserKey;
    pos += 1;

    const edge_type_term = findComponentTerminator(key, pos).?;
    const edge_type = try decodeBodyAlloc(alloc, key[pos..edge_type_term]);
    errdefer alloc.free(edge_type);
    pos = edge_type_term + 2;

    const target_term = findComponentTerminator(key, pos).?;
    const target_doc_key = try decodeBodyAlloc(alloc, key[pos..target_term]);

    return .{
        .doc_key = doc_key,
        .index_name = index_name,
        .edge_type = edge_type,
        .target_doc_key = target_doc_key,
    };
}

pub fn nextPrefixAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
    var out = try alloc.dupe(u8, prefix);
    errdefer alloc.free(out);

    var i = out.len;
    while (i > 0) {
        i -= 1;
        if (out[i] == 0xff) continue;
        out[i] += 1;
        return try alloc.realloc(out, i + 1);
    }

    alloc.free(out);
    return null;
}

pub fn replayEntryKey(hint_ordinal: u8, sequence: u64) [replay_key_len]u8 {
    var key: [replay_key_len]u8 = undefined;
    key[0] = replay_namespace;
    key[1] = hint_ordinal;
    std.mem.writeInt(u64, key[2..], sequence, .big);
    return key;
}

pub fn replayRangeLower(hint_ordinal: u8, from_sequence: u64) [replay_key_len]u8 {
    return replayEntryKey(hint_ordinal, from_sequence);
}

pub fn replayRangeUpper(hint_ordinal: u8) [2]u8 {
    return .{ replay_namespace, hint_ordinal + 1 };
}

pub fn replayLatestSequenceKey(hint_ordinal: u8) [4]u8 {
    return .{ replay_namespace, 0xff, replay_meta_latest_sequence_kind, hint_ordinal };
}

pub fn identityDocToOrdinalKeyAlloc(alloc: Allocator, doc_id: []const u8) ![]u8 {
    var key = try alloc.alloc(u8, 2 + encodedComponentLen(doc_id));
    key[0] = identity_namespace;
    key[1] = identity_doc_to_ordinal_kind;
    _ = encodeComponent(key[2..], doc_id);
    return key;
}

pub fn identityOrdinalToDocKey(ordinal: u32) [1 + 1 + @sizeOf(u32)]u8 {
    var key: [1 + 1 + @sizeOf(u32)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_ordinal_to_doc_kind;
    std.mem.writeInt(u32, key[2..][0..4], ordinal, .big);
    return key;
}

pub fn identityOrdinalStateKey(ordinal: u32) [1 + 1 + @sizeOf(u32)]u8 {
    var key: [1 + 1 + @sizeOf(u32)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_ordinal_state_kind;
    std.mem.writeInt(u32, key[2..][0..4], ordinal, .big);
    return key;
}

pub fn identityCanonicalToOrdinalKey(canonical_doc_id: u64) [1 + 1 + @sizeOf(u64)]u8 {
    var key: [1 + 1 + @sizeOf(u64)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_canonical_to_ordinal_kind;
    std.mem.writeInt(u64, key[2..][0..8], canonical_doc_id, .big);
    return key;
}

pub fn parseIdentityOrdinalKey(key: []const u8, kind: u8) ?u32 {
    if (key.len != 1 + 1 + @sizeOf(u32)) return null;
    if (key[0] != identity_namespace or key[1] != kind) return null;
    return std.mem.readInt(u32, key[2..][0..4], .big);
}

pub fn parseIdentityCanonicalKey(key: []const u8) ?u64 {
    if (key.len != 1 + 1 + @sizeOf(u64)) return null;
    if (key[0] != identity_namespace or key[1] != identity_canonical_to_ordinal_kind) return null;
    return std.mem.readInt(u64, key[2..][0..8], .big);
}

pub fn parseReplayEntrySequence(key: []const u8, hint_ordinal: u8) ?u64 {
    if (key.len != replay_key_len) return null;
    if (key[0] != replay_namespace or key[1] != hint_ordinal) return null;
    return std.mem.readInt(u64, key[2..10], .big);
}

pub fn isReplayEntryKey(key: []const u8) bool {
    return key.len == replay_key_len and key[0] == replay_namespace;
}

pub fn isReplayMetaInitKey(key: []const u8) bool {
    return std.mem.eql(u8, key, &replay_meta_init_key);
}

test "internal key primary round trip with zero bytes" {
    const alloc = std.testing.allocator;
    const raw = "ab\x00cd";
    const key = try documentKeyAlloc(alloc, raw);
    defer alloc.free(key);

    try std.testing.expect(isPrimaryDocumentKey(key));

    const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(raw, decoded);
}

test "internal key prefix bounds preserve raw prefix grouping" {
    const alloc = std.testing.allocator;
    const lower = try documentRangeLowerAlloc(alloc, "ab");
    defer alloc.free(lower);
    const upper = (try documentRangeUpperAlloc(alloc, "ab")).?;
    defer alloc.free(upper);

    const exact = try documentKeyAlloc(alloc, "ab");
    defer alloc.free(exact);
    const extended = try documentKeyAlloc(alloc, "abz");
    defer alloc.free(extended);
    const outside = try documentKeyAlloc(alloc, "ac");
    defer alloc.free(outside);

    try std.testing.expect(std.mem.order(u8, lower, exact) != .gt);
    try std.testing.expect(std.mem.order(u8, lower, extended) != .gt);
    try std.testing.expect(std.mem.order(u8, exact, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, extended, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, outside, upper) != .lt);
}

test "internal key ordering matches raw document id ordering for adversarial bytes" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        ":",
        ":i:",
        ":e:",
        ":t",
        "\x00",
        "\x00\x00",
        "\xff",
        "abc\x00def",
        "abc\xffdef",
        "abc:",
    };

    for (raw_ids) |lhs| {
        const lhs_key = try documentKeyAlloc(alloc, lhs);
        defer alloc.free(lhs_key);
        for (raw_ids) |rhs| {
            const rhs_key = try documentKeyAlloc(alloc, rhs);
            defer alloc.free(rhs_key);
            try std.testing.expectEqual(std.mem.order(u8, lhs, rhs), std.mem.order(u8, lhs_key, rhs_key));
        }
    }
}

test "internal key round trips adversarial document ids" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        ":",
        ":i:",
        ":e:",
        ":t",
        "\x00",
        "\x00\x00",
        "\xff",
        "abc\x00def",
        "abc\xffdef",
        "abc:",
    };

    for (raw_ids) |raw| {
        const key = try documentKeyAlloc(alloc, raw);
        defer alloc.free(key);
        const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
        defer alloc.free(decoded);
        try std.testing.expectEqualSlices(u8, raw, decoded);
    }
}

test "relational row key shares document range but is not primary" {
    const alloc = std.testing.allocator;
    const raw = "doc\x00a";
    const column_path = "metrics:amount\x00p95";

    const primary = try documentKeyAlloc(alloc, raw);
    defer alloc.free(primary);
    const relational = try relationalRowKeyAlloc(alloc, raw);
    defer alloc.free(relational);
    const relational_column = try relationalColumnKeyAlloc(alloc, raw, column_path);
    defer alloc.free(relational_column);
    const relational_column_index = try relationalColumnIndexKeyAlloc(alloc, column_path, raw);
    defer alloc.free(relational_column_index);
    const relational_array_element_index = try relationalArrayElementIndexKeyAlloc(alloc, column_path, "hot\x00tag", raw);
    defer alloc.free(relational_array_element_index);
    const relational_array_value_index = try relationalArrayValueIndexKeyAlloc(alloc, column_path, "[hot\x00tag]", raw);
    defer alloc.free(relational_array_value_index);
    const relational_json_value_index = try relationalJsonValueIndexKeyAlloc(alloc, column_path, "attrs.plan", "\"pro\"", raw);
    defer alloc.free(relational_json_value_index);
    const relational_json_path_index = try relationalJsonPathIndexKeyAlloc(alloc, column_path, "attrs.plan", raw);
    defer alloc.free(relational_json_path_index);
    const relational_column_index_by_doc = try relationalColumnIndexByDocKeyAlloc(alloc, raw, column_path);
    defer alloc.free(relational_column_index_by_doc);
    const ordered_tuple = try relationalOrderedTupleIndexKeyAlloc(alloc, "orders_status_created_idx", "active\x001234", raw);
    defer alloc.free(ordered_tuple);
    const ordered_tuple_by_doc = try relationalOrderedTupleIndexByDocKeyAlloc(alloc, raw, "orders_status_created_idx", "active\x001234");
    defer alloc.free(ordered_tuple_by_doc);
    const ordered_tuple_unique_conflict = try relationalOrderedTupleUniqueConflictKeyAlloc(alloc, "orders_status_created_idx", "active\x001234");
    defer alloc.free(ordered_tuple_unique_conflict);
    const ordered_tuple_prefix = try relationalOrderedTupleIndexTuplePrefixAlloc(alloc, "orders_status_created_idx", "active\x001234");
    defer alloc.free(ordered_tuple_prefix);
    const ordered_tuple_upper = (try relationalOrderedTupleIndexTuplePrefixUpperAlloc(alloc, "orders_status_created_idx", "active\x001234")).?;
    defer alloc.free(ordered_tuple_upper);
    const ordered_tuple_by_doc_lower = try relationalOrderedTupleIndexByDocRangeLowerAlloc(alloc, raw);
    defer alloc.free(ordered_tuple_by_doc_lower);
    const ordered_tuple_by_doc_upper = (try relationalOrderedTupleIndexByDocRangeUpperAlloc(alloc, raw)).?;
    defer alloc.free(ordered_tuple_by_doc_upper);
    const fk_ref = try relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer\x00a", "orders", raw);
    defer alloc.free(fk_ref);
    const unique = try relationalUniqueKeyAlloc(alloc, "orders_external_id_key", "external\x00a");
    defer alloc.free(unique);
    const temporal_unique = try relationalTemporalUniqueKeyAlloc(alloc, "prices_sku_valid_time_key", "sku\x00a", "10", "20", raw);
    defer alloc.free(temporal_unique);
    const fk_conflict = try relationalForeignKeyConflictKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer\x00a");
    defer alloc.free(fk_conflict);
    const fk_ref_prefix = try relationalForeignKeyRefParentPrefixAlloc(alloc, "orders_customer_id_fkey", "customers", "customer\x00a");
    defer alloc.free(fk_ref_prefix);
    const fk_ref_upper = (try relationalForeignKeyRefParentPrefixUpperAlloc(alloc, "orders_customer_id_fkey", "customers", "customer\x00a")).?;
    defer alloc.free(fk_ref_upper);
    const lower = try documentRangeLowerAlloc(alloc, "doc");
    defer alloc.free(lower);
    const upper = (try documentRangeUpperAlloc(alloc, "doc")).?;
    defer alloc.free(upper);

    try std.testing.expect(isPrimaryDocumentKey(primary));
    try std.testing.expect(!isPrimaryDocumentKey(relational));
    try std.testing.expect(isRelationalRowKey(relational));
    try std.testing.expect(isStoredDocumentRowKey(relational));
    try std.testing.expect(isRelationalColumnKey(relational_column));
    try std.testing.expect(isRelationalColumnIndexKey(relational_column_index));
    try std.testing.expect(isRelationalArrayElementIndexKey(relational_array_element_index));
    try std.testing.expect(isRelationalArrayValueIndexKey(relational_array_value_index));
    try std.testing.expect(isRelationalJsonValueIndexKey(relational_json_value_index));
    try std.testing.expect(isRelationalJsonPathIndexKey(relational_json_path_index));
    try std.testing.expect(isRelationalColumnIndexByDocKey(relational_column_index_by_doc));
    try std.testing.expect(isRelationalOrderedTupleIndexKey(ordered_tuple));
    try std.testing.expect(isRelationalOrderedTupleIndexByDocKey(ordered_tuple_by_doc));
    try std.testing.expect(isRelationalOrderedTupleUniqueConflictKey(ordered_tuple_unique_conflict));
    try std.testing.expect(isRelationalForeignKeyRefKey(fk_ref));
    try std.testing.expect(isRelationalUniqueKey(unique));
    try std.testing.expect(isRelationalTemporalUniqueKey(temporal_unique));
    try std.testing.expect(isRelationalForeignKeyConflictKey(fk_conflict));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_column));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_column_index));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_array_element_index));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_array_value_index));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_json_value_index));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_json_path_index));
    try std.testing.expect(isInternalPhysicalTableDataKey(relational_column_index_by_doc));
    try std.testing.expect(isInternalPhysicalTableDataKey(ordered_tuple));
    try std.testing.expect(isInternalPhysicalTableDataKey(ordered_tuple_by_doc));
    try std.testing.expect(isInternalPhysicalTableDataKey(ordered_tuple_unique_conflict));
    try std.testing.expect(isInternalPhysicalTableDataKey(fk_ref));
    try std.testing.expect(isInternalPhysicalTableDataKey(unique));
    try std.testing.expect(isInternalPhysicalTableDataKey(temporal_unique));
    try std.testing.expect(isInternalPhysicalTableDataKey(fk_conflict));
    try std.testing.expect(!isInternalMetadataKey(relational_column_index));
    try std.testing.expect(!isInternalMetadataKey(relational_array_element_index));
    try std.testing.expect(!isInternalMetadataKey(relational_array_value_index));
    try std.testing.expect(!isInternalMetadataKey(relational_json_value_index));
    try std.testing.expect(!isInternalMetadataKey(relational_json_path_index));
    try std.testing.expect(!isInternalMetadataKey(relational_column_index_by_doc));
    try std.testing.expect(!isInternalMetadataKey(ordered_tuple));
    try std.testing.expect(!isInternalMetadataKey(ordered_tuple_by_doc));
    try std.testing.expect(!isInternalMetadataKey(ordered_tuple_unique_conflict));
    try std.testing.expect(!isInternalMetadataKey(fk_ref));
    try std.testing.expect(!isInternalMetadataKey(unique));
    try std.testing.expect(!isInternalMetadataKey(temporal_unique));
    try std.testing.expect(!isInternalMetadataKey(fk_conflict));
    try std.testing.expect(!isStoredDocumentRowKey(relational_column));
    try std.testing.expect(!isStoredDocumentRowKey(relational_column_index));
    try std.testing.expect(!isStoredDocumentRowKey(relational_array_element_index));
    try std.testing.expect(!isStoredDocumentRowKey(relational_array_value_index));
    try std.testing.expect(!isStoredDocumentRowKey(relational_json_value_index));
    try std.testing.expect(!isStoredDocumentRowKey(relational_json_path_index));
    try std.testing.expect(!isStoredDocumentRowKey(relational_column_index_by_doc));
    try std.testing.expect(!isStoredDocumentRowKey(ordered_tuple));
    try std.testing.expect(!isStoredDocumentRowKey(ordered_tuple_by_doc));
    try std.testing.expect(!isStoredDocumentRowKey(ordered_tuple_unique_conflict));
    const decoded_relational = (try decodeStoredDocumentRowKeyAlloc(alloc, relational)).?;
    defer alloc.free(decoded_relational);
    try std.testing.expectEqualSlices(u8, raw, decoded_relational);
    var decoded_column = (try decodeRelationalColumnKeyAlloc(alloc, relational_column)).?;
    defer decoded_column.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_column.doc_key);
    try std.testing.expectEqualSlices(u8, column_path, decoded_column.column_path);
    var decoded_column_index = (try decodeRelationalColumnIndexKeyAlloc(alloc, relational_column_index)).?;
    defer decoded_column_index.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_column_index.doc_key);
    try std.testing.expectEqualSlices(u8, column_path, decoded_column_index.column_path);
    var decoded_array_element_index = (try decodeRelationalArrayElementIndexKeyAlloc(alloc, relational_array_element_index)).?;
    defer decoded_array_element_index.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_array_element_index.doc_key);
    try std.testing.expectEqualSlices(u8, column_path, decoded_array_element_index.column_path);
    try std.testing.expectEqualSlices(u8, "hot\x00tag", decoded_array_element_index.element_key);
    var decoded_json_value_index = (try decodeRelationalJsonValueIndexKeyAlloc(alloc, relational_json_value_index)).?;
    defer decoded_json_value_index.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_json_value_index.doc_key);
    try std.testing.expectEqualSlices(u8, column_path, decoded_json_value_index.column_path);
    try std.testing.expectEqualSlices(u8, "attrs.plan", decoded_json_value_index.json_path);
    try std.testing.expectEqualSlices(u8, "\"pro\"", decoded_json_value_index.value_key);
    var decoded_column_index_by_doc = (try decodeRelationalColumnIndexByDocKeyAlloc(alloc, relational_column_index_by_doc)).?;
    defer decoded_column_index_by_doc.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_column_index_by_doc.doc_key);
    try std.testing.expectEqualSlices(u8, column_path, decoded_column_index_by_doc.column_path);
    var decoded_ordered_tuple = (try decodeRelationalOrderedTupleIndexKeyAlloc(alloc, ordered_tuple)).?;
    defer decoded_ordered_tuple.deinit(alloc);
    try std.testing.expectEqualSlices(u8, "orders_status_created_idx", decoded_ordered_tuple.index_id);
    try std.testing.expectEqualSlices(u8, "active\x001234", decoded_ordered_tuple.encoded_tuple);
    try std.testing.expectEqualSlices(u8, raw, decoded_ordered_tuple.doc_key);
    var decoded_ordered_tuple_by_doc = (try decodeRelationalOrderedTupleIndexByDocKeyAlloc(alloc, ordered_tuple_by_doc)).?;
    defer decoded_ordered_tuple_by_doc.deinit(alloc);
    try std.testing.expectEqualSlices(u8, raw, decoded_ordered_tuple_by_doc.doc_key);
    try std.testing.expectEqualSlices(u8, "orders_status_created_idx", decoded_ordered_tuple_by_doc.index_id);
    try std.testing.expectEqualSlices(u8, "active\x001234", decoded_ordered_tuple_by_doc.encoded_tuple);
    var decoded_fk_ref = (try decodeRelationalForeignKeyRefKeyAlloc(alloc, fk_ref)).?;
    defer decoded_fk_ref.deinit(alloc);
    try std.testing.expectEqualSlices(u8, "orders_customer_id_fkey", decoded_fk_ref.constraint_name);
    try std.testing.expectEqualSlices(u8, "customers", decoded_fk_ref.parent_table);
    try std.testing.expectEqualSlices(u8, "customer\x00a", decoded_fk_ref.parent_key);
    try std.testing.expectEqualSlices(u8, "orders", decoded_fk_ref.child_table);
    try std.testing.expectEqualSlices(u8, raw, decoded_fk_ref.child_key);
    try std.testing.expect(std.mem.startsWith(u8, fk_ref, fk_ref_prefix));
    try std.testing.expect(std.mem.order(u8, fk_ref_prefix, fk_ref) != .gt);
    try std.testing.expect(std.mem.order(u8, fk_ref, fk_ref_upper) == .lt);
    try std.testing.expect(std.mem.startsWith(u8, ordered_tuple, ordered_tuple_prefix));
    try std.testing.expect(std.mem.order(u8, ordered_tuple_prefix, ordered_tuple) != .gt);
    try std.testing.expect(std.mem.order(u8, ordered_tuple, ordered_tuple_upper) == .lt);
    try std.testing.expect(std.mem.order(u8, ordered_tuple_by_doc_lower, ordered_tuple_by_doc) != .gt);
    try std.testing.expect(std.mem.order(u8, ordered_tuple_by_doc, ordered_tuple_by_doc_upper) == .lt);
    try std.testing.expect(std.mem.order(u8, lower, relational) != .gt);
    try std.testing.expect(std.mem.order(u8, relational, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, lower, relational_column) != .gt);
    try std.testing.expect(std.mem.order(u8, relational_column, upper) == .lt);
    try std.testing.expect(relational_column_index[0] == relational_column_index_namespace);
    try std.testing.expect(relational_array_element_index[0] == relational_array_element_index_namespace);
    try std.testing.expect(relational_column_index_by_doc[0] == relational_column_index_by_doc_namespace);
    try std.testing.expect(ordered_tuple[0] == relational_ordered_tuple_index_namespace);
    try std.testing.expect(ordered_tuple_by_doc[0] == relational_ordered_tuple_index_by_doc_namespace);
    try std.testing.expect(fk_ref[0] == relational_foreign_key_ref_namespace);
}

test "relational ordered tuple index keys preserve tuple and doc ordering" {
    const alloc = std.testing.allocator;
    const index_id = "orders_status_created_idx";
    const tuples = [_][]const u8{ "active\x000001", "active\x000002", "closed\x000001" };
    const docs = [_][]const u8{ "doc:a", "doc:b" };

    for (tuples) |lhs_tuple| {
        for (tuples) |rhs_tuple| {
            const lhs = try relationalOrderedTupleIndexKeyAlloc(alloc, index_id, lhs_tuple, "doc:a");
            defer alloc.free(lhs);
            const rhs = try relationalOrderedTupleIndexKeyAlloc(alloc, index_id, rhs_tuple, "doc:a");
            defer alloc.free(rhs);
            try std.testing.expectEqual(std.mem.order(u8, lhs_tuple, rhs_tuple), std.mem.order(u8, lhs, rhs));
        }
    }

    for (docs) |lhs_doc| {
        for (docs) |rhs_doc| {
            const lhs = try relationalOrderedTupleIndexKeyAlloc(alloc, index_id, "active\x000001", lhs_doc);
            defer alloc.free(lhs);
            const rhs = try relationalOrderedTupleIndexKeyAlloc(alloc, index_id, "active\x000001", rhs_doc);
            defer alloc.free(rhs);
            try std.testing.expectEqual(std.mem.order(u8, lhs_doc, rhs_doc), std.mem.order(u8, lhs, rhs));
        }
    }
}

test "internal key binary prefix bounds select only matching document ids" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        "\x00",
        "\x00a",
        "\x00\x00",
        "\x00\xff",
        "\x01",
        "abc",
        "abc\x00def",
        "abc\xffdef",
        "abd",
    };
    const prefixes = [_][]const u8{
        "\x00",
        "abc",
        "abc\x00",
    };

    for (prefixes) |prefix| {
        const lower = try documentRangeLowerAlloc(alloc, prefix);
        defer alloc.free(lower);
        const upper = try documentRangeUpperAlloc(alloc, prefix);
        defer if (upper) |u| alloc.free(u);

        for (raw_ids) |raw| {
            const key = try documentKeyAlloc(alloc, raw);
            defer alloc.free(key);
            const in_range = std.mem.order(u8, key, lower) != .lt and
                (upper == null or std.mem.order(u8, key, upper.?) == .lt);
            try std.testing.expectEqual(std.mem.startsWith(u8, raw, prefix), in_range);
        }
    }
}

test "internal key encoded shard boundaries contain encoded primary keys" {
    const alloc = std.testing.allocator;
    const lower = try documentRangeLowerAlloc(alloc, "ab\x00");
    defer alloc.free(lower);
    const upper = (try documentRangeUpperAlloc(alloc, "ab\x00")).?;
    defer alloc.free(upper);

    const inside = try documentKeyAlloc(alloc, "ab\x00c");
    defer alloc.free(inside);
    const outside_before = try documentKeyAlloc(alloc, "ab");
    defer alloc.free(outside_before);
    const outside_after = try documentKeyAlloc(alloc, "ab\x01");
    defer alloc.free(outside_after);

    try std.testing.expect(std.mem.order(u8, inside, lower) != .lt);
    try std.testing.expect(std.mem.order(u8, inside, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, outside_before, lower) == .lt);
    try std.testing.expect(std.mem.order(u8, outside_after, upper) != .lt);
}

test "replay entry key round trip" {
    const key = replayEntryKey(3, 42);
    try std.testing.expect(isReplayEntryKey(&key));
    try std.testing.expectEqual(@as(?u64, 42), parseReplayEntrySequence(&key, 3));
    try std.testing.expectEqual(@as(?u64, null), parseReplayEntrySequence(&key, 2));

    const lower = replayRangeLower(3, 42);
    const upper = replayRangeUpper(3);
    try std.testing.expect(std.mem.order(u8, &lower, &key) != .gt);
    try std.testing.expect(std.mem.order(u8, &key, &upper) == .lt);
}

test "isEmbeddingArtifactKey round trip" {
    const alloc = std.testing.allocator;
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, "my-doc", "my-index");
    defer alloc.free(key);

    try std.testing.expect(isEmbeddingArtifactKey(key));
    try std.testing.expect(!isPrimaryDocumentKey(key));
    try std.testing.expect(!isSummaryArtifactKey(key));
    try std.testing.expect(!isDerivedEmbeddingArtifactKey(key));

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("my-doc", parsed.doc_key);
    try std.testing.expectEqualStrings("my-index", parsed.artifact_name);

    const view = (try parseEmbeddingArtifactKeyView(key)).?;
    try std.testing.expectEqualStrings("my-doc", view.doc_key);
    try std.testing.expectEqualStrings("my-index", view.artifact_name);
}

test "embedding artifact key round trip with zero bytes in doc key" {
    const alloc = std.testing.allocator;
    const raw = "ab\x00cd";
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, raw, "dense");
    defer alloc.free(key);

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings(raw, parsed.doc_key);
    try std.testing.expectEqualStrings("dense", parsed.artifact_name);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyView(key));
}

test "embedding artifact key round trip with arbitrary doc and artifact bytes" {
    const alloc = std.testing.allocator;
    const raw_doc = "ab\x00:i:\xff";
    const raw_name = "dense\x00name\xff";
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, raw_doc, raw_name);
    defer alloc.free(key);

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualSlices(u8, raw_doc, parsed.doc_key);
    try std.testing.expectEqualSlices(u8, raw_name, parsed.artifact_name);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyView(key));
}

test "matchesEmbeddingArtifactName matches exact embedding artifact name" {
    const alloc = std.testing.allocator;
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, "my-doc", "my-index");
    defer alloc.free(key);

    try std.testing.expect(matchesEmbeddingArtifactName(key, "my-index"));
    try std.testing.expect(!matchesEmbeddingArtifactName(key, "other-index"));
}

test "derivedEmbeddingBaseKeyAlloc returns chunk artifact key" {
    const alloc = std.testing.allocator;
    const chunk_key = try chunkArtifactKeyAlloc(alloc, "doc1", "chunks", 7);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    const base = (try derivedEmbeddingBaseKeyAlloc(alloc, embedding_key)).?;
    defer alloc.free(base);
    try std.testing.expectEqualStrings(chunk_key, base);
}

test "document unit chunk artifact key is recognized as chunk record" {
    const alloc = std.testing.allocator;
    const key = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "page:000001", 3);
    defer alloc.free(key);

    try std.testing.expect(isChunkArtifactRecordKey(key));
    try std.testing.expect(matchesChunkArtifactName(key, "document_chunks_v1"));
    try std.testing.expect(!matchesChunkArtifactName(key, "other_chunks_v1"));
}

test "document unit chunk derived embedding key is recognized" {
    const alloc = std.testing.allocator;
    const chunk_key = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "page:000001", 3);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    try std.testing.expect(isDerivedEmbeddingArtifactKey(embedding_key));
    try std.testing.expect(matchesDerivedEmbeddingArtifactName(embedding_key, "dense"));
    try std.testing.expect(!matchesDerivedEmbeddingArtifactName(embedding_key, "other"));
    const base = (try derivedEmbeddingBaseKeyAlloc(alloc, embedding_key)).?;
    defer alloc.free(base);
    try std.testing.expectEqualStrings(chunk_key, base);
}

test "matchesDerivedEmbeddingArtifactName matches exact derived embedding artifact name" {
    const alloc = std.testing.allocator;
    const chunk_key = try chunkArtifactKeyAlloc(alloc, "doc1", "chunks", 7);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    try std.testing.expect(matchesDerivedEmbeddingArtifactName(embedding_key, "dense"));
    try std.testing.expect(!matchesDerivedEmbeddingArtifactName(embedding_key, "other"));
}

test "isSummaryArtifactKey" {
    const alloc = std.testing.allocator;
    const key = try artifactNamedPrefixAlloc(alloc, "doc1", "summary", "my-summary");
    defer alloc.free(key);

    try std.testing.expect(isSummaryArtifactKey(key));
    try std.testing.expect(!isEmbeddingArtifactKey(key));
    try std.testing.expect(!isPrimaryDocumentKey(key));
}

test "parseEmbeddingArtifactKeyAlloc returns null for non-embedding" {
    const alloc = std.testing.allocator;
    const doc_key = try documentKeyAlloc(alloc, "doc1");
    defer alloc.free(doc_key);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyAlloc(alloc, doc_key));
}

test "graph edge artifact key round trip" {
    const alloc = std.testing.allocator;
    const key = try graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");
    defer alloc.free(key);

    try std.testing.expect(isGraphEdgeArtifactKey(key));
    try std.testing.expect(!isEmbeddingArtifactKey(key));

    const parsed = (try parseGraphEdgeArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.index_name);
    defer alloc.free(parsed.edge_type);
    defer alloc.free(parsed.target_doc_key);
    try std.testing.expectEqualStrings("doc:a", parsed.doc_key);
    try std.testing.expectEqualStrings("gr_v1", parsed.index_name);
    try std.testing.expectEqualStrings("links", parsed.edge_type);
    try std.testing.expectEqualStrings("doc:b", parsed.target_doc_key);
}

test "graph edge artifact key round trip with arbitrary source and target ids" {
    const alloc = std.testing.allocator;
    const source = "doc\x00:i:\xffsource";
    const target = "\x00target:out:\xff";
    const edge_type = "links\x00typed";
    const key = try graphEdgeArtifactKeyAlloc(alloc, source, "gr\x00v1", edge_type, target);
    defer alloc.free(key);

    try std.testing.expect(isGraphEdgeArtifactKey(key));
    const parsed = (try parseGraphEdgeArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.index_name);
    defer alloc.free(parsed.edge_type);
    defer alloc.free(parsed.target_doc_key);
    try std.testing.expectEqualSlices(u8, source, parsed.doc_key);
    try std.testing.expectEqualSlices(u8, "gr\x00v1", parsed.index_name);
    try std.testing.expectEqualSlices(u8, edge_type, parsed.edge_type);
    try std.testing.expectEqualSlices(u8, target, parsed.target_doc_key);
}

test "resolution artifact key round-trips and is distinct from asset" {
    const alloc = std.testing.allocator;
    const key = try resolutionArtifactKeyAlloc(alloc, "doc:article-123", "resolution_v1");
    defer alloc.free(key);

    try std.testing.expect(isResolutionArtifactKey(key));
    try std.testing.expect(!isAssetArtifactKey(key));
    try std.testing.expect(!isSummaryArtifactKey(key));

    const asset = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "relations_v1");
    defer alloc.free(asset);
    try std.testing.expect(!isResolutionArtifactKey(asset));

    const parsed = (try parseResolutionArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("doc:article-123", parsed.doc_key);
    try std.testing.expectEqualStrings("resolution_v1", parsed.artifact_name);
}

test "parseAssetArtifactKeyAlloc returns doc key and artifact name" {
    const alloc = std.testing.allocator;
    const key = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "relations_v1");
    defer alloc.free(key);
    const parsed = (try parseAssetArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("doc:article-123", parsed.doc_key);
    try std.testing.expectEqualStrings("relations_v1", parsed.artifact_name);

    const res = try resolutionArtifactKeyAlloc(alloc, "doc:article-123", "resolution_v1");
    defer alloc.free(res);
    try std.testing.expect((try parseAssetArtifactKeyAlloc(alloc, res)) == null);
}

test "matchesAssetArtifactName matches top-level and document unit assets" {
    const alloc = std.testing.allocator;
    const asset = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "document_units_v1");
    defer alloc.free(asset);
    const unit = try documentUnitArtifactKeyAlloc(alloc, "doc:article-123", "document_units_v1", "page:000001");
    defer alloc.free(unit);
    const chunk = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:article-123", "document_units_v1", "page:000001", 0);
    defer alloc.free(chunk);

    try std.testing.expect(matchesAssetArtifactName(asset, "document_units_v1"));
    try std.testing.expect(matchesAssetArtifactName(unit, "document_units_v1"));
    try std.testing.expect(!matchesAssetArtifactName(unit, "other_units_v1"));
    try std.testing.expect(!matchesAssetArtifactName(chunk, "document_units_v1"));
}

test "asset artifact source index keys group by source artifact" {
    const alloc = std.testing.allocator;
    const root = try assetArtifactSourceIndexRootPrefixAlloc(alloc);
    defer alloc.free(root);
    const prefix = try assetArtifactSourceIndexPrefixAlloc(alloc, "relations_v1");
    defer alloc.free(prefix);
    const key = try assetArtifactSourceIndexKeyAlloc(alloc, "relations_v1", "doc:article-123");
    defer alloc.free(key);
    const other = try assetArtifactSourceIndexKeyAlloc(alloc, "other_v1", "doc:article-123");
    defer alloc.free(other);

    try std.testing.expect(std.mem.startsWith(u8, prefix, root));
    try std.testing.expect(std.mem.startsWith(u8, key, prefix));
    try std.testing.expect(!std.mem.startsWith(u8, other, prefix));
}

test "decodePrimaryDocumentKeyAlloc round-trips and rejects non-primary keys" {
    const alloc = std.testing.allocator;
    const key = try documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(key);
    try std.testing.expect(isPrimaryDocumentKey(key));
    const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("person/ada_lovelace", decoded);

    // An asset artifact key is not a primary document key.
    const asset = try artifactNamedPrefixAlloc(alloc, "person/ada_lovelace", "asset", "relations_v1");
    defer alloc.free(asset);
    try std.testing.expect(!isPrimaryDocumentKey(asset));
    try std.testing.expect((try decodePrimaryDocumentKeyAlloc(alloc, asset)) == null);
}
