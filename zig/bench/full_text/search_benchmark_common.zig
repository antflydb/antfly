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
pub const antfly = @import("antfly-zig");

pub const types = antfly.db.types;

pub const index_name = "search_benchmark";
pub const text_field = "text";

pub const index_config_json =
    "{\"type\":\"full_text\",\"analysis_config\":{\"field_analyzers\":{\"text\":\"simple\"}}}";

pub const ParsedArgs = struct {
    db_path: []const u8,
    max_text_bytes: ?usize = null,
};

pub fn parseArgs(args_in: std.process.Args) !ParsedArgs {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    const db_path = args.next() orelse return error.MissingDatabasePath;
    var parsed = ParsedArgs{ .db_path = db_path };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--max-text-bytes")) {
            parsed.max_text_bytes = try std.fmt.parseInt(usize, args.next() orelse return error.MissingArgument, 10);
        } else {
            return error.UnknownArgument;
        }
    }
    return parsed;
}

pub fn openDb(alloc: std.mem.Allocator, db_path: []const u8) !antfly.db.DB {
    return try antfly.db.DB.open(alloc, db_path, .{
        .map_size = 64 * 1024 * 1024 * 1024,
        .no_sync = true,
        .executor = .{ .backend = .manual },
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
    });
}

pub fn ensureIndex(db: *antfly.db.DB) !void {
    db.addIndex(.{
        .name = index_name,
        .kind = .full_text,
        .config_json = index_config_json,
    }) catch |err| switch (err) {
        error.IndexAlreadyExists => {},
        else => return err,
    };
}

pub const QueryCommand = union(enum) {
    count: void,
    top: u32,
    top_count: u32,
    unsupported: void,
};

pub fn parseCommand(raw: []const u8) QueryCommand {
    if (std.mem.eql(u8, raw, "COUNT")) return .{ .count = {} };
    if (std.mem.startsWith(u8, raw, "TOP_")) {
        const rest = raw["TOP_".len..];
        if (std.mem.endsWith(u8, rest, "_COUNT")) {
            const n_raw = rest[0 .. rest.len - "_COUNT".len];
            const n = std.fmt.parseInt(u32, n_raw, 10) catch return .{ .unsupported = {} };
            if (n == 0) return .{ .unsupported = {} };
            return .{ .top_count = n };
        }
        const n = std.fmt.parseInt(u32, rest, 10) catch return .{ .unsupported = {} };
        if (n == 0) return .{ .unsupported = {} };
        return .{ .top = n };
    }
    return .{ .unsupported = {} };
}

const ClauseKind = enum {
    should,
    must,
    must_not,
};

const Clause = struct {
    kind: ClauseKind,
    text: []const u8,
    phrase: bool,
};

pub fn parseLuceneQuery(alloc: std.mem.Allocator, raw_query: []const u8) !types.TextQuery {
    var clauses = std.ArrayListUnmanaged(Clause).empty;
    defer clauses.deinit(alloc);

    var i: usize = 0;
    while (i < raw_query.len) {
        while (i < raw_query.len and std.ascii.isWhitespace(raw_query[i])) : (i += 1) {}
        if (i >= raw_query.len) break;

        var kind: ClauseKind = .should;
        if (raw_query[i] == '+') {
            kind = .must;
            i += 1;
        } else if (raw_query[i] == '-') {
            kind = .must_not;
            i += 1;
        }

        if (i >= raw_query.len) break;

        var phrase = false;
        const start: usize = i;
        var end: usize = i;
        if (raw_query[i] == '"') {
            phrase = true;
            i += 1;
            const phrase_start = i;
            while (i < raw_query.len and raw_query[i] != '"') : (i += 1) {}
            end = i;
            if (i < raw_query.len and raw_query[i] == '"') i += 1;
            if (end > phrase_start) {
                try clauses.append(alloc, .{ .kind = kind, .text = raw_query[phrase_start..end], .phrase = true });
            }
            continue;
        }

        while (i < raw_query.len and !std.ascii.isWhitespace(raw_query[i])) : (i += 1) {}
        end = i;
        if (end > start) {
            try clauses.append(alloc, .{ .kind = kind, .text = raw_query[start..end], .phrase = phrase });
        }
    }

    if (clauses.items.len == 0) return .{ .match_none = {} };

    var must = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer must.deinit(alloc);
    var should = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer should.deinit(alloc);
    var must_not = std.ArrayListUnmanaged(types.TextQuery).empty;
    errdefer must_not.deinit(alloc);

    var should_terms = std.ArrayListUnmanaged([]const u8).empty;
    defer should_terms.deinit(alloc);

    for (clauses.items) |clause| {
        const query = if (clause.phrase)
            types.TextQuery{ .match_phrase = .{
                .field = text_field,
                .text = clause.text,
                .analyzer = "simple",
            } }
        else
            types.TextQuery{ .match = .{
                .field = text_field,
                .text = clause.text,
                .analyzer = "simple",
            } };

        switch (clause.kind) {
            .must => try must.append(alloc, query),
            .must_not => try must_not.append(alloc, query),
            .should => if (!clause.phrase) {
                try should_terms.append(alloc, clause.text);
            } else {
                try should.append(alloc, query);
            },
        }
    }

    if (should_terms.items.len > 0) {
        const joined = try joinTerms(alloc, should_terms.items);
        try should.append(alloc, .{ .match = .{
            .field = text_field,
            .text = joined,
            .analyzer = "simple",
        } });
    }

    const must_slice = try must.toOwnedSlice(alloc);
    const should_slice = try should.toOwnedSlice(alloc);
    const must_not_slice = try must_not.toOwnedSlice(alloc);

    if (must_slice.len == 0 and should_slice.len == 1 and must_not_slice.len == 0) {
        const out = should_slice[0];
        alloc.free(should_slice);
        return out;
    }
    if (must_slice.len == 1 and should_slice.len == 0 and must_not_slice.len == 0) {
        const out = must_slice[0];
        alloc.free(must_slice);
        return out;
    }

    return .{ .bool_query = .{
        .must = must_slice,
        .should = should_slice,
        .must_not = must_not_slice,
        .min_should = if (should_slice.len > 0 and must_slice.len == 0) 1 else 0,
    } };
}

fn joinTerms(alloc: std.mem.Allocator, terms: []const []const u8) ![]const u8 {
    if (terms.len == 0) return "";
    var total: usize = 0;
    for (terms) |term| total += term.len;
    total += terms.len - 1;

    const out = try alloc.alloc(u8, total);
    var pos: usize = 0;
    for (terms, 0..) |term, i| {
        if (i > 0) {
            out[pos] = ' ';
            pos += 1;
        }
        @memcpy(out[pos..][0..term.len], term);
        pos += term.len;
    }
    return out;
}

pub fn countQuery(db: *antfly.db.DB, alloc: std.mem.Allocator, query: types.TextQuery) !u32 {
    var result = try db.search(alloc, .{
        .index_name = index_name,
        .full_text = query,
        .count_only = true,
        .limit = 1,
        .include_stored = false,
        .include_all_fields = false,
    });
    defer result.deinit();
    return result.total_hits;
}

pub fn topQuery(db: *antfly.db.DB, alloc: std.mem.Allocator, query: types.TextQuery, limit: u32) !void {
    var result = try db.search(alloc, .{
        .index_name = index_name,
        .full_text = query,
        .limit = limit,
        .include_stored = false,
        .include_all_fields = false,
    });
    defer result.deinit();
}
