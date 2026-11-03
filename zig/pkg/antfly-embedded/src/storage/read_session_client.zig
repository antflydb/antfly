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

//! Pure client facades over complete owner-issued read operations. DB and
//! engine layout checks occur only at issuance, never while pulling a page.
const std = @import("std");
const Types = @import("read_session_types").types;
const contract = Types.contract;
const A = std.mem.Allocator;
pub const Statement = struct {
    handle: contract.Handle,
    schema_version: u32,
    pub fn fromIssued(value: contract.Statement) !Statement {
        try value.handle.validate(.statement);
        return .{ .handle = value.handle, .schema_version = value.schema_version };
    }
    pub fn deinit(self: *Statement) void {
        contract.call("close", .statement, self.handle, .{self.handle}) catch @panic("incompatible read session owner");
        self.* = undefined;
    }
};
pub const Relational = struct {
    handle: contract.Handle,
    alloc: A,
    pub fn fromHandle(alloc: A, handle: contract.Handle) !*Relational {
        try handle.validate(.relational);
        errdefer contract.call("close", .relational, handle, .{handle}) catch @panic("incompatible read session owner");
        const self = try alloc.create(Relational);
        self.* = .{ .alloc = alloc, .handle = handle };
        return self;
    }
    pub fn deinit(self: *Relational) void {
        contract.call("close", .relational, self.handle, .{self.handle}) catch @panic("incompatible read session owner");
        self.alloc.destroy(self);
    }
    pub fn checkpoint(self: *const Relational) !void {
        return contract.call("checkpoint", .relational, self.handle, .{self.handle});
    }
    pub fn rangeProofs(self: *Relational, alloc: A) ![]Types.range_protection.Proof {
        return contract.call("range_proofs", .relational, self.handle, .{ self.handle, alloc });
    }
    pub fn normalizeRows(self: *Relational, alloc: A, writes: []const Types.types.BatchWrite) ![]Types.types.BatchWrite {
        return contract.call("normalize", .relational, self.handle, .{ self.handle, alloc, writes });
    }
    pub fn nextTypedPage(self: *Relational, alloc: A, io: ?std.Io, budget: Types.values.Budget) !Types.values.Page {
        var retained_io = io;
        const borrow: ?contract.IoBorrow = if (retained_io) |*value| .init(value) else null;
        return contract.call("next_typed", .relational, self.handle, .{ self.handle, alloc, borrow, budget });
    }
    pub fn nextViewPage(self: *Relational, alloc: A, limit: u32) !Types.view.Page {
        return contract.call("next_view", .relational, self.handle, .{ self.handle, alloc, limit });
    }
};
pub const Document = struct {
    handle: contract.Handle,
    alloc: A,
    pub fn fromHandle(alloc: A, handle: contract.Handle) !*Document {
        try handle.validate(.document);
        errdefer contract.call("close", .document, handle, .{handle}) catch @panic("incompatible read session owner");
        const self = try alloc.create(Document);
        self.* = .{ .alloc = alloc, .handle = handle };
        return self;
    }
    pub fn deinit(self: *Document) void {
        contract.call("close", .document, self.handle, .{self.handle}) catch @panic("incompatible read session owner");
        self.alloc.destroy(self);
    }
    pub fn rangeProofs(self: *Document, alloc: A) ![]Types.range_protection.Proof {
        return contract.call("range_proofs", .document, self.handle, .{ self.handle, alloc });
    }
    pub fn normalizeRows(self: *Document, alloc: A, writes: []const Types.types.BatchWrite) ![]Types.types.BatchWrite {
        return contract.call("normalize", .document, self.handle, .{ self.handle, alloc, writes });
    }
    pub fn next(self: *Document, alloc: A, limit: u32) !Types.view.Page {
        return contract.call("next_view", .document, self.handle, .{ self.handle, alloc, limit });
    }
};

/// Source-compatible reader namespace with no private Reader implementation.
pub const RelationalRows = struct {
    pub const Value = Types.reader_values.Value;
    pub const Condition = Types.reader_values.Condition;
    pub const Bound = Types.reader_values.Bound;
    pub const PrimaryBound = Types.reader_values.PrimaryBound;
    pub const RowFilter = Types.reader_values.RowFilter;
    pub const Request = Types.reader_values.Request;
    pub const Budget = Types.values.Budget;
    pub const Row = Types.values.Row;
    pub const Page = Types.values.Page;
    pub const Reader = struct {
        handle: contract.Handle,
        pub const Hydrated = Types.reader_values.Hydrated;
        pub fn fromHandle(handle: contract.Handle) !Reader {
            try handle.validate(.reader);
            return .{ .handle = handle };
        }
        pub fn deinit(self: *Reader) void {
            contract.call("close", .reader, self.handle, .{self.handle}) catch @panic("incompatible read owner");
            self.* = undefined;
        }
        pub fn nextPage(self: *Reader, alloc: A, io: ?std.Io, budget: Budget) !Page {
            var retained_io = io;
            const borrow: ?contract.IoBorrow = if (retained_io) |*value| .init(value) else null;
            return contract.call("next_page", .reader, self.handle, .{ self.handle, alloc, borrow, budget });
        }
        pub fn nextTypedPage(self: *Reader, alloc: A, io: ?std.Io, budget: Budget) !Page {
            var retained_io = io;
            const borrow: ?contract.IoBorrow = if (retained_io) |*value| .init(value) else null;
            return contract.call("next_typed", .reader, self.handle, .{ self.handle, alloc, borrow, budget });
        }
        pub fn lookupTypedRow(self: *Reader, alloc: A, key: []const u8) !?Row {
            return contract.call("lookup_one", .reader, self.handle, .{ self.handle, alloc, key, false, null });
        }
        pub fn lookupSqlRow(self: *Reader, alloc: A, key: []const u8) !?Row {
            return contract.call("lookup_one", .reader, self.handle, .{ self.handle, alloc, key, true, null });
        }
        pub fn lookupSqlRowWithLayout(self: *Reader, alloc: A, key: []const u8, shared: *?*const Types.reader_values.TypedLayout) !?Row {
            return contract.call("lookup_one", .reader, self.handle, .{ self.handle, alloc, key, true, shared });
        }
        pub fn lookupSqlBatch(self: *Reader, alloc: A, keys: []const []const u8, layout: *const Types.reader_values.TypedLayout, max_bytes: usize) !Hydrated {
            return contract.call("lookup_batch", .reader, self.handle, .{ self.handle, alloc, keys, layout, max_bytes });
        }
    };
};

pub fn QueryReadLease(comptime DB: type, comptime SearchResult: type) type {
    return struct {
        const Self = @This();
        pub const Result = SearchResult;
        pub const Protocol = Types.query_lease.Protocol(SearchResult);
        handle: Protocol.Handle,
        pub fn fromHandle(handle: Protocol.Handle) !Self {
            try handle.validate();
            return .{ .handle = handle };
        }
        pub fn release(self: *Self) void {
            Protocol.call("close", self.handle, .{self.handle}) catch @panic("incompatible query lease owner");
            self.* = undefined;
        }
        pub fn relationalRows(self: *const Self, alloc: A, fields: []const []const u8, version: u32) !RelationalRows.Reader {
            return RelationalRows.Reader.fromHandle(try Protocol.call("reader", self.handle, .{ self.handle, alloc, fields, version }));
        }
        pub fn documentRows(self: *const Self, alloc: A, version: u32) !DB.DocumentRows {
            return DB.DocumentRows.fromHandle(try Protocol.call("documents", self.handle, .{ self.handle, alloc, version }));
        }
        pub fn search(self: Self, alloc: A, req: Types.types.SearchRequest) !SearchResult {
            return Protocol.call("search", self.handle, .{ self.handle, alloc, req });
        }
    };
}

// Physical bounded windows never occur in a client module graph.
pub const DocumentPrimaryReadWindow = void;
pub fn DocumentRows(comptime DB: type) type {
    _ = DB;
    return struct {
        const Self = @This();
        const Values = Types.document_values;
        pub const Source = Values.Source;
        pub const Batch = Values.Batch;
        pub const ProjectedBatch = Values.ProjectedBatch;
        handle: contract.Handle,
        pub fn fromHandle(handle: contract.Handle) !Self {
            try handle.validate(.document_reader);
            return .{ .handle = handle };
        }
        pub fn lookup(self: *Self, key: []const u8) ![]const u8 {
            return contract.call("lookup_document", .document_reader, self.handle, .{ self.handle, key });
        }
        pub fn lookupBatch(self: *Self, keys: []const []const u8, max_bytes: usize) !Batch {
            return contract.call("lookup_documents", .document_reader, self.handle, .{ self.handle, keys, max_bytes });
        }
        pub fn lookupProjectedBatch(self: *Self, keys: []const []const u8, fields: []const []const u8, max_bytes: usize) !ProjectedBatch {
            return contract.call("lookup_projected_documents", .document_reader, self.handle, .{ self.handle, keys, fields, max_bytes });
        }
        pub fn releasePage(self: *Self) void {
            contract.call("release_page", .document_reader, self.handle, .{self.handle}) catch @panic("incompatible document reader owner");
        }
        pub fn deinit(self: *Self) void {
            contract.call("close", .document_reader, self.handle, .{self.handle}) catch @panic("incompatible document reader owner");
            self.* = undefined;
        }
    };
}
