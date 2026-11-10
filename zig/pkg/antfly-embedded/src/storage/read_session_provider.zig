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

//! Complete retained read operations stay in the physical storage owner.
const std = @import("std");
const Types = @import("read_session_types").types;
const contract = Types.contract;
const Backend = @import("read_session_backend");
const types = Types.types;
const Relational = Backend.Relational;
const Document = Backend.Document;
const Rows = Types.values;
const Reader = Backend.RelationalRows.Reader;
const ReaderValues = Types.reader_values;
const View = Types.view;
const A = std.mem.Allocator;
const FenceOwner = struct {
    alloc: A,
    value: Backend.StatementReadFence,
    db: *anyopaque,
    capture: *const fn (*FenceOwner) anyerror!contract.Statement,
};
pub fn tryFence(db: anytype, allow_unresolved: bool) !?contract.Handle {
    var value = (try if (allow_unresolved) db.tryEmbeddedBackupReadFence() else db.tryStatementReadFence()) orelse return null;
    errdefer value.release();
    const owner = try db.alloc.create(FenceOwner);
    const Capture = struct {
        fn run(held: *FenceOwner) !contract.Statement {
            const database: *@TypeOf(db.*) = @ptrCast(@alignCast(held.db));
            const retained = try database.alloc.create(StatementOwner);
            errdefer database.alloc.destroy(retained);
            retained.* = .{ .alloc = database.alloc, .value = try database.captureRelationalStatementSnapshotAtFence(&held.value) };
            return .{ .handle = issue(.statement, retained), .schema_version = retained.value.schema_version };
        }
    };
    owner.* = .{ .alloc = db.alloc, .value = value, .db = db, .capture = Capture.run };
    return issue(.statement_fence, owner);
}
pub fn resolveFence(handle: contract.Handle, db: *const anyopaque) !*const Backend.StatementReadFence {
    const owner = try handle.resolve(.statement_fence, FenceOwner);
    if (owner.db != db) return error.InvalidArgument;
    return &owner.value;
}
pub fn closeFence(handle: contract.Handle) !void {
    const owner = try handle.resolve(.statement_fence, FenceOwner);
    const alloc = owner.alloc;
    owner.value.release();
    alloc.destroy(owner);
}
const StatementOwner = struct { alloc: A, value: Backend.Statement };
pub fn capture(db: anytype) !contract.Statement {
    const retained = try db.alloc.create(StatementOwner);
    errdefer db.alloc.destroy(retained);
    retained.* = .{ .alloc = db.alloc, .value = try db.captureRelationalStatementSnapshot() };
    return .{ .handle = issue(.statement, retained), .schema_version = retained.value.schema_version };
}
pub fn closeStatement(handle: contract.Handle) !void {
    const owner = try handle.resolve(.statement, StatementOwner);
    const alloc = owner.alloc;
    owner.value.deinit();
    alloc.destroy(owner);
}
pub fn openRelational(db: anytype, alloc: A, from: []const u8, to: []const u8, opts: types.ScanOptions, snapshot: ?contract.Handle) !contract.Handle {
    const cut = if (snapshot) |handle| &(try handle.resolve(.statement, StatementOwner)).value else null;
    return issue(.relational, try db.openRelationalReadSessionAtSnapshot(alloc, from, to, opts, cut));
}
pub fn openDocument(db: anytype, alloc: A, from: []const u8, to: []const u8, opts: types.ScanOptions) !contract.Handle {
    return issue(.document, try db.openDocumentReadSession(alloc, from, to, opts));
}
pub fn retainDocument(session: *Document) contract.Handle {
    return issue(.document, session);
}
pub fn retainRelational(session: *Relational) contract.Handle {
    return issue(.relational, session);
}
pub fn closeRelational(handle: contract.Handle) !void {
    (try handle.resolve(.relational, Relational)).deinit();
}
pub fn closeDocument(handle: contract.Handle) !void {
    (try handle.resolve(.document, Document)).deinit();
}
pub fn checkpoint(handle: contract.Handle) !void {
    try (try handle.resolve(.relational, Relational)).checkpoint();
}
pub fn relationalProofs(handle: contract.Handle, alloc: A) ![]Types.range_protection.Proof {
    return (try handle.resolve(.relational, Relational)).rangeProofs(alloc);
}
pub fn documentProofs(handle: contract.Handle, alloc: A) ![]Types.range_protection.Proof {
    return (try handle.resolve(.document, Document)).rangeProofs(alloc);
}
pub fn normalizeRelational(handle: contract.Handle, alloc: A, writes: []const types.BatchWrite) ![]types.BatchWrite {
    return (try handle.resolve(.relational, Relational)).normalizeRows(alloc, writes);
}
pub fn normalizeDocument(handle: contract.Handle, alloc: A, writes: []const types.BatchWrite) ![]types.BatchWrite {
    return (try handle.resolve(.document, Document)).normalizeRows(alloc, writes);
}
/// Export only materialized immutable values. Pattern programs and columnar
/// spill/JSON owners are execution-private representations, never native read
/// transport. Check the discriminator before following any private pointer.
pub fn validateExportedRow(row: anytype) !void {
    if (row.typed_cells) |cells| {
        if (cells.columnar != null) return error.UnsupportedVersion;
        for (cells.values) |datum| {
            if (datum.patterns != null) return error.UnsupportedVersion;
            if (datum.array) |array| for (array.elements) |element| {
                if (element.patterns != null or element.array != null) return error.UnsupportedVersion;
            };
        }
    }
}
pub fn validateExportedPage(page: anytype) !void {
    if (comptime @hasField(@TypeOf(page), "lease")) {
        if (page.lease != null or page.retained_pages.len != 0 or page.columnar_blocks.len != 0) return error.UnsupportedVersion;
    }
    for (page.rows) |row| try validateExportedRow(row);
}
/// Payload arenas and native typed layouts remain owned by this provider.
/// The response has a separate empty arena for optional caller-side metadata.
/// One explicit release destroys the complete provider page independently of
/// session close. No row callback or IO receiver escapes this operation.
fn ownPage(page: anytype, alloc: A) !@TypeOf(page) {
    try validateExportedPage(page);
    const Page = @TypeOf(page);
    const Owner = struct {
        alloc: A,
        value: Page,
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner_alloc = self.alloc;
            self.value.deinit();
            owner_alloc.destroy(self);
        }
    };
    const owner = try alloc.create(Owner);
    owner.* = .{ .alloc = alloc, .value = page };
    var output = page;
    output.arena = std.heap.ArenaAllocator.init(alloc);
    output.owner_release = .{ .context = owner, .release = Owner.release };
    return output;
}
pub fn nextTyped(handle: contract.Handle, alloc: A, io: ?std.Io, budget: Rows.Budget) !Rows.Page {
    var page = try (try handle.resolve(.relational, Relational)).nextTypedPage(alloc, io, budget);
    errdefer page.deinit();
    return ownPage(page, alloc);
}
pub fn nextView(handle: contract.Handle, alloc: A, limit: u32) !View.Page {
    var page = try (try handle.resolve(.relational, Relational)).nextViewPage(alloc, limit);
    errdefer page.deinit();
    return ownPage(page, alloc);
}
pub fn nextDocument(handle: contract.Handle, alloc: A, limit: u32) !View.Page {
    var page = try (try handle.resolve(.document, Document)).next(alloc, limit);
    errdefer page.deinit();
    return ownPage(page, alloc);
}

const ReaderOwner = struct { alloc: A, value: Reader };
fn retainReader(alloc: A, value: Reader) !contract.Handle {
    var owned = value;
    errdefer owned.deinit();
    const box = try alloc.create(ReaderOwner);
    box.* = .{ .alloc = alloc, .value = owned };
    return issue(.reader, box);
}
pub fn beginReader(db: anytype, alloc: A, request: ReaderValues.Request) !contract.Handle {
    return retainReader(alloc, try db.beginRelationalRows(alloc, request));
}
pub fn readerUnderLease(lease: anytype, alloc: A, fields: []const []const u8, version: u32) !contract.Handle {
    return retainReader(alloc, try lease.relationalRows(alloc, fields, version));
}
pub fn closeReader(handle: contract.Handle) !void {
    const box = try handle.resolve(.reader, ReaderOwner);
    const alloc = box.alloc;
    box.value.deinit();
    alloc.destroy(box);
}
pub fn readerNextPage(handle: contract.Handle, alloc: A, io: ?std.Io, budget: Rows.Budget) !Rows.Page {
    var page = try (try handle.resolve(.reader, ReaderOwner)).value.nextPage(alloc, io, budget);
    errdefer page.deinit();
    return ownPage(page, alloc);
}
pub fn readerNextTyped(handle: contract.Handle, alloc: A, io: ?std.Io, budget: Rows.Budget) !Rows.Page {
    var page = try (try handle.resolve(.reader, ReaderOwner)).value.nextTypedPage(alloc, io, budget);
    errdefer page.deinit();
    return ownPage(page, alloc);
}
pub fn readerLookupBatch(handle: contract.Handle, alloc: A, keys: []const []const u8, layout: *const ReaderValues.TypedLayout, max_bytes: usize) !ReaderValues.Hydrated {
    const result = try (try handle.resolve(.reader, ReaderOwner)).value.lookupSqlBatch(alloc, keys, layout, max_bytes);
    for (result.rows) |row| if (row) |value| try validateExportedRow(value);
    return result;
}
pub fn readerLookupOne(handle: contract.Handle, alloc: A, key: []const u8, native_cells: bool, shared: ?*?*const ReaderValues.TypedLayout) !?Rows.Row {
    const reader = &(try handle.resolve(.reader, ReaderOwner)).value;
    const result = try if (native_cells) if (shared) |layout| reader.lookupSqlRowWithLayout(alloc, key, layout) else reader.lookupSqlRow(alloc, key) else reader.lookupTypedRow(alloc, key);
    if (result) |row| try validateExportedRow(row);
    return result;
}

const owner_c_allocator = std.heap.c_allocator;
fn issue(comptime kind: contract.Kind, value: anytype) contract.Handle {
    return .issue(kind, value, switch (kind) {
        .statement => &statement_ops,
        .statement_fence => &fence_ops,
        .relational => &relational_ops,
        .document => &document_ops,
        .reader => &reader_ops,
        else => unreachable,
    }, contract.Boundary.local_dispatch, &owner_c_allocator);
}
const Outcome = @import("read_session_types").types.contract.NativeOutcome;
const statement_ops: contract.Operations = .{ .close = Adapters.closeStatement };
const relational_ops: contract.Operations = .{ .metadata = Adapters.metadata, .set_read_time = Adapters.setReadTime, .next_page = Adapters.relationalNextPage, .seek_prefix = Adapters.seekPrefix, .visit_cypher = Adapters.visitCypher, .close = Adapters.closeRelational, .checkpoint = Adapters.checkpoint, .range_proofs = Adapters.relationalProofs, .normalize = Adapters.normalizeRelational, .next_typed = Adapters.nextTyped, .next_view = Adapters.nextView };
const reader_ops: contract.Operations = .{ .reader_seek_prefix = Adapters.readerSeekPrefix, .close = Adapters.closeReader, .next_page = Adapters.readerNextPage, .next_typed = Adapters.readerNextTyped, .lookup_batch = Adapters.readerLookupBatch, .lookup_one = Adapters.readerLookupOne };
const document_ops: contract.Operations = .{ .close = Adapters.closeDocument, .range_proofs = Adapters.documentProofs, .normalize = Adapters.normalizeDocument, .next_view = Adapters.nextDocument };
const provider = @This();
const Adapters = struct {
    fn metadata(h: contract.Handle) Outcome(Rows.RelationalMetadata) {
        const session = h.resolve(.relational, Relational) catch |err| return failure(Rows.RelationalMetadata, err);
        return .{ .value = session.readMetadata() };
    }
    fn setReadTime(h: contract.Handle, now: u64) Outcome(void) {
        const session = h.resolve(.relational, Relational) catch |err| return failure(void, err);
        session.setReadTime(now) catch |err| return failure(void, err);
        return .{ .value = {} };
    }
    fn relationalNextPage(h: contract.Handle, alloc: A, io: ?contract.IoBorrow, budget: Rows.Budget) Outcome(Rows.Page) {
        var receiver: ?contract.IoReceiver = if (io) |borrow| borrow.receive() catch |err| return failure(Rows.Page, err) else null;
        const session = h.resolve(.relational, Relational) catch |err| return failure(Rows.Page, err);
        var page = session.nextPage(alloc, if (receiver) |*value| value.io() else null, budget) catch |err| return failure(Rows.Page, err);
        return .{ .value = ownPage(page, alloc) catch |err| {
            page.deinit();
            return failure(Rows.Page, err);
        } };
    }
    fn seekPrefix(h: contract.Handle, values: []const std.json.Value, after: ?[]const u8) Outcome(void) {
        const session = h.resolve(.relational, Relational) catch |err| return failure(void, err);
        session.seekPrefix(values, after) catch |err| return failure(void, err);
        return .{ .value = {} };
    }
    fn readerSeekPrefix(h: contract.Handle, values: []const ReaderValues.Value, after: ?[]const u8) Outcome(void) {
        const owner = h.resolve(.reader, ReaderOwner) catch |err| return failure(void, err);
        owner.value.seekPrefix(values, after) catch |err| return failure(void, err);
        return .{ .value = {} };
    }
    fn visitCypher(h: contract.Handle, alloc: A, limit: usize, visitor: types.ScanVisitor) Outcome(void) {
        const session = h.resolve(.relational, Relational) catch |err| return failure(void, err);
        session.visitCypherRows(alloc, limit, visitor) catch |err| return failure(void, err);
        return .{ .value = {} };
    }
    fn closeReader(h: contract.Handle) Outcome(void) {
        return .{ .value = provider.closeReader(h) catch |err| return failure(void, err) };
    }
    fn readerNextPage(h: contract.Handle, a: A, io: ?contract.IoBorrow, budget: Rows.Budget) Outcome(Rows.Page) {
        var receiver: ?contract.IoReceiver = if (io) |borrow| borrow.receive() catch |err| return failure(Rows.Page, err) else null;
        return .{ .value = provider.readerNextPage(h, a, if (receiver) |*value| value.io() else null, budget) catch |err| return failure(Rows.Page, err) };
    }
    fn readerNextTyped(h: contract.Handle, a: A, io: ?contract.IoBorrow, budget: Rows.Budget) Outcome(Rows.Page) {
        var receiver: ?contract.IoReceiver = if (io) |borrow| borrow.receive() catch |err| return failure(Rows.Page, err) else null;
        return .{ .value = provider.readerNextTyped(h, a, if (receiver) |*value| value.io() else null, budget) catch |err| return failure(Rows.Page, err) };
    }
    fn readerLookupBatch(h: contract.Handle, a: A, keys: []const []const u8, layout: *const ReaderValues.TypedLayout, max_bytes: usize) Outcome(ReaderValues.Hydrated) {
        return .{ .value = provider.readerLookupBatch(h, a, keys, layout, max_bytes) catch |err| return failure(ReaderValues.Hydrated, err) };
    }
    fn readerLookupOne(h: contract.Handle, a: A, key: []const u8, native_cells: bool, shared: ?*?*const ReaderValues.TypedLayout) Outcome(?Rows.Row) {
        return .{ .value = provider.readerLookupOne(h, a, key, native_cells, shared) catch |err| return failure(?Rows.Row, err) };
    }
    fn closeStatement(h: contract.Handle) Outcome(void) {
        return .{ .value = provider.closeStatement(h) catch |err| return failure(void, err) };
    }
    fn closeRelational(h: contract.Handle) Outcome(void) {
        return .{ .value = provider.closeRelational(h) catch |err| return failure(void, err) };
    }
    fn closeDocument(h: contract.Handle) Outcome(void) {
        return .{ .value = provider.closeDocument(h) catch |err| return failure(void, err) };
    }
    fn checkpoint(h: contract.Handle) Outcome(void) {
        return .{ .value = provider.checkpoint(h) catch |err| return failure(void, err) };
    }
    fn relationalProofs(h: contract.Handle, a: A) Outcome([]Types.range_protection.Proof) {
        return .{ .value = provider.relationalProofs(h, a) catch |err| return failure([]Types.range_protection.Proof, err) };
    }
    fn documentProofs(h: contract.Handle, a: A) Outcome([]Types.range_protection.Proof) {
        return .{ .value = provider.documentProofs(h, a) catch |err| return failure([]Types.range_protection.Proof, err) };
    }
    fn normalizeRelational(h: contract.Handle, a: A, writes: []const types.BatchWrite) Outcome([]types.BatchWrite) {
        return .{ .value = provider.normalizeRelational(h, a, writes) catch |err| return failure([]types.BatchWrite, err) };
    }
    fn normalizeDocument(h: contract.Handle, a: A, writes: []const types.BatchWrite) Outcome([]types.BatchWrite) {
        return .{ .value = provider.normalizeDocument(h, a, writes) catch |err| return failure([]types.BatchWrite, err) };
    }
    fn nextTyped(h: contract.Handle, a: A, io: ?contract.IoBorrow, budget: Rows.Budget) Outcome(Rows.Page) {
        var receiver: ?contract.IoReceiver = if (io) |borrow| borrow.receive() catch |err| return failure(Rows.Page, err) else null;
        return .{ .value = provider.nextTyped(h, a, if (receiver) |*value| value.io() else null, budget) catch |err| return failure(Rows.Page, err) };
    }
    fn nextView(h: contract.Handle, a: A, limit: u32) Outcome(View.Page) {
        return .{ .value = provider.nextView(h, a, limit) catch |err| return failure(View.Page, err) };
    }
    fn nextDocument(h: contract.Handle, a: A, limit: u32) Outcome(View.Page) {
        return .{ .value = provider.nextDocument(h, a, limit) catch |err| return failure(View.Page, err) };
    }
};
fn failure(comptime T: type, err: anyerror) Outcome(T) {
    return .{ .failure = .init(@errorName(err)) };
}

/// The complete lease remains allocated with the DB owner. Its apply fence,
/// foreground-query admission and row-policy lease are released together.
pub fn beginQueryLease(db: anytype) !@TypeOf(db.*).QueryReadLease.Protocol.Handle {
    const DB = @TypeOf(db.*);
    const Protocol = DB.QueryReadLease.Protocol;
    const Owner = struct {
        alloc: A,
        value: DB.QueryReadLease,
        fn close(handle: Protocol.Handle) contract.NativeOutcome(void) {
            const self = handle.resolve(@This()) catch |err| return .{ .failure = .init(@errorName(err)) };
            const alloc = self.alloc;
            self.value.release();
            alloc.destroy(self);
            return .{ .value = {} };
        }
        fn reader(handle: Protocol.Handle, alloc: A, fields: []const []const u8, version: u32) contract.NativeOutcome(contract.Handle) {
            const self = handle.resolve(@This()) catch |err| return .{ .failure = .init(@errorName(err)) };
            return .{ .value = readerUnderLease(&self.value, alloc, fields, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
        }
        fn documents(handle: Protocol.Handle, alloc: A, version: u32) contract.NativeOutcome(contract.Handle) {
            const self = handle.resolve(@This()) catch |err| return .{ .failure = .init(@errorName(err)) };
            return .{ .value = documentReaderUnderLease(&self.value, alloc, version) catch |err| return .{ .failure = .init(@errorName(err)) } };
        }
        fn search(handle: Protocol.Handle, alloc: A, req: types.SearchRequest) contract.NativeOutcome(DB.QueryReadLease.Result) {
            const self = handle.resolve(@This()) catch |err| return .{ .failure = .init(@errorName(err)) };
            var source = req.native_key_predicate;
            var received = req;
            if (source) |*predicate| received.native_key_predicate = predicate.receiver();
            var value = self.value.search(alloc, received) catch |err| return .{ .failure = .init(@errorName(err)) };
            // The receiver is call-scoped; captured requests keep the original.
            value.request.native_key_predicate = req.native_key_predicate;
            return .{ .value = value };
        }
        const operations: Protocol.Operations = .{ .close = close, .reader = reader, .documents = documents, .search = search };
        const owner_c_allocator = std.heap.c_allocator;
    };
    const owner = try db.alloc.create(Owner);
    errdefer db.alloc.destroy(owner);
    owner.* = .{ .alloc = db.alloc, .value = try db.beginQueryReadLease() };
    return .{ .context = owner, .operations = &Owner.operations, .dispatch = Protocol.Boundary.local_dispatch, .owner_allocator = &Owner.owner_c_allocator };
}

/// Borrowed document batch windows stay in the owner until the next operation,
/// explicit page release or close, preserving the source API lifetime contract.
pub fn documentReaderUnderLease(lease: anytype, alloc: A, version: u32) !contract.Handle {
    const Value = @TypeOf(try lease.documentRows(alloc, version));
    const Owner = struct {
        alloc: A,
        value: Value,
        fn close(h: contract.Handle) Outcome(void) {
            const self = h.resolve(.document_reader, @This()) catch |err| return failure(void, err);
            const a = self.alloc;
            self.value.deinit();
            a.destroy(self);
            return .{ .value = {} };
        }
        fn release(h: contract.Handle) Outcome(void) {
            const self = h.resolve(.document_reader, @This()) catch |err| return failure(void, err);
            self.value.releasePage();
            return .{ .value = {} };
        }
        fn one(h: contract.Handle, key: []const u8) Outcome([]const u8) {
            const self = h.resolve(.document_reader, @This()) catch |err| return failure([]const u8, err);
            return .{ .value = self.value.lookup(key) catch |err| return failure([]const u8, err) };
        }
        fn batch(h: contract.Handle, keys: []const []const u8, max: usize) Outcome(Types.document_values.Batch) {
            const self = h.resolve(.document_reader, @This()) catch |err| return failure(Types.document_values.Batch, err);
            return .{ .value = self.value.lookupBatch(keys, max) catch |err| return failure(Types.document_values.Batch, err) };
        }
        fn projected(h: contract.Handle, keys: []const []const u8, fields: []const []const u8, max: usize) Outcome(Types.document_values.ProjectedBatch) {
            const self = h.resolve(.document_reader, @This()) catch |err| return failure(Types.document_values.ProjectedBatch, err);
            return .{ .value = self.value.lookupProjectedBatch(keys, fields, max) catch |err| return failure(Types.document_values.ProjectedBatch, err) };
        }
        const operations: contract.Operations = .{ .close = close, .release_page = release, .lookup_document = one, .lookup_documents = batch, .lookup_projected_documents = projected };
    };
    const owner = try alloc.create(Owner);
    errdefer alloc.destroy(owner);
    owner.* = .{ .alloc = alloc, .value = try lease.documentRows(alloc, version) };
    return .issue(.document_reader, owner, &Owner.operations, contract.Boundary.local_dispatch, &owner_c_allocator);
}

fn fenceClose(handle: contract.Handle) contract.NativeOutcome(void) {
    return .{ .value = closeFence(handle) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
fn fenceCapture(handle: contract.Handle) contract.NativeOutcome(contract.Statement) {
    const owner = handle.resolve(.statement_fence, FenceOwner) catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = owner.capture(owner) catch |err| return .{ .failure = .init(@errorName(err)) } };
}
const fence_ops: contract.Operations = .{ .close = fenceClose, .capture_statement = fenceCapture };
