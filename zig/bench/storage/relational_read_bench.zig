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
const antfly = @import("antfly_zig");

const db_mod = antfly.db;
const db_types = db_mod.types;
const platform_time = antfly.platform_time;
const resource_manager_mod = antfly.resource_manager;
const storage_schema = antfly.schema;
const table_schema = antfly.table_schema;

const jsonl_schema_version: u32 = 1;

const Config = struct {
    samples: usize = 1,
    rows: usize = 10_000,
    repeats: usize = 3,
    batch_size: usize = 1000,
    limit: u32 = 100,
    low_selectivity_ppm: u32 = 100_000,
    high_selectivity_ppm: u32 = 900_000,
    mode: ?IndexMode = null,
    selectivity: ?SelectivityBand = null,
    shape: ?QueryShape = null,
    total_mode: ?TotalModeCase = null,
};

const IndexMode = enum {
    document_table,
    no_index,
    scalar_index,
    ordered_tuple,

    fn label(self: IndexMode) []const u8 {
        return switch (self) {
            .document_table => "document_table",
            .no_index => "no_index",
            .scalar_index => "scalar_index",
            .ordered_tuple => "ordered_tuple",
        };
    }

    fn isRelational(self: IndexMode) bool {
        return switch (self) {
            .document_table => false,
            .no_index, .scalar_index, .ordered_tuple => true,
        };
    }

    fn parse(value: []const u8) ?IndexMode {
        if (std.mem.eql(u8, value, "document_table")) return .document_table;
        if (std.mem.eql(u8, value, "no_index")) return .no_index;
        if (std.mem.eql(u8, value, "scalar_index")) return .scalar_index;
        if (std.mem.eql(u8, value, "ordered_tuple")) return .ordered_tuple;
        return null;
    }
};

const SelectivityBand = enum {
    low,
    high,

    fn label(self: SelectivityBand) []const u8 {
        return switch (self) {
            .low => "low",
            .high => "high",
        };
    }

    fn status(self: SelectivityBand) []const u8 {
        return switch (self) {
            .low => "low",
            .high => "high",
        };
    }

    fn parse(value: []const u8) ?SelectivityBand {
        if (std.mem.eql(u8, value, "low")) return .low;
        if (std.mem.eql(u8, value, "high")) return .high;
        return null;
    }
};

const QueryShape = enum {
    filter_page,
    ordered_page,

    fn label(self: QueryShape) []const u8 {
        return switch (self) {
            .filter_page => "filter_page",
            .ordered_page => "ordered_page",
        };
    }

    fn parse(value: []const u8) ?QueryShape {
        if (std.mem.eql(u8, value, "filter_page")) return .filter_page;
        if (std.mem.eql(u8, value, "ordered_page")) return .ordered_page;
        return null;
    }
};

const TotalModeCase = enum {
    exact,
    bounded,
    none,
    count_only,

    fn label(self: TotalModeCase) []const u8 {
        return switch (self) {
            .exact => "exact",
            .bounded => "bounded",
            .none => "none",
            .count_only => "count_only",
        };
    }

    fn requestTotalMode(self: TotalModeCase) db_types.RelationalRowsQueryRequest.TotalMode {
        return switch (self) {
            .exact, .count_only => .exact,
            .bounded => .bounded,
            .none => .none,
        };
    }

    fn requestLimit(self: TotalModeCase, cfg: Config) u32 {
        return switch (self) {
            .count_only => 0,
            else => cfg.limit,
        };
    }

    fn parse(value: []const u8) ?TotalModeCase {
        if (std.mem.eql(u8, value, "exact")) return .exact;
        if (std.mem.eql(u8, value, "bounded")) return .bounded;
        if (std.mem.eql(u8, value, "none")) return .none;
        if (std.mem.eql(u8, value, "count_only")) return .count_only;
        return null;
    }
};

const PlanClass = enum {
    stream,
    doc_set,
    base_scan,
    unknown,

    fn label(self: PlanClass) []const u8 {
        return switch (self) {
            .stream => "stream",
            .doc_set => "doc_set",
            .base_scan => "base_scan",
            .unknown => "unknown",
        };
    }
};

fn planClass(access_method: db_types.RelationalRowsQueryResult.AccessMethod) PlanClass {
    return switch (access_method) {
        .ordered_tuple_stream => .stream,
        .resolved_doc_set,
        .scalar_doc_set,
        .array_doc_set,
        .json_doc_set,
        .text_search_doc_set,
        .mixed_doc_set,
        .ordered_tuple_doc_set,
        => .doc_set,
        .base_scan => .base_scan,
        .unknown => .unknown,
    };
}

fn optionalLabel(comptime T: type, value: ?T) []const u8 {
    return if (value) |item| item.label() else "all";
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.smp_allocator;
    const cfg = try parseArgs(alloc, init.minimal.args);

    var stdout_buffer: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;

    try out.print(
        "{{\"event\":\"relational_read_bench_config\",\"schema_version\":{d},\"samples\":{d},\"rows\":{d},\"repeats\":{d},\"batch_size\":{d},\"limit\":{d},\"low_selectivity_ppm\":{d},\"high_selectivity_ppm\":{d},\"mode\":\"{s}\",\"selectivity\":\"{s}\",\"shape\":\"{s}\",\"total_mode\":\"{s}\"}}\n",
        .{
            jsonl_schema_version,
            cfg.samples,
            cfg.rows,
            cfg.repeats,
            cfg.batch_size,
            cfg.limit,
            cfg.low_selectivity_ppm,
            cfg.high_selectivity_ppm,
            optionalLabel(IndexMode, cfg.mode),
            optionalLabel(SelectivityBand, cfg.selectivity),
            optionalLabel(QueryShape, cfg.shape),
            optionalLabel(TotalModeCase, cfg.total_mode),
        },
    );

    const modes = [_]IndexMode{ .document_table, .no_index, .scalar_index, .ordered_tuple };
    const bands = [_]SelectivityBand{ .low, .high };
    const shapes = [_]QueryShape{ .filter_page, .ordered_page };
    const total_modes = [_]TotalModeCase{ .exact, .bounded, .none, .count_only };

    for (modes) |mode| {
        if (cfg.mode) |wanted| {
            if (mode != wanted) continue;
        }
        for (0..cfg.samples) |sample| {
            var bench_db = try openBenchDb(alloc, mode);
            defer bench_db.deinit(alloc);

            const load_start_ns = nanotime();
            try loadRows(alloc, &bench_db.db, cfg);
            const load_ns = nanotime() - load_start_ns;
            try out.print(
                "{{\"event\":\"relational_read_bench_load\",\"schema_version\":{d},\"mode\":\"{s}\",\"sample\":{d},\"rows\":{d},\"load_ns\":{d}}}\n",
                .{ jsonl_schema_version, mode.label(), sample, cfg.rows, load_ns },
            );
            try stdout_writer.flush();

            for (bands) |band| {
                if (cfg.selectivity) |wanted| {
                    if (band != wanted) continue;
                }
                for (shapes) |shape| {
                    if (cfg.shape) |wanted| {
                        if (shape != wanted) continue;
                    }
                    for (total_modes) |total_case| {
                        if (cfg.total_mode) |wanted| {
                            if (total_case != wanted) continue;
                        }
                        if (total_case == .count_only and shape != .filter_page) continue;
                        const result = try runCase(alloc, &bench_db.db, bench_db.schema, cfg, mode, band, shape, total_case);
                        try printResult(out, cfg, sample, mode, band, shape, total_case, result);
                    }
                }
            }
            try stdout_writer.flush();
        }
    }
}

const BenchDb = struct {
    path: []u8,
    db: db_mod.DB,
    schema: storage_schema.TableSchema,
    resource_manager: *resource_manager_mod.ResourceManager,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.db.close();
        alloc.destroy(self.resource_manager);
        storage_schema.freeSchema(alloc, self.schema);
        cleanupPath(self.path);
        alloc.free(self.path);
        self.* = undefined;
    }
};

fn openBenchDb(alloc: std.mem.Allocator, mode: IndexMode) !BenchDb {
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-relational-read-bench-{s}-{d}", .{ mode.label(), nanotime() });
    errdefer alloc.free(path);
    cleanupPath(path);
    errdefer cleanupPath(path);

    const resource_manager = try alloc.create(resource_manager_mod.ResourceManager);
    resource_manager.* = resource_manager_mod.ResourceManager.init(.{});
    errdefer alloc.destroy(resource_manager);

    var db = try db_mod.DB.open(alloc, path, .{
        .primary_backend = .{ .lsm_memory = .{} },
        .start_index_workers = false,
        .resource_manager = resource_manager,
    });
    errdefer db.close();

    var parsed_schema = try table_schema.parseValidatedTableSchema(alloc, schemaJson(mode));
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema.deriveRuntimeTableSchema(alloc, parsed_schema);
    errdefer storage_schema.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    return .{
        .path = path,
        .db = db,
        .schema = runtime_schema,
        .resource_manager = resource_manager,
    };
}

fn schemaJson(mode: IndexMode) []const u8 {
    return switch (mode) {
        .document_table =>
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"additionalProperties":true}}}}
        ,
        .no_index =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
        ,
        .scalar_index =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_scalar_idx","owner_kind":"relational_column","owner_name":"status","access_method":"scalar_column","columns":["status"]}]}
        ,
        .ordered_tuple =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"relational_indexes":[{"name":"status_amount_tuple_idx","owner_kind":"relational_column","owner_name":"status","access_method":"ordered_tuple","columns":["status"],"keys":[{"column":"status"},{"column":"amount"}],"include_columns":["id","amount"],"lifecycle":"ready","generation":7,"schema_fingerprint":"secondary-index-v1:status_amount_tuple_idx","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
        ,
    };
}

fn loadRows(alloc: std.mem.Allocator, db: *db_mod.DB, cfg: Config) !void {
    const low_rows = selectivityRows(cfg.rows, cfg.low_selectivity_ppm);
    const high_rows = selectivityRows(cfg.rows, cfg.high_selectivity_ppm);
    if (low_rows + high_rows > cfg.rows) return error.InvalidArgument;

    var next_row: usize = 0;
    while (next_row < cfg.rows) {
        const batch_len = @min(cfg.batch_size, cfg.rows - next_row);
        const writes = try alloc.alloc(db_types.BatchWrite, batch_len);
        defer {
            for (writes) |write| {
                alloc.free(write.key);
                alloc.free(write.value);
            }
            alloc.free(writes);
        }

        for (writes, 0..) |*write, i| {
            const row_index = next_row + i;
            const status = rowStatus(row_index, low_rows, high_rows);
            write.* = .{
                .key = try std.fmt.allocPrint(alloc, "row:{d:0>9}", .{row_index}),
                .value = try std.fmt.allocPrint(
                    alloc,
                    "{{\"id\":\"{d:0>9}\",\"status\":\"{s}\",\"amount\":{d}}}",
                    .{ row_index, status, row_index },
                ),
            };
        }
        try db.batch(.{ .writes = writes, .sync_level = .write });
        next_row += batch_len;
    }
}

fn rowStatus(row_index: usize, low_rows: usize, high_rows: usize) []const u8 {
    if (row_index < low_rows) return "low";
    if (row_index < low_rows + high_rows) return "high";
    return "cold";
}

fn selectivityRows(rows: usize, ppm: u32) usize {
    return @intCast((@as(u128, rows) * @as(u128, ppm)) / 1_000_000);
}

const BenchResult = struct {
    repeats: usize,
    rows_returned: u64,
    total_sum: u64,
    checksum: u64,
    ns: u64,
    last_total_exact: bool,
    access_method: db_types.RelationalRowsQueryResult.AccessMethod,
    fallback_reason: db_types.RelationalRowsQueryResult.FallbackReason,
    index_entries_scanned: u64,
    candidate_rows: u64,
    hydrated_rows: u64,
    projected_rows: u64,
    candidate_stream_emitted: u64,
    retained_candidate_rows: u64,
    base_scan_rows: u64,
    candidate_gate_limit: u64,
    candidate_gate_observed: u64,
    candidate_gate_exceeded: bool,
    selected_candidate_selectivity_ppm: u64,
    ordered_tuple_probe_selectivity_ppm: u64,
    covering_payload_rows: u64,
    covering_payload_hydration_avoided_rows: u64,
};

fn runCase(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    runtime_schema: storage_schema.TableSchema,
    cfg: Config,
    mode: IndexMode,
    band: SelectivityBand,
    shape: QueryShape,
    total_case: TotalModeCase,
) !BenchResult {
    if (!mode.isRelational()) {
        return try runDocumentCase(alloc, db, cfg, band, shape, total_case);
    }

    const status_json = switch (band) {
        .low => "\"low\"",
        .high => "\"high\"",
    };
    const predicates = [_]storage_schema.RelationalCheck{.{
        .name = "",
        .field = "status",
        .op = .eq,
        .value_json = status_json,
    }};
    const select = [_][]const u8{ "id", "amount" };
    const order_by = [_]db_types.RelationalRowsQueryOrder{.{
        .field = "amount",
        .direction = .asc,
    }};

    var out = BenchResult{
        .repeats = cfg.repeats,
        .rows_returned = 0,
        .total_sum = 0,
        .checksum = 0,
        .ns = 0,
        .last_total_exact = false,
        .access_method = .unknown,
        .fallback_reason = .none,
        .index_entries_scanned = 0,
        .candidate_rows = 0,
        .hydrated_rows = 0,
        .projected_rows = 0,
        .candidate_stream_emitted = 0,
        .retained_candidate_rows = 0,
        .base_scan_rows = 0,
        .candidate_gate_limit = 0,
        .candidate_gate_observed = 0,
        .candidate_gate_exceeded = false,
        .selected_candidate_selectivity_ppm = 0,
        .ordered_tuple_probe_selectivity_ppm = 0,
        .covering_payload_rows = 0,
        .covering_payload_hydration_avoided_rows = 0,
    };

    const start_ns = nanotime();
    for (0..cfg.repeats) |_| {
        var result = try db.queryRelationalRows(alloc, runtime_schema, .{
            .predicates = predicates[0..],
            .select = select[0..],
            .select_all = false,
            .order_by = if (shape == .ordered_page) order_by[0..] else &.{},
            .limit = total_case.requestLimit(cfg),
            .total_mode = total_case.requestTotalMode(),
            .profile = true,
        });
        defer result.deinit(alloc);

        out.rows_returned += result.rows.len;
        out.total_sum += result.total;
        out.last_total_exact = result.total_exact;
        out.access_method = result.profile.access_method;
        out.fallback_reason = result.profile.fallback_reason;
        out.index_entries_scanned += result.profile.index_entries_scanned;
        out.candidate_rows += result.profile.candidate_rows;
        out.hydrated_rows += result.profile.hydrated_rows;
        out.projected_rows += result.profile.projected_rows;
        out.candidate_stream_emitted += result.profile.candidate_stream_emitted;
        out.retained_candidate_rows += result.profile.retained_candidate_rows;
        out.base_scan_rows = @max(out.base_scan_rows, result.profile.base_scan_rows);
        out.candidate_gate_limit = @max(out.candidate_gate_limit, result.profile.candidate_gate_limit);
        out.candidate_gate_observed = @max(out.candidate_gate_observed, result.profile.candidate_gate_observed);
        out.candidate_gate_exceeded = out.candidate_gate_exceeded or result.profile.candidate_gate_exceeded;
        out.selected_candidate_selectivity_ppm = @max(out.selected_candidate_selectivity_ppm, result.profile.selected_candidate_selectivity_ppm);
        out.ordered_tuple_probe_selectivity_ppm = @max(out.ordered_tuple_probe_selectivity_ppm, result.profile.ordered_tuple_probe_selectivity_ppm);
        out.covering_payload_rows += result.profile.covering_payload_rows;
        out.covering_payload_hydration_avoided_rows += result.profile.covering_payload_hydration_avoided_rows;
        out.checksum +%= result.total;
        for (result.rows) |row| {
            out.checksum +%= std.hash.Wyhash.hash(0, row);
        }
    }
    out.ns = nanotime() - start_ns;
    return out;
}

const DocumentCandidate = struct {
    amount: u64,
    row_json: []u8,
};

fn runDocumentCase(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    cfg: Config,
    band: SelectivityBand,
    shape: QueryShape,
    total_case: TotalModeCase,
) !BenchResult {
    var out = BenchResult{
        .repeats = cfg.repeats,
        .rows_returned = 0,
        .total_sum = 0,
        .checksum = 0,
        .ns = 0,
        .last_total_exact = false,
        .access_method = .base_scan,
        .fallback_reason = .none,
        .index_entries_scanned = 0,
        .candidate_rows = 0,
        .hydrated_rows = 0,
        .projected_rows = 0,
        .candidate_stream_emitted = 0,
        .retained_candidate_rows = 0,
        .base_scan_rows = 0,
        .candidate_gate_limit = 0,
        .candidate_gate_observed = 0,
        .candidate_gate_exceeded = false,
        .selected_candidate_selectivity_ppm = 0,
        .ordered_tuple_probe_selectivity_ppm = 0,
        .covering_payload_rows = 0,
        .covering_payload_hydration_avoided_rows = 0,
    };

    const start_ns = nanotime();
    for (0..cfg.repeats) |_| {
        var scan = try db.scan(alloc, "row:", "row:\xff", .{
            .include_documents = true,
            .include_all_fields = true,
        });
        defer scan.deinit(alloc);

        out.base_scan_rows = @max(out.base_scan_rows, scan.documents.len);
        out.hydrated_rows += scan.documents.len;

        var page_rows = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (page_rows.items) |row| alloc.free(row);
            page_rows.deinit(alloc);
        }

        var ordered_candidates = std.ArrayListUnmanaged(DocumentCandidate).empty;
        defer {
            for (ordered_candidates.items) |candidate| alloc.free(candidate.row_json);
            ordered_candidates.deinit(alloc);
        }

        const exact_total = total_case == .exact or total_case == .count_only;
        var exact_matches: u64 = 0;
        for (scan.documents) |doc| {
            if (!try documentMatchesBand(alloc, doc.json, band)) continue;
            exact_matches += 1;
            out.candidate_rows += 1;
            out.candidate_stream_emitted += 1;

            if (total_case == .count_only) continue;

            const projected = try projectDocumentBenchRowAlloc(alloc, doc.json);
            if (shape == .ordered_page) {
                errdefer alloc.free(projected.row_json);
                try ordered_candidates.append(alloc, .{
                    .amount = projected.amount,
                    .row_json = projected.row_json,
                });
            } else if (total_case.requestLimit(cfg) > 0 and page_rows.items.len < total_case.requestLimit(cfg)) {
                errdefer alloc.free(projected.row_json);
                try page_rows.append(alloc, projected.row_json);
            } else {
                alloc.free(projected.row_json);
            }

            if (!exact_total and shape == .filter_page and page_rows.items.len >= total_case.requestLimit(cfg)) break;
        }

        if (shape == .ordered_page) {
            std.mem.sort(DocumentCandidate, ordered_candidates.items, {}, documentCandidateAmountLessThan);
            const page_len = @min(@as(usize, total_case.requestLimit(cfg)), ordered_candidates.items.len);
            for (ordered_candidates.items[0..page_len]) |*candidate| {
                const row_json = candidate.row_json;
                candidate.row_json = "";
                errdefer alloc.free(row_json);
                try page_rows.append(alloc, row_json);
            }
        }

        out.retained_candidate_rows += switch (shape) {
            .filter_page => page_rows.items.len,
            .ordered_page => ordered_candidates.items.len,
        };
        out.projected_rows += page_rows.items.len;
        out.rows_returned += page_rows.items.len;
        const reported_total = if (exact_total) exact_matches else page_rows.items.len;
        out.total_sum += reported_total;
        out.last_total_exact = exact_total;
        out.checksum +%= reported_total;
        for (page_rows.items) |row| {
            out.checksum +%= std.hash.Wyhash.hash(0, row);
        }
    }
    out.ns = nanotime() - start_ns;
    return out;
}

const ProjectedDocumentBenchRow = struct {
    amount: u64,
    row_json: []u8,
};

fn projectDocumentBenchRowAlloc(alloc: std.mem.Allocator, json: []const u8) !ProjectedDocumentBenchRow {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    const id = object.get("id") orelse return error.InvalidArgument;
    const amount_value = object.get("amount") orelse return error.InvalidArgument;
    if (id != .string) return error.InvalidArgument;
    const amount = jsonNumberAsU64(amount_value) orelse return error.InvalidArgument;
    return .{
        .amount = amount,
        .row_json = try std.fmt.allocPrint(alloc, "{{\"id\":\"{s}\",\"amount\":{d}}}", .{ id.string, amount }),
    };
}

fn documentMatchesBand(alloc: std.mem.Allocator, json: []const u8, band: SelectivityBand) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    defer parsed.deinit();
    const status = parsed.value.object.get("status") orelse return false;
    if (status != .string) return false;
    return std.mem.eql(u8, status.string, band.status());
}

fn jsonNumberAsU64(value: std.json.Value) ?u64 {
    return switch (value) {
        .integer => |integer| if (integer >= 0) @intCast(integer) else null,
        .float => |float| if (float >= 0 and @floor(float) == float and float <= @as(f64, @floatFromInt(std.math.maxInt(u64)))) @intFromFloat(float) else null,
        else => null,
    };
}

fn documentCandidateAmountLessThan(_: void, lhs: DocumentCandidate, rhs: DocumentCandidate) bool {
    return lhs.amount < rhs.amount;
}

fn printResult(
    writer: anytype,
    cfg: Config,
    sample: usize,
    mode: IndexMode,
    band: SelectivityBand,
    shape: QueryShape,
    total_case: TotalModeCase,
    result: BenchResult,
) !void {
    const ns_per_query = @as(f64, @floatFromInt(result.ns)) / @as(f64, @floatFromInt(@max(result.repeats, 1)));
    const repeats_u64 = @as(u64, @intCast(@max(result.repeats, 1)));
    const rows_returned_per_query = result.rows_returned / repeats_u64;
    const total_per_query = result.total_sum / repeats_u64;
    const index_entries_scanned_per_query = result.index_entries_scanned / repeats_u64;
    const candidate_rows_per_query = result.candidate_rows / repeats_u64;
    const hydrated_rows_per_query = result.hydrated_rows / repeats_u64;
    const projected_rows_per_query = result.projected_rows / repeats_u64;
    const stream_emitted_per_query = result.candidate_stream_emitted / repeats_u64;
    const retained_candidate_rows_per_query = result.retained_candidate_rows / repeats_u64;
    const covering_payload_rows_per_query = result.covering_payload_rows / repeats_u64;
    const hydration_avoided_per_query = result.covering_payload_hydration_avoided_rows / repeats_u64;
    try writer.print(
        "{{\"event\":\"relational_read_bench_result\",\"schema_version\":{d},\"sample\":{d},\"mode\":\"{s}\",\"rows\":{d},\"limit\":{d},\"selectivity\":\"{s}\",\"status\":\"{s}\",\"shape\":\"{s}\",\"total_mode\":\"{s}\",\"repeats\":{d},\"ns\":{d},\"ns_per_query\":{d:.2},\"rows_returned\":{d},\"rows_returned_per_query\":{d},\"total_sum\":{d},\"total_per_query\":{d},\"total_exact\":{},\"checksum\":{d}",
        .{
            jsonl_schema_version,
            sample,
            mode.label(),
            cfg.rows,
            cfg.limit,
            band.label(),
            band.status(),
            shape.label(),
            total_case.label(),
            result.repeats,
            result.ns,
            ns_per_query,
            result.rows_returned,
            rows_returned_per_query,
            result.total_sum,
            total_per_query,
            result.last_total_exact,
            result.checksum,
        },
    );
    try writer.print(
        ",\"access_method\":\"{s}\",\"plan_class\":\"{s}\",\"fallback_reason\":\"{s}\",\"index_entries_scanned\":{d},\"index_entries_scanned_per_query\":{d},\"candidate_rows\":{d},\"candidate_rows_per_query\":{d},\"hydrated_rows\":{d},\"hydrated_rows_per_query\":{d},\"projected_rows\":{d},\"projected_rows_per_query\":{d},\"candidate_stream_emitted\":{d},\"candidate_stream_emitted_per_query\":{d},\"retained_candidate_rows\":{d},\"retained_candidate_rows_per_query\":{d}",
        .{
            @tagName(result.access_method),
            planClass(result.access_method).label(),
            @tagName(result.fallback_reason),
            result.index_entries_scanned,
            index_entries_scanned_per_query,
            result.candidate_rows,
            candidate_rows_per_query,
            result.hydrated_rows,
            hydrated_rows_per_query,
            result.projected_rows,
            projected_rows_per_query,
            result.candidate_stream_emitted,
            stream_emitted_per_query,
            result.retained_candidate_rows,
            retained_candidate_rows_per_query,
        },
    );
    try writer.print(
        ",\"base_scan_rows\":{d},\"candidate_gate_limit\":{d},\"candidate_gate_observed\":{d},\"candidate_gate_exceeded\":{},\"selected_candidate_selectivity_ppm\":{d},\"ordered_tuple_probe_selectivity_ppm\":{d},\"covering_payload_rows\":{d},\"covering_payload_rows_per_query\":{d},\"covering_payload_hydration_avoided_rows\":{d},\"covering_payload_hydration_avoided_rows_per_query\":{d}}}\n",
        .{
            result.base_scan_rows,
            result.candidate_gate_limit,
            result.candidate_gate_observed,
            result.candidate_gate_exceeded,
            result.selected_candidate_selectivity_ppm,
            result.ordered_tuple_probe_selectivity_ppm,
            result.covering_payload_rows,
            covering_payload_rows_per_query,
            result.covering_payload_hydration_avoided_rows,
            hydration_avoided_per_query,
        },
    );
}

fn parseArgs(alloc: std.mem.Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--samples")) {
            cfg.samples = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--rows")) {
            cfg.rows = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--limit")) {
            cfg.limit = std.math.cast(u32, try parseNextUsize(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--low-selectivity-ppm")) {
            cfg.low_selectivity_ppm = std.math.cast(u32, try parseNextUsize(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--high-selectivity-ppm")) {
            cfg.high_selectivity_ppm = std.math.cast(u32, try parseNextUsize(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--mode")) {
            cfg.mode = IndexMode.parse(try parseNextString(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--selectivity")) {
            cfg.selectivity = SelectivityBand.parse(try parseNextString(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--shape")) {
            cfg.shape = QueryShape.parse(try parseNextString(&args, arg)) orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--total-mode")) {
            cfg.total_mode = TotalModeCase.parse(try parseNextString(&args, arg)) orelse return error.InvalidArgument;
        } else {
            return error.InvalidArgument;
        }
    }

    if (cfg.samples == 0 or cfg.rows == 0 or cfg.repeats == 0 or cfg.batch_size == 0 or cfg.limit == 0) return error.InvalidArgument;
    if (cfg.low_selectivity_ppm == 0 or cfg.high_selectivity_ppm == 0) return error.InvalidArgument;
    if (@as(u64, cfg.low_selectivity_ppm) + @as(u64, cfg.high_selectivity_ppm) > 1_000_000) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, value, 10);
}

fn parseNextString(args: *std.process.Args.Iterator, flag: []const u8) ![]const u8 {
    return args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
}

fn cleanupPath(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
}

fn nanotime() u64 {
    return platform_time.monotonicNs();
}
