// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed coordinator preparation capability. Never encode this provider in
//! a command, row, schema, restored object, or public request. It runs outside
//! native apply and only produces unpublished candidates and effect receipts.
const std = @import("std");
const definition = @import("trigger_definition.zig");
const domains = @import("../schema/domain_row.zig");
const common = @import("../common/topology_records.zig");
const reads = @import("../api/table_read_source.zig");
const A = std.mem.Allocator;
pub const Input = struct {
    table: common.TableRecord,
    key: []const u8,
    event: definition.Event,
    before: ?*const reads.LookupResponse = null,
    before_typed: ?*const @import("catalog.zig").Row = null,
    /// Borrow one already parsed immutable logical row. Physical source guards
    /// remain independent of OLD when an earlier staged INSERT has version0.
    before_parsed: ?struct { value: std.json.Value, json_null_fields: []const []const u8, version: u64, digest: ?[32]u8 } = null,
    /// The ingress owns the mutable parsed DOM and consumes it into its
    /// ordinary typed prepared-row pipeline after this callback returns.
    after: ?*std.json.Value,
    json_null_fields: []const []const u8 = &.{},
    update_columns: []const []const u8 = &.{},
    candidate_prepared: bool = false,
    adopt_existing: bool = false,
    domain_admission: ?domains.Admission = null,
};
pub const Prepared = struct {
    skipped: bool,
    json_null_fields: []const []const u8,
    /// Live statement-owned stage identity, never an executable wire token.
    stage: *anyopaque,
};
/// A request-owned continuation of the existing native FK planner. Allocation
/// is the durable, non-reused constraint catalog identity, not a name hash.
pub const ReferentialAction = struct {
    child_table_id: u64,
    allocation: u64,
    ptr: *anyopaque,
    run: *const fn (*anyopaque) anyerror!void,
};
pub const Provider = struct {
    ptr: *anyopaque,
    reuse_explicit: bool = false,
    /// Exact active child declarations and full authoritative table records
    /// captured with their parent stamp; never discover children by row scan.
    incoming: ?*const fn (*anyopaque, common.TableRecord) anyerror![]const common.TableRecord = null,
    table_record: ?*const fn (*anyopaque, []const u8) anyerror!?common.TableRecord = null,
    begin_statement: ?*const fn (*anyopaque, common.TableRecord, definition.Event, []const []const u8) anyerror!void = null,
    end_statement: ?*const fn (*anyopaque, common.TableRecord, definition.Event, []const []const u8) anyerror!void = null,
    queue_action: ?*const fn (*anyopaque, ?*anyopaque, ReferentialAction) anyerror!void = null,
    drain: ?*const fn (*anyopaque) anyerror!void = null,
    before: *const fn (*anyopaque, A, Input) anyerror!Prepared,
    /// Caller has checked the final candidate's RLS and integrity claims.
    after: *const fn (*anyopaque, *anyopaque) anyerror!void,
};
