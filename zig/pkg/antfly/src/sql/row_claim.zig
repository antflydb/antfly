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

const ast = @import("ast.zig");
const db_mod = @import("../storage/db/mod.zig");
const generated_parser = @import("generated_parser.zig");
const grammar = @import("grammar.zig");
const std = @import("std");
const token_mod = @import("token.zig");

const Token = token_mod.Token;

pub fn sqlRowClaimForClause(clause: ast.SqlRowClaimClause) db_mod.types.RowClaimRequest {
    return .{
        .mode = clause.mode,
        .wait_policy = clause.wait_policy,
        .skip_locked = clause.wait_policy == .skip_locked,
    };
}

pub fn rowClaimModeForGeneratedMode(
    mode: generated_parser.GeneratedSqlRowLockMode,
) db_mod.types.RowClaimMode {
    return switch (mode) {
        .update => .for_update,
        .no_key_update => .for_no_key_update,
        .share => .for_share,
        .key_share => .for_key_share,
    };
}

pub fn rowClaimWaitPolicyForGeneratedPolicy(
    wait_policy: generated_parser.GeneratedSqlRowLockWaitPolicy,
) db_mod.types.RowClaimWaitPolicy {
    return switch (wait_policy) {
        .wait => .wait,
        .nowait => .nowait,
        .skip_locked => .skip_locked,
    };
}

pub fn setSqlRowClaimClause(claim: *db_mod.types.RowClaimRequest, clause: ast.SqlRowClaimClause) void {
    claim.mode = clause.mode;
    claim.wait_policy = clause.wait_policy;
    claim.skip_locked = clause.wait_policy == .skip_locked;
}

pub fn parseGeneratedReadRowClaimForClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    keyword_index: usize,
    pos: *usize,
    allowed_targets: []const []const u8,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?db_mod.types.RowClaimRequest {
    const read_ast = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.@"for")) return null;
    const range = read_ast.row_lock_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != keyword_index or range.start + 1 != pos.* or range.end > tokens.len) return error.UnsupportedSqlShape;

    try validateGeneratedReadRowLockClauseAlloc(alloc, tokens, range, read_ast.row_lock_mode, read_ast.row_lock_wait_policy, allowed_targets);
    pos.* = range.end;

    const mode = rowClaimModeForGeneratedMode(read_ast.row_lock_mode orelse return error.UnsupportedSqlShape);
    const wait_policy = rowClaimWaitPolicyForGeneratedPolicy(read_ast.row_lock_wait_policy orelse return error.UnsupportedSqlShape);
    return .{
        .mode = mode,
        .wait_policy = wait_policy,
        .skip_locked = wait_policy == .skip_locked,
    };
}

const ParsedGeneratedRowLockMode = struct {
    mode: generated_parser.GeneratedSqlRowLockMode,
    end: usize,
};

fn parseGeneratedRowLockMode(tokens: []const Token, index: usize, end: usize) !ParsedGeneratedRowLockMode {
    if (index >= end) return error.UnsupportedSqlShape;
    if (tokens[index].matchesKeywordTag(.update)) return .{ .mode = .update, .end = index + 1 };
    if (tokens[index].matchesKeywordTag(.share)) return .{ .mode = .share, .end = index + 1 };
    if (index + 2 < end and
        tokens[index].matchesKeywordTag(.no) and
        tokens[index + 1].matchesKeywordTag(.key) and
        tokens[index + 2].matchesKeywordTag(.update))
    {
        return .{ .mode = .no_key_update, .end = index + 3 };
    }
    if (index + 1 < end and
        tokens[index].matchesKeywordTag(.key) and
        tokens[index + 1].matchesKeywordTag(.share))
    {
        return .{ .mode = .key_share, .end = index + 2 };
    }
    return error.UnsupportedSqlShape;
}

fn generatedRowLockWaitPolicyStartsAt(tokens: []const Token, index: usize, end: usize) bool {
    if (index >= end) return false;
    if (tokens[index].matchesKeywordTag(.nowait)) return true;
    return index + 1 < end and tokens[index].matchesKeywordTag(.skip) and tokens[index + 1].matchesKeywordTag(.locked);
}

const ParsedGeneratedRowLockWaitPolicy = struct {
    wait_policy: generated_parser.GeneratedSqlRowLockWaitPolicy,
    end: usize,
};

fn parseGeneratedRowLockWaitPolicy(tokens: []const Token, index: usize, end: usize) !ParsedGeneratedRowLockWaitPolicy {
    if (index >= end) return .{ .wait_policy = .wait, .end = index };
    if (tokens[index].matchesKeywordTag(.nowait)) return .{ .wait_policy = .nowait, .end = index + 1 };
    if (index + 1 < end and tokens[index].matchesKeywordTag(.skip) and tokens[index + 1].matchesKeywordTag(.locked)) {
        return .{ .wait_policy = .skip_locked, .end = index + 2 };
    }
    return error.UnsupportedSqlShape;
}

fn validateGeneratedReadRowLockClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    lock_tokens: generated_parser.GeneratedSqlTokenRange,
    expected_mode: ?generated_parser.GeneratedSqlRowLockMode,
    expected_wait_policy: ?generated_parser.GeneratedSqlRowLockWaitPolicy,
    allowed_targets: []const []const u8,
) !void {
    if (lock_tokens.start >= lock_tokens.end or lock_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[lock_tokens.start].matchesKeywordTag(.@"for")) return error.UnsupportedSqlShape;
    const mode = try parseGeneratedRowLockMode(tokens, lock_tokens.start + 1, lock_tokens.end);
    if (mode.mode != (expected_mode orelse return error.UnsupportedSqlShape)) return error.UnsupportedSqlShape;
    var cursor = mode.end;

    if (cursor < lock_tokens.end and tokens[cursor].matchesKeywordTag(.of)) {
        cursor += 1;
        var saw_target = false;
        var expect_target = true;
        while (cursor < lock_tokens.end and !generatedRowLockWaitPolicyStartsAt(tokens, cursor, lock_tokens.end)) {
            if (!expect_target) {
                if (tokens[cursor].kind != .comma) return error.UnsupportedSqlShape;
                expect_target = true;
                cursor += 1;
                continue;
            }
            if (tokens[cursor].matchesKeywordTag(.only)) cursor += 1;
            if (cursor >= lock_tokens.end or
                generatedRowLockWaitPolicyStartsAt(tokens, cursor, lock_tokens.end) or
                tokens[cursor].kind != .identifier)
            {
                return error.UnsupportedSqlShape;
            }
            if (!grammar.rowClaimTargetAllowed(alloc, tokens[cursor].text, allowed_targets)) return error.UnsupportedSqlShape;
            saw_target = true;
            expect_target = false;
            cursor += 1;
        }
        if (!saw_target or expect_target) return error.UnsupportedSqlShape;
    }

    const wait_policy = try parseGeneratedRowLockWaitPolicy(tokens, cursor, lock_tokens.end);
    if (wait_policy.wait_policy != (expected_wait_policy orelse return error.UnsupportedSqlShape)) return error.UnsupportedSqlShape;
    if (wait_policy.end != lock_tokens.end) return error.UnsupportedSqlShape;
}

pub fn sqlRowClaimModeName(mode: db_mod.types.RowClaimMode) []const u8 {
    return switch (mode) {
        .for_update => "for_update",
        .for_no_key_update => "for_no_key_update",
        .for_share => "for_share",
        .for_key_share => "for_key_share",
    };
}

pub fn sqlRowClaimWaitPolicyName(wait_policy: db_mod.types.RowClaimWaitPolicy) []const u8 {
    return switch (wait_policy) {
        .wait => "wait",
        .nowait => "nowait",
        .skip_locked => "skip_locked",
    };
}

pub fn sqlRowClaimFingerprintName(claim: db_mod.types.RowClaimRequest) []const u8 {
    return switch (claim.mode) {
        .for_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "locked",
            .nowait => "nowait",
            .skip_locked => "skip_locked",
        },
        .for_no_key_update => switch (claim.effectiveWaitPolicy()) {
            .wait => "no_key_update",
            .nowait => "no_key_update_nowait",
            .skip_locked => "no_key_update_skip_locked",
        },
        .for_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "share",
            .nowait => "share_nowait",
            .skip_locked => "share_skip_locked",
        },
        .for_key_share => switch (claim.effectiveWaitPolicy()) {
            .wait => "key_share",
            .nowait => "key_share_nowait",
            .skip_locked => "key_share_skip_locked",
        },
    };
}
