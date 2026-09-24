// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! A single admission contract for ordered keys and all their continuations.
//! Includes the physical generation prefix, tuple, escaped document ID, footer.
//! Enforced before publication for writes, backfill, and reverse reconstruction.
pub const max_stored_key_bytes: usize = 1024 * 1024;
pub const max_cursor_key_bytes = max_stored_key_bytes;

pub fn admit(bytes: usize) !void {
    if (bytes > max_stored_key_bytes) return error.RelationalIndexKeyTooLarge;
}
