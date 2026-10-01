// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Control-safe progress for one bounded posting-repair page.
pub const Progress = struct {
    repaired: usize = 0,
    scanned: usize = 0,
    pending: bool = false,
    yield_after_page: bool = false,
    needs_write: bool = false,
};
