// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const Limits = struct {
    max_windows: usize = 64,
    max_document_bytes: usize = 1024 * 1024,
    max_document_words: usize = 131072,
    max_request_tokens: usize = 64 * 1024,
};
pub const qualified: Limits = .{};
