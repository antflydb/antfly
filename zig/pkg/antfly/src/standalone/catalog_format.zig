// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable standalone catalog keys shared by the server and offline operators.
pub const head_key = "catalog-v2/head";
pub const row_prefix = "catalog-v2/row/";
pub const Head = struct {
    version: u16 = 1,
    epoch: u64,
    revision: u64,
    next_id: u64,
};
