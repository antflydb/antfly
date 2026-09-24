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

use pgrx::pg_sys;

/// Encode a ctid (block number, offset) into a string document ID.
/// Format: "{block}_{offset}" e.g. "42_3"
pub fn ctid_to_doc_id(ctid: pg_sys::ItemPointerData) -> String {
    let block = ((ctid.ip_blkid.bi_hi as u32) << 16) | (ctid.ip_blkid.bi_lo as u32);
    let offset = ctid.ip_posid;
    format!("{}_{}", block, offset)
}

/// Decode a string document ID back into a ctid.
/// Returns None if the string is not in "{block}_{offset}" format.
pub fn doc_id_to_ctid(doc_id: &str) -> Option<pg_sys::ItemPointerData> {
    let (block_str, offset_str) = doc_id.split_once('_')?;
    let block: u32 = block_str.parse().ok()?;
    let offset: u16 = offset_str.parse().ok()?;
    Some(pg_sys::ItemPointerData {
        ip_blkid: pg_sys::BlockIdData {
            bi_hi: (block >> 16) as u16,
            bi_lo: (block & 0xFFFF) as u16,
        },
        ip_posid: offset,
    })
}
