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
