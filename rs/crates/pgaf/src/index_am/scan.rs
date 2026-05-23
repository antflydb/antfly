use pgrx::pg_sys;

use super::ctid::doc_id_to_ctid;
use super::options;
use crate::client::AntflyClient;

struct AntflyScanState {
    results: Vec<(pg_sys::ItemPointerData, f64)>,
    current: usize,
    query: Option<String>,
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambeginscan(
    index_relation: pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_orderbys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let scan = unsafe { pg_sys::RelationGetIndexScan(index_relation, n_keys, n_orderbys) };

    let state = AntflyScanState {
        results: Vec::new(),
        current: 0,
        query: None,
    };

    unsafe {
        (*scan).opaque = pgrx::PgMemoryContexts::CurrentMemoryContext
            .leak_and_drop_on_delete(state) as *mut std::os::raw::c_void;
    }

    scan
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _n_orderbys: std::os::raw::c_int,
) {
    // Copy scan keys into the descriptor
    if !keys.is_null() && n_keys > 0 {
        unsafe {
            std::ptr::copy(keys, (*scan).keyData, n_keys as usize);
        }
    }

    let state = unsafe { &mut *((*scan).opaque as *mut AntflyScanState) };
    state.results.clear();
    state.current = 0;

    if n_keys == 0 {
        state.query = None;
        return;
    }

    // Extract query text from the first scan key (RHS of @@@)
    let key = unsafe { &*(*scan).keyData };
    let is_null = (key.sk_flags & pg_sys::SK_ISNULL as i32) != 0;

    if is_null {
        state.query = None;
        return;
    }

    let query_text: Option<String> =
        unsafe { pgrx::datum::FromDatum::from_datum(key.sk_argument, false) };
    state.query = query_text;
}

/// Build the query body from the RHS of @@@.
///
/// If the text parses as a JSON object (from a query builder function),
/// use it as a structured query. Otherwise, treat it as a plain full-text
/// search string.
fn build_query_body(query: &str, limit: i64) -> serde_json::Value {
    if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(query) {
        if v.is_object() {
            // Structured query from pgaf.search() / pgaf.semantic() / pgaf.hybrid()
            v["limit"] = serde_json::json!(limit);
            return v;
        }
    }

    // Plain text — wrap as full-text search
    serde_json::json!({
        "full_text_search": { "query": query },
        "limit": limit,
    })
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    direction: pg_sys::ScanDirection::Type,
) -> bool {
    if direction != pg_sys::ScanDirection::ForwardScanDirection {
        pgrx::error!("pgaf: only forward scan is supported");
    }

    let state = unsafe { &mut *((*scan).opaque as *mut AntflyScanState) };

    // Execute search on first call
    if state.current == 0 && state.results.is_empty() {
        if let Some(ref query) = state.query {
            let (url, collection) =
                unsafe { options::get_options((*scan).indexRelation) };

            let client = AntflyClient::new(&url).unwrap_or_else(|e| {
                pgrx::error!("pgaf: failed to create client: {}", e);
            });

            let body = build_query_body(query, 10000);

            let hits = client.search_raw(&collection, &body).unwrap_or_else(|e| {
                pgrx::error!("pgaf: search failed: {}", e);
            });

            for hit in hits {
                if let Some(ctid) = doc_id_to_ctid(&hit.id) {
                    state.results.push((ctid, hit.score));
                }
            }
        }
    }

    if state.current < state.results.len() {
        let (ctid, _score) = state.results[state.current];
        unsafe {
            (*scan).xs_heaptid = ctid;
            (*scan).xs_recheck = false;
        }
        state.current += 1;
        true
    } else {
        false
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amendscan(scan: pg_sys::IndexScanDesc) {
    let state = unsafe { &mut *((*scan).opaque as *mut AntflyScanState) };
    state.results.clear();
    state.current = 0;
    state.query = None;
}
