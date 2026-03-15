use pgrx::pg_sys;

use super::ctid::ctid_to_doc_id;
use super::options;
use crate::client::AntflyClient;

struct BuildState {
    client: AntflyClient,
    collection: String,
    count: f64,
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let (url, collection) = unsafe { options::get_options(index_relation) };

    let client = AntflyClient::new(&url).unwrap_or_else(|e| {
        pgrx::error!("pgaf: failed to create Antfly client: {}", e);
    });

    // Ensure the table exists in Antfly before syncing documents.
    if let Err(e) = client.ensure_table(&collection) {
        pgrx::warning!("pgaf: failed to create table in Antfly: {}", e);
    }

    let mut state = BuildState {
        client,
        collection,
        count: 0.0,
    };

    // Call through the table AM's index_build_range_scan (table_index_build_scan
    // is a static inline in tableam.h so not available as a direct binding)
    let reltuples = unsafe {
        let heap_ref = heap_relation.as_ref().unwrap();
        let table_am = heap_ref.rd_tableam.as_ref().unwrap();
        table_am.index_build_range_scan.unwrap()(
            heap_relation,
            index_relation,
            index_info,
            true,                        // allow_sync
            false,                       // anyvisible
            false,                       // progress
            0,                           // start_blockno
            pg_sys::InvalidBlockNumber,  // numblocks
            Some(build_callback),
            &mut state as *mut BuildState as *mut std::os::raw::c_void,
            std::ptr::null_mut(),        // scan
        )
    };

    let result = unsafe {
        pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBuildResult>())
            as *mut pg_sys::IndexBuildResult
    };
    unsafe {
        (*result).heap_tuples = reltuples;
        (*result).index_tuples = state.count;
    }

    result
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn build_callback(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    unsafe {
        let state = &mut *(state as *mut BuildState);

        if *is_null.add(0) {
            return;
        }

        let doc_id = ctid_to_doc_id(*ctid);
        let text: Option<String> = pgrx::datum::FromDatum::from_datum(*values.add(0), false);

        if let Some(content) = text {
            let doc = serde_json::json!({
                "id": doc_id,
                "content": content,
            });

            if let Err(e) = state.client.sync_document(&state.collection, &doc_id, &doc) {
                pgrx::warning!("pgaf: failed to sync document {}: {}", doc_id, e);
            } else {
                state.count += 1.0;
            }
        }
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn ambuildempty(_index_relation: pg_sys::Relation) {
    // No-op for remote index.
}

#[pgrx::pg_guard]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C-unwind" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    unsafe {
        if *is_null.add(0) {
            return false;
        }

        let (url, collection) = options::get_options(index_relation);
        let doc_id = ctid_to_doc_id(*heap_tid);

        let text: Option<String> = pgrx::datum::FromDatum::from_datum(*values.add(0), false);

        if let Some(content) = text {
            let client = AntflyClient::new(&url).unwrap_or_else(|e| {
                pgrx::error!("pgaf: failed to create client: {}", e);
            });

            let doc = serde_json::json!({
                "id": doc_id,
                "content": content,
            });

            if let Err(e) = client.sync_document(&collection, &doc_id, &doc) {
                pgrx::warning!("pgaf: failed to sync document to antfly: {}", e);
            }
        }

        false
    }
}
