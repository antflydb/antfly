use pgrx::pg_sys;
use std::ffi::CStr;
use std::sync::OnceLock;

/// Reloptions struct stored in rd_options.
/// String options use the offset pattern: the i32 field stores the byte offset
/// from the start of the struct to a null-terminated C string appended after it.
#[repr(C)]
pub struct AntflyReloption {
    vl_len_: i32,
    url_offset: i32,
    collection_offset: i32,
}

impl AntflyReloption {
    unsafe fn get_string(this: *const Self, offset: i32, default: &CStr) -> &CStr {
        if this.is_null() || offset == 0 {
            return default;
        }
        unsafe {
            let ptr = (this as *const u8).add(offset as usize);
            CStr::from_ptr(ptr.cast())
        }
    }

    pub unsafe fn url(this: *const Self) -> &'static CStr {
        unsafe { Self::get_string(this, (*this).url_offset, c"http://localhost:8080") }
    }

    pub unsafe fn collection(this: *const Self) -> &'static CStr {
        unsafe { Self::get_string(this, (*this).collection_offset, c"") }
    }
}

static RELOPT_KIND: OnceLock<pg_sys::relopt_kind::Type> = OnceLock::new();

/// Register custom reloption kind and options. Must be called from `_PG_init`.
pub fn init() {
    RELOPT_KIND.get_or_init(|| unsafe {
        let kind = pg_sys::add_reloption_kind();
        pg_sys::add_string_reloption(
            kind as _,
            c"url".as_ptr(),
            c"Antfly server URL".as_ptr(),
            c"http://localhost:8080".as_ptr(),
            None,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        );
        pg_sys::add_string_reloption(
            kind as _,
            c"collection".as_ptr(),
            c"Antfly collection name (defaults to table name)".as_ptr(),
            c"".as_ptr(),
            None,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        );
        kind
    });
}

fn relopt_table() -> Vec<pg_sys::relopt_parse_elt> {
    vec![
        pg_sys::relopt_parse_elt {
            optname: c"url".as_ptr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: std::mem::offset_of!(AntflyReloption, url_offset) as i32,
            isset_offset: 0,
        },
        pg_sys::relopt_parse_elt {
            optname: c"collection".as_ptr(),
            opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
            offset: std::mem::offset_of!(AntflyReloption, collection_offset) as i32,
            isset_offset: 0,
        },
    ]
}

/// `amoptions` callback: parse reloptions from the WITH clause.
#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    let relopt_kind = RELOPT_KIND.get().copied().expect("options::init() not called");
    let table = relopt_table();
    unsafe {
        pg_sys::build_reloptions(
            reloptions,
            validate,
            relopt_kind,
            std::mem::size_of::<AntflyReloption>(),
            table.as_ptr(),
            table.len() as _,
        ) as *mut pg_sys::bytea
    }
}

/// Extract URL and collection from an index relation's reloptions.
pub unsafe fn get_options(index_relation: pg_sys::Relation) -> (String, String) {
    unsafe {
        let rd_options = (*index_relation).rd_options as *const AntflyReloption;
        let url = AntflyReloption::url(rd_options)
            .to_str()
            .unwrap_or("http://localhost:8080")
            .to_string();
        let collection_raw = AntflyReloption::collection(rd_options)
            .to_str()
            .unwrap_or("");

        let collection = if collection_raw.is_empty() {
            let heap_oid = (*(*index_relation).rd_index).indrelid;
            let heap_rel = pg_sys::RelationIdGetRelation(heap_oid);
            let name = CStr::from_ptr((*(*heap_rel).rd_rel).relname.data.as_ptr())
                .to_str()
                .unwrap_or("unknown")
                .to_string();
            pg_sys::RelationClose(heap_rel);
            name
        } else {
            collection_raw.to_string()
        };

        (url, collection)
    }
}
