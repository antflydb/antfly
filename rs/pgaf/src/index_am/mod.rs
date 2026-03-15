mod build;
mod cost;
mod ctid;
pub mod operator;
pub mod options;
mod scan;
mod vacuum;

use pgrx::datum::Internal;
use pgrx::pg_sys::Datum;

#[pgrx::pg_extern(sql = "")]
fn _antfly_amhandler(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Internal {
    type T = pgrx::pg_sys::IndexAmRoutine;
    unsafe {
        let p = pgrx::pg_sys::palloc0(std::mem::size_of::<T>()) as *mut T;
        p.write(AM_HANDLER);
        Internal::from(Some(Datum::from(p)))
    }
}

const AM_HANDLER: pgrx::pg_sys::IndexAmRoutine = {
    let mut am = unsafe { std::mem::MaybeUninit::<pgrx::pg_sys::IndexAmRoutine>::zeroed().assume_init() };

    am.type_ = pgrx::pg_sys::NodeTag::T_IndexAmRoutine;

    am.amstrategies = 1;
    am.amsupport = 0;

    am.amcanorder = false;
    am.amcanorderbyop = false;
    am.amcanbackward = false;
    am.amcanunique = false;
    am.amcanmulticol = false;
    am.amoptionalkey = true;
    am.amsearcharray = false;
    am.amsearchnulls = false;
    am.amstorage = false;
    am.amclusterable = false;
    am.ampredlocks = false;
    am.amcanparallel = false;
    am.amcaninclude = false;
    am.amusemaintenanceworkmem = false;
    am.amkeytype = pgrx::pg_sys::InvalidOid;

    am.amvalidate = Some(amvalidate);
    am.amoptions = Some(options::amoptions);
    am.amcostestimate = Some(cost::amcostestimate);

    am.ambuild = Some(build::ambuild);
    am.ambuildempty = Some(build::ambuildempty);
    am.aminsert = Some(build::aminsert);

    am.ambulkdelete = Some(vacuum::ambulkdelete);
    am.amvacuumcleanup = Some(vacuum::amvacuumcleanup);

    am.ambeginscan = Some(scan::ambeginscan);
    am.amrescan = Some(scan::amrescan);
    am.amgettuple = Some(scan::amgettuple);
    am.amendscan = Some(scan::amendscan);

    am
};

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amvalidate(_opclass_oid: pgrx::pg_sys::Oid) -> bool {
    true
}
