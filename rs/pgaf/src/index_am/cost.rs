use pgrx::pg_sys;

#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    unsafe {
        // Without a @@@ clause, discourage the planner from using this index.
        if (*path).indexclauses.is_null()
            || (*(*path).indexclauses).length == 0
        {
            *index_startup_cost = f64::MAX;
            *index_total_cost = f64::MAX;
            *index_selectivity = 0.0;
            *index_correlation = 0.0;
            *index_pages = 0.0;
            return;
        }

        *index_pages = 0.0;
        *index_correlation = 0.0;
        // Assume remote search is selective (~1% of rows)
        *index_selectivity = 0.01;
        // Startup cost: HTTP connection overhead
        *index_startup_cost = 10.0;
        // Total cost: network round trip. Keep below seq scan to encourage usage.
        *index_total_cost = 100.0;
    }
}
