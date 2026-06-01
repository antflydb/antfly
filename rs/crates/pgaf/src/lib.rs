::pgrx::pg_module_magic!();

mod client;
mod e2e_tests;
mod functions;
mod index_am;
mod query;
mod triggers;

#[allow(non_snake_case)]
#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    index_am::options::init();
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
