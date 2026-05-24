use pgrx::prelude::*;

/// The @@@ operator function. Returns true unconditionally because the index AM
/// does the actual filtering (amgettuple returns only matching ctids).
///
/// During a sequential scan fallback (no index), this matches all rows.
#[pg_extern(immutable, parallel_safe)]
fn antfly_match(_element: &str, _query: &str) -> bool {
    true
}

pgrx::extension_sql!(
    r#"
CREATE OPERATOR @@@ (
    PROCEDURE = antfly_match,
    LEFTARG = text,
    RIGHTARG = text,
    COMMUTATOR = @@@
);

CREATE FUNCTION antfly_amhandler(internal) RETURNS index_am_handler
    IMMUTABLE STRICT PARALLEL SAFE
    LANGUAGE c AS 'MODULE_PATHNAME', '_antfly_amhandler_wrapper';

CREATE ACCESS METHOD antfly TYPE INDEX HANDLER antfly_amhandler;

CREATE OPERATOR CLASS antfly_text_ops
    DEFAULT FOR TYPE text USING antfly AS
    OPERATOR 1 @@@(text, text),
    STORAGE text;
"#,
    name = "antfly_am_setup",
    requires = [antfly_match, _antfly_amhandler]
);
