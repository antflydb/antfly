//! Cross-checks the `extern "C"` declarations in `antfly-lite-sys` against
//! `zig/pkg/antfly/include/antfly.h` *without* linking against libantfly.
//!
//! This works purely on source text (via `include_str!`, resolved at compile
//! time) so that it never references any of the crate's `extern "C"` items --
//! doing so would require the dylib to be present at link time. Instead it
//! extracts `antfly_*(...)` call-shaped signatures from both texts and
//! compares function names and parameter counts, to catch the header
//! changing out from under the hand-written bindings.

const HEADER_SRC: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../zig/pkg/antfly/include/antfly.h"
));
const SYS_SRC: &str = include_str!("../src/lib.rs");

/// Strips `//` and `/* */` comments (not string/char-literal aware, but
/// neither source file needs that).
fn strip_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let bytes = src.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'/') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Finds the index just past the matching close-paren for the '(' at
/// `open_idx`, respecting nested parens.
fn matching_close_paren(src: &[u8], open_idx: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (offset, &b) in src[open_idx..].iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open_idx + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn count_params(params: &str) -> usize {
    let mut params = params.trim();
    // A trailing comma before the closing paren (common in multi-line Rust
    // fn signatures) is not an extra empty parameter.
    if let Some(stripped) = params.strip_suffix(',') {
        params = stripped.trim_end();
    }
    if params.is_empty() || params == "void" {
        return 0;
    }
    let mut depth = 0i32;
    let mut count = 1usize;
    for c in params.chars() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            ',' if depth == 0 => count += 1,
            _ => {}
        }
    }
    count
}

/// Extracts `(name, param_count)` for every `antfly_<ident>(` occurrence in
/// `src`, treating each as a function declaration/signature. This matches
/// both C prototypes (`ret antfly_foo(a, b);`) and Rust `extern "C"` items
/// (`pub fn antfly_foo(a: T, b: U) -> V;`), since both put the parameter
/// list directly after the name in parens.
fn extract_signatures(src: &str) -> Vec<(String, usize)> {
    let cleaned = strip_comments(src);
    let bytes = cleaned.as_bytes();
    let mut sigs = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"antfly_") {
            let start = i;
            let mut end = i;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            let name = &cleaned[start..end];
            // Skip whitespace to see if a '(' follows directly (a call/decl
            // site), as opposed to a bare type reference.
            let mut j = end;
            while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            if j < bytes.len()
                && bytes[j] == b'('
                && let Some(close) = matching_close_paren(bytes, j)
            {
                let params = &cleaned[j + 1..close];
                sigs.push((name.to_string(), count_params(params)));
                i = close + 1;
                continue;
            }
            i = end;
        } else {
            i += 1;
        }
    }
    sigs
}

#[test]
fn every_sys_function_exists_in_header_with_matching_arity() {
    let header_sigs = extract_signatures(HEADER_SRC);
    let sys_sigs = extract_signatures(SYS_SRC);

    // Only look at declarations inside the `unsafe extern "C" { ... }` block
    // in lib.rs; `extract_signatures` already only matches call-shaped sites,
    // and lib.rs has no other antfly_*(...) call sites, so `sys_sigs` is
    // exactly the extern block's functions.
    assert!(
        sys_sigs.len() > 80,
        "expected the sys crate to declare a large number of antfly_* functions, got {}: \
         is SYS_SRC being read correctly?",
        sys_sigs.len()
    );
    assert!(
        header_sigs.len() >= sys_sigs.len(),
        "header has fewer antfly_* signatures ({}) than lite-sys declares ({})",
        header_sigs.len(),
        sys_sigs.len()
    );

    use std::collections::HashMap;
    let header_map: HashMap<&str, usize> =
        header_sigs.iter().map(|(n, c)| (n.as_str(), *c)).collect();

    let mut missing = Vec::new();
    let mut mismatched = Vec::new();
    for (name, sys_count) in &sys_sigs {
        match header_map.get(name.as_str()) {
            None => missing.push(name.clone()),
            Some(header_count) if header_count != sys_count => {
                mismatched.push(format!(
                    "{name}: lite-sys has {sys_count} params, header has {header_count}"
                ));
            }
            _ => {}
        }
    }

    assert!(
        missing.is_empty(),
        "lite-sys declares antfly_* functions not found in antfly.h: {missing:?}"
    );
    assert!(
        mismatched.is_empty(),
        "parameter count drift between lite-sys and antfly.h: {mismatched:?}"
    );
}

#[test]
fn every_header_function_is_declared_in_sys() {
    let header_sigs = extract_signatures(HEADER_SRC);
    let sys_sigs = extract_signatures(SYS_SRC);
    use std::collections::HashSet;
    let sys_names: HashSet<&str> = sys_sigs.iter().map(|(n, _)| n.as_str()).collect();

    // Filter out header signatures that are actually macro/type-like false
    // positives (there are none currently, since ANTFLY_* macros are
    // uppercase and don't match `antfly_` case-sensitively, and struct tags
    // never appear directly followed by '(').
    let missing: Vec<&str> = header_sigs
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|n| !sys_names.contains(n))
        .collect();

    assert!(
        missing.is_empty(),
        "antfly.h declares functions that lite-sys does not: {missing:?}"
    );
}
