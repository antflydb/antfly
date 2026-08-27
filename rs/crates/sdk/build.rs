use std::fs;
use std::path::Path;

fn main() {
    let spec_path = Path::new("../../../openapi.yaml");
    println!("cargo::rerun-if-changed={}", spec_path.display());

    let yaml = fs::read_to_string(spec_path).expect("failed to read OpenAPI spec");
    let mut spec: serde_yaml::Value =
        serde_yaml::from_str(&yaml).expect("failed to parse OpenAPI spec");

    // Progenitor doesn't support multiple media types per operation or
    // heterogeneous error response schemas. Preprocess the spec to fix both.
    strip_non_json_media_types(&mut spec);
    unify_error_response_schemas(&mut spec);
    mark_openapi_code_fences_as_text(&mut spec);

    let openapi: openapiv3::OpenAPI =
        serde_yaml::from_value(spec).expect("failed to deserialize filtered spec");

    let mut generator = progenitor::Generator::default();
    let tokens = generator
        .generate_tokens(&openapi)
        .expect("failed to generate client");

    let ast = syn::parse2(tokens).expect("failed to parse generated tokens");
    let mut code = prettyplease::unparse(&ast);
    inject_create_index_validation(&mut code);
    inject_create_table_validation(&mut code);

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("client.rs");
    fs::write(&out_path, code).expect("failed to write generated client");
}

/// Progenitor cannot express Antfly's cross-field OpenAPI extensions. Keep the
/// generated transport ergonomic by validating the one affected operation
/// before reqwest allocates or sends a request. The guarded anchors make a
/// generator-shape change fail during compilation instead of silently dropping
/// client-side validation.
fn inject_create_index_validation(code: &mut String) {
    let method = "    pub async fn create_index<'a>(";
    let method_start = code
        .find(method)
        .expect("generated client must contain create_index");
    assert!(
        code[method_start + method.len()..].find(method).is_none(),
        "generated client must contain exactly one create_index"
    );
    let request_start = code[method_start..]
        .find("        let url = format!(")
        .map(|offset| method_start + offset)
        .expect("generated create_index must build its URL before sending");
    code.insert_str(
        request_start,
        "        crate::validate_create_index_request_relationships(body)\n            .map_err(|error| Error::InvalidRequest(error.to_string()))?;\n",
    );
}

/// Apply the same relationship preflight to index configurations embedded in
/// create-table requests.
fn inject_create_table_validation(code: &mut String) {
    let method = "    pub async fn create_table<'a>(";
    let method_start = code
        .find(method)
        .expect("generated client must contain create_table");
    assert!(
        code[method_start + method.len()..].find(method).is_none(),
        "generated client must contain exactly one create_table"
    );
    let request_start = code[method_start..]
        .find("        let url = format!(")
        .map(|offset| method_start + offset)
        .expect("generated create_table must build its URL before sending");
    code.insert_str(
        request_start,
        "        crate::validate_create_table_request_relationships(body)\n            .map_err(|error| Error::InvalidRequest(error.to_string()))?;\n",
    );
}

/// OpenAPI descriptions document wire payloads and templates, not Rust source.
/// Rustdoc treats bare and unknown-language Markdown fences as Rust doctests,
/// so mark every opening fence as text before Progenitor turns descriptions
/// into doc comments. This keeps crate-owned doctests enabled.
fn mark_openapi_code_fences_as_text(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::String(text) => {
            let had_trailing_newline = text.ends_with('\n');
            let mut in_fence = false;
            let rewritten = text
                .lines()
                .map(|line| {
                    let trimmed = line.trim_start();
                    if !trimmed.starts_with("```") {
                        return line.to_owned();
                    }
                    if in_fence {
                        in_fence = false;
                        return line.to_owned();
                    }
                    in_fence = true;
                    let indent_len = line.len() - trimmed.len();
                    format!("{}```text", &line[..indent_len])
                })
                .collect::<Vec<_>>()
                .join("\n");
            *text = rewritten;
            if had_trailing_newline {
                text.push('\n');
            }
        }
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                mark_openapi_code_fences_as_text(value);
            }
        }
        serde_yaml::Value::Mapping(mapping) => {
            for value in mapping.values_mut() {
                mark_openapi_code_fences_as_text(value);
            }
        }
        serde_yaml::Value::Tagged(tagged) => mark_openapi_code_fences_as_text(&mut tagged.value),
        _ => {}
    }
}

/// Keep only `application/json` in content maps. Progenitor doesn't support
/// multiple media types per operation. Streaming (SSE, NDJSON) is better
/// handled manually.
fn strip_non_json_media_types(spec: &mut serde_yaml::Value) {
    let json_key = serde_yaml::Value::String("application/json".into());

    if let Some(paths) = spec.get_mut("paths").and_then(|p| p.as_mapping_mut()) {
        for (_path, methods) in paths.iter_mut() {
            if let Some(methods) = methods.as_mapping_mut() {
                for (_method, operation) in methods.iter_mut() {
                    strip_content_map(operation.get_mut("requestBody"), &json_key);

                    if let Some(responses) = operation
                        .get_mut("responses")
                        .and_then(|r| r.as_mapping_mut())
                    {
                        for (_status, resp) in responses.iter_mut() {
                            strip_content_map(Some(resp), &json_key);
                        }
                    }
                }
            }
        }
    }
}

/// Progenitor asserts that all error responses share the same type. Replace
/// any non-Error error response schema with the standard Error $ref.
fn unify_error_response_schemas(spec: &mut serde_yaml::Value) {
    // Progenitor requires every error response on an operation to share one
    // schema. Keep this compatibility envelope private to Rust generation so
    // the public OpenAPI contract retains its precise per-status response
    // types for the other SDKs.
    let create_index_error: serde_yaml::Value = serde_yaml::from_str(
        r#"
type: object
additionalProperties: false
required: [error]
properties:
  error:
    type: string
  code:
    type: string
  message:
    type: string
  retryable:
    type: boolean
  retry_after_ms:
    type: integer
    minimum: 1
"#,
    )
    .unwrap();
    spec.get_mut("components")
        .and_then(|value| value.get_mut("schemas"))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .expect("OpenAPI components.schemas must be a mapping")
        .insert(
            serde_yaml::Value::String("CreateIndexError".to_owned()),
            create_index_error,
        );

    if let Some(paths) = spec.get_mut("paths").and_then(|p| p.as_mapping_mut()) {
        for (_path, methods) in paths.iter_mut() {
            if let Some(methods) = methods.as_mapping_mut() {
                for (_method, operation) in methods.iter_mut() {
                    let error_type = if operation
                        .get("operationId")
                        .and_then(|value| value.as_str())
                        == Some("createIndex")
                    {
                        "CreateIndexError"
                    } else {
                        "Error"
                    };
                    let error_schema: serde_yaml::Value = serde_yaml::from_str(&format!(
                        "content:\n  application/json:\n    schema:\n      $ref: '#/components/schemas/{error_type}'\n"
                    ))
                    .unwrap();
                    if let Some(responses) = operation
                        .get_mut("responses")
                        .and_then(|r| r.as_mapping_mut())
                    {
                        for (code, resp) in responses.iter_mut() {
                            let code_str = match code {
                                serde_yaml::Value::Number(n) => n.to_string(),
                                serde_yaml::Value::String(s) => s.clone(),
                                _ => continue,
                            };
                            // Only fix 4xx/5xx responses (not 2xx)
                            if !code_str.starts_with('4') && !code_str.starts_with('5') {
                                continue;
                            }
                            // Resolve both inline responses and response-level
                            // $refs to one error shape. Progenitor cannot emit
                            // an operation with heterogeneous error bodies.
                            let desc = resp.get("description").cloned().unwrap_or_else(|| {
                                serde_yaml::Value::String("Error response".into())
                            });
                            *resp = error_schema.clone();
                            if let Some(mapping) = resp.as_mapping_mut() {
                                mapping
                                    .insert(serde_yaml::Value::String("description".into()), desc);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn strip_content_map(node: Option<&mut serde_yaml::Value>, keep: &serde_yaml::Value) {
    let Some(node) = node else { return };
    let Some(content) = node.get_mut("content").and_then(|c| c.as_mapping_mut()) else {
        return;
    };
    let keys_to_remove: Vec<_> = content.keys().filter(|k| *k != keep).cloned().collect();
    for key in keys_to_remove {
        content.remove(&key);
    }
    // If content map is now empty, remove it entirely so Progenitor
    // treats this as a no-body response.
    if content.is_empty() {
        node.as_mapping_mut()
            .unwrap()
            .remove(&serde_yaml::Value::String("content".into()));
    }
}
