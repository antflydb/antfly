use std::fs;
use std::path::Path;

fn main() {
    let spec_path = Path::new("../../openapi.yaml");
    println!("cargo::rerun-if-changed={}", spec_path.display());

    let yaml = fs::read_to_string(spec_path).expect("failed to read openapi.yaml");
    let mut spec: serde_yaml::Value =
        serde_yaml::from_str(&yaml).expect("failed to parse openapi.yaml");

    // Progenitor doesn't support multiple media types per operation or
    // heterogeneous error response schemas. Preprocess the spec to fix both.
    strip_non_json_media_types(&mut spec);
    unify_error_response_schemas(&mut spec);

    let openapi: openapiv3::OpenAPI =
        serde_yaml::from_value(spec).expect("failed to deserialize filtered spec");

    let mut generator = progenitor::Generator::default();
    let tokens = generator
        .generate_tokens(&openapi)
        .expect("failed to generate client");

    let ast = syn::parse2(tokens).expect("failed to parse generated tokens");
    let code = prettyplease::unparse(&ast);

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("client.rs");
    fs::write(&out_path, code).expect("failed to write generated client");
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

                    if let Some(responses) =
                        operation.get_mut("responses").and_then(|r| r.as_mapping_mut())
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
    let error_schema: serde_yaml::Value = serde_yaml::from_str(
        r#"
content:
  application/json:
    schema:
      $ref: '#/components/schemas/Error'
"#,
    )
    .unwrap();

    if let Some(paths) = spec.get_mut("paths").and_then(|p| p.as_mapping_mut()) {
        for (_path, methods) in paths.iter_mut() {
            if let Some(methods) = methods.as_mapping_mut() {
                for (_method, operation) in methods.iter_mut() {
                    if let Some(responses) =
                        operation.get_mut("responses").and_then(|r| r.as_mapping_mut())
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
                            // If it has content with a non-Error schema, replace it
                            if let Some(content) =
                                resp.get("content").and_then(|c| c.as_mapping())
                            {
                                let json_key =
                                    serde_yaml::Value::String("application/json".into());
                                if let Some(media) = content.get(&json_key) {
                                    if let Some(schema) = media.get("schema") {
                                        let is_error_ref = schema
                                            .get("$ref")
                                            .and_then(|r| r.as_str())
                                            .is_some_and(|r| r.ends_with("/Error"));
                                        if !is_error_ref {
                                            // Replace with Error schema, keep description
                                            let desc = resp.get("description").cloned();
                                            if let Some(resp) = resp.as_mapping_mut() {
                                                resp.remove(&serde_yaml::Value::String(
                                                    "content".into(),
                                                ));
                                                if let Some(ec) = error_schema.as_mapping() {
                                                    for (k, v) in ec {
                                                        resp.insert(k.clone(), v.clone());
                                                    }
                                                }
                                                if let Some(d) = desc {
                                                    resp.insert(
                                                        serde_yaml::Value::String(
                                                            "description".into(),
                                                        ),
                                                        d,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
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
