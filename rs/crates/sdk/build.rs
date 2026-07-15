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
    collapse_equivalent_success_responses(&mut spec);
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

/// Progenitor 0.13 can still treat two success statuses as distinct response
/// types even when their JSON schemas are identical. Replace equivalent
/// statuses with one typed `2XX` range so the generated method accepts every
/// documented success status without creating multiple body types.
fn collapse_equivalent_success_responses(spec: &mut serde_yaml::Value) {
    if let Some(paths) = spec.get_mut("paths").and_then(|p| p.as_mapping_mut()) {
        for (_path, methods) in paths.iter_mut() {
            let Some(methods) = methods.as_mapping_mut() else {
                continue;
            };
            for (_method, operation) in methods.iter_mut() {
                let Some(responses) = operation
                    .get_mut("responses")
                    .and_then(|r| r.as_mapping_mut())
                else {
                    continue;
                };
                let success_keys: Vec<_> = responses
                    .keys()
                    .filter(|key| match key {
                        serde_yaml::Value::Number(number) => number.to_string().starts_with('2'),
                        serde_yaml::Value::String(status) => status.starts_with('2'),
                        _ => false,
                    })
                    .cloned()
                    .collect();
                if success_keys.len() < 2 {
                    continue;
                }

                let first_schema = responses
                    .get(&success_keys[0])
                    .and_then(|response| response.get("content"))
                    .and_then(|content| content.get("application/json"))
                    .and_then(|media| media.get("schema"));
                const MISSING_SCHEMA: serde_yaml::Value = serde_yaml::Value::Null;
                let first_schema = first_schema.unwrap_or(&MISSING_SCHEMA);
                if !success_keys.iter().skip(1).all(|key| {
                    responses
                        .get(key)
                        .and_then(|response| response.get("content"))
                        .and_then(|content| content.get("application/json"))
                        .and_then(|media| media.get("schema"))
                        .unwrap_or(&MISSING_SCHEMA)
                        == first_schema
                }) {
                    continue;
                }

                const SUCCESS_RANGE: &str = "2XX";
                let representative = success_keys
                    .iter()
                    .find(|key| key.as_str() == Some("200"))
                    .and_then(|key| responses.get(key))
                    .cloned()
                    .unwrap_or_else(|| responses[&success_keys[0]].clone());
                for key in success_keys {
                    responses.remove(&key);
                }
                responses.insert(
                    serde_yaml::Value::String(SUCCESS_RANGE.into()),
                    representative,
                );
            }
        }
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
/// every non-2xx response, including empty and default responses, with the
/// standard Error schema so one operation cannot mix `None` and `Error`.
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
            let Some(methods) = methods.as_mapping_mut() else {
                continue;
            };
            for (_method, operation) in methods.iter_mut() {
                let Some(responses) = operation
                    .get_mut("responses")
                    .and_then(|r| r.as_mapping_mut())
                else {
                    continue;
                };
                for (code, resp) in responses.iter_mut() {
                    let code_str = match code {
                        serde_yaml::Value::Number(n) => n.to_string(),
                        serde_yaml::Value::String(s) => s.clone(),
                        _ => continue,
                    };
                    if code_str.starts_with('2') {
                        continue;
                    }

                    let desc = resp.get("description").cloned().unwrap_or_else(|| {
                        serde_yaml::Value::String("Standard API error response".into())
                    });
                    if let Some(resp) = resp.as_mapping_mut() {
                        resp.remove(&serde_yaml::Value::String("$ref".into()));
                        resp.remove(&serde_yaml::Value::String("content".into()));
                        if let Some(ec) = error_schema.as_mapping() {
                            for (k, v) in ec {
                                resp.insert(k.clone(), v.clone());
                            }
                        }
                        resp.insert(serde_yaml::Value::String("description".into()), desc);
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
