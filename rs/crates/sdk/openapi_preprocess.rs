//! Narrow compatibility transforms for Progenitor's current OpenAPI limits.

/// Progenitor cannot represent multiple equivalent success schemas for one
/// operation. Collapse only a set of two or more documented, schema-equivalent
/// 2xx responses. The resulting `2XX` range is a generator compatibility
/// compromise; heterogeneous successes are left untouched and fail generation
/// instead of silently changing their types.
pub fn collapse_equivalent_success_responses(spec: &mut serde_yaml::Value) {
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
                        serde_yaml::Value::String(status) => {
                            status.len() == 3 && status.starts_with('2')
                        }
                        _ => false,
                    })
                    .cloned()
                    .collect();
                if success_keys.len() < 2 {
                    continue;
                }

                let schema = |key: &serde_yaml::Value| {
                    responses
                        .get(key)
                        .and_then(|response| response.get("content"))
                        .and_then(|content| content.get("application/json"))
                        .and_then(|media| media.get("schema"))
                };
                if !success_keys
                    .iter()
                    .skip(1)
                    .all(|key| schema(key) == schema(&success_keys[0]))
                {
                    continue;
                }

                let representative = success_keys
                    .iter()
                    .find(|key| key.as_str() == Some("200"))
                    .and_then(|key| responses.get(key))
                    .cloned()
                    .unwrap_or_else(|| responses[&success_keys[0]].clone());
                for key in success_keys {
                    responses.remove(&key);
                }
                responses.insert(serde_yaml::Value::String("2XX".into()), representative);
            }
        }
    }
}

pub fn strip_non_json_media_types(spec: &mut serde_yaml::Value) {
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
                        for (_status, response) in responses.iter_mut() {
                            strip_content_map(Some(response), &json_key);
                        }
                    }
                }
            }
        }
    }
}

/// Normalize only actual HTTP error statuses and `default`. Redirects remain
/// intact and are never rewritten into Antfly's JSON Error shape.
pub fn unify_error_response_schemas(spec: &mut serde_yaml::Value) {
    let error_schema: serde_yaml::Value = serde_yaml::from_str(
        r#"
content:
  application/json:
    schema:
      $ref: '#/components/schemas/Error'
"#,
    )
    .expect("static Error schema is valid YAML");

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
                for (code, response) in responses.iter_mut() {
                    let code = match code {
                        serde_yaml::Value::Number(number) => number.to_string(),
                        serde_yaml::Value::String(status) => status.clone(),
                        _ => continue,
                    };
                    if code != "default" && !code.starts_with('4') && !code.starts_with('5') {
                        continue;
                    }
                    let description = response.get("description").cloned().unwrap_or_else(|| {
                        serde_yaml::Value::String("Standard API error response".into())
                    });
                    if let Some(mapping) = response.as_mapping_mut() {
                        mapping.remove(serde_yaml::Value::String("$ref".into()));
                        mapping.remove(serde_yaml::Value::String("content".into()));
                        for (key, value) in error_schema.as_mapping().expect("mapping") {
                            mapping.insert(key.clone(), value.clone());
                        }
                        mapping
                            .insert(serde_yaml::Value::String("description".into()), description);
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
    let remove: Vec<_> = content.keys().filter(|key| *key != keep).cloned().collect();
    for key in remove {
        content.remove(&key);
    }
    if content.is_empty() {
        node.as_mapping_mut()
            .expect("content owner is a mapping")
            .remove(serde_yaml::Value::String("content".into()));
    }
}
