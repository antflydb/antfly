#[path = "../openapi_preprocess.rs"]
mod openapi_preprocess;

use openapi_preprocess::{
    collapse_equivalent_success_responses, strip_non_json_media_types, unify_error_response_schemas,
};

#[test]
fn error_normalization_preserves_redirects() {
    let mut spec: serde_yaml::Value = serde_yaml::from_str(
        r#"
paths:
  /example:
    get:
      responses:
        '302': {description: redirect, headers: {Location: {schema: {type: string}}}}
        '400': {description: bad request}
        default: {description: fallback}
"#,
    )
    .unwrap();
    unify_error_response_schemas(&mut spec);
    let responses = spec["paths"]["/example"]["get"]["responses"]
        .as_mapping()
        .unwrap();
    assert!(responses["302"].get("content").is_none());
    assert!(responses["302"].get("headers").is_some());
    assert_eq!(
        responses["400"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Error"
    );
    assert_eq!(
        responses["default"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Error"
    );
}

#[test]
fn success_collapse_requires_equivalent_schemas() {
    let mut equivalent: serde_yaml::Value = serde_yaml::from_str(
        r#"
paths:
  /example:
    post:
      responses:
        '200': {description: ok, content: {application/json: {schema: {type: object}}}}
        '201': {description: created, content: {application/json: {schema: {type: object}}}}
"#,
    )
    .unwrap();
    collapse_equivalent_success_responses(&mut equivalent);
    let responses = equivalent["paths"]["/example"]["post"]["responses"]
        .as_mapping()
        .unwrap();
    assert!(responses.contains_key("2XX"));
    assert!(!responses.contains_key("200"));

    let mut different: serde_yaml::Value = serde_yaml::from_str(
        r#"
paths:
  /example:
    post:
      responses:
        '200': {description: ok, content: {application/json: {schema: {type: object}}}}
        '201': {description: created, content: {application/json: {schema: {type: string}}}}
"#,
    )
    .unwrap();
    collapse_equivalent_success_responses(&mut different);
    let responses = different["paths"]["/example"]["post"]["responses"]
        .as_mapping()
        .unwrap();
    assert!(responses.contains_key("200"));
    assert!(responses.contains_key("201"));
}

#[test]
fn media_filter_keeps_only_json() {
    let mut spec: serde_yaml::Value = serde_yaml::from_str(
        r#"
paths:
  /example:
    get:
      responses:
        '200':
          description: ok
          content:
            application/json: {schema: {type: object}}
            text/event-stream: {schema: {type: string}}
"#,
    )
    .unwrap();
    strip_non_json_media_types(&mut spec);
    let content = spec["paths"]["/example"]["get"]["responses"]["200"]["content"]
        .as_mapping()
        .unwrap();
    assert_eq!(content.len(), 1);
    assert!(content.contains_key("application/json"));
}
