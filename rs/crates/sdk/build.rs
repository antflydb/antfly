use std::fs;
use std::path::Path;

mod openapi_preprocess;

fn main() {
    let spec_path = Path::new("../../../openapi.yaml");
    println!("cargo::rerun-if-changed={}", spec_path.display());
    println!("cargo::rerun-if-changed=openapi_preprocess.rs");

    let yaml = fs::read_to_string(spec_path).expect("failed to read OpenAPI spec");
    let mut spec: serde_yaml::Value =
        serde_yaml::from_str(&yaml).expect("failed to parse OpenAPI spec");

    // Apply narrowly scoped workarounds for Progenitor's current response and
    // media-type limitations. Each transform has regression coverage.
    openapi_preprocess::strip_non_json_media_types(&mut spec);
    openapi_preprocess::collapse_equivalent_success_responses(&mut spec);
    openapi_preprocess::unify_error_response_schemas(&mut spec);

    let openapi: openapiv3::OpenAPI =
        serde_yaml::from_value(spec).expect("failed to deserialize filtered spec");
    let tokens = progenitor::Generator::default()
        .generate_tokens(&openapi)
        .expect("failed to generate client");
    let ast = syn::parse2(tokens).expect("failed to parse generated tokens");
    let code = prettyplease::unparse(&ast);

    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR is set by Cargo");
    fs::write(Path::new(&out_dir).join("client.rs"), code)
        .expect("failed to write generated client");
}
