// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for the embedded inference runtime binding (`Inference`). Mirrors
//! the conventions in `tests/conformance.rs`/`tests/concurrency.rs`: bodies
//! run on a thread with exactly `MIN_THREAD_STACK_SIZE`. Requires linking
//! against the real library (`--features libantfly`).
//!
//! Most tests here use a scratch models directory rather than the caller's
//! real `~/.antfly/inference/models`, so they behave the same in CI (no
//! models installed) as on a workstation. A couple of tests are gated on
//! specific local/network preconditions and skip (print + return) when
//! those are not met, rather than failing the suite.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use antfly_lite::{Inference, InferenceOptions, PullProgress};

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "antfly-lite-inference-{tag}-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Spawns with exactly `MIN_THREAD_STACK_SIZE`, the documented minimum for
/// threads calling libantfly. See `tests/concurrency.rs`'s identical helper.
fn run_with_stack<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(antfly_lite::MIN_THREAD_STACK_SIZE)
        .spawn(f)
        .expect("spawn thread")
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}

#[test]
fn inference_is_send_and_sync_compile_time() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Inference>();
}

#[test]
fn open_default_and_with_options() {
    run_with_stack(|| {
        let default = Inference::open_default().expect("open_default");
        default.close().expect("close default");

        let models_dir = tmp_dir("open-with-options");
        let inference = Inference::open(&InferenceOptions::new().models_dir(&models_dir))
            .expect("open with explicit models_dir");
        inference.close().expect("close");
    });
}

#[test]
fn chunk_returns_data() {
    run_with_stack(|| {
        let models_dir = tmp_dir("chunk");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let body = inference
            .chunk(r#"{"input":"Ants live in colonies. Workers gather food."}"#)
            .expect("chunk");
        let body = String::from_utf8(body).expect("chunk response is valid UTF-8");
        assert!(
            body.contains("\"data\""),
            "chunk response missing \"data\": {body}"
        );

        inference.close().expect("close");
    });
}

#[test]
fn list_models_on_empty_dir_has_empty_data() {
    run_with_stack(|| {
        let models_dir = tmp_dir("list-models-empty");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let body = inference.list_models().expect("list_models");
        let body = String::from_utf8(body).expect("list_models response is valid UTF-8");
        assert!(
            body.contains("\"data\":[]") || body.contains("\"data\": []"),
            "expected an empty data array for an empty models dir: {body}"
        );

        inference.close().expect("close");
    });
}

#[test]
fn embed_with_missing_model_is_not_found() {
    run_with_stack(|| {
        let models_dir = tmp_dir("embed-missing-model");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let err = inference
            .embed(r#"{"model":"no/such-model","input":["hello"]}"#)
            .expect_err("embed with a missing model should fail");
        assert_eq!(err.error, antfly_lite::Error::NotFound, "err = {err:?}");
        assert!(
            err.body.contains("MODEL_NOT_FOUND"),
            "expected the JSON error body to carry MODEL_NOT_FOUND: {err:?}"
        );

        inference.close().expect("close");
    });
}

#[test]
fn pull_empty_request_is_invalid_argument() {
    run_with_stack(|| {
        let models_dir = tmp_dir("pull-empty-request");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let err = inference
            .pull("{}", None)
            .expect_err("pull with no \"model\" field should fail");
        assert_eq!(
            err.error,
            antfly_lite::Error::InvalidArgument,
            "err = {err:?}"
        );
        assert!(
            !err.body.is_empty(),
            "expected a JSON error body for an invalid pull request"
        );

        inference.close().expect("close");
    });
}

#[test]
fn generate_with_stream_true_is_invalid_argument() {
    run_with_stack(|| {
        let models_dir = tmp_dir("generate-stream");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let err = inference
            .generate(r#"{"model":"no/such-model","messages":[{"role":"user","content":"hi"}],"stream":true}"#)
            .expect_err("a streaming generate request should fail");
        assert_eq!(
            err.error,
            antfly_lite::Error::InvalidArgument,
            "err = {err:?}"
        );

        inference.close().expect("close");
    });
}

#[test]
fn use_after_close_fails() {
    run_with_stack(|| {
        let models_dir = tmp_dir("use-after-close");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");
        inference.close().expect("close");

        let err = inference
            .list_models()
            .expect_err("a call on a closed handle should fail");
        assert_eq!(
            err.error,
            antfly_lite::Error::InvalidArgument,
            "err = {err:?}"
        );
    });
}

#[test]
fn double_close_is_ok() {
    run_with_stack(|| {
        let models_dir = tmp_dir("double-close");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");
        inference.close().expect("first close");
        inference.close().expect("second close");
    });
}

/// Finds an installed local GGUF embedding model under
/// `~/.antfly/inference/models/Qwen/Qwen3-Embedding-0.6B-GGUF*`, matching
/// how `antfly inference pull` lays out downloaded models. Returns `None`
/// (causing the test to skip) when it is not present, so this test only
/// runs on a workstation that already pulled the model.
fn qwen_embedding_models_dir() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let qwen_dir = PathBuf::from(home).join(".antfly/inference/models/Qwen");
    let has_model = std::fs::read_dir(&qwen_dir).ok()?.any(|entry| {
        entry
            .ok()
            .map(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("Qwen3-Embedding-0.6B-GGUF")
            })
            .unwrap_or(false)
    });
    if has_model {
        // The models directory the runtime should scan is the parent of
        // `Qwen/`, i.e. `~/.antfly/inference/models`.
        Some(qwen_dir.parent()?.to_path_buf())
    } else {
        None
    }
}

#[test]
fn embed_two_inputs_with_local_model() {
    let Some(models_dir) = qwen_embedding_models_dir() else {
        eprintln!(
            "skipping embed_two_inputs_with_local_model: no local Qwen3-Embedding-0.6B-GGUF \
             model found under ~/.antfly/inference/models/Qwen"
        );
        return;
    };

    run_with_stack(move || {
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let body = inference
            .embed(
                r#"{"model":"Qwen/Qwen3-Embedding-0.6B-GGUF","input":["hello world","goodbye world"]}"#,
            )
            .expect("embed with a locally installed model");
        let body = String::from_utf8(body).expect("embed response is valid UTF-8");
        assert!(
            body.contains("\"data\""),
            "embed response missing \"data\": {body}"
        );

        inference.close().expect("close");
    });
}

#[test]
fn pull_reports_progress_and_lists_the_model() {
    let Ok(model) = std::env::var("ANTFLY_INFERENCE_PULL_TEST_MODEL") else {
        eprintln!(
            "skipping pull_reports_progress_and_lists_the_model: \
             ANTFLY_INFERENCE_PULL_TEST_MODEL is not set"
        );
        return;
    };

    run_with_stack(move || {
        let models_dir = tmp_dir("pull-network");
        let inference =
            Inference::open(&InferenceOptions::new().models_dir(&models_dir)).expect("open");

        let progress_calls = AtomicUsize::new(0);
        let mut on_progress = |p: &PullProgress| {
            progress_calls.fetch_add(1, Ordering::SeqCst);
            assert!(!p.model.is_empty(), "progress report missing model: {p:?}");
        };

        let request = format!(r#"{{"model":"{model}"}}"#);
        let body = inference
            .pull(&request, Some(&mut on_progress))
            .unwrap_or_else(|err| panic!("pull {model}: {err:?}"));
        let body = String::from_utf8(body).expect("pull response is valid UTF-8");
        assert!(
            body.contains("\"models\""),
            "pull response missing \"models\": {body}"
        );
        assert!(
            progress_calls.load(Ordering::SeqCst) > 0,
            "expected at least one progress callback invocation"
        );

        let list_body = inference.list_models().expect("list_models after pull");
        let list_body = String::from_utf8(list_body).expect("list_models response is valid UTF-8");
        let model_name = model.split(':').next().unwrap_or(&model);
        assert!(
            list_body.contains(model_name),
            "expected list_models to contain {model_name}: {list_body}"
        );

        inference.close().expect("close");
    });
}
