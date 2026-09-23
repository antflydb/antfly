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

//! Runs the shared libantfly conformance cases (see
//! `zig/pkg/antfly/capi-conformance/README.md`) through this crate's public
//! API, porting `go/pkg/lite/conformance_cgo_test.go`'s semantics exactly.
//! Requires linking against the real library (`--features libantfly`).

use std::path::{Path, PathBuf};

use antfly_lite::{
    Database, Error, GraphDirection, OpenMode, OpenOptions, Profile, TxnId, TxnStatus, WriteIntent,
};
use serde::Deserialize;
use serde_json::Value;

#[test]
fn conformance_cases() {
    // Run on a thread with exactly the documented minimum stack: the test
    // harness's own threads are smaller, and running at the minimum turns
    // any stack growth in libantfly into a test failure.
    std::thread::Builder::new()
        .stack_size(antfly_lite::MIN_THREAD_STACK_SIZE)
        .spawn(conformance_cases_inner)
        .expect("spawn thread")
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}

fn conformance_cases_inner() {
    let cases_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../zig/pkg/antfly/capi-conformance/cases");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&cases_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", cases_dir.display()))
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no conformance cases in {}",
        cases_dir.display()
    );

    let mut failures = Vec::new();
    for file in &files {
        let raw = std::fs::read_to_string(file).unwrap_or_else(|e| panic!("read {file:?}: {e}"));
        let case: ConformanceCase =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {file:?}: {e}"));

        let tmp_dir = std::env::temp_dir().join(format!(
            "antfly-lite-conformance-{}-{}-{}",
            case.name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&tmp_dir).expect("create temp dir");

        let mut runner = Runner {
            dir: tmp_dir,
            db: None,
            path: PathBuf::new(),
            backup: Vec::new(),
        };
        if let Err(e) = runner.open_current(&case.open) {
            failures.push(format!("{}: open: {e}", case.name));
            continue;
        }
        for (i, step) in case.steps.iter().enumerate() {
            if let Err(e) = runner.run_step(step) {
                failures.push(format!("{}: step {i} ({}): {e}", case.name, step.op));
                break;
            }
        }
        runner.close_current();
    }

    assert!(
        failures.is_empty(),
        "conformance failures:\n{}",
        failures.join("\n")
    );
}

#[derive(Debug, Deserialize)]
struct ConformanceCase {
    name: String,
    #[serde(default)]
    open: ConformanceOpen,
    #[serde(default)]
    steps: Vec<ConformanceStep>,
}

#[derive(Debug, Default, Deserialize)]
struct ConformanceOpen {
    #[serde(default)]
    create: bool,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    no_sync: bool,
    #[serde(default)]
    busy_timeout_ms: u64,
    #[serde(default)]
    path: String,
}

#[derive(Debug, Default, Deserialize)]
struct ConformanceWrite {
    key: String,
    #[serde(default)]
    value: Value,
    #[serde(default)]
    delete: bool,
}

#[derive(Debug, Default, Deserialize)]
struct ConformanceStep {
    #[serde(flatten)]
    open: ConformanceOpen,
    op: String,
    #[serde(default)]
    writes: Vec<ConformanceWrite>,
    #[serde(default)]
    timestamp: u64,
    #[serde(default)]
    key: String,
    #[serde(default)]
    request: Value,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    schema: Value,
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    txn_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    commit_version: u64,
    #[serde(default)]
    index: String,
    #[serde(default)]
    edge_type: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    expect: Option<ConformanceExpect>,
}

#[derive(Debug, Default, Deserialize)]
struct ConformanceExpect {
    #[serde(default)]
    error: String,
    #[serde(default)]
    json_subset: Option<Value>,
    #[serde(default)]
    contains: Vec<String>,
    #[serde(default)]
    not_contains: Vec<String>,
    #[serde(default)]
    equals: Option<Value>,
}

/// The result of executing one step, in enough of a typed shape to render
/// as the "result text" the README's expectation table describes.
enum StepOutput {
    None,
    Json(Vec<u8>),
    Bool(bool),
    U64(u64),
    Str(String),
}

impl StepOutput {
    fn as_text(&self) -> String {
        match self {
            StepOutput::None => String::new(),
            StepOutput::Json(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            StepOutput::Bool(b) => b.to_string(),
            StepOutput::U64(n) => n.to_string(),
            // json.Marshal on a Go string quotes it; serde_json does the same.
            StepOutput::Str(s) => serde_json::to_string(s).expect("string always serializes"),
        }
    }
}

struct Runner {
    dir: PathBuf,
    db: Option<Database>,
    path: PathBuf,
    backup: Vec<u8>,
}

impl Runner {
    fn resolve_path(&self, name: &str) -> PathBuf {
        let name = if name.is_empty() { "db.aflite" } else { name };
        self.dir.join(name)
    }

    fn open_options(o: &ConformanceOpen) -> Result<OpenOptions, ExecError> {
        let mode = match o.mode.as_str() {
            "" | "writer" => OpenMode::Writer,
            "readonly" => OpenMode::Readonly,
            "status_only" => OpenMode::StatusOnly,
            other => return Err(ExecError::Other(format!("unknown mode {other:?}"))),
        };
        let profile = match o.profile.as_str() {
            "" | "native" => Profile::Native,
            "hosted" => Profile::Hosted,
            other => return Err(ExecError::Other(format!("unknown profile {other:?}"))),
        };
        let mut opts = OpenOptions::new()
            .mode(mode)
            .profile(profile)
            .no_sync(o.no_sync);
        if o.busy_timeout_ms > 0 {
            opts = opts.busy_timeout(std::time::Duration::from_millis(o.busy_timeout_ms));
        }
        Ok(opts)
    }

    fn open_db(&self, path: &Path, o: &ConformanceOpen) -> Result<Database, ExecError> {
        let opts = Self::open_options(o)?;
        let db = if o.create {
            Database::create(path, &opts)?
        } else {
            Database::open(path, &opts)?
        };
        Ok(db)
    }

    fn open_current(&mut self, o: &ConformanceOpen) -> Result<(), ExecError> {
        let path = self.resolve_path(&o.path);
        let db = self.open_db(&path, o)?;
        self.db = Some(db);
        self.path = path;
        Ok(())
    }

    fn close_current(&mut self) {
        if let Some(db) = self.db.take() {
            let _ = db.close();
        }
    }

    fn run_step(&mut self, step: &ConformanceStep) -> Result<(), String> {
        let result = self.execute(step);
        let Some(expect) = &step.expect else {
            return result.map(|_| ()).map_err(|e| e.to_string());
        };
        check_step(result, expect)
    }
}

fn check_step(
    result: Result<StepOutput, ExecError>,
    expect: &ConformanceExpect,
) -> Result<(), String> {
    if !expect.error.is_empty() {
        return match result {
            Ok(_) => Err(format!("succeeded, want {}", expect.error)),
            Err(ExecError::Ffi(err)) if err.name() == expect.error => Ok(()),
            Err(ExecError::Ffi(err)) => Err(format!(
                "error {} ({}), want {}",
                err.name(),
                err,
                expect.error
            )),
            Err(other) => Err(format!("error {other}, want {}", expect.error)),
        };
    }
    let output = result.map_err(|e| e.to_string())?;
    check_result(&output, expect)
}

impl Runner {
    fn execute(&mut self, step: &ConformanceStep) -> Result<StepOutput, ExecError> {
        match step.op.as_str() {
            "batch" => {
                let writes: Vec<WriteIntent> = step
                    .writes
                    .iter()
                    .map(|w| {
                        if w.delete {
                            WriteIntent::delete(w.key.as_bytes())
                        } else {
                            WriteIntent::put(w.key.as_bytes(), value_bytes(&w.value))
                        }
                    })
                    .collect();
                self.db()?.batch(&writes, step.timestamp)?;
                Ok(StepOutput::None)
            }
            "batch_json" => Ok(StepOutput::Json(
                self.db()?.batch_json(value_bytes(&step.request))?,
            )),
            "lookup" => Ok(StepOutput::Json(
                self.db()?.lookup_json(step.key.as_bytes())?,
            )),
            "scan" => Ok(StepOutput::Json(
                self.db()?.scan_json(value_bytes(&step.request))?,
            )),
            "search" => Ok(StepOutput::Json(
                self.db()?.search_json(value_bytes(&step.request))?,
            )),
            "stats" => Ok(StepOutput::Json(self.db()?.stats_json()?)),
            "status" => Ok(StepOutput::Json(self.db()?.status_json()?)),
            "capabilities" => Ok(StepOutput::Json(self.db()?.capabilities_json()?)),
            "check" => Ok(StepOutput::Json(self.db()?.check_json()?)),
            "pending_work_stats" => Ok(StepOutput::Json(self.db()?.pending_work_stats_json()?)),
            "run_until_idle" => {
                self.db()?.run_until_idle()?;
                Ok(StepOutput::None)
            }
            "get_schema" => Ok(StepOutput::Json(self.db()?.schema_json()?)),
            "set_schema" => {
                self.db()?.set_schema_json(value_bytes(&step.schema))?;
                Ok(StepOutput::None)
            }
            "list_indexes" => Ok(StepOutput::Json(self.db()?.indexes_json()?)),
            "add_index" => {
                self.db()?.add_index_json(value_bytes(&step.config))?;
                Ok(StepOutput::None)
            }
            "delete_index" => Ok(StepOutput::Bool(self.db()?.delete_index(&step.name)?)),
            "get_edges" => {
                let direction = parse_direction(&step.direction)?;
                Ok(StepOutput::Json(self.db()?.edges_json(
                    &step.index,
                    &step.key,
                    &step.edge_type,
                    direction,
                )?))
            }
            "list_enrichments" => Ok(StepOutput::Json(self.db()?.enrichments_json()?)),
            "add_enrichment" => {
                self.db()?.add_enrichment_json(value_bytes(&step.config))?;
                Ok(StepOutput::None)
            }
            "delete_enrichment" => Ok(StepOutput::Bool(
                self.db()?.delete_enrichment(&step.kind, &step.name)?,
            )),
            "begin_transaction"
            | "write_transaction"
            | "resolve_transaction"
            | "transaction_status"
            | "commit_version" => self.execute_transaction(step),
            "backup" => {
                let backup = self.db()?.backup()?;
                self.backup = backup;
                Ok(StepOutput::None)
            }
            "restore_open" => {
                let path = self.resolve_path(&step.open.path);
                antfly_lite::restore_backup(&path, &self.backup, false)?;
                self.close_current();
                self.open_current(&step.open)?;
                Ok(StepOutput::None)
            }
            "reopen" => {
                self.close_current();
                let mut o = ConformanceOpen {
                    path: step.open.path.clone(),
                    ..copy_open(&step.open)
                };
                if o.path.is_empty() {
                    o.path = self
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("db.aflite")
                        .to_string();
                }
                self.open_current(&o)?;
                Ok(StepOutput::None)
            }
            "open_second" => {
                let path = self.resolve_path(&step.open.path);
                let second = self.open_db(&path, &step.open)?;
                let _ = second.close();
                Ok(StepOutput::None)
            }
            "close" => {
                self.close_current();
                Ok(StepOutput::None)
            }
            other => Err(ExecError::Other(format!("unknown op {other:?}"))),
        }
    }

    fn execute_transaction(&mut self, step: &ConformanceStep) -> Result<StepOutput, ExecError> {
        let id = parse_txn_id(&step.txn_id)?;
        match step.op.as_str() {
            "begin_transaction" => {
                self.db()?.begin_transaction(id, step.timestamp, &[])?;
                Ok(StepOutput::None)
            }
            "write_transaction" => {
                let writes: Vec<WriteIntent> = step
                    .writes
                    .iter()
                    .map(|w| {
                        if w.delete {
                            WriteIntent::delete(w.key.as_bytes())
                        } else {
                            WriteIntent::put(w.key.as_bytes(), value_bytes(&w.value))
                        }
                    })
                    .collect();
                self.db()?.write_transaction(id, &writes)?;
                Ok(StepOutput::None)
            }
            "resolve_transaction" => {
                let status = match step.status.as_str() {
                    "committed" => TxnStatus::Committed,
                    "aborted" => TxnStatus::Aborted,
                    other => return Err(ExecError::Other(format!("unknown status {other:?}"))),
                };
                self.db()?
                    .resolve_transaction(id, status, step.commit_version)?;
                Ok(StepOutput::None)
            }
            "transaction_status" => {
                let status = self.db()?.transaction_status(id)?;
                let name = match status {
                    TxnStatus::Pending => "pending",
                    TxnStatus::Committed => "committed",
                    TxnStatus::Aborted => "aborted",
                };
                Ok(StepOutput::Str(name.to_string()))
            }
            _ => Ok(StepOutput::U64(self.db()?.commit_version(id)?)),
        }
    }

    fn db(&self) -> Result<&Database, ExecError> {
        self.db
            .as_ref()
            .ok_or_else(|| ExecError::Other("no open database".to_string()))
    }
}

fn copy_open(o: &ConformanceOpen) -> ConformanceOpen {
    ConformanceOpen {
        create: o.create,
        mode: o.mode.clone(),
        profile: o.profile.clone(),
        no_sync: o.no_sync,
        busy_timeout_ms: o.busy_timeout_ms,
        path: o.path.clone(),
    }
}

fn parse_direction(s: &str) -> Result<GraphDirection, ExecError> {
    match s {
        "" | "out" => Ok(GraphDirection::Out),
        "in" => Ok(GraphDirection::In),
        "both" => Ok(GraphDirection::Both),
        other => Err(ExecError::Other(format!("unknown direction {other:?}"))),
    }
}

fn parse_txn_id(hex_id: &str) -> Result<TxnId, ExecError> {
    if hex_id.len() != 32 {
        return Err(ExecError::Other(format!(
            "txn_id {hex_id:?} must be 32 hex characters"
        )));
    }
    let mut id = [0u8; 16];
    for i in 0..16 {
        let byte = u8::from_str_radix(&hex_id[i * 2..i * 2 + 2], 16)
            .map_err(|e| ExecError::Other(format!("txn_id {hex_id:?}: {e}")))?;
        id[i] = byte;
    }
    Ok(id)
}

/// Serializes a JSON case value to the bytes a C ABI call would receive.
/// `Value::Null` (an absent field in the case file) becomes an empty
/// payload, matching Go's `json.RawMessage` zero value.
fn value_bytes(v: &Value) -> Vec<u8> {
    if v.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(v).expect("case JSON always re-serializes")
    }
}

#[derive(Debug)]
enum ExecError {
    Ffi(Error),
    Other(String),
}

impl From<Error> for ExecError {
    fn from(err: Error) -> Self {
        ExecError::Ffi(err)
    }
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Ffi(err) => write!(f, "{err}"),
            ExecError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

fn check_result(output: &StepOutput, expect: &ConformanceExpect) -> Result<(), String> {
    let text = output.as_text();
    if let Some(want) = &expect.json_subset {
        let got: Value =
            serde_json::from_str(&text).map_err(|e| format!("result is not JSON: {e}: {text}"))?;
        if !json_subset(want, &got) {
            return Err(format!("result {text} does not contain {want}"));
        }
    }
    for s in &expect.contains {
        if !text.contains(s.as_str()) {
            return Err(format!("result {text} does not contain {s:?}"));
        }
    }
    for s in &expect.not_contains {
        if text.contains(s.as_str()) {
            return Err(format!("result {text} unexpectedly contains {s:?}"));
        }
    }
    if let Some(want) = &expect.equals {
        let got: Value = serde_json::from_str(&text)
            .map_err(|e| format!("scalar result {text:?} is not JSON: {e}"))?;
        if want != &got {
            return Err(format!("result {got}, want {want}"));
        }
    }
    Ok(())
}

fn json_subset(want: &Value, got: &Value) -> bool {
    match want {
        Value::Object(want_map) => match got {
            Value::Object(got_map) => want_map
                .iter()
                .all(|(k, wv)| got_map.get(k).is_some_and(|gv| json_subset(wv, gv))),
            _ => false,
        },
        Value::Array(want_arr) => match got {
            Value::Array(got_arr) if got_arr.len() == want_arr.len() => want_arr
                .iter()
                .zip(got_arr.iter())
                .all(|(w, g)| json_subset(w, g)),
            _ => false,
        },
        other => other == got,
    }
}
