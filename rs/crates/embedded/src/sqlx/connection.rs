// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::{
    Antfly, AntflyColumn, AntflyQueryResult, AntflyRow, AntflyStatement, AntflyTypeInfo,
    AntflyValue,
};
use crate::sql::SqlError;
use crate::{Database, MIN_THREAD_STACK_SIZE, OpenOptions};
use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, TryStreamExt};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    describe::Describe,
    error::{DatabaseError, Error, ErrorKind},
    executor::{Execute, Executor},
    sql_str::SqlStr,
    transaction::{Transaction, TransactionManager},
};
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock, Weak, mpsc},
    time::Duration,
};
use tokio::sync::oneshot;
use url::Url;

impl DatabaseError for SqlError {
    fn message(&self) -> &str {
        &self.message
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        self.sqlstate.as_deref().map(Cow::Borrowed)
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> ErrorKind {
        match self.sqlstate.as_deref() {
            Some("23505") => ErrorKind::UniqueViolation,
            Some("23503") => ErrorKind::ForeignKeyViolation,
            Some("23502") => ErrorKind::NotNullViolation,
            Some("23514") => ErrorKind::CheckViolation,
            _ => ErrorKind::Other,
        }
    }
}
fn native_error(e: SqlError) -> Error {
    if e.sqlstate.is_some() {
        Error::Database(Box::new(e))
    } else {
        Error::Protocol(e.to_string())
    }
}
fn protocol(e: impl fmt::Display) -> Error {
    Error::Protocol(e.to_string())
}
type Job = Box<dyn FnOnce(&Database) + Send>;
#[derive(Clone, Copy)]
enum ResourceKind {
    Session,
    Cursor,
}
struct PendingResource {
    id: u64,
    sender: Option<mpsc::Sender<Job>>,
    kind: ResourceKind,
}
impl PendingResource {
    fn claim(mut self) -> u64 {
        self.sender.take();
        self.id
    }
}
impl Drop for PendingResource {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let id = self.id;
            let kind = self.kind;
            let _ = sender.send(Box::new(move |db| match kind {
                ResourceKind::Session => {
                    let _ = db.sql_session_close(id);
                }
                ResourceKind::Cursor => {
                    let _ = db.sql_cursor_close(id);
                }
            }));
        }
    }
}
struct Worker {
    sender: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
    no_sync: bool,
}
impl Drop for Worker {
    fn drop(&mut self) {
        // Drain queued resource cleanup and release the writer lock before
        // the last connection's close returns.
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeWorker").finish_non_exhaustive()
    }
}
static WORKERS: OnceLock<Mutex<HashMap<PathBuf, Weak<Worker>>>> = OnceLock::new();
impl Worker {
    fn open(options: &AntflyConnectOptions) -> Result<Arc<Self>, Error> {
        let path = if options.path.is_absolute() {
            options.path.clone()
        } else {
            std::env::current_dir()
                .map_err(protocol)?
                .join(&options.path)
        };
        // Resolve existing file aliases, or the parent of a new database, so
        // every connection to the same file shares its native writer owner.
        let path = std::fs::canonicalize(&path)
            .or_else(|error| match (path.parent(), path.file_name()) {
                (Some(parent), Some(name)) => std::fs::canonicalize(parent).map(|p| p.join(name)),
                _ => Err(error),
            })
            .unwrap_or(path);
        let mut registry = WORKERS
            .get_or_init(Default::default)
            .lock()
            .map_err(protocol)?;
        if let Some(worker) = registry.get(&path).and_then(Weak::upgrade) {
            if worker.no_sync != options.no_sync {
                return Err(protocol(
                    "database is already open with different no_sync settings",
                ));
            }
            return Ok(worker);
        }
        let (sender, receiver) = mpsc::channel::<Job>();
        let (started, start) = mpsc::sync_channel(1);
        let native_options = OpenOptions {
            no_sync: options.no_sync,
            ..Default::default()
        };
        let thread_path = path.clone();
        let thread = std::thread::Builder::new()
            .name("antfly-sqlx".into())
            .stack_size(MIN_THREAD_STACK_SIZE)
            .spawn(move || {
                let database = match Database::open(&thread_path, &native_options) {
                    Ok(db) => Ok(db),
                    Err(_e) if !thread_path.exists() => {
                        Database::create(&thread_path, &native_options)
                    }
                    Err(e) => Err(e),
                };
                match database {
                    Ok(db) => {
                        if started.send(Ok(())).is_err() {
                            return;
                        }
                        for job in receiver {
                            job(&db)
                        }
                    }
                    Err(e) => {
                        let _ = started.send(Err(e));
                    }
                }
            })
            .map_err(protocol)?;
        start
            .recv()
            .map_err(protocol)?
            .map_err(|e| native_error(e.into()))?;
        let worker = Arc::new(Worker {
            sender: Some(sender),
            thread: Some(thread),
            no_sync: options.no_sync,
        });
        registry.insert(path, Arc::downgrade(&worker));
        Ok(worker)
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Database) -> crate::sql::Result<T> + Send + 'static,
    ) -> Result<T, Error> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .as_ref()
            .ok_or_else(|| protocol("native SQL worker has stopped"))?
            .send(Box::new(move |db| {
                let _ = sender.send(operation(db));
            }))
            .map_err(|_| protocol("native SQL worker has stopped"))?;
        receiver.await.map_err(protocol)?.map_err(native_error)
    }
    fn fire(&self, operation: impl FnOnce(&Database) + Send + 'static) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(Box::new(operation));
        }
    }
    async fn run_resource(
        &self,
        kind: ResourceKind,
        operation: impl FnOnce(&Database) -> crate::sql::Result<u64> + Send + 'static,
    ) -> Result<u64, Error> {
        let jobs = self
            .sender
            .as_ref()
            .ok_or_else(|| protocol("native SQL worker has stopped"))?;
        let cleanup = jobs.clone();
        let (sender, receiver) = oneshot::channel();
        jobs.send(Box::new(move |db| {
            let resource = operation(db).map(|id| PendingResource {
                id,
                sender: Some(cleanup),
                kind,
            });
            // The resource also owns cleanup when cancellation occurs after
            // native open but before the receiving future claims its ID.
            let _ = sender.send(resource);
        }))
        .map_err(|_| protocol("native SQL worker has stopped"))?;
        receiver
            .await
            .map_err(protocol)?
            .map(PendingResource::claim)
            .map_err(native_error)
    }
}
#[derive(Debug, Clone)]
pub struct AntflyConnectOptions {
    pub path: PathBuf,
    pub no_sync: bool,
}
impl AntflyConnectOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            no_sync: false,
        }
    }
    pub fn no_sync(mut self, value: bool) -> Self {
        self.no_sync = value;
        self
    }
}
impl FromStr for AntflyConnectOptions {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Error> {
        Self::from_url(&Url::parse(value).map_err(protocol)?)
    }
}
impl ConnectOptions for AntflyConnectOptions {
    type Connection = AntflyConnection;
    fn from_url(url: &Url) -> Result<Self, Error> {
        if !matches!(url.scheme(), "file" | "antfly")
            || url.host_str().is_some_and(|h| !h.is_empty())
        {
            return Err(protocol(
                "expected antfly:///path/database.aflite or file:/path/database.aflite",
            ));
        }
        let path = Url::parse(&format!("file://{}", url.path()))
            .map_err(protocol)?
            .to_file_path()
            .map_err(|_| protocol("invalid database path"))?;
        let mut options = Self::new(path);
        for (name, value) in url.query_pairs() {
            match (name.as_ref(), value.as_ref()) {
                ("no_sync", "1") => options.no_sync = true,
                ("no_sync", "0") => {}
                _ => return Err(protocol("unknown embedded SQL connection option")),
            }
        }
        Ok(options)
    }
    fn to_url_lossy(&self) -> Url {
        let path = if self.path.is_absolute() {
            self.path.clone()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("/"))
                .join(&self.path)
        };
        let mut url = Url::from_file_path(path).expect("database path");
        if self.no_sync {
            url.set_query(Some("no_sync=1"))
        }
        url
    }
    async fn connect(&self) -> Result<AntflyConnection, Error> {
        let options = self.clone();
        let worker = tokio::task::spawn_blocking(move || Worker::open(&options))
            .await
            .map_err(protocol)??;
        let session = worker
            .run_resource(ResourceKind::Session, |db| Ok(db.sql_session_open()?))
            .await?;
        Ok(AntflyConnection {
            worker,
            session,
            depth: 0,
            pending: false,
            closed: false,
        })
    }
    fn log_statements(self, _: log::LevelFilter) -> Self {
        self
    }
    fn log_slow_statements(self, _: log::LevelFilter, _: Duration) -> Self {
        self
    }
}
#[derive(Debug)]
pub struct AntflyConnection {
    worker: Arc<Worker>,
    session: u64,
    depth: usize,
    pending: bool,
    closed: bool,
}
impl AntflyConnection {
    async fn control(&mut self, statement: String) -> Result<(), Error> {
        let session = self.session;
        self.pending = true;
        let result = self
            .worker
            .run(move |db| {
                db.sql_json(
                    serde_json::to_vec(&json!({"statement":statement,"session_id":session}))
                        .expect("JSON encoding"),
                )
            })
            .await;
        self.pending = false;
        result.map(|_| ())
    }
    async fn ensure(&mut self) -> Result<(), Error> {
        if self.pending {
            self.control("ROLLBACK".into()).await?;
            self.depth = 0
        }
        Ok(())
    }
    async fn description(
        &mut self,
        statement: SqlStr,
        parameters: Vec<AntflyTypeInfo>,
    ) -> Result<AntflyStatement, Error> {
        self.ensure().await?;
        let body = serde_json::to_vec(
            &json!({"statement":statement.as_str(),"parameter_types":parameters}),
        )
        .map_err(protocol)?;
        let bytes = self
            .worker
            .run(move |db| db.sql_describe_json(body))
            .await?;
        #[derive(Deserialize)]
        struct Description {
            columns: Vec<AntflyColumn>,
            parameter_types: Vec<Option<AntflyTypeInfo>>,
        }
        let mut description: Description = serde_json::from_slice(&bytes).map_err(protocol)?;
        for (index, column) in description.columns.iter_mut().enumerate() {
            column.ordinal = index
        }
        Ok(AntflyStatement {
            sql: statement,
            columns: description.columns,
            parameters: description
                .parameter_types
                .into_iter()
                .map(|t| t.unwrap_or(AntflyTypeInfo("null".into())))
                .collect(),
        })
    }
}
impl Drop for AntflyConnection {
    fn drop(&mut self) {
        if !self.closed {
            let id = self.session;
            self.worker.fire(move |db| {
                let _ = db.sql_session_close(id);
            });
            self.closed = true
        }
    }
}
impl Connection for AntflyConnection {
    type Database = Antfly;
    type Options = AntflyConnectOptions;
    async fn close(mut self) -> Result<(), Error> {
        let id = self.session;
        self.worker
            .run(move |db| Ok(db.sql_session_close(id)?))
            .await?;
        self.closed = true;
        Ok(())
    }
    async fn close_hard(self) -> Result<(), Error> {
        self.close().await
    }
    async fn ping(&mut self) -> Result<(), Error> {
        self.ensure().await?;
        self.worker
            .run(|db| Ok(db.status_json()?))
            .await
            .map(|_| ())
    }
    async fn begin(&mut self) -> Result<Transaction<'_, Antfly>, Error> {
        Transaction::begin(self, None).await
    }
    fn shrink_buffers(&mut self) {}
    async fn flush(&mut self) -> Result<(), Error> {
        self.ensure().await
    }
    fn should_flush(&self) -> bool {
        self.pending
    }
}
pub struct AntflyTransactionManager;
impl TransactionManager for AntflyTransactionManager {
    type Database = Antfly;
    async fn begin(conn: &mut AntflyConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        conn.ensure().await?;
        if conn.depth != 0 && statement.is_some() {
            return Err(protocol(
                "custom BEGIN is only supported for the outermost transaction",
            ));
        }
        let command = if conn.depth == 0 {
            statement
                .map(|s| s.as_str().to_owned())
                .unwrap_or_else(|| "BEGIN ISOLATION LEVEL READ COMMITTED".into())
        } else {
            format!("SAVEPOINT antfly_sqlx_{}", conn.depth)
        };
        conn.control(command).await?;
        conn.depth += 1;
        Ok(())
    }
    async fn commit(conn: &mut AntflyConnection) -> Result<(), Error> {
        if conn.depth == 0 {
            return Ok(());
        }
        let command = if conn.depth == 1 {
            "COMMIT".into()
        } else {
            format!("RELEASE SAVEPOINT antfly_sqlx_{}", conn.depth - 1)
        };
        conn.control(command).await?;
        conn.depth -= 1;
        Ok(())
    }
    async fn rollback(conn: &mut AntflyConnection) -> Result<(), Error> {
        if conn.depth == 0 {
            return Ok(());
        }
        if conn.depth == 1 {
            conn.control("ROLLBACK".into()).await?
        } else {
            conn.control(format!(
                "ROLLBACK TO SAVEPOINT antfly_sqlx_{}",
                conn.depth - 1
            ))
            .await?;
            conn.control(format!("RELEASE SAVEPOINT antfly_sqlx_{}", conn.depth - 1))
                .await?
        }
        conn.depth -= 1;
        Ok(())
    }
    fn start_rollback(conn: &mut AntflyConnection) {
        if conn.depth == 0 {
            return;
        }
        let session = conn.session;
        let depth = conn.depth;
        conn.depth -= 1;
        conn.worker.fire(move |db| {
            let commands = if depth == 1 {
                vec!["ROLLBACK".to_owned()]
            } else {
                vec![
                    format!("ROLLBACK TO SAVEPOINT antfly_sqlx_{}", depth - 1),
                    format!("RELEASE SAVEPOINT antfly_sqlx_{}", depth - 1),
                ]
            };
            for statement in commands {
                let _ = db.sql_json(
                    serde_json::to_vec(&json!({"statement":statement,"session_id":session}))
                        .unwrap(),
                );
            }
        });
    }
    fn get_transaction_depth(conn: &AntflyConnection) -> usize {
        conn.depth
    }
}
#[derive(Deserialize)]
struct Output {
    columns: Vec<AntflyColumn>,
    rows: Vec<Vec<Json>>,
    sql_nulls: Option<Vec<Vec<bool>>>,
    rows_affected: u64,
}
fn rows(mut output: Output) -> Result<(Vec<AntflyRow>, AntflyQueryResult), Error> {
    for (i, column) in output.columns.iter_mut().enumerate() {
        column.ordinal = i
    }
    let columns: Arc<[AntflyColumn]> = output.columns.into();
    let mut rows = Vec::with_capacity(output.rows.len());
    for (ri, row) in output.rows.into_iter().enumerate() {
        if row.len() != columns.len() {
            return Err(protocol("invalid SQL row width"));
        }
        let values = row
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let null = output
                    .sql_nulls
                    .as_ref()
                    .and_then(|flags| flags.get(ri))
                    .and_then(|flags| flags.get(i))
                    .copied()
                    .unwrap_or(value.is_null());
                AntflyValue {
                    kind: columns[i].kind.clone(),
                    value,
                    null,
                }
            })
            .collect();
        rows.push(AntflyRow {
            columns: columns.clone(),
            values,
        });
    }
    Ok((
        rows,
        AntflyQueryResult {
            affected: output.rows_affected,
        },
    ))
}
struct CursorGuard {
    worker: Arc<Worker>,
    id: u64,
}
impl Drop for CursorGuard {
    fn drop(&mut self) {
        let id = self.id;
        self.worker.fire(move |db| {
            let _ = db.sql_cursor_close(id);
        });
    }
}
impl<'c> Executor<'c> for &'c mut AntflyConnection {
    type Database = Antfly;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<AntflyQueryResult, AntflyRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Antfly>,
    {
        Box::pin(async_stream::try_stream! {
         self.ensure().await?;
         let args=query.take_arguments().map_err(Error::Encode)?.unwrap_or_default().values;let statement=query.sql();let session=self.session;
         let request=serde_json::to_vec(&json!({"statement":statement.as_str(),"parameters":args,"session_id":session})).map_err(protocol)?;
         let cursor_request=request.clone();self.pending=true;
         let opened=self.worker.run_resource(ResourceKind::Cursor, move|db|db.sql_cursor_open_json(cursor_request)).await;
         self.pending=false;
         match opened{
          Ok(id)=>{let cursor=CursorGuard{worker:self.worker.clone(),id};loop{
           let bytes=self.worker.run(move|db|db.sql_cursor_fetch_json(id,128)).await?;
           #[derive(Deserialize)]struct Page{result:Output,exhausted:bool}
           let page:Page=serde_json::from_slice(&bytes).map_err(protocol)?;let (page_rows,result)=rows(page.result)?;
           for row in page_rows{yield Either::Right(row)}if page.exhausted{yield Either::Left(result);break}
          }drop(cursor);},
          Err(Error::Database(error)) if error.code().as_deref()==Some("0A000")=>{self.pending=true;let executed=self.worker.run(move|db|db.sql_json({let mut value:Json=serde_json::from_slice(&request).expect("request");value["limit"]=json!(4096);serde_json::to_vec(&value).expect("request")})).await;self.pending=false;let bytes=executed?;let (output,result)=rows(serde_json::from_slice(&bytes).map_err(protocol)?)?;for row in output{yield Either::Right(row)}yield Either::Left(result);},
          Err(error)=>Err(error)?,
         }
        })
    }
    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<AntflyRow>, Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Antfly>,
    {
        async move { self.fetch(query).try_next().await }.boxed()
    }
    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [AntflyTypeInfo],
    ) -> BoxFuture<'e, Result<AntflyStatement, Error>>
    where
        'c: 'e,
    {
        self.description(sql, parameters.to_vec()).boxed()
    }
    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Antfly>, Error>>
    where
        'c: 'e,
    {
        async move {
            let statement = self.description(sql, Vec::new()).await?;
            Ok(Describe {
                nullable: vec![None; statement.columns.len()],
                columns: statement.columns,
                parameters: Some(Either::Left(statement.parameters)),
            })
        }
        .boxed()
    }
}
