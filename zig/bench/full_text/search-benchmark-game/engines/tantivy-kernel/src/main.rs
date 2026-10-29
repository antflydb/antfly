use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tantivy::collector::{Count, TopDocs};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::{TextAnalyzer, Token, TokenStream, Tokenizer};
use tantivy::{DocAddress, Index, IndexReader, ReloadPolicy, Score, TantivyDocument, Term};

const BATCH_SIZE: usize = 5_000;
const GRAMMAR: &str = "V1";
const ANALYZER: &str = "antfly_ascii_simple";

#[derive(Clone, Copy)]
enum SegmentMode {
    Single,
    Production,
}

struct Args {
    db_path: PathBuf,
    segment_mode: SegmentMode,
    manifest: Option<PathBuf>,
    bm25_k1: f64,
    bm25_b: f64,
}

fn parse_args(mut raw: impl Iterator<Item = String>) -> Result<(String, Args)> {
    let operation = raw.next().context("missing index/query operation")?;
    let db_path = PathBuf::from(raw.next().context("missing index path")?);
    let mut args = Args {
        db_path,
        segment_mode: SegmentMode::Production,
        manifest: None,
        bm25_k1: 1.2,
        bm25_b: 0.75,
    };
    while let Some(flag) = raw.next() {
        match flag.as_str() {
            "--segment-mode" => {
                args.segment_mode = match raw.next().context("missing segment mode")?.as_str() {
                    "single" => SegmentMode::Single,
                    "production" => SegmentMode::Production,
                    _ => bail!("invalid segment mode"),
                }
            }
            "--manifest" => {
                args.manifest = Some(PathBuf::from(raw.next().context("missing manifest path")?))
            }
            "--bm25-k1" => args.bm25_k1 = raw.next().context("missing k1")?.parse()?,
            "--bm25-b" => args.bm25_b = raw.next().context("missing b")?.parse()?,
            _ => bail!("unknown argument {flag}"),
        }
    }
    if (args.bm25_k1 - 1.2).abs() > f64::EPSILON || (args.bm25_b - 0.75).abs() > f64::EPSILON {
        bail!("Tantivy 0.25 has fixed BM25 k1=1.2,b=0.75; non-default values are unsupported");
    }
    Ok((operation, args))
}

fn schema() -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(ANALYZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text = builder.add_text_field(
        "text",
        TextOptions::default().set_indexing_options(indexing),
    );
    let ordinal = builder.add_u64_field("corpus_ordinal", NumericOptions::default().set_fast());
    (builder.build(), text, ordinal)
}

fn register_analyzer(index: &Index) {
    index.tokenizers().register(
        ANALYZER,
        TextAnalyzer::builder(AntflyTokenizer::default()).build(),
    );
}

/// Mirrors Antfly's current `unicode_words` implementation exactly: ASCII
/// letters/digits and every non-ASCII UTF-8 codepoint are token characters;
/// ASCII punctuation/whitespace are separators.
#[derive(Clone, Default)]
struct AntflyTokenizer {
    token: Token,
}

struct AntflyTokenStream<'a> {
    text: &'a str,
    cursor: usize,
    token: &'a mut Token,
}

fn antfly_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte >= 0x80
}

fn utf8_width(text: &str, cursor: usize) -> usize {
    text[cursor..]
        .chars()
        .next()
        .expect("cursor is in range")
        .len_utf8()
}

impl Tokenizer for AntflyTokenizer {
    type TokenStream<'a> = AntflyTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.token.reset();
        AntflyTokenStream {
            text,
            cursor: 0,
            token: &mut self.token,
        }
    }
}

impl TokenStream for AntflyTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        let bytes = self.text.as_bytes();
        while self.cursor < bytes.len() && !antfly_alphanumeric(bytes[self.cursor]) {
            self.cursor += utf8_width(self.text, self.cursor);
        }
        if self.cursor >= bytes.len() {
            return false;
        }
        let start = self.cursor;
        while self.cursor < bytes.len() && antfly_alphanumeric(bytes[self.cursor]) {
            self.cursor += utf8_width(self.text, self.cursor);
        }
        self.token.position = self.token.position.wrapping_add(1);
        self.token.offset_from = start;
        self.token.offset_to = self.cursor;
        self.token.text.push_str(&self.text[start..self.cursor]);
        true
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

fn ascii_lowercase(text: &str) -> String {
    let mut bytes = text.as_bytes().to_vec();
    for byte in &mut bytes {
        if byte.is_ascii_uppercase() {
            *byte = byte.to_ascii_lowercase();
        }
    }
    String::from_utf8(bytes).expect("ASCII case mapping preserves UTF-8")
}

fn normalized_text(line: &str) -> String {
    match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(object)) => object
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        _ => line.to_owned(),
    }
}

fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn index_corpus(args: Args) -> Result<()> {
    fs::create_dir_all(&args.db_path)?;
    let started = Instant::now();
    let (schema, text_field, ordinal_field) = schema();
    let index = Index::create_in_dir(&args.db_path, schema)?;
    register_analyzer(&index);
    let mut writer = index.writer(512_000_000)?;
    // In single-segment mode, keep the segment set stable until the explicit
    // final merge. Otherwise Tantivy's background merge policy can replace a
    // segment between searchable_segment_ids() and writer.merge().
    if matches!(args.segment_mode, SegmentMode::Single) {
        writer.set_merge_policy(Box::new(NoMergePolicy));
    }
    let stdin = io::stdin();
    let mut hasher = Sha256::new();
    let mut documents = 0usize;
    let mut normalized_bytes = 0u64;
    let mut empty_text_documents = 0usize;
    for raw in stdin.lock().lines() {
        let raw = raw?;
        let line = raw.trim_matches(|c| matches!(c, ' ' | '\t' | '\r' | '\n'));
        if line.is_empty() {
            continue;
        }
        if documents > u32::MAX as usize {
            bail!("corpus exceeds u32 ordinal space");
        }
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
        normalized_bytes = normalized_bytes.saturating_add(line.len() as u64 + 1);
        let source_text = normalized_text(line);
        if source_text.is_empty() {
            empty_text_documents += 1;
        }
        let mut document = TantivyDocument::default();
        document.add_text(text_field, ascii_lowercase(&source_text));
        document.add_u64(ordinal_field, documents as u64);
        writer.add_document(document)?;
        documents += 1;
        if documents % BATCH_SIZE == 0 {
            writer.commit()?;
        }
    }
    writer.commit()?;
    if matches!(args.segment_mode, SegmentMode::Single) {
        let segments = index.searchable_segment_ids()?;
        if segments.len() > 1 {
            writer.merge(&segments).wait()?;
            writer.commit()?;
        }
    }
    writer.wait_merging_threads()?;
    let segments = index.searchable_segment_metas()?;
    if matches!(args.segment_mode, SegmentMode::Single) && segments.len() != 1 {
        bail!(
            "single segment invariant failed: {} segments",
            segments.len()
        );
    }
    let manifest = json!({
        "schema_version": 1,
        "result_schema_version": 1,
        "query_grammar": GRAMMAR,
        "engine": "tantivy-0.25",
        "corpus": {
            "normalization": "trim_ascii_whitespace_drop_blank_append_lf_json_or_plain_text",
            "sha256": hex_digest(hasher.finalize()),
            "uncompressed_bytes": normalized_bytes,
            "input_documents": documents,
            "indexed_documents": documents,
            "rejected_documents": 0,
            "empty_text_documents": empty_text_documents,
            "ordinal_assignment": "zero_based_normalized_input_order_u32"
        },
        "segment_mode": if matches!(args.segment_mode, SegmentMode::Single) { "single" } else { "production" },
        "ingestion_batch_size": BATCH_SIZE,
        "analysis": {"name": "simple", "tokenizer": "unicode_words", "filters": ["ascii_lowercase"], "stop_words": false, "stemming": false},
        "bm25": {"k1": args.bm25_k1, "b": args.bm25_b},
        "index_elapsed_ns": started.elapsed().as_nanos(),
        "layout": {"global_doc_count": documents, "total_bytes": directory_bytes(&args.db_path)?, "segment_count": segments.len(), "segments": segments.iter().map(|segment| json!({"segment_id": segment.id().uuid_string(), "doc_count": segment.max_doc(), "deleted_count": segment.num_deleted_docs()})).collect::<Vec<_>>()}
    });
    let encoded = serde_json::to_string_pretty(&manifest)?;
    if let Some(path) = args.manifest {
        fs::write(path, format!("{encoded}\n"))?;
    }
    eprintln!("TANTIVY_SEARCH_BENCH_MANIFEST {encoded}");
    Ok(())
}

enum Operation {
    Term,
    Union,
    Intersection,
    Phrase,
}

struct ParsedQuery<'a> {
    operation: Operation,
    terms: Vec<&'a str>,
}

fn parse_query(raw: &str) -> Option<ParsedQuery<'_>> {
    let parts: Vec<&str> = raw.split_ascii_whitespace().collect();
    if parts.len() < 4 || parts[0] != GRAMMAR || parts[2] != "text" {
        return None;
    }
    let operation = match parts[1] {
        "TERM" if parts.len() == 4 => Operation::Term,
        "UNION" => Operation::Union,
        "INTERSECTION" => Operation::Intersection,
        "PHRASE" => Operation::Phrase,
        _ => return None,
    };
    Some(ParsedQuery {
        operation,
        terms: parts[3..].to_vec(),
    })
}

fn build_query(parsed: ParsedQuery<'_>, field: Field) -> Box<dyn Query> {
    let terms: Vec<Term> = parsed
        .terms
        .iter()
        .map(|term| Term::from_field_text(field, term))
        .collect();
    match parsed.operation {
        Operation::Term => Box::new(TermQuery::new(
            terms[0].clone(),
            IndexRecordOption::WithFreqsAndPositions,
        )),
        Operation::Phrase => Box::new(PhraseQuery::new(terms)),
        Operation::Union | Operation::Intersection => {
            let occur = if matches!(parsed.operation, Operation::Union) {
                Occur::Should
            } else {
                Occur::Must
            };
            Box::new(BooleanQuery::new(
                terms
                    .into_iter()
                    .map(|term| {
                        (
                            occur,
                            Box::new(TermQuery::new(
                                term,
                                IndexRecordOption::WithFreqsAndPositions,
                            )) as Box<dyn Query>,
                        )
                    })
                    .collect(),
            ))
        }
    }
}

#[derive(Serialize)]
struct Hit {
    id: u32,
    score: Score,
}

#[derive(Serialize)]
struct Verification {
    schema_version: u16,
    query_grammar: &'static str,
    total_hits: usize,
    relation: &'static str,
    hits: Vec<Hit>,
}

fn ordinal(reader: &IndexReader, address: DocAddress) -> Result<u32> {
    let searcher = reader.searcher();
    let segment = searcher.segment_reader(address.segment_ord);
    let column = segment.fast_fields().u64("corpus_ordinal")?;
    let value = column
        .first(address.doc_id)
        .context("missing corpus ordinal")?;
    Ok(u32::try_from(value)?)
}

fn verify(reader: &IndexReader, query: &dyn Query, limit: usize) -> Result<Verification> {
    let searcher = reader.searcher();
    let total_hits = searcher.search(query, &Count)?;
    let docs = searcher.search(query, &TopDocs::with_limit(limit))?;
    let mut hits = Vec::with_capacity(docs.len());
    for (score, address) in docs {
        hits.push(Hit {
            id: ordinal(reader, address)?,
            score,
        });
    }
    Ok(Verification {
        schema_version: 1,
        query_grammar: GRAMMAR,
        total_hits,
        relation: "exact",
        hits,
    })
}

fn explain(
    reader: &IndexReader,
    query: &dyn Query,
    text_field: Field,
    target_ordinal: u32,
) -> Result<Value> {
    let searcher = reader.searcher();
    let mut address = None;
    for (segment_ord, segment) in searcher.segment_readers().iter().enumerate() {
        let ordinals = segment.fast_fields().u64("corpus_ordinal")?;
        for doc_id in 0..segment.max_doc() {
            if ordinals.first(doc_id) == Some(target_ordinal as u64) {
                address = Some(DocAddress::new(segment_ord as u32, doc_id));
                break;
            }
        }
        if address.is_some() {
            break;
        }
    }
    let address = address.context("corpus ordinal not found")?;
    let total_num_tokens = searcher
        .segment_readers()
        .iter()
        .try_fold(0u64, |total, segment| {
            Ok::<u64, tantivy::TantivyError>(
                total + segment.inverted_index(text_field)?.total_num_tokens(),
            )
        })?;
    let explanation = query.explain(&searcher, address)?;
    Ok(json!({
        "global_doc_count": searcher.num_docs(),
        "total_field_len": total_num_tokens,
        "average_doc_length": total_num_tokens as f64 / searcher.num_docs() as f64,
        "explanation": explanation,
    }))
}

fn analyze(text: &str) -> Result<Value> {
    let normalized = ascii_lowercase(text);
    let mut tokenizer = AntflyTokenizer::default();
    let mut stream = tokenizer.token_stream(&normalized);
    let mut tokens = Vec::new();
    while stream.advance() {
        let token = stream.token();
        tokens.push(json!({"term": token.text, "position": token.position, "start_byte": token.offset_from, "end_byte": token.offset_to}));
    }
    Ok(json!({"schema_version": 1, "analyzer": "simple", "tokens": tokens}))
}

fn query_loop(args: Args) -> Result<()> {
    let index = Index::open_in_dir(&args.db_path)?;
    register_analyzer(&index);
    let schema = index.schema();
    let text_field = schema.get_field("text")?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let stdin = BufReader::new(io::stdin());
    let mut stdout = io::BufWriter::new(io::stdout());
    for raw in stdin.lines() {
        let raw = raw?;
        let Some((command, payload)) = raw.split_once('\t') else {
            writeln!(stdout, "UNSUPPORTED")?;
            stdout.flush()?;
            continue;
        };
        if command == "ANALYZE" {
            writeln!(stdout, "{}", serde_json::to_string(&analyze(payload)?)?)?;
            stdout.flush()?;
            continue;
        }
        let Some(parsed) = parse_query(payload) else {
            writeln!(stdout, "UNSUPPORTED")?;
            stdout.flush()?;
            continue;
        };
        let query = build_query(parsed, text_field);
        if command == "COUNT" {
            writeln!(
                stdout,
                "{}",
                reader.searcher().search(query.as_ref(), &Count)?
            )?;
        } else if let Some(target_ordinal) = command
            .strip_prefix("EXPLAIN_")
            .and_then(|value| value.parse::<u32>().ok())
        {
            writeln!(
                stdout,
                "{}",
                serde_json::to_string(&explain(
                    &reader,
                    query.as_ref(),
                    text_field,
                    target_ordinal
                )?)?
            )?;
        } else if let Some(limit) = command
            .strip_prefix("VERIFY_TOP_")
            .and_then(|value| value.strip_suffix("_COUNT").or(Some(value)))
            .and_then(|value| value.parse::<usize>().ok())
        {
            writeln!(
                stdout,
                "{}",
                serde_json::to_string(&verify(&reader, query.as_ref(), limit)?)?
            )?;
        } else if let Some(limit) = command
            .strip_prefix("TOP_")
            .and_then(|value| value.strip_suffix("_COUNT").or(Some(value)))
            .and_then(|value| value.parse::<usize>().ok())
        {
            let _ = reader
                .searcher()
                .search(query.as_ref(), &TopDocs::with_limit(limit))?;
            if command.ends_with("_COUNT") {
                writeln!(
                    stdout,
                    "{}",
                    reader.searcher().search(query.as_ref(), &Count)?
                )?;
            } else {
                writeln!(stdout, "1")?;
            }
        } else {
            writeln!(stdout, "UNSUPPORTED")?;
        }
        stdout.flush()?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let (operation, args) = parse_args(std::env::args().skip(1))?;
    match operation.as_str() {
        "index" => index_corpus(args),
        "query" => query_loop(args),
        _ => bail!("operation must be index or query"),
    }
}
