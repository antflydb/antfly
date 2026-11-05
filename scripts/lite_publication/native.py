# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Construct and audit native Lite checkpoints with the shipped Antfly CLI."""

import hashlib
import json
import os
import stat
import subprocess
import tempfile
from pathlib import Path

from .contract import (
    DENSE,
    DOC_FIELDS,
    FT,
    INDEX,
    MAPPING,
    MAX_CHUNK,
    MAX_DOCUMENT,
    MAX_INPUT,
    PROFILE,
    SCHEMA,
    Error,
    chunk_id,
    coverage,
    digest,
    fields,
    locator,
    manifest,
    number,
    provenance,
    require,
    rows,
    same_json,
    string,
    validate_outcomes,
    vector,
)

MAX_ARTIFACT = 2 << 30


def unique(items):
    obj = {}
    for key, value in items:
        require(key not in obj, f"Duplicate JSON key: {key}")
        obj[key] = value
    return obj


def load_json(path, limit=MAX_INPUT):
    with open(path, "rb") as stream:
        before = os.fstat(stream.fileno())
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
        require(
            (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            == (after.st_size, after.st_mtime_ns, after.st_ctime_ns),
            "Input changed while freezing the publication",
        )
    require(len(data) <= limit, "JSON exceeds input size limit")
    return json.loads(data, object_pairs_hook=unique)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def sync_directory(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def lite(command, path, *args, body=None):
    # Commands are fixed by this module. In particular hosted calls never open
    # a writer handle or pass through a reader-supplied native request.
    cmd = [
        os.environ.get("ANTFLY_BIN", "antfly"),
        "lite",
        *command.split(),
        str(path),
        *args,
    ]
    with tempfile.TemporaryDirectory(prefix="afpub-request-") as directory:
        if body is not None:
            request = Path(directory, "request.json")
            request.write_text(json.dumps(body, allow_nan=False, separators=(",", ":")))
            cmd.extend(["--file", str(request)])
        result = subprocess.run(cmd, check=False, capture_output=True, timeout=300)
    if result.returncode:
        detail = result.stderr.decode(errors="replace").strip().splitlines()
        raise Error(
            "native_error",
            f"lite {command} failed: {(detail or ['no diagnostic'])[-1][:256]}",
        )
    return json.loads(result.stdout, object_pairs_hook=unique)


def build(source, out):
    require(
        str(out).endswith(".aflite") and not out.exists(),
        "Output must be a new .aflite path",
    )
    corpus = load_json(source)
    with tempfile.TemporaryDirectory(
        dir=out.parent, prefix=".afpub-build-"
    ) as directory:
        db = Path(directory, "builder.aflite")
        lite("init", db)
        lite("schema set", db, body=SCHEMA)
        lite(
            "index create",
            db,
            body={
                "name": DENSE,
                "kind": "dense_vector",
                "config_json": json.dumps(INDEX),
            },
        )
        batch = {}
        size = 0
        for key, row in rows(corpus):
            batch[key] = row
            size += len(json.dumps(row))
            if size >= 4 << 20 or len(batch) >= 128:
                lite("batch", db, body={"inserts": batch, "sync_level": "full_index"})
                batch, size = {}, 0
        if batch:
            lite("batch", db, body={"inserts": batch, "sync_level": "full_index"})
        frozen = Path(directory, "copy.aflite")
        lite("snapshot", db, "--out", str(frozen))
        result = inspect(frozen)
        os.link(frozen, out)  # no overwrite, even with concurrent builders
        sync_directory(out.parent)
        return result


def scan(path):
    # The native cursor pages the immutable file without reopening its indexes
    # for each page. A bounded frame and a checked child exit distinguish end
    # of input from a truncated or failed scan.
    with tempfile.TemporaryDirectory(prefix="afpub-scan-") as directory:
        request = Path(directory, "projection.json")
        request.write_text(
            json.dumps({"fields": ["*", "_embeddings", "_chunks", "_artifacts"]})
        )
        with tempfile.TemporaryFile() as errors:
            command = [
                os.environ.get("ANTFLY_BIN", "antfly"),
                "lite",
                "scan-stream",
                str(path),
                "--file",
                str(request),
            ]
            with subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=errors
            ) as proc:
                try:
                    start = ""
                    while line := proc.stdout.readline(32 << 20):
                        require(
                            line.endswith(b"\n"),
                            "Invalid or oversized native scan frame",
                        )
                        entry = json.loads(line, object_pairs_hook=unique)
                        key = entry["_id"]
                        require(key > start, "Native scan failed to advance")
                        start = key
                        yield (
                            key,
                            json.loads(
                                entry["document_json"], object_pairs_hook=unique
                            ),
                        )
                    if proc.wait():
                        errors.seek(0)
                        raise Error(
                            "native_error",
                            "lite scan-stream failed: "
                            + errors.read(256).decode(errors="replace"),
                        )
                finally:
                    if proc.poll() is None:
                        proc.kill()


def check_catalog(path):
    require(
        lite("root-catalog", path) == {"tables": ["default"], "owner": "embedded"},
        "Expected a single embedded root for publication v1",
    )
    require(
        lite("schema get", path) == SCHEMA,
        "Incompatible publication schema. Rebuild the copy with publication-v1/schema.json",
    )
    require(
        lite("enrichment list", path) == [],
        "Publication has executable enrichments. Build the isolated copy with completed vectors and omit local producers",
    )
    actual = lite("index list", path)
    require(
        len(actual) == 2 and {x["name"] for x in actual} == {FT, DENSE},
        "Unexpected publication indexes",
    )
    for index in actual:
        kind, config = (
            ("full_text", {}) if index["name"] == FT else ("dense_vector", INDEX)
        )
        require(
            index["kind"] == kind
            and same_json(
                json.loads(index["config_json"], object_pairs_hook=unique), config
            ),
            f"Incompatible index configuration: {index['name']}",
        )


def inspect(path, expected=None):
    initial = path.lstat()
    require(
        stat.S_ISREG(initial.st_mode) and 0 < initial.st_size <= MAX_ARTIFACT,
        "Expected a regular nonempty .aflite file of at most 2 GiB",
    )
    actual_digest = sha256_file(path)
    require(
        expected is None or expected == actual_digest,
        "Artifact checksum mismatch",
        "digest_mismatch",
    )
    integrity = lite("check", path)
    require(
        integrity["valid"] is True and integrity["tail_bytes"] == 0,
        "Incomplete native checkpoint",
    )
    check_catalog(path)
    status = lite("status", path)
    version = status["storage"]["format_version"]
    require(version == 7, f"Unsupported native Lite format {version}")
    metadata = lite("lookup", path, "--key", "publication")
    require(metadata["found"], "Missing publication manifest")
    metadata = metadata["_source"]
    fields(
        metadata,
        "_type contract_version corpus_revision embedding mapping failures exclusions coverage",
    )
    number(metadata["contract_version"], "contract version", 1, 1)
    string(metadata["corpus_revision"], "corpus revision")
    require(
        metadata["embedding"] == PROFILE and metadata["mapping"] == MAPPING,
        "Incompatible publication profile",
    )
    outcomes = validate_outcomes(metadata["failures"], metadata["exclusions"])
    # Retain only bounded provenance and slice digests. Never hold the full
    # corpus or duplicate its entire vector set while inspecting an upload.
    parents, children = {}, {}
    vectors = count = total_text = 0
    keyword_key = None
    query_vector = None
    for key, row in scan(path):
        count += 1
        require(count <= 100000, "Too many publication rows")
        if key == "publication":
            require(row == metadata, "Unexpected manifest fields or artifacts")
            continue
        is_parent = key.startswith("d:")
        extra = (
            ""
            if is_parent
            else " chunk_id parent_id filename_tokens start_byte end_byte locator"
        )
        fields(
            row,
            "_type " + DOC_FIELDS + " content" + extra,
            "" if is_parent else "_artifacts _embeddings vector_sha256",
        )
        provenance(row)
        common = hashlib.sha256(
            "\x00".join(row[k] for k in DOC_FIELDS.split()).encode()
        ).digest()
        ident = row["document_id"]
        require(
            ident not in outcomes, "A published file also has a failed/excluded outcome"
        )
        string(row["content"], "content", MAX_DOCUMENT)
        data = row["content"].encode()
        total_text += len(data)
        require(total_text <= 2 * MAX_INPUT, "Expanded text exceeds publication limit")
        if is_parent:
            require(
                key == "d:" + ident
                and row["_type"] == "source_document"
                and digest(data) == row["document_revision"],
                "Inconsistent parent row",
            )
            # c: keys sort before d: keys in the native scan.
            spans = sorted(children.pop(ident, []))
            end = 0
            prev = -1
            for start, stop, text_hash, child_common in spans:
                require(
                    prev < start <= end < stop <= len(data)
                    and common == child_common
                    and digest(data[start:stop]) == text_hash,
                    "Chunk/parent revision or span mismatch",
                )
                prev, end = start, stop
            require(end == len(data), "Incomplete keyword coverage")
            parents[ident] = len(spans)
        else:
            require(
                row["_type"] == "passage"
                and key == row["chunk_id"]
                and row["parent_id"] == "d:" + ident
                and row["filename_tokens"] == row["filename"],
                "Invalid passage identity",
            )
            keyword_key = keyword_key or key
            start, end = row["start_byte"], row["end_byte"]
            number(start, "chunk start", 0, MAX_DOCUMENT)
            number(end, "chunk end", start + 1, min(MAX_DOCUMENT, start + MAX_CHUNK))
            require(
                len(data) == end - start and key == chunk_id(row, start, end),
                "Invalid chunk revision",
            )
            locator(row["locator"])
            children.setdefault(ident, []).append((start, end, digest(data), common))
            if "_embeddings" in row:
                fields(row.get("_artifacts"), DENSE)
                artifact = row["_artifacts"][DENSE]
                fields(
                    artifact,
                    "artifact_id artifact_ref kind content_type status dims value",
                )
                require(
                    artifact["artifact_ref"]
                    == {"document_id": key, "name": DENSE, "kind": "embedding"}
                    and artifact["kind"] == "embedding"
                    and artifact["status"] == "ready"
                    and artifact["dims"] == 768
                    and artifact["value"] is None
                    and artifact["content_type"]
                    == "application/vnd.antfly.embedding+binary",
                    "Unexpected native embedding artifact",
                )
                fields(row["_embeddings"], DENSE)
                require(
                    vector(row["_embeddings"][DENSE]) == row.get("vector_sha256"),
                    "Stored vector checksum mismatch",
                )
                if query_vector is None:
                    query_vector = row["_embeddings"][DENSE]
                vectors += 1
            else:
                require(
                    "vector_sha256" not in row and "_artifacts" not in row,
                    "Declared vector is missing",
                )
    require(not children, "Orphaned passages")
    cov = coverage(
        len(parents),
        sum(parents.values()),
        vectors,
        metadata["failures"],
        metadata["exclusions"],
    )
    require(
        same_json(manifest(metadata, cov), metadata),
        "Frozen manifest or coverage mismatch",
    )
    counts = {x["name"]: x["doc_count"] for x in status["stats"]["indexes"]}
    require(
        counts == {FT: count, DENSE: vectors},
        "Native index counts disagree with stored content",
    )
    # Prove the reopened native indexes actually serve their postings before
    # changing the destination. Counts alone can come from a stored catalog.
    keyword_hits = query_hits(
        path,
        {
            "full_text_search": {"term": keyword_key or "empty", "field": "chunk_id"},
            "indexes": [FT],
            "limit": 1,
            "fields": ["chunk_id", "content"],
        },
    )
    require(
        (
            [hit["_id"] for hit in keyword_hits]
            == ([keyword_key] if keyword_key else [])
        ),
        "Keyword index failed its publication read probe",
    )
    if query_vector is not None:
        vector_hits = query_hits(
            path,
            {
                "embeddings": {DENSE: query_vector},
                "indexes": [DENSE],
                "limit": 1,
                "fields": ["chunk_id", "content", "vector_sha256"],
            },
        )
        require(
            len(vector_hits) == 1 and vector_hits[0]["_source"].get("vector_sha256"),
            "Dense index failed its publication read probe",
        )
    final = path.stat()
    require(
        (initial.st_dev, initial.st_ino, initial.st_size, initial.st_ctime_ns)
        == (final.st_dev, final.st_ino, final.st_size, final.st_ctime_ns),
        "Artifact changed during inspection",
    )
    return {
        "artifact_sha256": actual_digest,
        "format_version": version,
        "contract_version": 1,
        "corpus_revision": metadata["corpus_revision"],
        "coverage": cov,
        "embedding": PROFILE,
        "mapping": MAPPING,
        "size_bytes": path.stat().st_size,
    }


def search(path, query):
    fields(query, "", "text limit vector embedding")
    limit = query.get("limit", 20)
    number(limit, "query limit", 1, 100)
    if "text" in query:
        fields(query, "text", "limit")
        string(query["text"], "query text", 4096)
        arms = [
            {"match": {"field": field, "text": query["text"]}}
            for field in ("content", "filename_tokens")
        ]
        body = {"full_text_search": {"disjuncts": arms}, "indexes": [FT]}
    else:
        fields(query, "vector embedding", "limit")
        require(
            same_json(query["embedding"], PROFILE),
            "Incompatible query model or task",
            "incompatible_model",
        )
        vector(query["vector"])
        body = {"embeddings": {DENSE: query["vector"]}, "indexes": [DENSE]}
    body.update(
        {
            "limit": limit,
            "fields": ["*"],
            "filter_query": {"term": "passage", "field": "_type"},
        }
    )
    return query_hits(path, body)


def query_hits(path, body):
    result = lite("query", path, body=body)["responses"][0]
    require(result["status"] == 200, "Native query failed", "native_error")
    return result["hits"]["hits"]


def search_page(path, destination, generation, request):
    require(type(request) is dict, "Expected a query object")
    query = request.copy()
    cursor = query.pop("cursor", None)
    page_size = query.pop("page_size", query.get("limit", 20))
    number(page_size, "page size", 1, 100)
    hits = sorted(search(path, query), key=lambda hit: (-hit["_score"], hit["_id"]))
    # A resumed query must reproduce the same bounded, ordered window. A
    # changed native candidate order fails explicitly, even within a revision.
    binding = digest(
        json.dumps(
            [destination, generation, query, page_size, [h["_id"] for h in hits]],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    offset = 0
    if cursor is not None:
        fields(cursor, "generation offset binding")
        require(
            cursor["generation"] == generation,
            "Continuation is from another generation",
            "stale_generation",
        )
        require(
            cursor["binding"] == binding,
            "Query or candidate window changed",
            "stale_cursor",
        )
        offset = cursor["offset"]
        number(offset, "page offset", 1, len(hits))
    stop = offset + page_size
    result = {"generation": generation, "hits": hits[offset:stop]}
    if stop < len(hits):
        result["next_cursor"] = {
            "generation": generation,
            "offset": stop,
            "binding": binding,
        }
    return result
