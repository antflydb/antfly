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

"""The Drive-only portable row profile. No input field is executable."""

import hashlib
import json
import math
import re
import struct
from importlib.resources import files

DATA = files(__name__.rpartition(".")[0])
PROFILE = json.loads((DATA / "embedding.json").read_bytes())
SCHEMA = json.loads((DATA / "schema.json").read_bytes())
FT = "full_text_index_v0"
DENSE = "chunk_vectors"
INDEX = {"field": "embedding", "dims": 768, "metric": "cosine", "external": True}
MAPPING = {
    "table": "files",
    "physical_table": "default",
    "keyword_index": FT,
    "vector_index": DENSE,
    "text_field": "content",
    "document_prefix": "d:",
    "chunk_prefix": "c:",
}
DOC_FIELDS = "document_id source_revision document_revision source_url source_path filename mime_type"
MAX_INPUT = 64 * 1024 * 1024
MAX_DOCUMENT = 4 * 1024 * 1024
MAX_CHUNK = 64 * 1024
ID = re.compile(r"[A-Za-z0-9_-]{1,200}\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")


class Error(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def require(ok, message, code="invalid_artifact"):
    if not ok:
        raise Error(code, message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def same_json(left, right):
    """Preserve JSON types, including for booleans and numeric capabilities."""
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(
        right, sort_keys=True, allow_nan=False
    )


def fields(obj, names, optional=""):
    required = set(names.split())
    require(
        type(obj) is dict
        and required <= obj.keys() <= required | set(optional.split()),
        f"Expected fields {names} (optional: {optional})",
    )


def string(value, name, limit=1024):
    require(
        type(value) is str and 0 < len(value.encode()) <= limit and "\x00" not in value,
        f"Invalid {name}",
    )


def number(value, name, low, high):
    require(type(value) is int and low <= value <= high, f"Invalid {name}")


def vector(values):
    require(
        type(values) is list and len(values) == 768, "Expected 768 vector components"
    )
    require(
        all(type(x) in (int, float) and -1.001 <= x <= 1.001 for x in values),
        "Vector components must be finite normalized numbers",
    )
    require(
        abs(math.fsum(x * x for x in values) - 1) <= 0.002,
        "Expected an L2-normalized vector",
    )
    return digest(struct.pack("<768f", *values))


def chunk_id(doc, start, end):
    parts = [
        doc["document_id"],
        doc["source_revision"],
        doc["document_revision"],
        str(start),
        str(end),
    ]
    return "c:" + digest("\x00".join(parts).encode())


def locator(value):
    require(
        type(value) is dict and type(value.get("kind")) is str,
        "Expected a typed locator",
    )
    kind = value["kind"]
    layouts = {
        "text": "",
        "page": "page",
        "slide": "slide",
        "sheet": "sheet",
        "rows": "sheet row_start row_end",
    }
    require(kind in layouts, "Unknown locator kind")
    fields(value, "kind " + layouts[kind])
    for name in layouts[kind].split():
        if name == "sheet":
            string(value[name], name)
        else:
            number(value[name], name, 1, 2**31 - 1)
    if kind == "rows":
        require(value["row_end"] >= value["row_start"], "Reversed row range")


def provenance(doc):
    for k in DOC_FIELDS.split():
        string(doc[k], k, 4096)
    ident = doc["document_id"]
    require(ID.fullmatch(ident), "Invalid Drive document ID")
    path = doc["source_path"].split("/")
    require(
        all(x not in ("", ".", "..") and "\\" not in x for x in path)
        and path[-1] == doc["filename"],
        "Expected a relative Drive display path",
    )
    urls = {
        f"https://drive.google.com/open?id={ident}",
        f"https://drive.google.com/file/d/{ident}/view",
    }
    urls.update(
        f"https://docs.google.com/{app}/d/{ident}/{action}"
        for app in ("document", "presentation", "spreadsheets")
        for action in ("edit", "view")
    )
    require(
        doc["source_url"] in urls, "Expected a canonical Google URL for this Drive ID"
    )
    require(SHA.fullmatch(doc["document_revision"]), "Invalid document revision")


def validate_outcomes(failures, exclusions):
    seen = set()
    for group in (failures, exclusions):
        require(type(group) is list, "Expected extraction outcomes")
        for record in group:
            fields(record, "document_id source_revision reason")
            ident = record["document_id"]
            string(ident, "document_id")
            string(record["source_revision"], "source_revision")
            string(record["reason"], "reason", 64)
            require(
                ID.fullmatch(ident)
                and re.fullmatch("[a-z][a-z0-9_]*", record["reason"]),
                "Invalid extraction outcome",
            )
            require(ident not in seen, "Duplicate Drive outcome")
            seen.add(ident)
    return seen


def coverage(documents, chunks, vectors, failures, exclusions):
    return {
        "selected_files": documents + len(failures) + len(exclusions),
        "extracted_documents": documents,
        "failed_files": len(failures),
        "excluded_files": len(exclusions),
        "keyword_documents": documents,
        "keyword_chunks": chunks,
        "total_chunks": chunks,
        "semantic_chunks": vectors,
    }


def manifest(corpus, cov):
    return {
        "_type": "publication",
        "contract_version": 1,
        "corpus_revision": corpus["corpus_revision"],
        "embedding": PROFILE,
        "mapping": MAPPING,
        "failures": corpus["failures"],
        "exclusions": corpus["exclusions"],
        "coverage": cov,
    }


def rows(corpus):
    fields(
        corpus,
        "contract_version corpus_revision embedding failures exclusions documents",
    )
    number(corpus["contract_version"], "contract version", 1, 1)
    require(same_json(corpus["embedding"], PROFILE), "Incompatible embedding profile")
    string(corpus["corpus_revision"], "corpus revision")
    seen = validate_outcomes(corpus["failures"], corpus["exclusions"])
    documents = corpus["documents"]
    require(type(documents) is list, "Expected documents")
    chunks = vectors = 0
    for doc in documents:
        fields(doc, DOC_FIELDS + " content chunks")
        provenance(doc)
        string(doc["content"], "extracted content", MAX_DOCUMENT)
        data = doc["content"].encode()
        require(
            digest(data) == doc["document_revision"],
            "Extracted content revision mismatch",
        )
        ident = doc["document_id"]
        require(ident not in seen, "Duplicate Drive document")
        seen.add(ident)
        common = {key: doc[key] for key in DOC_FIELDS.split()}
        parent = "d:" + ident
        yield parent, {"_type": "source_document", **common, "content": doc["content"]}
        require(type(doc["chunks"]) is list, "Expected chunks")
        covered = 0
        previous_start = -1
        for chunk in doc["chunks"]:
            fields(chunk, "start_byte end_byte locator", "embedding")
            start, end = chunk["start_byte"], chunk["end_byte"]
            number(start, "chunk start", previous_start + 1, covered)
            number(
                end,
                "chunk end",
                max(covered, start) + 1,
                min(len(data), start + MAX_CHUNK),
            )
            covered, previous_start = end, start
            locator(chunk["locator"])
            key = chunk_id(doc, start, end)
            row = {
                "_type": "passage",
                **common,
                "chunk_id": key,
                "parent_id": parent,
                "filename_tokens": doc["filename"],
                "start_byte": start,
                "end_byte": end,
                "locator": chunk["locator"],
                "content": data[start:end].decode(),
            }
            if "embedding" in chunk:
                emb = chunk["embedding"]
                fields(emb, "chunk_id model task values")
                require(
                    emb["chunk_id"] == key
                    and emb["task"] == PROFILE["document_task"]
                    and emb["model"] == PROFILE["model"],
                    "Vector chunk revision/model/task mismatch",
                )
                row["vector_sha256"] = vector(emb["values"])
                row["_embeddings"] = {DENSE: emb["values"]}
                vectors += 1
            chunks += 1
            yield key, row
        require(
            covered == len(data), "Chunks must cover every byte of extracted content"
        )
    yield (
        "publication",
        manifest(
            corpus,
            coverage(
                len(documents),
                chunks,
                vectors,
                corpus["failures"],
                corpus["exclusions"],
            ),
        ),
    )
