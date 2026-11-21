#!/usr/bin/env python3
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

"""Memorilla playground (plan section 8.35): a local page over the Zig span hybrid.

Serves ``index.html`` and a small JSON API that turns pasted chunks and a
question into ``POST /ai/v1/memorilla/generate`` calls on a running Zig
inference server (``run.sh`` starts both):

1. every chunk is split into sentence-greedy passages of at most 64 encoder
   tokens (Qwen3-Embedding tokenizer, no special tokens; the rule of the Zig
   pooled-pass chunker ``chunkDocument``);
2. ranking averages each raw passage score and its score with the source
   opening sentence prepended. Dense scores use cosine between stock
   Qwen3-Embedding-4B vectors from the server's ``/ai/v1/embed`` route (the
   upstream Memorilla retrieval the cascade studies' candidate lists used,
   ``qwen_hybrid_eval.retrieval_order``); ``--ranking bm25`` is a CPU
   fallback;
3. ``document_texts`` = every passage in ranked order (the soft half pools
   them all, the span reads take the leading ones), with the chosen
   ``read_mode`` and ``include_diagnostics``; "compare" adds a second call
   with ``read_mode: none`` (stock Memorilla over the same pooled vectors).

Stdlib only, except ``tokenizers`` for exact passage token counts. Binds to
127.0.0.1 by default.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import re
import sys
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve().parent
ASCII_WHITESPACE = " \t\n\r\x0b\x0c"
# Service limits (memorilla_request / grounding): texts per request, bytes per
# text, bytes per request.
MAX_DOCUMENTS = 64
MAX_TEXT_BYTES = 8192
MAX_TOTAL_BYTES = 32768
# memorilla_span.default_read_documents (the narrow read).
NARROW_DOCUMENTS = 2
READ_MODES = ("auto", "narrow", "wide", "none", "default")


# --- passages ---------------------------------------------------------------------


class TokenCounter:
    """Encoder token counts without special tokens (``tokenizer(text,
    add_special_tokens=False)``, the read budget's count)."""

    def __init__(self, tokenizer_path: Path):
        from tokenizers import Tokenizer  # local import: the only dependency

        self.tokenizer = Tokenizer.from_file(str(tokenizer_path))
        self.cache: dict[str, int] = {}
        self.lock = threading.Lock()

    def __call__(self, text: str) -> int:
        with self.lock:
            found = self.cache.get(text)
            if found is None:
                found = len(self.tokenizer.encode(text, add_special_tokens=False).ids)
                if len(self.cache) > 50_000:
                    self.cache.clear()
                self.cache[text] = found
            return found


def _trim(text: str, start: int, end: int) -> tuple[int, int] | None:
    while start < end and text[start] in ASCII_WHITESPACE:
        start += 1
    while end > start and text[end - 1] in ASCII_WHITESPACE:
        end -= 1
    return (start, end) if start < end else None


def _pack(
    text: str, units: list[tuple[int, int]], count, max_tokens: int
) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    held: tuple[int, int] | None = None
    for unit in units:
        if held is not None:
            candidate = (held[0], unit[1])
            if count(text[candidate[0] : candidate[1]]) <= max_tokens:
                held = candidate
                continue
            out.append(held)
        held = unit
    if held is not None:
        out.append(held)
    return out


def _split_unit(
    text: str, unit: tuple[int, int], count, max_tokens: int
) -> list[tuple[int, int]]:
    words: list[tuple[int, int]] = []
    i, end = unit
    while i < end:
        while i < end and text[i] in ASCII_WHITESPACE:
            i += 1
        if i == end:
            break
        j = i
        while j < end and text[j] not in ASCII_WHITESPACE:
            j += 1
        if count(text[i:j]) <= max_tokens:
            words.append((i, j))
        else:
            piece = i
            while piece < j:
                stop = min(j, piece + max_tokens - 1)
                words.append((piece, stop))
                piece = stop
        i = j
    return _pack(text, words, count, max_tokens)


def split_passages(text: str, count, max_tokens: int) -> list[tuple[int, int]]:
    """Sentence-greedy passages of ``text`` (Zig ``chunkDocument``'s rule):
    sentences end after ``.``, ``!`` or ``?`` followed by whitespace, or at a
    newline; consecutive sentences merge while the passage fits
    ``max_tokens``; an oversized sentence splits at whitespace. Returns
    trimmed ``[start, end)`` character ranges of ``text``, in order."""
    if max_tokens < 2:
        raise ValueError("max_tokens must be at least 2")
    units: list[tuple[int, int]] = []
    start = 0

    def add(range_):
        if range_ is None:
            return
        if count(text[range_[0] : range_[1]]) <= max_tokens:
            units.append(range_)
        else:
            units.extend(_split_unit(text, range_, count, max_tokens))

    for i, ch in enumerate(text):
        boundary = ch == "\n" or (
            ch in ".!?" and (i + 1 == len(text) or text[i + 1] in ASCII_WHITESPACE)
        )
        if boundary:
            add(_trim(text, start, i + 1))
            start = i + 1
    add(_trim(text, start, len(text)))
    return _pack(text, units, count, max_tokens)


def wide_read(tokens: list[int], budget: int) -> tuple[int, int]:
    """``memorilla_span.readBudget``: leading documents while the token total
    stays within ``budget`` (the first always) -> (documents, tokens)."""
    read, total = 0, 0
    for n in tokens:
        if read > 0 and total + n > budget:
            break
        total += n
        read += 1
    return read, total


# --- ranking ----------------------------------------------------------------------


def cosine_order(vectors: list[list[float]], query: list[float]) -> list[float]:
    def norm(v):
        n = math.sqrt(sum(x * x for x in v))
        return n if n > 0 else 1.0

    qn = norm(query)
    return [sum(a * b for a, b in zip(v, query)) / (norm(v) * qn) for v in vectors]


WORD = re.compile(r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)*")


def bm25_scores(
    passages: list[str], question: str, k1: float = 1.5, b: float = 0.75
) -> list[float]:
    docs = [[w.lower() for w in WORD.findall(p)] for p in passages]
    query = [w.lower() for w in WORD.findall(question)]
    n = len(docs)
    avg = sum(len(d) for d in docs) / max(n, 1)
    df = Counter(w for d in docs for w in set(d))
    scores = []
    for d in docs:
        tf = Counter(d)
        s = 0.0
        for w in query:
            if w not in tf:
                continue
            idf = math.log(1 + (n - df[w] + 0.5) / (df[w] + 0.5))
            s += (
                idf
                * tf[w]
                * (k1 + 1)
                / (tf[w] + k1 * (1 - b + b * len(d) / max(avg, 1e-9)))
            )
        scores.append(s)
    return scores


# --- Zig server client --------------------------------------------------------------


class ZigError(RuntimeError):
    def __init__(self, status: int, body):
        super().__init__(f"HTTP {status}: {body}")
        self.status = status
        self.body = body


class Zig:
    def __init__(self, base_url: str, timeout_ms: int):
        self.base = base_url.rstrip("/")
        self.timeout_ms = timeout_ms

    def post(self, path: str, body: dict) -> tuple[dict, float]:
        raw = json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        request = Request(
            f"{self.base}/ai/v1/{path}",
            raw,
            {
                "Content-Type": "application/json",
                "X-Antfly-Inference-Timeout-Ms": str(self.timeout_ms),
            },
            method="POST",
        )
        started = time.perf_counter()
        try:
            with urlopen(request, timeout=self.timeout_ms / 1000 + 30) as response:
                payload = json.loads(response.read())
        except HTTPError as err:
            text = err.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = text
            raise ZigError(err.code, parsed) from None
        except URLError as err:
            raise ZigError(
                0, f"Zig server unreachable at {self.base}: {err.reason}"
            ) from None
        return payload, (time.perf_counter() - started) * 1000.0

    def healthy(self) -> bool:
        try:
            with urlopen(f"{self.base}/ai/v1/models", timeout=5) as response:
                return response.status == 200
        except (HTTPError, URLError, OSError):
            return False


# --- the playground -----------------------------------------------------------------


def byte_to_char(text: str, offset: int) -> int:
    return len(text.encode("utf-8")[:offset].decode("utf-8", "ignore"))


def segments(
    text: str, window: tuple[int, int] | None, span: tuple[int, int] | None
) -> list[dict]:
    """Splits ``text`` into plain / window / span pieces (character ranges)."""
    cuts = {0, len(text)}
    for r in (window, span):
        if r:
            cuts.update(r)
    cuts = sorted(c for c in cuts if 0 <= c <= len(text))
    out = []
    for a, b in itertools.pairwise(cuts):
        if a == b:
            continue
        kind = "plain"
        if window and window[0] <= a and b <= window[1]:
            kind = "window"
        if span and span[0] <= a and b <= span[1]:
            kind = "span"
        if out and out[-1]["kind"] == kind:
            out[-1]["text"] += text[a:b]
        else:
            out.append({"kind": kind, "text": text[a:b]})
    return out


class Playground:
    def __init__(self, args):
        self.args = args
        self.zig = Zig(args.zig_url, args.timeout_ms)
        self.count = TokenCounter(args.tokenizer)
        self.vectors: dict[tuple[str, str], list[float]] = {}
        self.identity: str | None = args.encoder_identity
        self.lock = threading.Lock()
        self.samples = json.loads((HERE / "samples.json").read_text())
        self.read_tokens, self.wide_window = args.read_tokens, args.wide_window

    # Embeddings: the stock encoder through the server's embedding route.
    def embed(self, texts: list[str], task: str) -> list[list[float]]:
        missing = [t for t in dict.fromkeys(texts) if (task, t) not in self.vectors]
        for start in range(0, len(missing), 32):
            batch = missing[start : start + 32]
            payload, _ = self.zig.post(
                "embed",
                {"model": self.args.embed_model, "input": batch, "task_type": task},
            )
            rows = payload.get("embeddings")
            if rows is None:
                rows = [item["embedding"] for item in payload["data"]]
            if len(rows) != len(batch):
                raise RuntimeError("embedding route returned the wrong number of rows")
            identity = payload.get("model_identity")
            if identity:
                self.identity = identity
            for text, row in zip(batch, rows):
                self.vectors[(task, text)] = row
        return [self.vectors[(task, t)] for t in texts]

    def encoder_identity(self) -> str:
        if not self.identity:
            self.embed(["identity probe"], "RETRIEVAL_QUERY")
        if not self.identity:
            raise RuntimeError(
                "the embedding route reported no model_identity; pass --encoder-identity"
            )
        return self.identity

    def passages(self, chunks: list[str]) -> list[dict]:
        out = []
        for c, chunk in enumerate(chunks):
            for p, (a, b) in enumerate(
                split_passages(chunk, self.count, self.args.passage_tokens)
            ):
                text = chunk[a:b]
                out.append(
                    {
                        "chunk": c,
                        "index": p,
                        "text": text,
                        "tokens": self.count(text),
                        "start": a,
                        "end": b,
                    }
                )
        return out

    def rank(self, passages: list[dict], question: str) -> tuple[list[dict], dict]:
        texts = [p["text"] for p in passages]
        # A later passage may name the key holder without naming the site.
        # Use the opening sentence of its own chunk as retrieval context.
        # The reader and its citation offsets still use the original slices.
        openings = {
            p["chunk"]: re.split(
                r"(?<=[.!?])[ \t\n\r\x0b\x0c]|\n", p["text"], maxsplit=1
            )[0]
            for p in passages
            if p["index"] == 0
        }
        contextual = [
            openings[p["chunk"]] + "\n" + p["text"] if p["index"] else p["text"]
            for p in passages
        ]
        started = time.perf_counter()
        method = self.args.ranking
        if method == "dense":
            query = self.embed([question], "RETRIEVAL_QUERY")[0]

            def score(documents):
                return cosine_order(self.embed(documents, "RETRIEVAL_DOCUMENT"), query)

            label = "dense cosine, stock Qwen3-Embedding-4B"
        else:

            def score(documents):
                return bm25_scores(documents, question)

            label = "BM25 (k1 1.5, b 0.75)"
        scores = score(texts)
        if contextual != texts:
            scores = [(raw + ctx) / 2 for raw, ctx in zip(scores, score(contextual))]
        label += ", mean passage and source-context scores"
        order = sorted(range(len(passages)), key=lambda i: (-scores[i], i))
        ranked = []
        for rank, i in enumerate(order):
            ranked.append(
                {**passages[i], "rank": rank, "score": round(float(scores[i]), 6)}
            )
        return ranked, {
            "method": label,
            "kind": method,
            "ms": round((time.perf_counter() - started) * 1000.0, 1),
        }

    def call(
        self, question: str, documents: list[str], mode: str, max_new_tokens: int
    ) -> dict:
        body = {
            "model": self.args.profile,
            "question": question,
            "encoder_execution_identity": self.encoder_identity(),
            "document_texts": documents,
            "max_new_tokens": max_new_tokens,
            "include_diagnostics": True,
        }
        if mode != "default":
            body["read_mode"] = mode
        try:
            payload, wall = self.zig.post("memorilla/generate", body)
        except ZigError as err:
            return {
                "error": {"status": err.status, "body": err.body},
                "read_mode": mode,
            }
        return {"response": payload, "wall_ms": round(wall, 1), "read_mode": mode}

    @staticmethod
    def decorate(run: dict, ranked: list[dict]) -> dict:
        """Adds per-passage highlight segments and passage labels to a run."""
        response = run.get("response")
        if not response:
            return run
        diag = response.get("diagnostics") or {}
        marks: dict[int, dict] = {}
        for region in (diag.get("window") or {}).get("regions") or []:
            i = region["document_index"]
            text = ranked[i]["text"]
            marks.setdefault(i, {})["window"] = (
                byte_to_char(text, region["start"]),
                byte_to_char(text, region["end"]),
            )
        span = diag.get("span")
        if span:
            i = span["document_index"]
            text = ranked[i]["text"]
            marks.setdefault(i, {})["span"] = (
                byte_to_char(text, span["start"]),
                byte_to_char(text, span["end"]),
            )
        run["highlights"] = [
            {
                "document_index": i,
                "segments": segments(ranked[i]["text"], m.get("window"), m.get("span")),
            }
            for i, m in sorted(marks.items())
        ]
        for citation in response.get("citations") or []:
            p = ranked[citation["document_index"]]
            citation["passage"] = f"chunk {p['chunk'] + 1}, passage {p['index'] + 1}"
        return run

    def ask(self, body: dict) -> dict:
        chunks = [c for c in body.get("chunks", []) if isinstance(c, str) and c.strip()]
        question = (body.get("question") or "").strip()
        mode = body.get("read_mode") or "auto"
        compare = bool(body.get("compare", True))
        max_new_tokens = int(body.get("max_new_tokens") or self.args.max_new_tokens)
        if not chunks:
            raise ValueError("paste at least one chunk")
        if not question:
            raise ValueError("type a question")
        if mode not in READ_MODES:
            raise ValueError(f"read_mode must be one of {READ_MODES}")
        if not 1 <= max_new_tokens <= 128:
            raise ValueError("max_new_tokens must be 1..128")
        with self.lock:
            passages = self.passages(chunks)
            if len(passages) > MAX_DOCUMENTS:
                raise ValueError(
                    f"{len(passages)} passages; the route takes at most {MAX_DOCUMENTS}"
                )
            sizes = [len(p["text"].encode()) for p in passages]
            if max(sizes) > MAX_TEXT_BYTES or sum(sizes) > MAX_TOTAL_BYTES:
                raise ValueError(
                    f"passages total {sum(sizes)} bytes; the route takes at most {MAX_TOTAL_BYTES}"
                )
            ranked, ranking = self.rank(passages, question)
            documents = [p["text"] for p in ranked]
            wide_documents, wide_tokens = wide_read(
                [p["tokens"] for p in ranked], self.read_tokens
            )
            runs = {
                "hybrid": self.decorate(
                    self.call(question, documents, mode, max_new_tokens), ranked
                )
            }
            if compare and mode != "none":
                runs["stock"] = self.decorate(
                    self.call(question, documents, "none", max_new_tokens), ranked
                )
        return {
            "question": question,
            "read_mode": mode,
            "ranking": ranking,
            "passages": ranked,
            "plan": {
                "narrow_documents": min(NARROW_DOCUMENTS, len(ranked)),
                "wide_documents": wide_documents,
                "wide_tokens": wide_tokens,
                "read_tokens": self.read_tokens,
            },
            "runs": runs,
        }

    def split(self, body: dict) -> dict:
        chunks = [c if isinstance(c, str) else "" for c in body.get("chunks", [])]
        out = []
        for c, chunk in enumerate(chunks):
            parts = (
                split_passages(chunk, self.count, self.args.passage_tokens)
                if chunk.strip()
                else []
            )
            out.append(
                {
                    "chunk": c,
                    "tokens": self.count(chunk) if chunk.strip() else 0,
                    "passages": [self.count(chunk[a:b]) for a, b in parts],
                }
            )
        return {"chunks": out}

    def warm(self) -> dict:
        """Loads the models with one tiny request (cold loads take minutes)."""
        started = time.perf_counter()
        chunk = self.samples["chunks"][0]["text"]
        result = self.ask(
            {
                "chunks": [chunk],
                "question": self.samples["questions"][0],
                "read_mode": "auto",
                "compare": True,
                "max_new_tokens": 16,
            }
        )
        errors = [r.get("error") for r in result["runs"].values() if r.get("error")]
        return {
            "ms": round((time.perf_counter() - started) * 1000.0, 1),
            "errors": errors,
        }


def load_protocol_budget(bundle: Path) -> tuple[int, int]:
    protocol = json.loads((bundle / "protocol.json").read_text())
    cascade = protocol.get("span_read_cascade") or {}
    return int(cascade.get("read_tokens", 512)), int(cascade.get("window_tokens", 64))


class Handler(BaseHTTPRequestHandler):
    server_version = "MemorillaPlayground/1"
    playground: Playground

    def log_message(self, fmt, *args):
        sys.stderr.write(f"{self.log_date_time_string()} {fmt % args}\n")

    def send_json(self, status: int, payload: dict):
        raw = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            raw = (HERE / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)
        elif self.path == "/api/config":
            p = self.playground
            self.send_json(
                200,
                {
                    "samples": p.samples,
                    "profile": p.args.profile,
                    "zig_url": p.args.zig_url,
                    "zig_healthy": p.zig.healthy(),
                    "passage_tokens": p.args.passage_tokens,
                    "ranking": p.args.ranking,
                    "read_tokens": p.read_tokens,
                    "max_new_tokens": p.args.max_new_tokens,
                },
            )
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 1 << 20:
                return self.send_json(413, {"error": "request too large"})
            body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/api/ask":
                return self.send_json(200, self.playground.ask(body))
            if self.path == "/api/split":
                return self.send_json(200, self.playground.split(body))
            if self.path == "/api/warm":
                return self.send_json(200, self.playground.warm())
            return self.send_json(404, {"error": "not found"})
        except ValueError as err:
            return self.send_json(400, {"error": str(err)})
        except ZigError as err:
            return self.send_json(
                502, {"error": str(err), "status": err.status, "body": err.body}
            )
        except Exception as err:  # noqa: BLE001 - surface anything to the page
            return self.send_json(500, {"error": f"{type(err).__name__}: {err}"})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--zig-url", default="http://127.0.0.1:18090")
    parser.add_argument("--profile", default="memorilla-span-cascade")
    parser.add_argument(
        "--embed-model",
        default="encoder",
        help="model name of the stock encoder for /ai/v1/embed",
    )
    parser.add_argument(
        "--encoder-identity",
        default=None,
        help="default: model_identity of the embedding route",
    )
    parser.add_argument(
        "--tokenizer", type=Path, required=True, help="the encoder's tokenizer.json"
    )
    parser.add_argument(
        "--bundle",
        type=Path,
        default=None,
        help="hybrid bundle (reads the cascade's read budget)",
    )
    parser.add_argument("--passage-tokens", type=int, default=64)
    parser.add_argument("--ranking", choices=("dense", "bm25"), default="dense")
    parser.add_argument("--max-new-tokens", type=int, default=64)
    parser.add_argument("--timeout-ms", type=int, default=600_000)
    parser.add_argument(
        "--warm",
        action="store_true",
        help="load the models with one request at startup",
    )
    args = parser.parse_args(argv)
    args.read_tokens, args.wide_window = (
        load_protocol_budget(args.bundle) if args.bundle else (512, 64)
    )
    playground = Playground(args)
    Handler.playground = playground
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(
        f"memorilla playground on http://{args.host}:{args.port}/ -> {args.zig_url} profile {args.profile}",
        flush=True,
    )
    if args.warm:

        def warm():
            try:
                print(f"warm-up: {json.dumps(playground.warm())}", flush=True)
            except Exception as err:  # noqa: BLE001
                print(f"warm-up failed: {err}", flush=True)

        threading.Thread(target=warm, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
