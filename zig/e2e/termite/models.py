# Copyright 2026 Antfly, Inc.
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

"""Model definitions and termite-backed lazy downloads for E2E tests.

Models are stored in the flat default Termite layout:
    models/{owner}/{name}/

When TERMITE_DOWNLOAD=1 is set, the E2E harness can lazily fetch missing models
by shelling out to `termite pull` instead of using huggingface_hub directly.
Set TERMITE_MODELS_DIR to control where models are stored.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TERMITE_BIN_CANDIDATES = (
    REPO_ROOT / "pkg" / "termite" / "zig-out" / "bin" / "termite",
    REPO_ROOT / "zig-out" / "bin" / "termite",
)

MODEL_TASKS = (
    "embedders",
    "chunkers",
    "rerankers",
    "generators",
    "recognizers",
    "classifiers",
    "rewriters",
    "readers",
    "transcribers",
    "extractors",
)


@dataclass(frozen=True)
class ModelSpec:
    """Defines a model used by the portable E2E suite."""

    name: str
    repo: str
    task: str
    variant: str = "auto"
    dim: int = 0
    multilingual: bool = False
    large: bool = False
    extra_files: tuple[str, ...] = field(default_factory=tuple)

    @property
    def request_name(self) -> str:
        return self.repo

    @property
    def pull_ref(self) -> str:
        if self.variant == "auto":
            return f"hf:{self.repo}"
        return f"hf:{self.repo}:{self.variant}"


EMBEDDER_MODELS = [
    ModelSpec(
        name="bge-small-en-v1.5",
        repo="BAAI/bge-small-en-v1.5",
        task="embedders",
        variant="native",
        dim=384,
    ),
    ModelSpec(
        name="splade-bert-tiny-nq-onnx",
        repo="sparse-encoder-testing/splade-bert-tiny-nq-onnx",
        task="embedders",
        variant="onnx",
    ),
    ModelSpec(
        name="clipclap",
        repo="antflydb/clipclap",
        task="embedders",
        variant="gguf:Q4_K",
        dim=512,
        large=True,
    ),
]

RERANKER_MODELS = [
    ModelSpec(
        name="mxbai-rerank-base-v1",
        repo="mixedbread-ai/mxbai-rerank-base-v1",
        task="rerankers",
    ),
]

CLASSIFIER_MODELS = [
    ModelSpec(
        name="nli-distilroberta-base",
        repo="cross-encoder/nli-distilroberta-base",
        task="classifiers",
        variant="native",
    ),
    ModelSpec(
        name="mDeBERTa-v3-base-mnli-xnli",
        repo="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
        task="classifiers",
        variant="native",
    ),
]

RECOGNIZER_MODELS = [
    ModelSpec(
        name="gliner2-base-v1",
        repo="fastino/gliner2-base-v1",
        task="recognizers",
        variant="native",
    ),
    ModelSpec(
        name="rebel-large",
        repo="Babelscape/rebel-large",
        task="recognizers",
    ),
    ModelSpec(
        name="bert-base-NER",
        repo="dslim/bert-base-NER",
        task="recognizers",
        variant="native",
    ),
    ModelSpec(
        name="pii-deberta-v3-xsmall",
        repo="mukuls9971/pii-deberta-v3-xsmall",
        task="recognizers",
        variant="native",
    ),
]

READER_MODELS = [
    ModelSpec(
        name="trocr-base-printed",
        repo="Xenova/trocr-base-printed",
        task="readers",
    ),
]

TRANSCRIBER_MODELS = [
    ModelSpec(
        name="whisper-tiny",
        repo="openai/whisper-tiny",
        task="transcribers",
    ),
]

DEFAULT_GENERATOR_MODEL = "openai-community/gpt2"
DEFAULT_TOOL_GENERATOR_MODEL = "ggml-org/gemma-4-e2b-it-gguf"
DEFAULT_MULTIMODAL_GENERATOR_MODEL = "ggml-org/gemma-4-e2b-it-gguf"

CURATED_MODELS = [
    *EMBEDDER_MODELS,
    *RERANKER_MODELS,
    *CLASSIFIER_MODELS,
    *RECOGNIZER_MODELS,
    *READER_MODELS,
    *TRANSCRIBER_MODELS,
]

CURATED_BY_NAME = {spec.request_name.lower(): spec for spec in CURATED_MODELS}

DEFAULT_MODEL_BY_PATH = {
    "/embed": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/embeddings": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/generate": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/chat/completions": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ml/v1/embed": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/ml/v1/embeddings": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/ml/v1/rerank": ("mixedbread-ai/mxbai-rerank-base-v1", "rerankers"),
    "/ml/v1/generate": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ml/v1/chat/completions": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ml/v1/extract": ("fastino/gliner2-base-v1", "recognizers"),
    "/ml/v1/read": ("Xenova/trocr-base-printed", "readers"),
    "/ml/v1/transcribe": ("openai/whisper-tiny", "transcribers"),
}

TASK_NAME_BY_DIR = {
    "embedders": "embed",
    "chunkers": "chunk",
    "rerankers": "rerank",
    "generators": "generate",
    "recognizers": "recognize",
    "classifiers": "classify",
    "rewriters": "rewrite",
    "readers": "read",
    "transcribers": "transcribe",
    "extractors": "extract",
}

LISTING_BOOTSTRAP = {
    "embedders": EMBEDDER_MODELS[0],
    "rerankers": RERANKER_MODELS[0],
    "classifiers": CLASSIFIER_MODELS[0],
    "recognizers": RECOGNIZER_MODELS[0],
    "readers": READER_MODELS[0],
    "transcribers": TRANSCRIBER_MODELS[0],
}

GENERATOR_ENV_VARS = (
    "TERMITE_DEFAULT_GENERATOR_MODEL",
    "TERMITE_TOOL_MODEL",
    "TERMITE_MULTIMODAL_GENERATOR_MODEL",
)

READER_ENV_VARS = (
    "TERMITE_TROCR_MODEL",
    "TERMITE_DONUT_MODEL",
    "TERMITE_MULTISTAGE_READER_MODEL",
    "TERMITE_PADDLEOCR_MODEL",
    "TERMITE_SURYA_READER_MODEL",
    "TERMITE_SURYA_MODEL",
    "TERMITE_MOONDREAM_MODEL",
    "TERMITE_PIX2STRUCT_MODEL",
)


def models_dir() -> Path:
    """Return the models directory, creating it if needed."""

    configured = os.environ.get("TERMITE_MODELS_DIR")
    if configured:
        directory = Path(configured)
    else:
        home = os.environ.get("HOME")
        directory = Path(home) / ".termite" / "models" if home else Path("./models")
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def termite_download_enabled() -> bool:
    return os.environ.get("TERMITE_DOWNLOAD") == "1"


def run_large_model_tests() -> bool:
    value = os.environ.get("RUN_LARGE_MODEL_TESTS", "")
    return value != "" and value not in {"0", "false", "False"}


def termite_bin() -> str:
    explicit = os.environ.get("TERMITE_PULL_BIN") or os.environ.get("TERMITE_BIN")
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    for candidate in DEFAULT_TERMITE_BIN_CANDIDATES:
        if candidate.exists():
            return str(candidate)
    discovered = shutil.which("termite")
    if discovered:
        return discovered
    raise RuntimeError(
        "TERMITE_DOWNLOAD=1 requires a termite binary. "
        "Set TERMITE_PULL_BIN or TERMITE_BIN, or build pkg/termite/zig-out/bin/termite."
    )


def _model_path(spec: ModelSpec) -> Path:
    return models_dir() / spec.repo


def _looks_like_model_dir(path: Path) -> bool:
    if not path.exists():
        return False
    for filename in ("config.json", "tokenizer.json", "genai_config.json", "termite_metadata.json"):
        if (path / filename).exists():
            return True
    if any(path.glob("*.gguf")):
        return True
    if (path / "onnx").is_dir():
        return True
    return False


def model_available(spec: ModelSpec) -> bool:
    """Check if a model is already downloaded."""

    path = find_local_model_path(spec.request_name, spec.task)
    if path is None:
        return False
    return all((path / extra).exists() for extra in spec.extra_files)


def _dynamic_spec(name: str, task: str) -> ModelSpec:
    return ModelSpec(
        name=name.rsplit("/", 1)[-1],
        repo=name,
        task=task,
    )


def find_local_model_path(name: str, task_hint: str | None = None) -> Path | None:
    if not name:
        return None

    root = models_dir()
    candidates: list[Path] = [root / name]

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if _looks_like_model_dir(candidate):
            return candidate
    return None


def local_model_exists(name: str, task_hint: str | None = None) -> bool:
    return find_local_model_path(name, task_hint) is not None


def spec_for_name(name: str, task_hint: str | None = None) -> ModelSpec | None:
    if not name:
        return None
    curated = CURATED_BY_NAME.get(name.lower())
    if curated is not None:
        return curated
    if task_hint is None:
        return None
    return _dynamic_spec(name, task_hint)


def ensure_model(spec: ModelSpec) -> Path:
    """Download a model with `termite pull` if not already present."""

    if (existing := find_local_model_path(spec.request_name, spec.task)) is not None:
        return existing

    command = [
        termite_bin(),
        "pull",
        spec.pull_ref,
        "--tasks",
        TASK_NAME_BY_DIR[spec.task],
    ]
    configured_models_dir = os.environ.get("TERMITE_MODELS_DIR")
    if configured_models_dir:
        command.extend(["--models-dir", str(models_dir())])
    print(f"Downloading {spec.pull_ref}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)

    resolved = find_local_model_path(spec.request_name, spec.task)
    if resolved is None:
        raise RuntimeError(f"termite pull finished but could not locate {spec.request_name} in {models_dir()}")
    return resolved


def ensure_model_by_name(name: str, task_hint: str | None = None) -> Path | None:
    spec = spec_for_name(name, task_hint)
    if spec is None:
        return None
    return ensure_model(spec)


def default_generator_model_name(available_generators: set[str] | None = None) -> str | None:
    override = os.environ.get("TERMITE_DEFAULT_GENERATOR_MODEL")
    if override:
        return override

    if available_generators:
        for candidate in (DEFAULT_GENERATOR_MODEL, DEFAULT_TOOL_GENERATOR_MODEL):
            if candidate in available_generators:
                return candidate
        return sorted(available_generators)[0]

    return DEFAULT_GENERATOR_MODEL


def detect_tool_call_format(model_path: Path) -> str | None:
    """Return the configured tool-call format for a local generator model."""

    genai_config = model_path / "genai_config.json"
    if genai_config.exists():
        try:
            data = json.loads(genai_config.read_text())
        except json.JSONDecodeError:
            data = {}
        tool_call_format = data.get("tool_call_format")
        if isinstance(tool_call_format, str) and tool_call_format:
            return tool_call_format

    for file_name in ("special_tokens_map.json", "tokenizer_config.json"):
        token_file = model_path / file_name
        if not token_file.exists():
            continue
        try:
            data = json.loads(token_file.read_text())
        except json.JSONDecodeError:
            continue
        serialized = json.dumps(data)
        if "start_function_call" in serialized and "end_function_call" in serialized:
            return "functiongemma"

    return None


def find_tool_model_name(available_generators: set[str] | None = None) -> str | None:
    """Find a local tool-capable generator model name for E2E tests."""

    override = os.environ.get("TERMITE_TOOL_MODEL")
    if override:
        return override

    seen: set[Path] = set()
    root = models_dir()
    if root.exists():
        for pattern in ("**/genai_config.json", "**/special_tokens_map.json", "**/tokenizer_config.json"):
            for metadata_path in root.glob(pattern):
                model_path = metadata_path.parent
                if model_path in seen:
                    continue
                seen.add(model_path)
                if detect_tool_call_format(model_path) is None:
                    continue
                model_name = model_path.relative_to(root).as_posix()
                if available_generators is None or model_name in available_generators:
                    return model_name

    if available_generators:
        if DEFAULT_TOOL_GENERATOR_MODEL in available_generators:
            return DEFAULT_TOOL_GENERATOR_MODEL

    return DEFAULT_TOOL_GENERATOR_MODEL


def detect_multimodal_generator(model_path: Path) -> bool:
    """Return True when a local generator model advertises image inputs."""

    config_path = model_path / "config.json"
    if not config_path.exists():
        return False
    try:
        config = json.loads(config_path.read_text())
    except json.JSONDecodeError:
        return False

    if isinstance(config.get("vision_config"), dict):
        return True
    if config.get("image_token_index") is not None:
        return True
    if config.get("mm_tokens_per_image") is not None:
        return True

    archs = config.get("architectures")
    if isinstance(archs, list):
        for arch in archs:
            if isinstance(arch, str) and ("ConditionalGeneration" in arch or "Vision" in arch):
                return True

    processor_path = model_path / "processor_config.json"
    if processor_path.exists():
        try:
            processor = json.loads(processor_path.read_text())
        except json.JSONDecodeError:
            processor = {}
        if processor.get("image_seq_length") is not None:
            return True

    return False


def find_multimodal_generator_model_name(available_generators: set[str] | None = None) -> str | None:
    """Find a local multimodal generator model name for E2E tests."""

    override = os.environ.get("TERMITE_MULTIMODAL_GENERATOR_MODEL")
    if override:
        return override

    seen: set[Path] = set()
    root = models_dir()

    if root.exists():
        for config_path in root.glob("**/config.json"):
            model_path = config_path.parent
            if model_path in seen:
                continue
            seen.add(model_path)
            if not detect_multimodal_generator(model_path):
                continue

            model_name = model_path.relative_to(root).as_posix()

            if available_generators is None or model_name in available_generators:
                return model_name

    if available_generators:
        if DEFAULT_MULTIMODAL_GENERATOR_MODEL in available_generators:
            return DEFAULT_MULTIMODAL_GENERATOR_MODEL

    return DEFAULT_MULTIMODAL_GENERATOR_MODEL


def request_model_name(path: str, payload: dict | None) -> tuple[str | None, str | None]:
    body = payload if isinstance(payload, dict) else {}
    model = body.get("model")
    if isinstance(model, str) and model.strip():
        return model.strip(), DEFAULT_MODEL_BY_PATH.get(path, (None, None))[1]
    return DEFAULT_MODEL_BY_PATH.get(path, (None, None))


def response_indicates_missing_model(response) -> bool:
    if response.status_code not in (400, 404):
        return False
    content_type = response.headers.get("content-type", "")
    if not content_type.startswith("application/json"):
        return False
    try:
        payload = response.json()
    except ValueError:
        return False
    error_code = str(payload.get("error", ""))
    message = str(payload.get("message", ""))
    normalized = message.lower()
    return (
        "MODEL_NOT_FOUND" in error_code
        or "INVALID_MODEL" in error_code
        or "model not found" in normalized
        or message in {"ModelNotFound", "ModelNotSpecified", "NoReaderModelAvailable"}
        or "no compatible reader model is available" in normalized
    )


def maybe_pull_missing_model(path: str, payload: dict | None, response) -> bool:
    if not termite_download_enabled():
        return False
    if not response_indicates_missing_model(response):
        return False

    model_name, task_hint = request_model_name(path, payload)
    if not model_name or not task_hint:
        return False
    try:
        return ensure_model_by_name(model_name, task_hint) is not None
    except subprocess.CalledProcessError:
        return False


def _env_model_specs() -> list[ModelSpec]:
    specs: list[ModelSpec] = []
    seen: set[tuple[str, str]] = set()

    for env_name in GENERATOR_ENV_VARS:
        value = os.environ.get(env_name, "").strip()
        if not value:
            continue
        key = ("generators", value.lower())
        if key in seen:
            continue
        seen.add(key)
        specs.append(_dynamic_spec(value, "generators"))

    for env_name in READER_ENV_VARS:
        value = os.environ.get(env_name, "").strip()
        if not value:
            continue
        key = ("readers", value.lower())
        if key in seen:
            continue
        seen.add(key)
        specs.append(_dynamic_spec(value, "readers"))

    return specs


def bootstrap_models_for_listing(listing: dict) -> bool:
    if not termite_download_enabled():
        return False

    planned: list[ModelSpec] = []
    for category, spec in LISTING_BOOTSTRAP.items():
        if listing.get(category):
            continue
        planned.append(spec)

    planned.extend(_env_model_specs())

    changed = False
    seen: set[tuple[str, str]] = set()
    for spec in planned:
        key = (spec.task, spec.request_name.lower())
        if key in seen:
            continue
        seen.add(key)
        if spec.large and not run_large_model_tests():
            continue
        if model_available(spec):
            continue
        ensure_model(spec)
        changed = True
    return changed


def prefetch_curated_models() -> None:
    seen: set[tuple[str, str]] = set()
    planned: list[ModelSpec] = []
    planned.extend(spec for spec in CURATED_MODELS if not spec.large or run_large_model_tests())
    planned.extend(_env_model_specs())

    for spec in planned:
        key = (spec.task, spec.request_name.lower())
        if key in seen:
            continue
        seen.add(key)
        ensure_model(spec)
