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

"""Model definitions and inference-backed lazy downloads for E2E tests.

Models are stored in the flat default inference layout:
    models/{owner}/{name}/

When ANTFLY_INFERENCE_DOWNLOAD=1 is set, the E2E harness can lazily fetch missing models
by shelling out to `antfly inference pull` instead of using huggingface_hub directly.
Set ANTFLY_INFERENCE_MODELS_DIR to control where models are stored.
Set ANTFLY_INFERENCE_ML_DIR to control where Traditional ML predictors are stored.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ANTFLY_BIN_CANDIDATES = (REPO_ROOT / "zig-out" / "bin" / "antfly",)
MANAGED_DOWNLOAD_IN_PROGRESS = ".antfly-download-in-progress"
MANAGED_DOWNLOAD_PLAN = ".antfly-download-plan.json"
MANAGED_DOWNLOAD_COMPLETE = ".antfly-download-complete.json"
MAX_MANAGED_DOWNLOAD_RECEIPT_BYTES = 16 * 1024 * 1024
MAX_MANAGED_DOWNLOAD_ARTIFACTS = 64 * 1024
SUPPORTED_MODEL_SUFFIXES = (".gguf", ".onnx", ".safetensors")

MODEL_TASKS = (
    "embedders",
    "chunkers",
    "rerankers",
    "generators",
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
    projector: str | None = None

    @property
    def request_name(self) -> str:
        return self.repo

    @property
    def listing_name(self) -> str:
        if self.variant == "auto":
            return self.repo
        return f"{self.repo}:{self.variant}"

    @property
    def pull_ref(self) -> str:
        if self.variant == "auto":
            return f"hf:{self.repo}"
        return f"hf:{self.repo}:{self.variant}"


@dataclass(frozen=True)
class LocalModel:
    """A model directory and the artifacts validated during discovery."""

    path: Path
    artifacts: tuple[str, ...]


# Models added while verifying the capability surface the website implies. Each is here
# because a claim needed evidence, not because CI needs breadth for its own sake.
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
    ModelSpec(
        name="bge-base-en-v1.5",
        repo="BAAI/bge-base-en-v1.5",
        task="embedders",
        dim=768,
    ),
    ModelSpec(
        name="bge-large-en-v1.5",
        repo="BAAI/bge-large-en-v1.5",
        task="embedders",
        dim=1024,
        large=True,
    ),
    ModelSpec(
        name="mxbai-embed-large-v1",
        repo="mixedbread-ai/mxbai-embed-large-v1",
        task="embedders",
        dim=1024,
    ),
    ModelSpec(
        name="all-MiniLM-L6-v2",
        repo="sentence-transformers/all-MiniLM-L6-v2",
        task="embedders",
        dim=384,
    ),
]

RERANKER_MODELS = [
    ModelSpec(
        name="ms-marco-MiniLM-L6-v2",
        repo="cross-encoder/ms-marco-MiniLM-L6-v2",
        task="rerankers",
    ),
    ModelSpec(
        name="mxbai-rerank-base-v1",
        repo="mixedbread-ai/mxbai-rerank-base-v1",
        task="rerankers",
    ),
]

# Chunking had no model in the curated set at all, despite chunkers being a declared task.
CHUNKER_MODELS = [
    ModelSpec(
        name="chonky_mmbert_small_multilingual_1",
        repo="mirth/chonky_mmbert_small_multilingual_1",
        task="chunkers",
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

DEFAULT_EXTRACTOR_MODEL = "antflydb/gliner2-base-v1"
DEFAULT_EXTRACTOR_VARIANT = "gguf:Q4_K"

EXTRACTOR_MODELS = [
    ModelSpec(
        name="gliner2-base-v1",
        repo=DEFAULT_EXTRACTOR_MODEL,
        task="extractors",
        variant=DEFAULT_EXTRACTOR_VARIANT,
        extra_files=(
            "gliner2-encoder.Q4_K.gguf",
            "gliner2-head.Q4_K.gguf",
        ),
    ),
    # CC-BY-NC-SA: non-commercial only, and ~3GB. Kept for relation-extraction coverage,
    # but it must not appear in any default bundle. GLiNER2 is the recommended extractor.
    ModelSpec(
        name="rebel-large",
        repo="Babelscape/rebel-large",
        task="extractors",
    ),
    ModelSpec(
        name="bert-base-NER",
        repo="dslim/bert-base-NER",
        task="extractors",
        variant="native",
    ),
    ModelSpec(
        name="pii-deberta-v3-xsmall",
        repo="mukuls9971/pii-deberta-v3-xsmall",
        task="extractors",
        variant="native",
    ),
]

READER_MODELS = [
    ModelSpec(
        name="florence-2-base",
        repo="antflydb/florence-2-base",
        task="readers",
        variant="gguf:Q4_K",
    ),
]

TRANSCRIBER_MODELS = [
    ModelSpec(
        name="whisper-tiny",
        repo="openai/whisper-tiny",
        task="transcribers",
    ),
]

# Generator models used to keep the decoder support tiers honest.
#
# Each family in models/gpt.zig has a SupportLevel; these are the artifacts those levels
# were measured against. Without them the tiers are assertions rather than results.
GENERATOR_MODELS = [
    ModelSpec(
        name="gemma-4-e2b-it-gguf",
        repo="ggml-org/gemma-4-e2b-it-gguf",
        task="generators",
        variant="gguf:Q4_0",
        large=True,
        projector="Q8_0",
    ),
    ModelSpec(
        name="Qwen3-1.7B-GGUF",
        repo="unsloth/Qwen3-1.7B-GGUF",
        task="generators",
        variant="gguf:Q4_K_M",
        large=True,
    ),
    ModelSpec(
        name="Llama-3.2-1B-Instruct-GGUF",
        repo="unsloth/Llama-3.2-1B-Instruct-GGUF",
        task="generators",
        large=True,
    ),
]


DEFAULT_GENERATOR_MODEL = "ggml-org/gemma-4-e2b-it-gguf"
DEFAULT_TOOL_GENERATOR_MODEL = DEFAULT_GENERATOR_MODEL
DEFAULT_MULTIMODAL_GENERATOR_MODEL = DEFAULT_GENERATOR_MODEL

CURATED_MODELS = [
    *EMBEDDER_MODELS,
    *GENERATOR_MODELS,
    *CHUNKER_MODELS,
    *RERANKER_MODELS,
    *CLASSIFIER_MODELS,
    *EXTRACTOR_MODELS,
    *READER_MODELS,
    *TRANSCRIBER_MODELS,
]

CURATED_BY_NAME = {spec.request_name.lower(): spec for spec in CURATED_MODELS}

DEFAULT_MODEL_BY_PATH = {
    "/embed": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/embeddings": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/generate": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/chat/completions": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ai/v1/embed": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/ai/v1/embeddings": ("BAAI/bge-small-en-v1.5", "embedders"),
    "/ai/v1/rerank": ("mixedbread-ai/mxbai-rerank-base-v1", "rerankers"),
    "/ai/v1/generate": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ai/v1/chat/completions": (DEFAULT_GENERATOR_MODEL, "generators"),
    "/ai/v1/extract": (DEFAULT_EXTRACTOR_MODEL, "extractors"),
    "/ai/v1/read": ("antflydb/florence-2-base", "readers"),
    "/ai/v1/transcribe": ("openai/whisper-tiny", "transcribers"),
}

TASK_NAME_BY_DIR = {
    "embedders": "embed",
    "chunkers": "chunk",
    "rerankers": "rerank",
    "generators": "generate",
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
    "extractors": EXTRACTOR_MODELS[0],
    "readers": READER_MODELS[0],
    "transcribers": TRANSCRIBER_MODELS[0],
}

GENERATOR_ENV_VARS = (
    "ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL",
    "ANTFLY_INFERENCE_DRAFT_MODEL",
    "ANTFLY_INFERENCE_TOOL_MODEL",
    "ANTFLY_INFERENCE_MULTIMODAL_GENERATOR_MODEL",
)

READER_ENV_VARS = (
    "ANTFLY_INFERENCE_FLORENCE_MODEL",
    "ANTFLY_INFERENCE_TROCR_MODEL",
    "ANTFLY_INFERENCE_DONUT_MODEL",
    "ANTFLY_INFERENCE_MULTISTAGE_READER_MODEL",
    "ANTFLY_INFERENCE_PADDLEOCR_MODEL",
    "ANTFLY_INFERENCE_SURYA_READER_MODEL",
    "ANTFLY_INFERENCE_SURYA_MODEL",
    "ANTFLY_INFERENCE_MOONDREAM_MODEL",
    "ANTFLY_INFERENCE_PIX2STRUCT_MODEL",
)


def models_dir() -> Path:
    """Return the models directory, creating it if needed."""

    configured = os.environ.get("ANTFLY_INFERENCE_MODELS_DIR")
    if configured:
        directory = Path(configured)
    else:
        home = os.environ.get("HOME")
        directory = (
            Path(home) / ".antfly" / "inference" / "models"
            if home
            else Path("./models")
        )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def ml_dir() -> Path:
    """Return the Traditional ML directory, creating it if needed."""

    configured = os.environ.get("ANTFLY_INFERENCE_ML_DIR")
    if configured:
        directory = Path(configured)
    else:
        home = os.environ.get("HOME")
        directory = (
            Path(home) / ".antfly" / "inference" / "ml" if home else Path("./ml")
        )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def inference_download_enabled() -> bool:
    return os.environ.get("ANTFLY_INFERENCE_DOWNLOAD") == "1"


def run_large_model_tests() -> bool:
    value = os.environ.get("RUN_LARGE_MODEL_TESTS", "")
    return value != "" and value not in {"0", "false", "False"}


def run_multimodal_generator_tests() -> bool:
    """Enable the targeted large Gemma smoke without enabling every large model."""

    value = os.environ.get("RUN_MULTIMODAL_GENERATOR_TESTS", "")
    return run_large_model_tests() or (
        value != "" and value not in {"0", "false", "False"}
    )


def run_clipclap_contract_tests() -> bool:
    """Require the published ClipClap pair to pass live server admission."""

    value = os.environ.get("RUN_CLIPCLAP_CONTRACT_TESTS", "")
    return run_large_model_tests() or (
        value != "" and value not in {"0", "false", "False"}
    )


def inference_command() -> list[str]:
    explicit = os.environ.get("ANTFLY_BIN")
    if explicit:
        return [str(Path(explicit).expanduser().resolve()), "inference"]
    for candidate in DEFAULT_ANTFLY_BIN_CANDIDATES:
        if candidate.exists():
            return [str(candidate), "inference"]
    raise RuntimeError(
        "ANTFLY_INFERENCE_DOWNLOAD=1 requires an antfly inference binary. "
        "Set ANTFLY_BIN, or build zig-out/bin/antfly with `zig build antfly`."
    )


def _model_path(spec: ModelSpec) -> Path:
    legacy_path = models_dir() / spec.repo
    if spec.variant == "auto":
        return legacy_path
    variant_hash = hashlib.sha256(spec.variant.encode()).hexdigest()[:16]
    return legacy_path.with_name(f"{legacy_path.name}--antfly-{variant_hash}")


def _validated_managed_artifacts(path: Path) -> tuple[str, ...] | None:
    """Return validated receipt paths, or None for an unmanaged directory.

    An empty tuple means a receipt exists but is invalid. Keeping that distinct
    from an unmanaged directory prevents a corrupt managed download from being
    accepted by the legacy filesystem scan.
    """

    completion_path = path / MANAGED_DOWNLOAD_COMPLETE
    if not completion_path.exists():
        return None

    root = path.resolve()
    try:
        with completion_path.open("rb") as receipt:
            serialized = receipt.read(MAX_MANAGED_DOWNLOAD_RECEIPT_BYTES + 1)
        if len(serialized) > MAX_MANAGED_DOWNLOAD_RECEIPT_BYTES:
            return ()
        completion = json.loads(serialized)
        artifacts = completion["artifacts"]
    except (OSError, KeyError, TypeError, UnicodeError, json.JSONDecodeError):
        return ()
    if type(completion.get("version")) is not int or completion["version"] not in {
        1,
        2,
    }:
        return ()
    if (
        not isinstance(artifacts, list)
        or not artifacts
        or len(artifacts) > MAX_MANAGED_DOWNLOAD_ARTIFACTS
    ):
        return ()

    artifact_paths: list[str] = []
    seen_paths: set[str] = set()
    has_supported_payload = False
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            return ()
        artifact_path = artifact.get("path")
        artifact_size = artifact.get("size")
        if (
            not isinstance(artifact_path, str)
            or type(artifact_size) is not int
            or artifact_size < 0
        ):
            return ()
        parts = artifact_path.split("/")
        relative = PurePosixPath(artifact_path)
        if (
            relative.is_absolute()
            or any(part in ("", ".", "..") for part in parts)
            or "\\" in artifact_path
            or ":" in artifact_path
            or "\x00" in artifact_path
            or artifact_path in seen_paths
        ):
            return ()
        seen_paths.add(artifact_path)
        candidate = path.joinpath(*relative.parts)
        try:
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(root) or not resolved.is_file():
                return ()
            if resolved.stat().st_size != artifact_size:
                return ()
        except OSError:
            return ()
        artifact_paths.append(artifact_path)
        if candidate.name.endswith(SUPPORTED_MODEL_SUFFIXES):
            has_supported_payload = True
    return tuple(artifact_paths) if has_supported_payload else ()


def _probe_model_dir(path: Path) -> LocalModel | None:
    if not path.is_dir():
        return None

    if (path / MANAGED_DOWNLOAD_IN_PROGRESS).exists() or (
        path / MANAGED_DOWNLOAD_PLAN
    ).exists():
        return None

    managed_artifacts = _validated_managed_artifacts(path)
    if managed_artifacts is not None:
        if not managed_artifacts:
            return None
        if (path / MANAGED_DOWNLOAD_IN_PROGRESS).exists() or (
            path / MANAGED_DOWNLOAD_PLAN
        ).exists():
            return None
        return LocalModel(path=path, artifacts=managed_artifacts)

    # Legacy or externally provisioned model directories do not have Antfly's
    # completion receipt. Preserve compatibility while still rejecting an
    # interrupted file that is visibly in progress.
    artifacts: list[str] = []
    for candidate in path.rglob("*"):
        if not candidate.is_file():
            continue
        if candidate.name.endswith(".part"):
            return None
        if candidate.name.endswith(SUPPORTED_MODEL_SUFFIXES):
            artifacts.append(candidate.relative_to(path).as_posix())
    if not artifacts:
        return None
    if (path / MANAGED_DOWNLOAD_IN_PROGRESS).exists() or (
        path / MANAGED_DOWNLOAD_PLAN
    ).exists():
        return None
    return LocalModel(path=path, artifacts=tuple(artifacts))


def _looks_like_model_dir(path: Path) -> bool:
    return _probe_model_dir(path) is not None


def _is_projector_gguf(artifact_path: str) -> bool:
    name = PurePosixPath(artifact_path).name
    if not name.endswith(".gguf"):
        return False
    stem = name[: -len(".gguf")]
    return (
        stem == "mmproj"
        or stem.startswith(("mmproj-", "mmproj_"))
        or stem.endswith(("-mmproj", "_mmproj"))
    )


def _projector_matches(artifact_path: str, selector: str) -> bool:
    if not selector or not _is_projector_gguf(artifact_path):
        return False
    name = PurePosixPath(artifact_path).name
    if artifact_path == selector or name == selector:
        return True

    stem = name[: -len(".gguf")].lower()
    quant = selector.lower()
    start = 0
    boundaries = "-_."
    while (start := stem.find(quant, start)) >= 0:
        end = start + len(quant)
        if (start == 0 or stem[start - 1] in boundaries) and (
            end == len(stem) or stem[end] in boundaries
        ):
            return True
        start += 1
    return False


def _model_satisfies_spec(model: LocalModel, spec: ModelSpec) -> bool:
    if not all((model.path / extra).exists() for extra in spec.extra_files):
        return False
    if spec.projector is None:
        return True
    return any(
        _projector_matches(candidate, spec.projector) for candidate in model.artifacts
    )


def model_available(spec: ModelSpec) -> bool:
    """Check if a model is already downloaded."""

    model = _find_local_model_for_spec(spec)
    if model is None:
        return False
    return _model_satisfies_spec(model, spec)


def _dynamic_spec(name: str, task: str) -> ModelSpec:
    return ModelSpec(
        name=name.rsplit("/", 1)[-1],
        repo=name,
        task=task,
    )


def _find_local_model(name: str, task_hint: str | None = None) -> LocalModel | None:
    if not name:
        return None

    root = models_dir()
    legacy_path = root / name
    candidates: list[Path] = [legacy_path]
    # Explicit variants are installed beside the legacy path so multiple
    # formats/quantizations can coexist. Bare names retain compatibility by
    # selecting a completed variant deterministically when no legacy install
    # exists; callers that know the variant use `_model_path` instead.
    try:
        candidates.extend(
            sorted(legacy_path.parent.glob(f"{legacy_path.name}--antfly-*"))
        )
    except OSError:
        pass

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if model := _probe_model_dir(candidate):
            return model
    return None


def _find_local_model_for_spec(spec: ModelSpec) -> LocalModel | None:
    """Resolve the exact variant first, then its pre-variant legacy location."""

    expected = _model_path(spec)
    candidates = [expected]
    legacy = models_dir() / spec.repo
    if legacy != expected:
        candidates.append(legacy)
    for candidate in candidates:
        model = _probe_model_dir(candidate)
        if model is not None and _model_satisfies_spec(model, spec):
            return model
    return None


def find_local_model_path(name: str, task_hint: str | None = None) -> Path | None:
    model = _find_local_model(name, task_hint)
    return model.path if model is not None else None


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
    """Download a model with `antfly inference pull` if not already present."""

    existing = _find_local_model_for_spec(spec)
    if existing is not None:
        return existing.path

    command = [
        *inference_command(),
        "pull",
        spec.pull_ref,
        "--tasks",
        TASK_NAME_BY_DIR[spec.task],
    ]
    if spec.projector is not None:
        command.extend(["--projector", spec.projector])
    configured_models_dir = os.environ.get("ANTFLY_INFERENCE_MODELS_DIR")
    if configured_models_dir:
        command.extend(["--models-dir", str(models_dir())])
    print(f"Downloading {spec.pull_ref}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)

    resolved = _probe_model_dir(_model_path(spec))
    legacy = models_dir() / spec.repo
    if resolved is None and legacy != _model_path(spec):
        resolved = _probe_model_dir(legacy)
    if resolved is None:
        raise RuntimeError(
            f"antfly inference pull finished but could not locate {spec.request_name} in {models_dir()}"
        )
    if not _model_satisfies_spec(resolved, spec):
        requirement = (
            f"projector {spec.projector}"
            if spec.projector is not None
            else "required artifacts"
        )
        raise RuntimeError(
            f"antfly inference pull finished but {spec.request_name} is missing {requirement}"
        )
    return resolved.path


def ensure_model_by_name(name: str, task_hint: str | None = None) -> Path | None:
    spec = spec_for_name(name, task_hint)
    if spec is None:
        return None
    return ensure_model(spec)


def listed_model_name(available_models: set[str], request_name: str) -> str | None:
    """Resolve a bare request name to the exact identifier advertised by /models."""

    exact = next(
        (
            name
            for name in available_models
            if name.casefold() == request_name.casefold()
        ),
        None,
    )
    if exact is not None:
        return exact

    spec = spec_for_name(request_name)
    if spec is not None:
        preferred = next(
            (
                name
                for name in available_models
                if name.casefold() == spec.listing_name.casefold()
            ),
            None,
        )
        if preferred is not None:
            return preferred

    prefix = f"{request_name}:".casefold()
    variants = sorted(
        name for name in available_models if name.casefold().startswith(prefix)
    )
    return variants[0] if variants else None


def _listed_name_for_model_path(
    root: Path, model_path: Path, available_models: set[str]
) -> str | None:
    relative = model_path.relative_to(root)
    parts = list(relative.parts)
    parts[-1] = parts[-1].split("--antfly-", 1)[0]
    return listed_model_name(available_models, PurePosixPath(*parts).as_posix())


def default_generator_model_name(
    available_generators: set[str] | None = None,
) -> str | None:
    override = os.environ.get("ANTFLY_INFERENCE_DEFAULT_GENERATOR_MODEL")
    if override:
        if available_generators is not None:
            return listed_model_name(available_generators, override) or override
        return override

    if available_generators is not None:
        if not available_generators:
            return None
        for candidate in (DEFAULT_GENERATOR_MODEL, DEFAULT_TOOL_GENERATOR_MODEL):
            if listed := listed_model_name(available_generators, candidate):
                return listed
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

    override = os.environ.get("ANTFLY_INFERENCE_TOOL_MODEL")
    if override:
        if available_generators is not None:
            return listed_model_name(available_generators, override) or override
        return override

    seen: set[Path] = set()
    root = models_dir()
    if root.exists():
        for pattern in (
            "**/genai_config.json",
            "**/special_tokens_map.json",
            "**/tokenizer_config.json",
        ):
            for metadata_path in root.glob(pattern):
                model_path = metadata_path.parent
                if model_path in seen:
                    continue
                seen.add(model_path)
                if detect_tool_call_format(model_path) is None:
                    continue
                if available_generators is None:
                    return model_path.relative_to(root).as_posix()
                if model_name := _listed_name_for_model_path(
                    root, model_path, available_generators
                ):
                    return model_name

    if available_generators is not None:
        return listed_model_name(available_generators, DEFAULT_TOOL_GENERATOR_MODEL)

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
            if isinstance(arch, str) and (
                "ConditionalGeneration" in arch or "Vision" in arch
            ):
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


def find_multimodal_generator_model_name(
    available_generators: set[str] | None = None,
) -> str | None:
    """Find a local multimodal generator model name for E2E tests."""

    override = os.environ.get("ANTFLY_INFERENCE_MULTIMODAL_GENERATOR_MODEL")
    if override:
        if available_generators is not None:
            return listed_model_name(available_generators, override) or override
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

            if available_generators is None:
                return model_path.relative_to(root).as_posix()
            if model_name := _listed_name_for_model_path(
                root, model_path, available_generators
            ):
                return model_name

    if available_generators is not None:
        # The curated fallback is capability-reviewed and remains usable when
        # a GGUF package does not carry Hugging Face processor metadata.
        return listed_model_name(
            available_generators, DEFAULT_MULTIMODAL_GENERATOR_MODEL
        )

    return DEFAULT_MULTIMODAL_GENERATOR_MODEL


def request_model_name(
    path: str, payload: dict | None
) -> tuple[str | None, str | None]:
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
    if not inference_download_enabled():
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
        spec = spec_for_name(value, "generators")
        if spec is not None:
            specs.append(spec)

    for env_name in READER_ENV_VARS:
        value = os.environ.get(env_name, "").strip()
        if not value:
            continue
        key = ("readers", value.lower())
        if key in seen:
            continue
        seen.add(key)
        spec = spec_for_name(value, "readers")
        if spec is not None:
            specs.append(spec)

    return specs


def bootstrap_models_for_listing(listing: dict) -> bool:
    if not inference_download_enabled():
        return False

    env_specs = _env_model_specs()
    explicit_keys = {(spec.task, spec.request_name.lower()) for spec in env_specs}
    planned: list[ModelSpec] = []
    for category, spec in LISTING_BOOTSTRAP.items():
        if listing.get(category):
            continue
        planned.append(spec)

    planned.extend(env_specs)

    changed = False
    seen: set[tuple[str, str]] = set()
    for spec in planned:
        key = (spec.task, spec.request_name.lower())
        if key in seen:
            continue
        seen.add(key)
        if spec.large and key not in explicit_keys and not run_large_model_tests():
            continue
        if model_available(spec):
            continue
        ensure_model(spec)
        changed = True
    return changed


def prefetch_curated_models() -> None:
    seen: set[tuple[str, str]] = set()
    planned: list[ModelSpec] = []
    planned.extend(
        spec for spec in CURATED_MODELS if not spec.large or run_large_model_tests()
    )
    planned.extend(_env_model_specs())

    for spec in planned:
        key = (spec.task, spec.request_name.lower())
        if key in seen:
            continue
        seen.add(key)
        ensure_model(spec)
