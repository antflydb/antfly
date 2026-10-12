# evalaf — Agent Notes

Embeddable Go evaluation framework for LLM applications (RAG, agents, general outputs), usable as a library or the `evalaf` CLI. User-facing docs, config format, and examples are in `README.md`.

Module: `github.com/antflydb/antfly/go/pkg/evalaf` (nested Go module; build from this directory with `GOWORK=off`). `plugins/antfly/` is a separate nested module.

## Layout

- `eval/`: core library: `Evaluator`, `Example`, `EvalResult`, `Report` in `types.go`; runner, datasets, config, built-in string metrics
- `genkit/`: LLM-as-judge evaluators via Firebase Genkit
- `genkitplugin/`: adapts evalaf evaluators to Genkit's eval system and Dev UI
- `rag/`, `agent/`: retrieval/citation metrics and agent classification/tool-selection evaluators
- `redteam/`: prompt-injection, jailbreak, PII, and harmful-content evaluators plus `*RedTeamPreset()` helpers
- `plugins/antfly/`: Antfly client, target functions, and `*EvaluatorPreset()` helpers
- `cmd/evalaf/`: cobra/viper CLI; `ui/`: dashboard embedding helpers
- `examples/`, `testdata/datasets/`: runnable examples and sample datasets

## Rules

- Keep `eval/` dependency-free beyond the standard library and `gopkg.in/yaml.v3`. LLM, Genkit, and Antfly dependencies belong in their own packages.
- New evaluators implement `eval.Evaluator` and get a test next to them. Pattern-based evaluators must not call an LLM so the quick presets stay usable in CI.
- Use temperature 0 for LLM-as-judge evaluators.

## Commands

```bash
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go build ./cmd/evalaf
(cd plugins/antfly && GOWORK=off go test ./...)
```
