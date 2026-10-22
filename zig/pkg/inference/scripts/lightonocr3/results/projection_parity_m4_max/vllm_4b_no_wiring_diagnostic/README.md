# Incomplete wiring diagnostic

This intentionally disabled MLX wiring through the saved sitecustomize hook.
The engine fell to roughly 2–3 generation tokens/s in its logs, versus about
80 in normal runs. After several minutes only this diagnostic process tree
was terminated. The resulting report is a failed, incomplete experiment:
**there is no valid page-throughput result**, and it is excluded from targets.
Normal `vllm_4b` retains vLLM Metal’s default wired limit (recommended working set).
