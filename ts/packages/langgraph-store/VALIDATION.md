# Validation

See the [suite validation report](../langchain/VALIDATION.md) and the committed
[memory test output](../langchain/validation/memory-tests.txt). Six tests pass
with the live profile enabled: five deterministic contract cases and one actual
two-user compiled LangGraph runtime lifecycle. Separate processes also recalled
both users' values after stopping and restarting Antfly with its persisted data.

The independent npm consumer verifies this package's ESM/CommonJS exports,
TypeScript interfaces and a live persistent roundtrip from its packed artifact.
No npm publication or execution-checkpointer implementation is claimed.
