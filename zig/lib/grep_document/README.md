# Grep document contract

Native Zig source-document v1 validation and exact miss-accounting primitives,
shared by antfly-grep and the query response owner. The library has no runtime
or build dependency beyond Zig's standard library.

`Scan.validate` bounds request literals and field paths. `certify` validates a
complete document against its retained source and returns a miss only when all
necessary literals are absent and binary/transcoding behavior is irrelevant.
`withoutContent` copies JSON containers along the selected path while borrowing
scalar bytes from the hit owner; it never mutates a retained source. Proofs must
be emitted by the final authorized snapshot owner and verified before client
accounting. This library does not authenticate arbitrary external proofs.

Run its standalone tests with `zig test zig/lib/grep_document/src/mod.zig`.
