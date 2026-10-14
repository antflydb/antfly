# Released server fixtures

These bytes were produced through the public standalone APIs of the official
`antfly_0.2.5_Darwin_arm64.tar.gz` release, not by a legacy test encoder.
`producer.json` records the executable and artifact SHA-256 hashes.

- `portable.afb`: a portable server backup containing two JSON documents,
  text/keyword/numeric mappings, an external dense index and graph edges.
- `schema.asch`: the active ASCH v12 value extracted verbatim from that backup.
- `schema.json`: its public schema JSON, also extracted verbatim.
- `native-generation.json`: the v0.2.5 native backup manifest, with the original
  v3 inventory and no `install_path` fields. Its corpus includes sparse indexes.
- `native/`: the complete unmodified snapshot described by that manifest; the
  manifest authenticates every primary, dense, sparse, full-text and graph file.
- `native-indexes.json`: the verbatim public index contract from its backup metadata.

The same server workflow on the v0.2.1, v0.2.2, v0.2.3 and v0.2.4 release
executables produces the identical 212-byte runtime schema (SHA-256
`c458649eff917637f86dcf20283cf8f4d3d62bf60f773b9473f45236ed0b946c`).
The archive preserves the released writer's document-before-metadata order.

Generate complete release directories and archives with
`scripts/ci/qualify-v02-server-upgrade.py --seed-only`. Its current corpus also
includes external sparse indexes and an independent unclean-stop directory.
The checked-in archive predates that additional sparse coverage.

Run the focused schema, archive and admission regressions with
`zig build antfly-v02-upgrade-test` from `zig/`.
