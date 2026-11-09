# Persisted document units

`unit_v5.json` was captured using the version-5 payload encoder and fingerprint
encoder from Antfly `57c6d76753454453443b484b50182082be25d6ec`. It is a complete
persisted unit with no rendering-policy field, rather than a version-6 payload
with its marker relabeled. Its content is synthetic (`hello world`).

- Fingerprint: `duf5:acda8ad8765b4ad978ed1792837bb279b11ea01f7a3509605320b8b26d1c7122`
- File SHA-256: `99a65fc7b9b875dd255acbe3ddfab45c1caa305468b4d447a55137a82ee4c11b`

The fixture is read-only input to codec and ordered child-publication tests.
Current writers must still emit version 6. Version-5 reads verify the original
hashed field set and reject rendering-policy fields, unknown versions and altered
content. Completed-state reuse still requires version 6, so parent re-extraction
and child work can proceed independently during an upgrade.
