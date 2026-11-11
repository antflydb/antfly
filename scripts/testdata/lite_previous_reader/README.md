# Historical Lite reader fixtures

`empty-v3.aflite` is an empty 4096-byte native Lite v3 file. It was generated with the real native writer at `3730dc38f3` using:

```zig
var file = try NativeFile.createWithIo(alloc, io, output_path, .{ .indexed_reclamation = false });
file.close();
```

SHA-256: `d0a509a1ddf87feaf017032b1f9762bc89fd67c6568760a6f867bd23baf95d48`.

The file contains no documents or application catalogs. Its generated header identities are synthetic. Do not construct it by editing another format's magic bytes.

The pinned `e8666a5b75` reader supports v3 and v4; current readers support v3 and v7. Creation defaults have changed, so compatibility tests copy this common physical format before applying catalog features. This lets the positive control open successfully and makes negative controls exercise catalog reader fences instead of an incompatible physical header. The pinned reader sources remain unchanged apart from the separately verified build-only repair.

The historical writable Lite opener automatically converts v3 to v4, which current Lite does not support. The already-open writer case therefore uses directory storage to verify the generation-owner exclusion, then checks historical catalog rejection after that owner closes. It does not claim simultaneous cross-version Lite writers are supported.
