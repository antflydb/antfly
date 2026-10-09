`pyiceberg_manifest.avro` is an independent PyIceberg 0.12 data manifest from
`test_iceberg_sql.py`: the retained west partition after append, field rename,
and copy-on-write deletion. Its `sequence_number` and `file_sequence_number`
are both 1. PyIceberg wrote the Avro schema, metrics, and partition records;
the E2E export helper only rewrote warehouse addresses to `object://antfly`.

The E2E regenerates complete tables on every run. This small retained manifest
also gives the unit suite coverage without installing Python writer packages.
