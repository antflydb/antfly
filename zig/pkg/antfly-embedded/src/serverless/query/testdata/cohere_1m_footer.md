# Cohere metadata fixture

`cohere_1m_footer.bin` is the 608-byte compact-Thrift footer of the local
VectorDBBench Cohere 1M training Parquet fixture. It contains schema, row-group,
and column statistics; vector pages are omitted. The original file is
3,131,995,162 bytes, SHA-256
`de6b84eb08fc5203a56eef619970a14f5cae08d0a0ae0bcd23b8058d7045f381`.

Extract it by reading the little-endian uint32 footer length eight bytes before
EOF, then the preceding metadata bytes. The regression covers a standard i16
row-group ordinal and a million-row `LIST<FLOAT>` column without embedding the
3 GB dataset in the repository.
