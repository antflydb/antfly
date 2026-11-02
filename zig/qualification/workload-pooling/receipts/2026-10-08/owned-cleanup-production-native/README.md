# Earlier cleanup candidate production native qualification

The frozen pooling-control-cleanup-owner candidate at 4d786fab4f completed its
native production provider/archive, SQL workload, actual worker-output/process
and cleanup-owner gate with exit 0: all 98 build steps succeeded. Thirty-seven
critical source hashes still match the compiled candidate. The log also contains
separate simple-runner counts; do not infer one aggregated test count from the
build summary. The earlier candidate uses runnable ABI 3. It does not include
newer ABI 4 team forwarding, SQL capability/constructor changes or completion
reservation, so it cannot qualify the current default proposal. Its pinned GNU
production counterpart remains running. Pooling stays disabled.
