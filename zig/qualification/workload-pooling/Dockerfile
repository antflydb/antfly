FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends python3 procps ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
ENTRYPOINT ["python3", "zig/qualification/workload-pooling/run_matrix.py"]
