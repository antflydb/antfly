# Shared preflight and geometry admission qualification

The shared ModelManager host domain now covers video inspection before model
loading, with bounded/reclaiming allocation and cleanup of temporary metadata
and decoder ownership. The model also acquires a host-only planning permit
before backend execution admission. Preparation estimates derive from geometry,
selected unique pictures and native decoder surface bounds; sequential video
parts take a maximum transient share. The 3344 MiB blanket workspace estimate
is replaced by 1536 MiB model work plus the planned preprocessing share.

Nineteen model tests pass, including allocation failure at every temporary-owner
position in MJPEG, VP9 and PAFF inspection. The selected controller binary passes
34 tests (33 import gates and the substantive shared-capacity/denial/retry test).
The CPU/Metal production server passes six original video/mixed cases, reduced
dimensions, admission/decode retry and eight WebM vectors with exact reference
rankings/top-k. Concurrent/sustained and disconnect/retry checks pass; the latter
does not prove cancellation completion.

Hashes bind source, production binary and reports. Cross-request stage overlap,
current Linux pretrained execution, isolated performance tuning and the full
remaining non-CUDA scope remain active. Shared-host timings do not establish
capacity or speedup. Compiler scheduling memory is separate from serving limits.
