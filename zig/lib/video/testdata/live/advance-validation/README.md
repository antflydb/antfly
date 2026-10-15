# Rolling and sparse live-window advance

Recorded run results and output filenames below describe the measured historical
epoch. Generated reports/logs were removed from the current tree; see
[qualification and historical recovery](../../../../../../docs/plans/native-video-noncuda-audit.md#artifact-policy-and-historical-evidence).
Fixture inputs and independent reference oracles remain checked in.

`Coordinator.Options.advance_ns` separates window advance from duration and frame
sampling. Null preserves consecutive windows. A positive representable interval
can be shorter than duration (overlap), equal (consecutive), or longer (sparse).
Zero/unrepresentable intervals fail initialization. Cursor arithmetic and all
retention probes precede commit mutation. Finished tails clip to the published
end; epoch changes drain old requested windows before resetting clocks.

Retention uses the next start rather than the delivered end. Prepared windows
own independent storage and remain readable after eviction. Sparse pumping keeps
only validated preroll while the cursor lies beyond the published frontier, so
skipped units do not accumulate until a distant requested interval. This still
obeys unit, packet, byte, deadline and window-count limits; required long-GOP
preroll may reach those limits. Admission/validation failures retain incoming
ownership and retry cursors.

MacOS ReleaseSafe passes 230 video, 19 live and 47 CPU/Metal model checks. Actual
Linux ARM64 passes 205 video (27 platform skips), 19 live and 42 model checks
(five Metal skips). New checks cover transactional commit failure, overlap
retention, sparse EOF, bounded ten-second sparse pumping under two unit slots,
old-epoch drain/reset clocks and independently held old packet storage.

Fresh pretrained CPU, Metal and actual Linux qualify the real six-second Sintel
clip with two-second windows advancing every second (six outputs, including the
one-second 124-token tail), and advancing every three seconds (two sparse outputs).
Every vector and token count is exactly equal to complete-clip source-time
inference. Pinned PyTorch ranking/top-k/score gates cover rolling windows 0, 2, 4,
which match its original non-overlapping intervals. Additional intervals are
qualified against the independent complete-clip native path, not new PyTorch
oracles. The reports retain that distinction.

The live adapter uses its explicit request-local cache during these calls. Its
conservative prefix keys change when one-second GOP preroll is evicted, so the
live telemetry records zero hits. The separate same-reader overlap control still
records reuse. No across-eviction cache benefit or sustained throughput claim is
made. Timings are diagnostic shared-host measurements.

Source, fixture, checkpoint, compiler, binary and artifact hashes are recorded in
`qualification.json`. No runtime dependency is added. The full non-CUDA goal
remains active, including incremental append validation, preroll-independent
projection reuse and broader real-stream/model/transport qualification.
