# Metal constants qualification

The final historical ReleaseFast source passes the five corpus comparisons and
two streamed TS checks. Actual Metal/model checks pass 66/66; actual Linux ARM64
portable model checks pass 54 with eight Metal skips. See the
[qualification summary](../../../../../docs/plans/native-video-noncuda-audit.md)
for results, unchanged gates, memory tradeoffs and platform/measurement limits.

[Source/binary provenance](staged-production-provenance.json) binds the measured
Metal implementation. [Comparison arguments](production-commands.json) reproduce
the five corpus runs with the verified checkpoint. Generated reports belong under
ignored `.benchmark-results/video/`.

Complete vectors, raw logs, intermediate failures and diagnostics remain in
[the pre-cleanup commit](https://github.com/antflydb/antfly/tree/bf9d632d025a6b12837997f6aeb86e1a818ec831/zig/lib/video/testdata/vision-weight-validation).
They retain their original status and source epochs. This directory keeps only
reproduction metadata; cleanup does not rerun or broaden qualification.
