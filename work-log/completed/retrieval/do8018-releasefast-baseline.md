# DO8018 ReleaseFast Baseline

Captured on 2026-05-29 from worktree commit `7d13cfc14c60` with `WAIT_CATCHUP=300s`.

Generated artifacts are intentionally local and untracked under:

`work-log/do8018/releasefast-baseline-20260529-102819/`

## Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 41.195659333s
- Async catch-up time: 33.664121834s
- Catch-up complete: true
- Throughput: 1,713.89 records/sec, 5.23 MiB/sec
- `ps` RSS: 1,326,350,336 bytes
- Process footprint metric: 135,108,560 bytes
- Live malloc metric: 106,470,160 bytes
- Malloc zone metric: 142,606,336 bytes
- vmmap footprint: 135,161,446 bytes
- vmmap peak footprint: 895,379,046 bytes
- vmmap mapped-file resident: 638,792,499 bytes
- vmmap malloc allocated: 27,892,121 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 638,733,058
- Stored fields bytes: 127,319,958
- Inverted bytes: 507,291,687
- Inverted postings bytes: 390,270,235
- Inverted term dictionary bytes: 111,134,590
- Term block bytes: 105,475,283
- Term index bytes: 2,850,461
- Term FST bytes: 2,731,286
- Typed doc values bytes: 3,447,364
- Doc ordinal bytes: 282,460
- Section index bytes: 391,269
- Text merges completed: 79
- Full-text build peak bytes: 165,371,084
- Full-text pending peak bytes: 505,295,350
- Text merge buffer peak bytes: 164,774,834
- LSM compaction peak bytes: 67,919,576
- LSM in-memory state peak bytes: 35,822,737

## Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 40.330435167s
- Async catch-up time: 19.417085042s
- Catch-up complete: true
- Throughput: 1,750.66 records/sec, 5.34 MiB/sec
- `ps` RSS: 1,325,694,976 bytes
- Process footprint metric: 136,501,320 bytes
- Live malloc metric: 89,339,216 bytes
- Malloc zone metric: 151,355,392 bytes
- vmmap footprint: 136,524,595 bytes
- vmmap peak footprint: 884,788,428 bytes
- vmmap mapped-file resident: 615,094,681 bytes
- vmmap malloc allocated: 27,577,548 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 615,001,305
- Stored fields bytes: 127,443,573
- Inverted bytes: 483,446,376
- Inverted postings bytes: 378,793,860
- Inverted term dictionary bytes: 98,928,066
- Term block bytes: 93,818,595
- Term index bytes: 2,561,673
- Term FST bytes: 2,472,078
- Typed doc values bytes: 3,442,815
- Doc ordinal bytes: 282,460
- Section index bytes: 385,761
- Text merges completed: 80
- Full-text build peak bytes: 165,117,191
- Full-text pending peak bytes: 534,387,102
- Text merge buffer peak bytes: 253,109,269
- LSM compaction peak bytes: 68,058,624
- LSM in-memory state peak bytes: 24,740,534

## Read

RSS remains dominated by file-backed mapped segment residency. Live malloc and process footprint are much smaller than RSS, while mapped-file resident bytes closely track final segment bytes.

The remaining segment size is now attributable without expensive per-term diagnostics: postings are the largest component (~379-390 MiB), stored fields are ~127 MiB, and the v13 term dictionary is still ~99-111 MiB. Within the dictionary, most bytes are prefix-compressed term blocks rather than the FST itself (~2.5-2.7 MiB).
