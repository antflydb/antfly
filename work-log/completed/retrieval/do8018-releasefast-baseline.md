# DO8018 ReleaseFast Baseline

Captured on 2026-05-29 from worktree commit `7d13cfc14c60` with `WAIT_CATCHUP=300s`.
Updated on 2026-05-29 from worktree commit `4ad4b59f5dd9` after the v15 prefix-run term dictionary and compact postings metadata codec changes.

Generated artifacts are intentionally local and untracked under:

`work-log/do8018/releasefast-baseline-20260529-102819/`

Latest v15 artifacts:

`work-log/do8018/releasefast-baseline-20260529-105254/`

## Latest v15 Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 36.752541458s
- Async catch-up time: 18.279028333s
- Catch-up complete: true
- Throughput: 1,921.09 records/sec, 5.86 MiB/sec
- `ps` RSS: 1,209,597,952 bytes
- Process footprint metric: 118,413,600 bytes
- Live malloc metric: 81,581,488 bytes
- Malloc zone metric: 177,143,808 bytes
- vmmap footprint: 118,384,230 bytes
- vmmap peak footprint: 747,529,830 bytes
- vmmap mapped-file resident: 518,311,116 bytes
- vmmap malloc allocated: 27,472,691 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 518,233,345
- Stored fields bytes: 127,408,313
- Inverted bytes: 386,730,207
- Inverted postings bytes: 282,317,925
- Inverted term dictionary bytes: 98,838,348
- Term block bytes: 93,720,038
- Term index bytes: 2,571,189
- Term FST bytes: 2,474,041
- Typed doc values bytes: 3,443,293
- Doc ordinal bytes: 282,460
- Section index bytes: 368,752
- Text merges completed: 77
- Full-text build peak bytes: 168,530,635
- Full-text pending peak bytes: 534,773,344
- Text merge buffer peak bytes: 181,786,832
- LSM compaction peak bytes: 67,922,600
- LSM in-memory state peak bytes: 33,664,540

## Latest v15 Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 37.55379525s
- Async catch-up time: 22.85081675s
- Catch-up complete: true
- Throughput: 1,880.10 records/sec, 5.74 MiB/sec
- `ps` RSS: 1,239,040,000 bytes
- Process footprint metric: 125,310,712 bytes
- Live malloc metric: 81,627,536 bytes
- Malloc zone metric: 158,580,736 bytes
- vmmap footprint: 125,304,832 bytes
- vmmap peak footprint: 859,727,462 bytes
- vmmap mapped-file resident: 525,546,291 bytes
- vmmap malloc allocated: 27,577,548 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 525,486,885
- Stored fields bytes: 127,352,626
- Inverted bytes: 394,056,953
- Inverted postings bytes: 284,730,267
- Inverted term dictionary bytes: 103,652,781
- Term block bytes: 98,375,002
- Term index bytes: 2,660,320
- Term FST bytes: 2,548,319
- Typed doc values bytes: 3,443,630
- Doc ordinal bytes: 282,460
- Section index bytes: 350,896
- Text merges completed: 83
- Full-text build peak bytes: 167,026,722
- Full-text pending peak bytes: 536,808,634
- Text merge buffer peak bytes: 133,531,569
- LSM compaction peak bytes: 67,718,088
- LSM in-memory state peak bytes: 41,450,782

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

After the v15 codec changes, final segment bytes fell to ~518-525 MiB and postings fell to ~282-285 MiB. The term dictionary remains ~99-104 MiB, with term blocks still ~94-98 MiB and the FST still only ~2.5 MiB. RSS also fell, but it still tracks mapped segment residency plus allocator/library resident pages much more closely than live heap.
