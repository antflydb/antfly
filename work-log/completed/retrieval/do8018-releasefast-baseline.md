# DO8018 ReleaseFast Baseline

Captured on 2026-05-29 from worktree commit `7d13cfc14c60` with `WAIT_CATCHUP=300s`.
Updated on 2026-05-29 from worktree commit `4ad4b59f5dd9` after the v15 prefix-run term dictionary and compact postings metadata codec changes.
Updated on 2026-05-29 from worktree commit `14dfe7f28446` after the v16 per-section norms codec change. The initial v16 top-level health metric under-reported norms because shard aggregation omitted `inverted_norm_bytes`; direct segment parsing showed the norm artifacts were present on disk.
Updated on 2026-05-29 from worktree commit `89a0e73e71d7` after byte-budgeted stored-field blocks.
Updated on 2026-05-29 from worktree commit `d255fecf291d` after caching per-segment layout stats and adding merge/retire clean-page advice.
Updated on 2026-05-29 after adding explicit current-scan helpers for maintenance paths so TTL/schema scans can avoid cloning the LSM mutable writer generation. This does not change the latest ReleaseFast baseline below; it is a working-set cleanup for the remaining mutable snapshot path.
Updated on 2026-05-29 after moving the segment container to v3 with a zero checksum sentinel. Segment publish/open no longer performs a full-file CRC pass by default, which avoids forcing newly-written mmap segment pages resident just to validate the footer.

Generated artifacts are intentionally local and untracked under:

`work-log/do8018/releasefast-baseline-20260529-102819/`

Latest v15 artifacts:

`work-log/do8018/releasefast-baseline-20260529-105254/`

Latest v16 artifacts:

`work-log/do8018/releasefast-baseline-20260529-112226/`

Latest stored-block-cap artifacts:

`work-log/do8018/releasefast-baseline-20260529-122215/`

Latest layout-cache/page-advice artifacts:

`work-log/do8018/releasefast-baseline-20260529-123427/`

## LSM Mutable Snapshot Cleanup

- Added `backend_scan.scanCurrent`, `scanPrefixCurrent`, and `scanRangeCurrent` so maintenance callers can request a live ordered cursor without using snapshot read transactions.
- Changed TTL candidate collection to use `scanCurrent`.
- Changed schema copying to use a probe read for the active schema key and `scanPrefixCurrent` for versioned schema rows.
- Added an LSM regression test proving current scan helpers do not increment `mutable_snapshot_clone_calls`.
- Verification:
  - `zig build lib-db-query-test --summary failures -- --test-filter "current scan helpers"`
  - `zig build lib-db-query-test --summary failures -- --test-filter "current scan does not"`
  - `zig build lib-db-query-test --summary failures -- --test-filter "ttl runtime"`
  - `zig build lib-db-query-test --summary failures -- --test-filter "schema"`
  - `zig build lib-db-query-test --summary failures`

## Segment Container v3

- Segment writers now emit v3 footers with checksum `0` as a sentinel for "not materialized".
- Segment readers still honor non-zero checksums, but normal v3 opens skip the full-segment CRC pass.
- File-backed segment publish and merge paths no longer read the entire just-written segment back through `crc32Prefix`, reducing page-cache/RSS pressure.
- Fixed a leaked merged-term allocation in the inverted-section merge loop that the file-backed merge test exposed.
- Verification:
  - `zig build lib-db-query-test --summary failures -- --test-filter "text segment"`
  - `zig build persistent-test --summary failures -- --test-filter "persistent index"`
  - `zig build index-manager-test --summary failures -- --test-filter "text merge"`
  - `zig build lib-db-query-test --summary failures`

## Latest Layout-Cache Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 37.679355958s
- Async catch-up time: 16.851997875s
- Catch-up complete: true
- Throughput: 1,873.84 records/sec, 5.72 MiB/sec
- `ps` RSS: 1,189,199,872 bytes
- Process footprint metric: 139,351,992 bytes
- Live malloc metric: 64,649,392 bytes
- Malloc zone metric: 148,111,360 bytes
- vmmap footprint: 139,355,750 bytes
- vmmap peak footprint: 862,034,329 bytes
- vmmap mapped-file resident: 533,725,184 bytes
- vmmap malloc allocated: 15,938,355 bytes
- Final full-text segment files: 7
- Final full-text segment bytes: 533,722,486
- Stored fields bytes: 127,416,442
- Inverted bytes: 402,266,056
- Inverted norms bytes: 21,790,557
- Inverted postings bytes: 273,274,095
- Inverted term dictionary bytes: 101,326,596
- Term block bytes: 96,190,950
- Term index bytes: 2,596,922
- Term FST bytes: 2,476,564
- Typed doc values bytes: 3,438,357
- Doc ordinal bytes: 282,455
- Section index bytes: 318,896
- Text merges completed: 100
- Full-text build peak bytes: 163,222,864
- Full-text pending peak bytes: 533,818,260
- Text merge buffer peak bytes: 261,072,840
- LSM compaction peak bytes: 67,648,368
- LSM in-memory state peak bytes: 35,419,809

## Latest Layout-Cache Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 40.21371225s
- Async catch-up time: 19.834223958s
- Catch-up complete: true
- Throughput: 1,755.74 records/sec, 5.36 MiB/sec
- `ps` RSS: 1,173,716,992 bytes
- Process footprint metric: 154,473,944 bytes
- Live malloc metric: 68,008,208 bytes
- Malloc zone metric: 148,996,096 bytes
- vmmap footprint: 155,189,248 bytes
- vmmap peak footprint: 744,279,244 bytes
- vmmap mapped-file resident: 518,835,404 bytes
- vmmap malloc allocated: 16,882,073 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 518,796,682
- Stored fields bytes: 127,491,815
- Inverted bytes: 387,210,961
- Inverted norms bytes: 11,440,900
- Inverted postings bytes: 264,474,445
- Inverted term dictionary bytes: 105,577,127
- Term block bytes: 100,132,018
- Term index bytes: 2,739,739
- Term FST bytes: 2,633,190
- Typed doc values bytes: 3,443,302
- Doc ordinal bytes: 282,460
- Section index bytes: 367,824
- Text merges completed: 97
- Full-text build peak bytes: 136,639,703
- Full-text pending peak bytes: 519,553,028
- Text merge buffer peak bytes: 117,712,340
- LSM compaction peak bytes: 67,447,216
- LSM in-memory state peak bytes: 35,251,321

## Latest Stored-Block-Cap Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 35.097770584s
- Async catch-up time: 25.09883575s
- Catch-up complete: true
- Throughput: 2,011.67 records/sec, 6.14 MiB/sec
- `ps` RSS: 1,199,521,792 bytes
- Process footprint metric: 140,416,904 bytes
- Live malloc metric: 81,549,760 bytes
- Malloc zone metric: 151,699,456 bytes
- vmmap footprint: 140,404,326 bytes
- vmmap peak footprint: 798,385,766 bytes
- vmmap mapped-file resident: 525,126,860 bytes
- vmmap malloc allocated: 27,996,979 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 525,074,405
- Stored fields bytes: 127,510,045
- Inverted bytes: 393,469,833
- Inverted norms bytes: 13,426,531
- Inverted postings bytes: 269,380,390
- Inverted term dictionary bytes: 104,902,777
- Term block bytes: 99,443,975
- Term index bytes: 2,742,613
- Term FST bytes: 2,643,409
- Typed doc values bytes: 3,444,193
- Doc ordinal bytes: 282,460
- Section index bytes: 367,554
- Text merges completed: 107
- Full-text build peak bytes: 166,304,194
- Full-text pending peak bytes: 525,060,607
- Text merge buffer peak bytes: 168,206,906
- LSM compaction peak bytes: 67,313,656
- LSM in-memory state peak bytes: 35,487,474

## Latest Stored-Block-Cap Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 38.071871083s
- Async catch-up time: 16.9697095s
- Catch-up complete: true
- Throughput: 1,854.52 records/sec, 5.66 MiB/sec
- `ps` RSS: 1,108,623,360 bytes
- Process footprint metric: 124,737,416 bytes
- Live malloc metric: 87,654,080 bytes
- Malloc zone metric: 152,043,520 bytes
- vmmap footprint: 124,780,544 bytes
- vmmap peak footprint: 857,210,880 bytes
- vmmap mapped-file resident: 521,876,275 bytes
- vmmap malloc allocated: 27,892,121 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 521,822,598
- Stored fields bytes: 127,429,474
- Inverted bytes: 390,300,684
- Inverted norms bytes: 11,413,914
- Inverted postings bytes: 265,725,162
- Inverted term dictionary bytes: 106,839,837
- Term block bytes: 101,368,692
- Term index bytes: 2,756,431
- Term FST bytes: 2,641,774
- Typed doc values bytes: 3,440,405
- Doc ordinal bytes: 282,460
- Section index bytes: 369,255
- Text merges completed: 112
- Full-text build peak bytes: 166,228,476
- Full-text pending peak bytes: 530,330,787
- Text merge buffer peak bytes: 127,868,138
- LSM compaction peak bytes: 67,379,736
- LSM in-memory state peak bytes: 24,821,443

## Latest v16 Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 36.475740917s
- Async catch-up time: 23.43362s
- Catch-up complete: true
- Throughput: 1,935.67 records/sec, 5.91 MiB/sec
- `ps` RSS: 1,188,052,992 bytes
- Process footprint metric: 140,220,320 bytes
- Live malloc metric: 78,916,448 bytes
- Malloc zone metric: 160,776,192 bytes
- vmmap footprint: 140,194,611 bytes
- vmmap peak footprint: 952,841,011 bytes
- vmmap mapped-file resident: 523,449,139 bytes
- vmmap malloc allocated: 27,682,406 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 523,396,046
- Stored fields bytes: 127,469,017
- Inverted bytes: 391,836,582
- Inverted norms bytes: 14,048,785 (direct segment parse; top-level health metric was missing aggregation)
- Inverted postings bytes: 265,156,118
- Inverted term dictionary bytes: 106,529,640
- Term block bytes: 101,047,551
- Term index bytes: 2,759,443
- Term FST bytes: 2,651,066
- Typed doc values bytes: 3,445,871
- Doc ordinal bytes: 282,460
- Section index bytes: 361,796
- Text merges completed: 75
- Full-text build peak bytes: 173,669,242
- Full-text pending peak bytes: 523,817,942
- Text merge buffer peak bytes: 138,321,873
- LSM compaction peak bytes: 68,527,232
- LSM in-memory state peak bytes: 20,184,267

## Latest v16 Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 35.710530083s
- Async catch-up time: 29.462190458s
- Catch-up complete: true
- Throughput: 1,977.15 records/sec, 6.03 MiB/sec
- `ps` RSS: 1,182,580,736 bytes
- Process footprint metric: 116,807,608 bytes
- Live malloc metric: 89,608,512 bytes
- Malloc zone metric: 158,646,272 bytes
- vmmap footprint: 116,916,224 bytes
- vmmap peak footprint: 748,263,833 bytes
- vmmap mapped-file resident: 527,433,728 bytes
- vmmap malloc allocated: 27,892,121 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 527,350,293
- Stored fields bytes: 127,555,983
- Inverted bytes: 395,700,555
- Inverted norms bytes: 13,759,899 (direct segment parse; top-level health metric was missing aggregation)
- Inverted postings bytes: 266,689,426
- Inverted term dictionary bytes: 109,138,919
- Term block bytes: 103,576,237
- Term index bytes: 2,804,401
- Term FST bytes: 2,686,941
- Typed doc values bytes: 3,446,804
- Doc ordinal bytes: 282,460
- Section index bytes: 364,171
- Text merges completed: 83
- Full-text build peak bytes: 149,230,943
- Full-text pending peak bytes: 529,366,226
- Text merge buffer peak bytes: 152,093,294
- LSM compaction peak bytes: 67,918,960
- LSM in-memory state peak bytes: 29,322,182

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

After the v16 codec change, postings fell again to ~265-267 MiB, with norms stored separately at ~13.8-14.0 MiB. Segment bytes stayed around ~523-527 MiB because the term dictionary varied upward on this run. Final RSS remains ~1.18-1.19 GiB while live malloc is only ~79-90 MiB and vmmap malloc allocated is ~28 MiB. The actionable RSS target is therefore mapped segment residency plus allocator retention, not a large live heap.

The stored-field section in the v16 metrics-off artifacts was ~127.5 MiB: ~123.6 MiB compressed payload, ~244.8 MiB raw payload, and ~3.9 MiB metadata. Fixed 128-doc stored-field blocks produced a worst raw block of ~16.3 MiB, so the next codec step capped stored-field blocks by raw bytes as well as doc count.

After the stored-block-cap change, stored-field bytes stayed around ~127.4-127.5 MiB and load time improved to ~35.1s metrics-off / ~38.1s metrics-on. The final block-compressed payload was ~123.6 MiB, raw payload was ~245.1 MiB, and the segment set used 704 stored-field blocks metrics-off / 687 metrics-on. The worst raw block is now ~5.5 MiB, down from ~16.3 MiB, because a single oversized document still occupies a block by itself; ordinary multi-doc blocks are capped at the 512 KiB raw target. RSS is still not live-heap dominated: final `ps` RSS is ~1.11-1.20 GiB, live malloc is ~82-88 MiB, vmmap malloc allocated is ~28 MiB, and mapped-file resident bytes closely track final full-text segment bytes at ~522-525 MiB. The next RSS slice should focus on segment mapping residency, reader release cleanup, and allocator-retained pages after transient merge/build work.

After caching per-segment layout stats and adding clean-page advice on merge sources/retired segments, normal metrics scrapes no longer need to re-parse mmap-backed segment bytes for section attribution. The post-change ReleaseFast run shows the expected allocator-side improvement but not a final RSS breakthrough: vmmap malloc allocated fell from ~28 MiB to ~16 MiB, health live malloc fell to ~65-68 MiB, and metrics-on vmmap peak footprint fell from ~857 MiB to ~744 MiB. Final `ps` RSS stayed around ~1.17-1.19 GiB because mapped-file resident bytes still closely track final segment bytes (~519-534 MiB) and macOS allocator/library resident pages remain counted in RSS. Metrics-off varied to 7 final segments and a larger 533.7 MiB segment set, so the remaining target is still the full-text segment bytes plus mmap/page-cache residency policy, not hidden live heap.
