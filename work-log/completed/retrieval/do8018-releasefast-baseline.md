# DO8018 ReleaseFast Baseline

Captured on 2026-05-29 from worktree commit `7d13cfc14c60` with `WAIT_CATCHUP=300s`.
Updated on 2026-05-29 from worktree commit `4ad4b59f5dd9` after the v15 prefix-run term dictionary and compact postings metadata codec changes.
Updated on 2026-05-29 from worktree commit `14dfe7f28446` after the v16 per-section norms codec change. The initial v16 top-level health metric under-reported norms because shard aggregation omitted `inverted_norm_bytes`; direct segment parsing showed the norm artifacts were present on disk.
Updated on 2026-05-29 from worktree commit `89a0e73e71d7` after byte-budgeted stored-field blocks.
Updated on 2026-05-29 from worktree commit `d255fecf291d` after caching per-segment layout stats and adding merge/retire clean-page advice.
Updated on 2026-05-29 after adding explicit current-scan helpers for maintenance paths so TTL/schema scans can avoid cloning the LSM mutable writer generation. This does not change the latest ReleaseFast baseline below; it is a working-set cleanup for the remaining mutable snapshot path.
Updated on 2026-05-29 after moving the segment container to v3 with a zero checksum sentinel. Segment publish/open no longer performs a full-file CRC pass by default, which avoids forcing newly-written mmap segment pages resident just to validate the footer.
Updated on 2026-05-29 from worktree commit `cf09767718c1` with a post-v3 ReleaseFast baseline. Final mapped-file resident dropped from about 519-534 MiB to about 32-33 MiB while final segment bytes stayed about 528-532 MiB; final `ps` RSS dropped from about 1.17-1.19 GiB to about 684-700 MiB.
Updated on 2026-05-31 from worktree commit `a2dfe4cec46a` after the v18 varint postings header codec change. The baseline script now builds with `zig build install -Doptimize=ReleaseFast`.
Updated on 2026-05-31 from worktree commit `eb338f2fdbca` after the v19 sparse block-max codec change.
Updated on 2026-05-31 from worktree commit `629ad9cdecd2` after the v20 compact tagged term-block value codec change.
Updated on 2026-05-31 from worktree commit `7f83fe7b7904` after the v21 bit-packed postings chunk metadata codec change.
Updated on 2026-05-31 from worktree commit `312fc58f215c` after fixing idle derived-backlog release and making the DO8018 wait gate full-text scoped. Metrics-on no longer hits the 300s catch-up ceiling.
Repeated on 2026-05-31 from worktree commit `12e7e4cb4` to check whether the corrected baseline represented a real slowdown or run noise.
Repeated on 2026-05-31 from worktree commit `1786a764e2f0` after keeping active mmap-backed text segments warm while still discarding retired merge source pages.

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

Latest segment-v3/no-eager-checksum artifacts:

`work-log/do8018/releasefast-baseline-20260529-130221/`

Latest v18 artifacts:

`work-log/do8018/releasefast-baseline-20260531-153844/`

Latest v19 artifacts:

`work-log/do8018/releasefast-baseline-20260531-163822/`

Latest v20 artifacts:

`work-log/do8018/releasefast-baseline-20260531-165023/`

Latest v21 artifacts:

`work-log/do8018/releasefast-baseline-20260531-210218/`

Latest corrected v21 artifacts:

`work-log/do8018/releasefast-baseline-20260531-213429/`

Corrected v21 repeat artifacts:

`work-log/do8018/releasefast-baseline-20260531-215403/`

Post active-mmap-advice artifacts:

`work-log/do8018/releasefast-baseline-20260531-220831/`

## Latest Segment-v3 Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 41.689561709s
- Async catch-up time: 18.30403375s
- Catch-up complete: true
- Throughput: 1,693.59 records/sec, 5.17 MiB/sec
- `ps` RSS: 684,179,456 bytes
- Process footprint metric: 122,788,128 bytes
- Live malloc metric: 99,002,224 bytes
- Malloc zone metric: 149,815,296 bytes
- vmmap footprint: 122,788,249 bytes
- vmmap peak footprint: 865,494,630 bytes
- vmmap mapped-file resident: 33,135,001 bytes
- vmmap malloc allocated: 27,577,548 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 527,514,173
- Stored fields bytes: 127,660,919
- Inverted bytes: 395,751,664
- Inverted norms bytes: 12,711,407
- Inverted postings bytes: 270,698,041
- Inverted term dictionary bytes: 106,504,141
- Term block bytes: 100,981,932
- Term index bytes: 2,778,761
- Term FST bytes: 2,669,948
- Typed doc values bytes: 3,446,943
- Doc ordinal bytes: 282,460
- Section index bytes: 371,867
- Text merges completed: 148
- Full-text build peak bytes: 173,817,800
- Full-text pending peak bytes: 532,064,716
- Text merge buffer peak bytes: 147,361,230
- LSM compaction peak bytes: 67,448,112
- LSM in-memory state peak bytes: 35,371,296

## Latest Segment-v3 Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 41.824538917s
- Async catch-up time: 18.33002275s
- Catch-up complete: true
- Throughput: 1,688.12 records/sec, 5.15 MiB/sec
- `ps` RSS: 699,514,880 bytes
- Process footprint metric: 134,371,832 bytes
- Live malloc metric: 91,245,552 bytes
- Malloc zone metric: 143,196,160 bytes
- vmmap footprint: 135,056,588 bytes
- vmmap peak footprint: 798,490,624 bytes
- vmmap mapped-file resident: 32,400,998 bytes
- vmmap malloc allocated: 28,101,836 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 532,198,379
- Stored fields bytes: 127,694,574
- Inverted bytes: 400,394,698
- Inverted norms bytes: 13,005,800
- Inverted postings bytes: 269,654,389
- Inverted term dictionary bytes: 111,359,228
- Term block bytes: 105,623,533
- Term index bytes: 2,885,593
- Term FST bytes: 2,774,402
- Typed doc values bytes: 3,444,316
- Doc ordinal bytes: 282,460
- Section index bytes: 382,011
- Text merges completed: 132
- Full-text build peak bytes: 170,753,841
- Full-text pending peak bytes: 530,364,540
- Text merge buffer peak bytes: 177,292,614
- LSM compaction peak bytes: 67,851,816
- LSM in-memory state peak bytes: 22,407,793

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

## Latest v17 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260529-143511/`.

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 42.915169291s
- Async catch-up time: 18.33134575s
- Catch-up complete: true
- Throughput: 1,645.22 records/sec, 5.02 MiB/sec
- `ps` RSS: 651,231,232 bytes
- Process footprint metric: 140,221,160 bytes
- Live malloc metric: 84,073,008 bytes
- Malloc zone metric: 154,959,872 bytes
- vmmap footprint: 140,194,611 bytes
- vmmap peak footprint: 701,077,913 bytes
- vmmap mapped-file resident: 30,513,561 bytes
- vmmap malloc allocated: 23,802,675 bytes
- Final full-text segment files: 7
- Final full-text segment bytes: 491,326,674
- Stored fields bytes: 127,631,373
- Inverted bytes: 359,619,589
- Inverted norms bytes: 10,753,273
- Inverted postings bytes: 233,951,318
- Inverted term dictionary bytes: 108,998,086
- Term block bytes: 103,493,749
- Term index bytes: 2,780,155
- Term FST bytes: 2,655,382
- Typed doc values bytes: 3,442,891
- Doc ordinal bytes: 282,455
- Section index bytes: 350,086
- Text merges completed: 130
- Full-text build peak bytes: 141,008,591
- Full-text pending peak bytes: 494,950,004
- Text merge buffer peak bytes: 101,753,688
- LSM compaction peak bytes: 67,649,264
- LSM in-memory state peak bytes: 25,481,960

## Latest v17 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260529-143511/`.

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 46.483675792s
- Async catch-up time: 19.862142s
- Catch-up complete: true
- Throughput: 1,518.92 records/sec, 4.63 MiB/sec
- `ps` RSS: 670,515,200 bytes
- Process footprint metric: 147,348,416 bytes
- Live malloc metric: 96,936,672 bytes
- Malloc zone metric: 139,640,832 bytes
- vmmap footprint: 147,324,928 bytes
- vmmap peak footprint: 741,657,804 bytes
- vmmap mapped-file resident: 30,932,992 bytes
- vmmap malloc allocated: 34,288,435 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 493,948,726
- Stored fields bytes: 127,596,177
- Inverted bytes: 362,252,258
- Inverted norms bytes: 11,972,506
- Inverted postings bytes: 239,859,443
- Inverted term dictionary bytes: 104,580,822
- Term block bytes: 99,225,207
- Term index bytes: 2,695,140
- Term FST bytes: 2,586,895
- Typed doc values bytes: 3,445,495
- Doc ordinal bytes: 282,460
- Section index bytes: 372,016
- Text merges completed: 136
- Full-text build peak bytes: 163,539,546
- Full-text pending peak bytes: 496,860,397
- Text merge buffer peak bytes: 190,419,768
- LSM compaction peak bytes: 67,783,440
- LSM in-memory state peak bytes: 13,994,581

## v17 Detailed Layout Probe

Artifacts: `work-log/do8018/releasefast-baseline-20260529-142737/`.

`ANTFLY_BENCH_MEMORY_LAYOUT_DETAIL=1` was run separately to attribute postings internals. This mode is intentionally not the RSS baseline: detailed per-term layout reads postings metadata and makes mapped segment pages resident. Metrics-off completed catch-up in 21.387183375s with `ps` RSS 1,003,667,456 bytes, vmmap mapped-file resident 361,758,720 bytes, final segment bytes 497,853,365, and postings bytes 237,567,801. The detailed postings split was:

- Postings header bytes: 79,436,712
- Block-max bytes: 33,762,534
- Chunk metadata bytes: 51,201,432
- Packed postings payload bytes: 26,338,859
- Positions bytes: 46,828,264
- Postings terms: 3,309,863

Metrics-on with detailed layout hit the 300s wait limit with no full-text pending bytes but 7,020 bytes of derived backlog. That confirms detailed diagnostics are too intrusive for normal baseline runs. Its final segment bytes were 477,512,670, postings bytes 225,451,374, and mapped-file resident 340,996,915 bytes.

## v17 Read

The v17 codec changes moved posting chunk metadata from 20-byte fixed records to 12-byte cumulative-end records. The reader derives `chunk_id` from `max_doc / chunk_size` and reconstructs each chunk payload offset from the previous cumulative end, so decode remains zero-copy while dropping redundant per-chunk fields. This is a codec break; old v16 sections are intentionally unsupported.

The comparable no-detail ReleaseFast run shows the RSS story is now mostly stable: final `ps` RSS is ~651-671 MiB, vmmap footprint is ~140-147 MiB, live malloc is ~84-97 MiB, and mapped-file resident is only ~30-31 MiB despite ~491-494 MiB of mapped segment bytes. Segment bytes also moved down from the previous ~528-532 MiB v3/no-eager-checksum run to ~491-494 MiB, with postings down from ~270 MiB to ~234-240 MiB. The remaining segment-size work is still real: term blocks are ~99-103 MiB, stored fields are ~127.6 MiB, postings headers are large, and block-max/chunk metadata still account for substantial bytes in the detailed probe.

## Latest v18 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260531-153844/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 39.109253125s
- Async catch-up time: 12.256525s
- Catch-up complete: true
- Throughput: 1,805.33 records/sec, 5.51 MiB/sec
- `ps` RSS: 788,725,760 bytes
- Process resident metric: 787,759,104 bytes
- Process footprint metric: 123,083,544 bytes
- Live malloc metric: 79,669,856 bytes
- Malloc zone metric: 154,959,872 bytes
- vmmap footprint: 124,046,540 bytes
- vmmap peak footprint: 741,238,374 bytes
- vmmap mapped-file resident: 31,666,995 bytes
- vmmap malloc allocated: 24,012,390 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 430,672,350
- Final mapped segment bytes: 430,672,350
- Max segment bytes: 134,859,783
- Stored fields bytes: 127,624,684
- Inverted bytes: 298,948,614
- Inverted header bytes: 121,968
- Inverted norms bytes: 10,252,155
- Inverted postings bytes: 171,837,052
- Inverted term dictionary bytes: 110,337,979
- Term block bytes: 104,617,317
- Term index bytes: 2,881,378
- Term FST bytes: 2,765,364
- Bloom bytes: 6,399,460
- Typed doc values bytes: 3,445,076
- Doc ordinal bytes: 282,460
- Section index bytes: 371,196
- Text merges completed: 144
- Full-text build peak bytes: 136,861,837
- Full-text pending peak bytes: 430,263,506
- Text merge buffer peak bytes: 133,713,039
- LSM compaction peak bytes: 68,460,648
- LSM in-memory state peak bytes: 21,118,110

## Latest v18 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260531-153844/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 40.067852084s
- Async catch-up wait: 5m0.200299291s
- Catch-up complete: false; full-text pending bytes were zero, but a 14,040-byte derived backlog kept the wait loop open
- Throughput: 1,762.14 records/sec, 5.38 MiB/sec
- `ps` RSS: 683,163,648 bytes
- Process resident metric: 683,409,408 bytes
- Process footprint metric: 119,606,776 bytes
- Live malloc metric: 93,763,520 bytes
- Malloc zone metric: 139,558,912 bytes
- vmmap footprint: 119,327,948 bytes
- vmmap peak footprint: 751,828,992 bytes
- vmmap mapped-file resident: 31,037,849 bytes
- vmmap malloc allocated: 34,603,008 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 429,247,307
- Final mapped segment bytes: 429,247,307
- Max segment bytes: 102,688,749
- Stored fields bytes: 127,638,562
- Inverted bytes: 297,499,802
- Inverted header bytes: 124,311
- Inverted norms bytes: 12,448,214
- Inverted postings bytes: 172,385,828
- Inverted term dictionary bytes: 106,571,237
- Term block bytes: 101,074,093
- Term index bytes: 2,760,930
- Term FST bytes: 2,660,874
- Bloom bytes: 5,970,212
- Typed doc values bytes: 3,446,061
- Doc ordinal bytes: 282,460
- Section index bytes: 380,102
- Text merges completed: 137
- Full-text build peak bytes: 149,473,482
- Full-text pending peak bytes: 428,351,067
- Text merge buffer peak bytes: 100,861,528
- LSM compaction peak bytes: 67,785,176
- LSM in-memory state peak bytes: 27,230,225

## v18 Read

The v18 codec removes the fixed 24-byte postings header and encodes the six small postings header fields as varints. This is another intentional codec break with no legacy read path. In the comparable no-detail ReleaseFast run, final segment bytes fell from the v17 range of ~491-494 MiB to ~429-431 MiB, and postings fell from ~234-240 MiB to ~172 MiB.

The RSS signal is not monotonic run-to-run: metrics-off ended at ~789 MiB RSS while metrics-on ended at ~683 MiB RSS. The lower-noise memory indicators are consistent with the previous slice: process footprint is ~120-123 MiB, live malloc is ~80-94 MiB, vmmap malloc allocated is ~24-35 MiB, and mapped-file resident is ~31 MiB. That means the old 2 GiB symptom is no longer explained by live heap, but there is still codec and policy work left in term blocks, stored fields, sparse block-max data, skip data, and merge/reader retirement.

## Latest v19 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260531-163822/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 42.247539708s
- Async catch-up time: 23.888125667s
- Catch-up complete: true
- Throughput: 1,671.22 records/sec, 5.10 MiB/sec
- `ps` RSS: 672,235,520 bytes
- Process resident metric: 672,235,520 bytes
- Process footprint metric: 139,729,568 bytes
- Live malloc metric: 97,712,720 bytes
- Malloc zone metric: 139,804,672 bytes
- vmmap footprint: 139,775,180 bytes
- vmmap peak footprint: 806,774,374 bytes
- vmmap mapped-file resident: 30,198,988 bytes
- vmmap malloc allocated: 24,012,390 bytes
- Final full-text segment files: 7
- Final full-text segment bytes: 425,319,572
- Final mapped segment bytes: 425,319,572
- Max segment bytes: 112,037,295
- Stored fields bytes: 127,749,388
- Inverted bytes: 293,498,980
- Inverted header bytes: 121,347
- Inverted norms bytes: 14,735,361
- Inverted postings bytes: 164,449,865
- Inverted term dictionary bytes: 107,911,212
- Term block bytes: 102,441,175
- Term index bytes: 2,761,706
- Term FST bytes: 2,641,971
- Bloom bytes: 6,171,195
- Typed doc values bytes: 3,446,657
- Doc ordinal bytes: 282,455
- Section index bytes: 341,812
- Text merges completed: 138
- Full-text build peak bytes: 166,429,970
- Full-text pending peak bytes: 430,327,541
- Text merge buffer peak bytes: 112,804,207
- LSM compaction peak bytes: 67,986,888
- LSM in-memory state peak bytes: 28,147,242

## Latest v19 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260531-163822/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 44.119132875s
- Async catch-up time: 16.795126375s
- Catch-up complete: true
- Throughput: 1,600.33 records/sec, 4.88 MiB/sec
- `ps` RSS: 646,397,952 bytes
- Process resident metric: 646,397,952 bytes
- Process footprint metric: 145,333,064 bytes
- Live malloc metric: 104,379,808 bytes
- Malloc zone metric: 138,133,504 bytes
- vmmap footprint: 146,066,636 bytes
- vmmap peak footprint: 774,268,518 bytes
- vmmap mapped-file resident: 30,198,988 bytes
- vmmap malloc allocated: 34,707,865 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 408,374,344
- Final mapped segment bytes: 408,374,344
- Max segment bytes: 99,685,519
- Stored fields bytes: 127,708,649
- Inverted bytes: 276,567,986
- Inverted header bytes: 130,870
- Inverted norms bytes: 11,126,861
- Inverted postings bytes: 159,397,490
- Inverted term dictionary bytes: 100,668,584
- Term block bytes: 95,422,427
- Term index bytes: 2,634,894
- Term FST bytes: 2,538,323
- Bloom bytes: 5,244,181
- Typed doc values bytes: 3,449,635
- Doc ordinal bytes: 282,460
- Section index bytes: 365,294
- Text merges completed: 156
- Full-text build peak bytes: 138,565,608
- Full-text pending peak bytes: 412,232,053
- Text merge buffer peak bytes: 101,708,220
- LSM compaction peak bytes: 67,379,736
- LSM in-memory state peak bytes: 32,662,179

## v19 Read

The v19 codec stores block-max records only for posting chunks that contain hits. It removes the dense `first_chunk_id + num_doc_chunks` header fields and aligns each 6-byte block-max record with the compact chunk metadata entry. `BlockMaxInfo.maxImpact` now returns zero for chunks absent from the postings chunk list.

Compared with the v18 baseline, v19 reduced final segment bytes from ~429-431 MiB to ~408-425 MiB and postings from ~172 MiB to ~159-164 MiB. The committed metrics-on run is the best apples-to-apples final state: RSS 646 MiB, footprint 145 MiB, live malloc 104 MiB, mapped-file resident 30 MiB, and final segment bytes 408 MiB. The remaining large on-disk components are stored fields (~127.7 MiB), term blocks (~95-102 MiB), and postings (~159-164 MiB).

## Latest v20 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260531-165023/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 40.963688167s
- Async catch-up time: 15.834332875s
- Catch-up complete: true
- Throughput: 1,723.60 records/sec, 5.26 MiB/sec
- `ps` RSS: 750,108,672 bytes
- Process resident metric: 750,108,672 bytes
- Process footprint metric: 120,167,096 bytes
- Live malloc metric: 87,823,056 bytes
- Malloc zone metric: 146,882,560 bytes
- vmmap footprint: 120,166,809 bytes
- vmmap peak footprint: 834,247,065 bytes
- vmmap mapped-file resident: 30,932,992 bytes
- vmmap malloc allocated: 24,326,963 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 422,375,811
- Final mapped segment bytes: 422,375,811
- Max segment bytes: 142,677,080
- Stored fields bytes: 127,826,216
- Inverted bytes: 290,464,462
- Inverted header bytes: 114,378
- Inverted norms bytes: 11,660,539
- Inverted postings bytes: 164,406,911
- Inverted term dictionary bytes: 107,807,270
- Term block bytes: 102,335,005
- Term index bytes: 2,760,822
- Term FST bytes: 2,642,123
- Bloom bytes: 6,475,364
- Typed doc values bytes: 3,448,452
- Doc ordinal bytes: 282,460
- Section index bytes: 353,901
- Text merges completed: 138
- Full-text build peak bytes: 168,161,365
- Full-text pending peak bytes: 426,531,368
- Text merge buffer peak bytes: 141,617,453
- LSM compaction peak bytes: 67,854,840
- LSM in-memory state peak bytes: 30,797,518

## Latest v20 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260531-165023/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 43.950323209s
- Async catch-up time: 18.380947833s
- Catch-up complete: true
- Throughput: 1,606.47 records/sec, 4.90 MiB/sec
- `ps` RSS: 625,737,728 bytes
- Process resident metric: 625,737,728 bytes
- Process footprint metric: 133,143,176 bytes
- Live malloc metric: 86,033,136 bytes
- Malloc zone metric: 137,822,208 bytes
- vmmap footprint: 133,169,152 bytes
- vmmap peak footprint: 750,570,700 bytes
- vmmap mapped-file resident: 30,513,561 bytes
- vmmap malloc allocated: 23,278,387 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 430,172,164
- Final mapped segment bytes: 430,172,164
- Max segment bytes: 134,725,738
- Stored fields bytes: 127,649,232
- Inverted bytes: 298,433,973
- Inverted header bytes: 117,084
- Inverted norms bytes: 11,562,392
- Inverted postings bytes: 166,685,030
- Inverted term dictionary bytes: 113,726,459
- Term block bytes: 107,955,444
- Term index bytes: 2,912,120
- Term FST bytes: 2,787,935
- Bloom bytes: 6,343,008
- Typed doc values bytes: 3,445,666
- Doc ordinal bytes: 282,460
- Section index bytes: 360,513
- Text merges completed: 149
- Full-text build peak bytes: 149,952,960
- Full-text pending peak bytes: 432,669,698
- Text merge buffer peak bytes: 136,904,324
- LSM compaction peak bytes: 67,312,032
- LSM in-memory state peak bytes: 19,961,332

## v20 Read

The v20 codec tags in-block term dictionary values as `(postings_offset << 1)` or `(doc_num << 1) | 1` for one-hit terms, so one-hit dictionary values no longer serialize as 10-byte high-bit `u64` varints. This is a clean codec break (`BTD4`) and leaves the external lookup representation unchanged.

The DO8018 baseline did not show a segment-size win from this change. Final segment bytes landed at ~422-430 MiB and term blocks at ~102-108 MiB, versus v19's ~408-425 MiB and ~95-102 MiB. Because merge composition varies between runs and detailed per-term layout was not enabled for this baseline, the strongest conclusion is that DO8018 is not dominated by one-hit term-block value bytes. The next term-dictionary work should target suffix bytes/block layout directly, or move to stored fields/postings, rather than assuming one-hit value varints are the root cause.

## Latest v21 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260531-210218/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 40.690677916s
- Async catch-up time: 15.248238584s
- Catch-up complete: true
- Throughput: 1,735.16 records/sec, 5.29 MiB/sec
- `ps` RSS: 693,485,568 bytes
- Process resident metric: 692,797,440 bytes
- Process footprint metric: 136,321,648 bytes
- Live malloc metric: 85,407,680 bytes
- Malloc zone metric: 147,881,984 bytes
- vmmap footprint: 137,048,883 bytes
- vmmap peak footprint: 806,459,801 bytes
- vmmap mapped-file resident: 30,723,276 bytes
- vmmap malloc allocated: 34,498,150 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 404,987,816
- Final mapped segment bytes: 404,987,816
- Max segment bytes: 108,481,232
- Stored fields bytes: 127,547,289
- Inverted bytes: 273,345,102
- Inverted header bytes: 120,384
- Inverted norms bytes: 11,587,473
- Inverted postings bytes: 144,876,901
- Inverted term dictionary bytes: 110,467,056
- Term block bytes: 104,841,668
- Term index bytes: 2,834,146
- Term FST bytes: 2,718,282
- Bloom bytes: 6,293,288
- Typed doc values bytes: 3,443,907
- Doc ordinal bytes: 282,460
- Section index bytes: 368,738
- Text merges completed: 141
- Full-text build peak bytes: 160,670,143
- Full-text pending peak bytes: 408,399,978
- Text merge buffer peak bytes: 107,789,322
- LSM compaction peak bytes: 67,514,976
- LSM in-memory state peak bytes: 18,617,873

## Latest v21 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260531-210218/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 43.521292084s
- Async catch-up time: 5m0.435123666s
- Catch-up complete: false; full-text pending bytes were zero, but a 7,020-byte derived backlog kept the wait loop open
- Throughput: 1,622.31 records/sec, 4.95 MiB/sec
- `ps` RSS: 713,506,816 bytes
- Process resident metric: 713,998,336 bytes
- Process footprint metric: 132,451,784 bytes
- Live malloc metric: 104,996,256 bytes
- Malloc zone metric: 140,853,248 bytes
- vmmap footprint: 131,806,003 bytes
- vmmap peak footprint: 853,121,433 bytes
- vmmap mapped-file resident: 31,037,849 bytes
- vmmap malloc allocated: 36,175,872 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 411,450,970
- Final mapped segment bytes: 411,450,970
- Max segment bytes: 95,603,417
- Stored fields bytes: 127,725,267
- Inverted bytes: 279,619,433
- Inverted header bytes: 123,552
- Inverted norms bytes: 12,565,644
- Inverted postings bytes: 146,348,260
- Inverted term dictionary bytes: 114,279,809
- Term block bytes: 108,439,746
- Term index bytes: 2,941,467
- Term FST bytes: 2,823,716
- Bloom bytes: 6,302,168
- Typed doc values bytes: 3,446,940
- Doc ordinal bytes: 282,460
- Section index bytes: 376,550
- Text merges completed: 150
- Full-text build peak bytes: 169,764,348
- Full-text pending peak bytes: 413,878,424
- Text merge buffer peak bytes: 95,627,698
- LSM compaction peak bytes: 68,123,640
- LSM in-memory state peak bytes: 19,664,254

## v21 Read

The v21 codec replaces fixed 12-byte postings chunk metadata records with bit-packed per-term metadata columns: chunk-id deltas, max-doc offsets, doc counts, and payload-end deltas. This is a clean codec break and keeps the query-facing chunk metadata semantics unchanged by decoding the compact columns into iterator-owned scratch.

The DO8018 baseline shows the intended byte reduction. Compared with v20, final segment bytes moved from ~422-430 MiB to ~405-411 MiB, and postings moved from ~164-167 MiB to ~145-146 MiB. RSS remains a noisy process-level number, while footprint is still ~132-137 MiB, live malloc is ~85-105 MiB, and mapped-file resident remains ~31 MiB. The remaining large full-text byte targets are now stored fields (~127.5 MiB), term blocks (~105-108 MiB), and the remaining postings payload (~145-146 MiB).

## Corrected v21 Metrics Off

Artifacts: `work-log/do8018/releasefast-baseline-20260531-213429/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 47.359518583s
- Async catch-up time: 18.287606458s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,490.83 records/sec, 4.55 MiB/sec
- `ps` RSS: 681,263,104 bytes
- Process resident metric: 681,263,104 bytes
- Process footprint metric: 120,232,632 bytes
- Live malloc metric: 96,003,056 bytes
- Malloc zone metric: 139,870,208 bytes
- vmmap footprint: 120,271,667 bytes
- vmmap peak footprint: 801,846,067 bytes
- vmmap mapped-file resident: 29,360,128 bytes
- vmmap malloc allocated: 35,861,299 bytes
- Final full-text segment files: 7
- Final full-text segment bytes: 396,343,271
- Final mapped segment bytes: 396,343,271
- Max segment bytes: 102,593,154
- Stored fields bytes: 127,798,681
- Inverted bytes: 264,468,040
- Inverted header bytes: 112,596
- Inverted norms bytes: 11,439,429
- Inverted postings bytes: 141,906,786
- Inverted term dictionary bytes: 104,954,377
- Term block bytes: 99,571,314
- Term index bytes: 2,711,912
- Term FST bytes: 2,602,911
- Bloom bytes: 6,054,852
- Typed doc values bytes: 3,446,299
- Doc ordinal bytes: 282,455
- Section index bytes: 347,516
- Text merges completed: 158
- Full-text build peak bytes: 163,933,004
- Full-text pending peak bytes: 397,010,478
- Text merge buffer peak bytes: 102,574,135
- LSM compaction peak bytes: 67,715,568
- LSM in-memory state peak bytes: 28,652,737

## Corrected v21 Metrics On

Artifacts: `work-log/do8018/releasefast-baseline-20260531-213429/`.

- Records: 70,605
- Input bytes: 225,883,530
- Payload bytes: 246,859,144
- Load time: 45.210656833s
- Async catch-up time: 10.671201s
- Catch-up complete: true, `scope=full-text`
- Final full-text pending bytes: 0
- Final derived backlog bytes: 0
- Final text merge buffer bytes: 0
- Throughput: 1,561.69 records/sec, 4.76 MiB/sec
- `ps` RSS: 681,836,544 bytes
- Process resident metric: 681,836,544 bytes
- Process footprint metric: 146,594,680 bytes
- Live malloc metric: 89,025,024 bytes
- Malloc zone metric: 138,362,880 bytes
- vmmap footprint: 146,590,924 bytes
- vmmap peak footprint: 821,978,726 bytes
- vmmap mapped-file resident: 29,989,273 bytes
- vmmap malloc allocated: 23,802,675 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 403,812,594
- Final mapped segment bytes: 403,812,594
- Max segment bytes: 85,934,862
- Stored fields bytes: 127,756,276
- Inverted bytes: 271,986,979
- Inverted header bytes: 110,154
- Inverted norms bytes: 10,947,875
- Inverted postings bytes: 146,531,243
- Inverted term dictionary bytes: 108,435,779
- Term block bytes: 102,954,927
- Term index bytes: 2,768,655
- Term FST bytes: 2,645,437
- Bloom bytes: 5,961,928
- Typed doc values bytes: 3,446,141
- Doc ordinal bytes: 282,460
- Section index bytes: 340,418
- Text merges completed: 148
- Full-text build peak bytes: 162,029,947
- Full-text pending peak bytes: 412,213,600
- Text merge buffer peak bytes: 85,716,342
- LSM compaction peak bytes: 67,446,096
- LSM in-memory state peak bytes: 20,724,659

## Corrected v21 Read

The corrected baseline confirms the earlier 300s metrics-on catch-up ceiling was not full-text merge/index work. With the benchmark wait gate scoped to full-text readiness and idle derived watermark persistence retrying, metrics-on catch-up completed in 10.67s with final `derived_backlog_bytes=0`.

The first-class memory targets remain stable: final RSS was ~681 MiB, but footprint was ~120-147 MiB, live malloc was ~89-96 MiB, and mapped-file resident was ~29-30 MiB. Final full-text segment bytes are now ~396-404 MiB. The remaining byte targets are still stored fields (~127.8 MiB), term blocks (~99.6-103.0 MiB), and postings (~141.9-146.5 MiB).

## Corrected v21 Repeat

Artifacts: `work-log/do8018/releasefast-baseline-20260531-215403/`.

This repeat was run to check the apparent timing slowdown after the derived-backlog fix. It does not prove a clean regression. Metrics-off landed between the prior v21 and first corrected run, while metrics-on was slower and also did substantially more merge work.

Metrics off:

- Load time: 44.424158833s
- Async catch-up time: 16.770946458s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,589.34 records/sec, 4.85 MiB/sec
- `ps` RSS: 676,069,376 bytes
- Peak sampled RSS: 1,120,239,616 bytes
- Process footprint metric: 133,602,288 bytes
- Peak sampled process footprint: 323,443,144 bytes
- Live malloc metric: 106,699,728 bytes
- Peak sampled live malloc: 280,740,192 bytes
- vmmap footprint: 133,588,582 bytes
- vmmap peak footprint: 711,039,385 bytes
- vmmap mapped-file resident: 30,723,276 bytes
- vmmap malloc allocated: 34,917,580 bytes
- Segment files: 8
- Segment bytes: 403,657,736
- Peak sampled mapped segment bytes: 405,155,421
- Stored fields bytes: 127,611,966
- Inverted bytes: 271,951,779
- Postings bytes: 145,145,088
- Term block bytes: 103,880,808
- Text merges completed: 160
- Full-text pending peak bytes: 411,377,884
- Text merge buffer peak bytes: 108,878,579

Metrics on:

- Load time: 47.850299666s
- Async catch-up time: 13.265462875s
- Catch-up complete: true, `scope=full-text`
- Final derived backlog bytes: 0
- Throughput: 1,475.54 records/sec, 4.50 MiB/sec
- `ps` RSS: 708,673,536 bytes
- Peak sampled RSS: 1,070,137,344 bytes
- Process footprint metric: 138,566,664 bytes
- Peak sampled process footprint: 295,164,432 bytes
- Live malloc metric: 105,499,744 bytes
- Peak sampled live malloc: 245,986,016 bytes
- vmmap footprint: 139,250,892 bytes
- vmmap peak footprint: 834,666,496 bytes
- vmmap mapped-file resident: 30,303,846 bytes
- vmmap malloc allocated: 36,595,302 bytes
- Segment files: 8
- Segment bytes: 409,536,277
- Peak sampled mapped segment bytes: 409,863,131
- Stored fields bytes: 127,639,967
- Inverted bytes: 277,817,286
- Postings bytes: 146,597,444
- Term block bytes: 108,210,054
- Text merges completed: 178
- Full-text pending peak bytes: 409,994,017
- Text merge buffer peak bytes: 90,641,284

Timing read:

- Prior v21 metrics-off load/catch-up was 40.69s/15.25s with 141 merges.
- First corrected metrics-off load/catch-up was 47.36s/18.29s with 158 merges.
- Repeat corrected metrics-off load/catch-up was 44.42s/16.77s with 160 merges.
- First corrected metrics-on load/catch-up was 45.21s/10.67s with 148 merges.
- Repeat corrected metrics-on load/catch-up was 47.85s/13.27s with 178 merges.

The derived-backlog fix itself is an idle-watermark retry path, so it is not expected to affect hot foreground writes materially. The observed timing spread tracks merge composition closely enough that the current evidence should be treated as run noise plus merge-scheduler variability, not a confirmed regression.

## Post Active-Mmap-Advice Repeat

Artifacts: `work-log/do8018/releasefast-baseline-20260531-220831/`.

This repeat was run from commit `1786a764e2f0` after changing snapshot publish behavior so active mmap-backed segments stay warm and only retired merge source pages receive `MADV_DONTNEED`. The result does not show a throughput regression from that change. Metrics-off remained in the same band as the corrected repeat; metrics-on was faster than the previous corrected repeat and did less merge work.

Metrics off:

- Load time: 44.273372791s
- Async catch-up time: 16.762287292s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,594.75 records/sec, 4.87 MiB/sec
- `ps` RSS: 699,662,336 bytes
- Peak sampled RSS: 942,096,384 bytes
- Process footprint metric: 131,324,672 bytes
- Peak sampled process footprint: 339,466,840 bytes
- Live malloc metric: 100,188,928 bytes
- Peak sampled live malloc: 269,564,016 bytes
- vmmap footprint: 131,281,715 bytes
- vmmap peak footprint: 879,860,121 bytes
- vmmap mapped-file resident: 29,674,700 bytes
- vmmap malloc allocated: 34,603,008 bytes
- Segment files: 8
- Segment bytes: 401,354,725
- Peak sampled segment bytes: 420,217,837
- Peak sampled mapped segment bytes: 401,892,045
- Stored fields bytes: 127,662,307
- Inverted bytes: 269,593,746
- Postings bytes: 143,744,556
- Term block bytes: 103,430,710
- Text merges completed: 159
- Full-text build peak bytes: 180,921,046
- Full-text pending peak bytes: 403,213,585
- Text merge buffer peak bytes: 139,500,050
- LSM compaction peak bytes: 68,054,256
- LSM state peak bytes: 24,719,380

Metrics on:

- Load time: 41.982961917s
- Async catch-up time: 14.220626916s
- Catch-up complete: true, `scope=full-text`
- Final derived backlog bytes: 0
- Throughput: 1,681.75 records/sec, 5.13 MiB/sec
- `ps` RSS: 732,413,952 bytes
- Peak sampled RSS: 1,058,766,848 bytes
- Process footprint metric: 154,393,320 bytes
- Peak sampled process footprint: 286,366,152 bytes
- Live malloc metric: 93,664,656 bytes
- Peak sampled live malloc: 288,753,696 bytes
- vmmap footprint: 155,084,390 bytes
- vmmap peak footprint: 865,704,345 bytes
- vmmap mapped-file resident: 30,198,988 bytes
- vmmap malloc allocated: 34,707,865 bytes
- Segment files: 7
- Segment bytes: 408,789,811
- Peak sampled segment bytes: 453,026,812
- Peak sampled mapped segment bytes: 411,198,285
- Stored fields bytes: 127,652,968
- Inverted bytes: 277,041,547
- Postings bytes: 147,045,092
- Term block bytes: 106,940,830
- Text merges completed: 144
- Full-text build peak bytes: 170,924,528
- Full-text pending peak bytes: 411,205,364
- Text merge buffer peak bytes: 94,487,820
- LSM compaction peak bytes: 67,311,864
- LSM state peak bytes: 30,598,832

Timing read:

- Corrected repeat metrics-off load/catch-up was 44.42s/16.77s with 160 merges.
- Post active-mmap-advice metrics-off load/catch-up was 44.27s/16.76s with 159 merges.
- Corrected repeat metrics-on load/catch-up was 47.85s/13.27s with 178 merges.
- Post active-mmap-advice metrics-on load/catch-up was 41.98s/14.22s with 144 merges.

RSS moved as expected for warmer active mappings, but the first-class memory targets stayed low: final footprint was ~131-155 MiB, final live malloc was ~94-100 MiB, and mapped-file resident was ~30 MiB. Final segment bytes remained ~401-409 MiB.

## Post LSM Snapshot-Clone Diagnostics Repeat

Artifacts: `work-log/do8018/releasefast-baseline-20260601-071337/`.

This repeat was run from commit `8ff42be91b9b` after adding LSM mutable snapshot clone counters to the DO8018 bench summary and resource logs. The run confirms that the maintenance clone path is now visible and is not the large RSS driver for this workload: clone totals were ~10.6-12.2 MiB across the full run, with ~1.0 MiB peak clone size.

Metrics off:

- Load time: 42.010433875s
- Async catch-up time: 13.233531292s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,680.65 records/sec, 5.13 MiB/sec
- `ps` RSS: 725,762,048 bytes
- Peak sampled RSS: 1,004,224,512 bytes
- Process footprint metric: 136,747,776 bytes
- Peak sampled process footprint: 278,894,976 bytes
- Live malloc metric: 84,162,736 bytes
- Peak sampled live malloc: 163,950,368 bytes
- vmmap footprint: 137,468,313 bytes
- vmmap peak footprint: 835,190,784 bytes
- vmmap mapped-file resident: 30,513,561 bytes
- vmmap malloc allocated: 23,802,675 bytes
- Segment files: 8
- Segment bytes: 406,527,856
- Stored fields bytes: 127,729,820
- Inverted bytes: 274,682,003
- Postings bytes: 146,862,612
- Term block bytes: 104,141,522
- Text merges completed: 148
- Full-text build peak bytes: 168,016,832
- Full-text pending peak bytes: 412,875,355
- Text merge buffer peak bytes: 183,352,759
- LSM compaction peak bytes: 67,649,376
- LSM state peak bytes: 19,187,537
- LSM mutable snapshot clone calls: 26
- LSM mutable snapshot clone bytes total: 10,592,753
- LSM mutable snapshot clone peak bytes: 1,035,811

Metrics on:

- Load time: 41.067259875s
- Async catch-up time: 14.407353792s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,719.25 records/sec, 5.25 MiB/sec
- `ps` RSS: 709,459,968 bytes
- Peak sampled RSS: 889,356,288 bytes
- Process footprint metric: 129,833,560 bytes
- Peak sampled process footprint: 268,048,288 bytes
- Live malloc metric: 84,615,744 bytes
- Peak sampled live malloc: 189,305,776 bytes
- vmmap footprint: 129,708,851 bytes
- vmmap peak footprint: 800,797,491 bytes
- vmmap mapped-file resident: 30,828,134 bytes
- vmmap malloc allocated: 24,956,108 bytes
- Segment files: 8
- Segment bytes: 393,482,410
- Stored fields bytes: 127,567,965
- Inverted bytes: 261,837,073
- Postings bytes: 140,140,869
- Term block bytes: 97,528,401
- Text merges completed: 140
- Full-text build peak bytes: 147,964,273
- Full-text pending peak bytes: 404,839,005
- Text merge buffer peak bytes: 106,396,247
- LSM compaction peak bytes: 67,852,152
- LSM state peak bytes: 28,477,592
- LSM mutable snapshot clone calls: 23
- LSM mutable snapshot clone bytes total: 12,215,835
- LSM mutable snapshot clone peak bytes: 1,016,963

Timing read:

- Post active-mmap-advice metrics-off load/catch-up was 44.27s/16.76s with 159 merges.
- Post snapshot-clone-diagnostics metrics-off load/catch-up was 42.01s/13.23s with 148 merges.
- Post active-mmap-advice metrics-on load/catch-up was 41.98s/14.22s with 144 merges.
- Post snapshot-clone-diagnostics metrics-on load/catch-up was 41.07s/14.41s with 140 merges.

The latest repeat does not show a slowdown from the diagnostics wiring. The broader slowdown versus the earlier ~35s best runs remains real enough to investigate, but this repeat points at merge/codec/write-path composition rather than metrics overhead. RSS is still mostly not live heap: final footprint is ~130-137 MiB, final live malloc is ~84-85 MiB, vmmap mapped-file resident is ~30-31 MiB, and final segment bytes are ~393-407 MiB.

## Post Text Accounting Optimization Baseline

Artifacts: `work-log/do8018/releasefast-baseline-20260601-075951/`.

This run was taken after removing repeated full postings/typed-value memory estimation from the per-field inverted-section build loop and adding an `inverted_build_ms` timing bucket. The prior run with text subphase timing enabled (`work-log/do8018/releasefast-baseline-20260601-074307/`) showed metrics-on load/catch-up of 45.996931583s/20.473509833s, total 1m6.470504333s. The post-change metrics-on run was 43.667769958s/12.209899375s, total 55.877737125s. That puts metrics-on back near metrics-off for this sample.

Metrics off:

- Load time: 44.402277625s
- Async catch-up time: 11.18264475s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,590.12 records/sec, 4.85 MiB/sec
- `ps` RSS: 721,780,736 bytes
- Peak sampled RSS: 1,161,281,536 bytes
- Process footprint metric: 133,765,984 bytes
- Peak sampled process footprint: 511,531,848 bytes
- Live malloc metric: 82,388,560 bytes
- Peak sampled live malloc: 216,244,384 bytes
- vmmap footprint: 133,798,297 bytes
- vmmap mapped-file resident: 30,932,992 bytes
- vmmap malloc allocated: 23,278,387 bytes
- Segment files: 8
- Segment bytes: 394,938,742
- Stored fields bytes: 127,671,754
- Inverted bytes: 263,187,407
- Postings bytes: 140,903,136
- Term block bytes: 98,281,260
- Text merges completed: 154
- Full-text build peak bytes: 165,443,397
- Full-text pending peak bytes: 400,955,177
- Text merge buffer peak bytes: 136,610,675
- LSM mutable snapshot clone calls: 31
- LSM mutable snapshot clone bytes total: 10,066,973
- LSM mutable snapshot clone peak bytes: 972,937

Metrics on:

- Load time: 43.667769958s
- Async catch-up time: 12.209899375s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,616.87 records/sec, 4.93 MiB/sec
- `ps` RSS: 672,546,816 bytes
- Peak sampled RSS: 1,196,015,616 bytes
- Process footprint metric: 119,151,576 bytes
- Peak sampled process footprint: 489,314,832 bytes
- Live malloc metric: 91,068,656 bytes
- Peak sampled live malloc: 217,804,992 bytes
- vmmap footprint: 119,118,233 bytes
- vmmap mapped-file resident: 30,198,988 bytes
- vmmap malloc allocated: 23,383,244 bytes
- Segment files: 8
- Segment bytes: 388,036,949
- Stored fields bytes: 127,570,705
- Inverted bytes: 256,362,319
- Postings bytes: 139,946,130
- Term block bytes: 95,072,489
- Text merges completed: 157
- Full-text build peak bytes: 165,571,728
- Full-text pending peak bytes: 391,273,807
- Text merge buffer peak bytes: 92,508,508
- LSM mutable snapshot clone calls: 23
- LSM mutable snapshot clone bytes total: 8,747,644
- LSM mutable snapshot clone peak bytes: 993,399

Metrics-on text timing aggregate:

- Timing lines: 167
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 33,155 ms
- Segment build timing: 33,137 ms
- Segment encode timing: 13,288 ms
- Inverted section build timing: 11,522 ms
- Section attach timing from this run still used the old inclusive label and was 11,604 ms; the code now records attach separately, so the next run should show actual attachment overhead.
- Segment assembly timing: 1,607 ms
- Stored compression timing: 525 ms
- Analyzer timing: 3,526 ms
- Term accumulation timing: 2,572 ms
- Hit materialization timing: 2,887 ms

Interpretation: the metrics-on slowdown in the previous sample was not stable. This change removed avoidable O(fields) accounting from the field-section loop, and the next dominant CPU target is the inverted codec builder itself: sorting terms, serializing postings, and building the blocked term dictionary/FST payload. RSS remains mostly mapped segment/file-cache accounting rather than live heap; final footprint stayed ~119-134 MiB and final live malloc stayed ~82-91 MiB.

## Post Inverted Codec Subphase Baseline

Artifacts: `work-log/do8018/releasefast-baseline-20260601-082303/`.

This run was taken after adding optional inverted-codec subphase timing and pre-sizing the blocked term-dictionary buffers. The metrics-off run improved in this sample, but the metrics-on catch-up time remained noisy. The important new evidence is the inverted subphase split: term dictionary construction is now the clear codec CPU target.

Metrics off:

- Load time: 43.183520458s
- Async catch-up time: 8.148286333s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,635.00 records/sec, 4.99 MiB/sec
- `ps` RSS: 651,935,744 bytes
- Peak sampled RSS: 1,038,188,544 bytes
- Process footprint metric: 138,074,976 bytes
- Peak sampled process footprint: 582,098,048 bytes
- Live malloc metric: 71,971,376 bytes
- Peak sampled live malloc: 233,092,432 bytes
- vmmap footprint: 138,097,459 bytes
- vmmap mapped-file resident: 31,247,564 bytes
- vmmap malloc allocated: 24,222,105 bytes
- Segment files: 8
- Segment bytes: 410,294,306
- Stored fields bytes: 127,631,488
- Inverted bytes: 278,556,559
- Postings bytes: 144,387,641
- Term block bytes: 106,606,806
- Text merges completed: 161
- Full-text build peak bytes: 163,838,968
- Full-text pending peak bytes: 410,479,403
- Text merge buffer peak bytes: 141,231,973
- LSM mutable snapshot clone calls: 33
- LSM mutable snapshot clone bytes total: 12,970,888
- LSM mutable snapshot clone peak bytes: 1,020,005

Metrics on:

- Load time: 43.5860295s
- Async catch-up time: 18.840851208s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,619.90 records/sec, 4.94 MiB/sec
- `ps` RSS: 649,248,768 bytes
- Peak sampled RSS: 1,020,526,592 bytes
- Process footprint metric: 125,164,216 bytes
- Peak sampled process footprint: 419,699,528 bytes
- Live malloc metric: 78,245,472 bytes
- Peak sampled live malloc: 286,103,360 bytes
- vmmap footprint: 125,199,974 bytes
- vmmap mapped-file resident: 30,723,276 bytes
- vmmap malloc allocated: 23,592,960 bytes
- Segment files: 8
- Segment bytes: 401,017,085
- Stored fields bytes: 127,539,604
- Inverted bytes: 269,370,474
- Postings bytes: 144,373,273
- Term block bytes: 101,577,520
- Text merges completed: 154
- Full-text build peak bytes: 145,439,252
- Full-text pending peak bytes: 407,101,934
- Text merge buffer peak bytes: 121,362,798
- LSM mutable snapshot clone calls: 24
- LSM mutable snapshot clone bytes total: 9,425,863
- LSM mutable snapshot clone peak bytes: 1,035,853

Metrics-on text timing aggregate:

- Timing lines: 167
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 31,777 ms
- Segment build timing: 31,765 ms
- Segment encode timing: 12,603 ms
- Inverted build timing: 10,987 ms
- Inverted term dictionary timing: 7,525 ms
- Inverted postings serialization timing: 2,164 ms
- Inverted sort timing: 578 ms
- Inverted final assembly timing: 299 ms
- Inverted norms timing: 2 ms
- Inverted bloom finish timing: 1 ms
- Section attach timing: 0 ms
- Segment assembly timing: 1,456 ms
- Stored compression timing: 474 ms
- Analyzer timing: 3,427 ms
- Term accumulation timing: 2,287 ms
- Hit materialization timing: 3,069 ms

Interpretation: the term-dictionary block data remains both large and CPU-heavy. The binary block index and FST are only ~2.8 MiB and ~2.7 MiB respectively in this run; the large component is the term block payload itself (~101.6 MiB metrics-on). The next Lucene-shaped codec work should target term-block payload shape and term statistics, not section attachment or stored-field compression.

## Post Term-Block Value Delta Baseline

Artifacts: `work-log/do8018/releasefast-baseline-20260601-084227/`.

This run was taken after a codec break to v22: term dictionary blocks now delta-code postings offsets within each block while preserving the one-hit tag encoding. This is a Lucene-shaped change because postings pointers are monotonic in sorted term order; storing deltas reduces term-block payload bytes without changing lookup semantics.

Metrics off:

- Load time: 44.817584208s
- Async catch-up time: 11.192383458s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,575.39 records/sec, 4.81 MiB/sec
- `ps` RSS: 618,151,936 bytes
- Peak sampled RSS: 1,227,423,744 bytes
- Process footprint metric: 145,120,528 bytes
- Peak sampled process footprint: 693,492,816 bytes
- Live malloc metric: 75,224,144 bytes
- Peak sampled live malloc: 229,571,152 bytes
- vmmap footprint: 145,856,921 bytes
- vmmap mapped-file resident: 31,562,137 bytes
- vmmap malloc allocated: 23,383,244 bytes
- Segment files: 7
- Segment bytes: 411,602,061
- Stored fields bytes: 127,782,043
- Inverted bytes: 279,725,467
- Postings bytes: 150,176,393
- Term block bytes: 102,639,384
- Text merges completed: 170
- Full-text build peak bytes: 147,089,571
- Full-text pending peak bytes: 411,763,010
- Text merge buffer peak bytes: 104,682,030
- LSM mutable snapshot clone calls: 30
- LSM mutable snapshot clone bytes total: 10,200,151
- LSM mutable snapshot clone peak bytes: 794,275

Metrics on:

- Load time: 41.939514875s
- Async catch-up time: 14.240694583s
- Catch-up complete: true, `scope=full-text`
- Throughput: 1,683.50 records/sec, 5.14 MiB/sec
- `ps` RSS: 645,251,072 bytes
- Peak sampled RSS: 1,024,671,744 bytes
- Process footprint metric: 137,337,744 bytes
- Peak sampled process footprint: 370,842,344 bytes
- Live malloc metric: 86,252,784 bytes
- Peak sampled live malloc: 267,335,136 bytes
- vmmap footprint: 137,992,601 bytes
- vmmap mapped-file resident: 30,723,276 bytes
- vmmap malloc allocated: 23,383,244 bytes
- Segment files: 7
- Segment bytes: 402,712,728
- Stored fields bytes: 127,717,987
- Inverted bytes: 270,907,915
- Postings bytes: 147,339,803
- Term block bytes: 96,722,169
- Text merges completed: 152
- Full-text build peak bytes: 168,786,705
- Full-text pending peak bytes: 402,575,115
- Text merge buffer peak bytes: 115,747,191
- LSM mutable snapshot clone calls: 16
- LSM mutable snapshot clone bytes total: 5,925,370
- LSM mutable snapshot clone peak bytes: 1,022,395

Metrics-on text timing aggregate:

- Timing lines: 168
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 30,449 ms
- Segment build timing: 30,438 ms
- Segment encode timing: 12,288 ms
- Inverted build timing: 10,670 ms
- Inverted term dictionary timing: 7,277 ms
- Inverted postings serialization timing: 2,157 ms
- Inverted sort timing: 602 ms
- Inverted final assembly timing: 226 ms
- Inverted norms timing: 3 ms
- Inverted bloom finish timing: 0 ms
- Section attach timing: 0 ms
- Segment assembly timing: 1,458 ms
- Stored compression timing: 480 ms
- Analyzer timing: 3,253 ms
- Term accumulation timing: 2,193 ms
- Hit materialization timing: 2,881 ms

Interpretation: v22 reduced metrics-on term block bytes from the prior 101,577,520 bytes to 96,722,169 bytes, a ~4.8 MiB reduction in this run. It also moved term-dictionary build timing from 7,525 ms to 7,277 ms, though timing remains noisy. The next dictionary-size work should target repeated term text itself and per-entry varint framing inside term blocks; the value stream is no longer the only obvious waste.

## Post Front-Coded Term Block Baseline

Run: `work-log/do8018/releasefast-baseline-20260601-090240`
Commit: `36eadde9db55`

Metrics off:

- Load time: 45.612584833s
- Async catch-up time: 10.189278791s
- Total elapsed: 55.801927542s
- Throughput: 1,547.93 records/sec, 4.72 MiB/sec
- `ps` RSS: 634,961,920 bytes
- Peak sampled RSS: 1,090,387,968 bytes
- Process footprint metric: 120,200,152 bytes
- Peak sampled process footprint: 695,884,688 bytes
- Live malloc metric: 78,468,576 bytes
- Peak sampled live malloc: 245,306,240 bytes
- Segment files: 8
- Segment bytes: 392,182,126
- Stored fields bytes: 127,622,654
- Inverted bytes: 260,489,030
- Postings bytes: 144,981,053
- Term block bytes: 93,353,740
- Text merges completed: 161
- Full-text build peak bytes: 164,634,188
- Full-text pending peak bytes: 403,316,076
- Text merge buffer peak bytes: 87,765,162

Metrics on:

- Load time: 47.294567458s
- Async catch-up time: 13.724819708s
- Total elapsed: 1m1.019452417s
- Throughput: 1,492.88 records/sec, 4.55 MiB/sec
- `ps` RSS: 611,368,960 bytes
- Peak sampled RSS: 930,430,976 bytes
- Process footprint metric: 114,318,200 bytes
- Peak sampled process footprint: 591,010,920 bytes
- Live malloc metric: 88,928,048 bytes
- Peak sampled live malloc: 251,188,704 bytes
- Segment files: 8
- Segment bytes: 395,727,382
- Peak full-text segment bytes: 394,771,859
- Text merges completed: 168
- Full-text build peak bytes: 169,324,343
- Full-text pending peak bytes: 397,264,046
- Text merge buffer peak bytes: 78,884,848

Metrics-on text timing aggregate:

- Timing lines: 181
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 34,032 ms
- Segment build timing: 34,018 ms
- Segment encode timing: 13,622 ms
- Inverted build timing: 11,832 ms
- Inverted term dictionary timing: 8,118 ms
- Inverted postings serialization timing: 2,416 ms
- Inverted sort timing: 602 ms
- Inverted final assembly timing: 260 ms
- Segment assembly timing: 1,618 ms
- Stored compression timing: 491 ms
- Analyzer timing: 3,617 ms
- Term accumulation timing: 2,662 ms
- Hit materialization timing: 3,141 ms

Interpretation: front-coded term blocks reduced metrics-off term block bytes from 102,639,384 to 93,353,740 bytes and segment bytes from 411,602,061 to 392,182,126 bytes. Metrics-off total elapsed was effectively flat at ~56 seconds, but metrics-on got slower versus the v22 baseline: total elapsed moved from 56.180276333s to 1m1.019452417s and metrics-on text build timing moved from 30,449 ms to 34,032 ms. The slowdown is larger than ordinary noise in the instrumented path and is concentrated in the term dictionary/inverted encoding work. Next work should keep the compact format but remove extra encoder overhead, likely by avoiding per-entry suffix comparison churn and reusing block scratch when writing/looking up front-coded terms.

## Post Word-At-A-Time Prefix Matcher Baseline

Run: `work-log/do8018/releasefast-baseline-20260601-092024`

Change under test: `commonPrefixLen` now compares word-at-a-time before the byte tail. This targets v23 front-coded term block encode overhead without changing the wire format.

Metrics off:

- Load time: 1m6.379795291s
- Async catch-up time: 37.324327875s
- Total elapsed: 1m43.7042285s
- Throughput: 1,063.65 records/sec, 3.25 MiB/sec
- `ps` RSS: 165,822,464 bytes
- Peak sampled RSS: 599,965,696 bytes
- Process footprint metric: 153,754,896 bytes
- Peak sampled process footprint: 584,949,008 bytes
- Live malloc metric: 117,697,808 bytes
- Peak sampled live malloc: 266,498,224 bytes
- Segment files: 7
- Segment bytes: 393,395,219
- Stored fields bytes: 127,583,212
- Inverted bytes: 261,768,827
- Postings bytes: 143,224,960
- Term block bytes: 89,065,890
- Text merges completed: 267
- Full-text build peak bytes: 154,756,243
- Full-text pending peak bytes: 402,195,082
- Text merge buffer peak bytes: 216,146,921

Metrics on:

- Load time: 1m0.948767959s
- Async catch-up time: 22.385270375s
- Total elapsed: 1m23.334102709s
- Throughput: 1,158.43 records/sec, 3.53 MiB/sec
- `ps` RSS: 616,857,600 bytes
- Peak sampled RSS: 987,054,080 bytes
- Process footprint metric: 133,619,272 bytes
- Peak sampled process footprint: 695,918,272 bytes
- Live malloc metric: 104,432,608 bytes
- Peak sampled live malloc: 345,195,568 bytes
- Segment files: 8
- Segment bytes: 385,025,743
- Stored fields bytes: 127,466,977
- Inverted bytes: 253,466,121
- Postings bytes: 141,520,392
- Term block bytes: 88,233,855
- Text merges completed: 254
- Full-text build peak bytes: 164,263,583
- Full-text pending peak bytes: 393,606,375
- Text merge buffer peak bytes: 118,942,532

Metrics-on text timing aggregate:

- Timing lines: 261
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 44,475 ms
- Segment build timing: 44,459 ms
- Segment encode timing: 16,699 ms
- Inverted build timing: 14,566 ms
- Inverted term dictionary timing: 10,490 ms
- Inverted postings serialization timing: 2,634 ms
- Inverted sort timing: 591 ms
- Inverted final assembly timing: 253 ms
- Segment assembly timing: 1,896 ms
- Stored compression timing: 540 ms
- Analyzer timing: 4,491 ms
- Term accumulation timing: 4,103 ms
- Hit materialization timing: 3,653 ms

Interpretation: this run is not a valid speed baseline. Both metrics-off and metrics-on ingest were much slower than the immediately preceding v23 run, object-read and batch-write latencies inflated, and the merge count rose sharply. The compact term block size held up (`88,233,855` metrics-on bytes), but the timing cannot prove that the prefix matcher recovered the front-coding CPU regression. Treat this as a noisy measurement and rerun before making throughput claims.

## Valid Word-At-A-Time Prefix Matcher Baseline

Run: `work-log/do8018/releasefast-baseline-20260601-093449`
Commit: `7e94bc06c`

Metrics off:

- Load time: 43.828865666s
- Async catch-up time: 12.260269584s
- Total elapsed: 56.0892095s
- Throughput: 1,610.92 records/sec, 4.92 MiB/sec
- `ps` RSS: 648,232,960 bytes
- Peak sampled RSS: 983,203,840 bytes
- Process footprint metric: 126,753,632 bytes
- Peak sampled process footprint: 729,390,088 bytes
- Live malloc metric: 78,363,088 bytes
- Peak sampled live malloc: 199,982,048 bytes
- vmmap footprint: 126,772,838 bytes
- vmmap mapped-file resident: 30,828,134 bytes
- vmmap malloc allocated: 23,383,244 bytes
- Segment files: 8
- Segment bytes: 399,996,687
- Stored fields bytes: 127,852,310
- Inverted bytes: 268,032,648
- Postings bytes: 147,064,526
- Term block bytes: 96,824,673
- Text merges completed: 165
- Full-text build peak bytes: 151,777,684
- Full-text pending peak bytes: 402,620,839
- Text merge buffer peak bytes: 97,885,086
- LSM mutable snapshot clone calls: 23
- LSM mutable snapshot clone bytes total: 9,166,563
- LSM mutable snapshot clone peak bytes: 1,019,373

Metrics on:

- Load time: 45.305085042s
- Async catch-up time: 5.110258583s
- Total elapsed: 50.415416667s
- Throughput: 1,558.43 records/sec, 4.75 MiB/sec
- `ps` RSS: 609,812,480 bytes
- Peak sampled RSS: 1,044,496,384 bytes
- Process footprint metric: 145,366,048 bytes
- Peak sampled process footprint: 729,619,800 bytes
- Live malloc metric: 85,401,264 bytes
- Peak sampled live malloc: 244,445,488 bytes
- vmmap footprint: 145,437,491 bytes
- vmmap mapped-file resident: 31,981,568 bytes
- vmmap malloc allocated: 33,554,432 bytes
- Segment files: 8
- Segment bytes: 395,329,542
- Stored fields bytes: 127,733,242
- Inverted bytes: 263,503,586
- Postings bytes: 144,925,013
- Term block bytes: 93,716,320
- Text merges completed: 157
- Full-text build peak bytes: 146,713,166
- Full-text pending peak bytes: 396,592,706
- Text merge buffer peak bytes: 129,678,722
- LSM mutable snapshot clone calls: 32
- LSM mutable snapshot clone bytes total: 15,954,614
- LSM mutable snapshot clone peak bytes: 1,038,688

Metrics-on text timing aggregate:

- Timing lines: 177
- Source/projection docs: 70,605 / 70,605
- Total text build timing: 33,663 ms
- Segment build timing: 33,646 ms
- Segment encode timing: 12,982 ms
- Inverted build timing: 11,214 ms
- Inverted term dictionary timing: 7,677 ms
- Inverted postings serialization timing: 2,338 ms
- Inverted sort timing: 556 ms
- Inverted final assembly timing: 244 ms
- Inverted norms timing: 1 ms
- Inverted bloom finish timing: 2 ms
- Section attach timing: 0 ms
- Segment assembly timing: 1,603 ms
- Stored compression timing: 516 ms
- Analyzer timing: 3,623 ms
- Term accumulation timing: 2,560 ms
- Hit materialization timing: 3,499 ms

Interpretation: this repeat is a valid speed comparison. The noisy `20260601-092024` run was environmental; this run returned to the normal ingest band. Compared with the front-coded v23 baseline (`20260601-090240`), metrics-on text timing is effectively flat/slightly better (`34,032 ms` to `33,663 ms`), and term-dictionary timing improved (`8,118 ms` to `7,677 ms`). The prefix matcher does not make front coding dramatically faster, but it removes most evidence of a CPU regression while keeping the compact term block format.
