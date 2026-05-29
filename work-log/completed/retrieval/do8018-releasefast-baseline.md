# DO8018 ReleaseFast Baseline

Captured on 2026-05-29 from worktree commit `d80063a65966` with `WAIT_CATCHUP=300s`.

Generated artifacts are intentionally local and untracked under:

`work-log/do8018/releasefast-baseline-20260529-092631/`

## Metrics Off

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 38.181454084s
- Async catch-up time: 32.5007075s
- Catch-up complete: true
- `ps` RSS: 1,254,424,576 bytes
- Process footprint metric: 126,211,520 bytes
- Live malloc metric: 76,017,632 bytes
- vmmap footprint: 126,248,550 bytes
- vmmap peak footprint: 847,039,692 bytes
- vmmap mapped-file resident: 642,567,372 bytes
- vmmap malloc allocated: 27,577,548 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 642,490,183

## Metrics On

- Records: 70,605
- Input bytes: 225,883,530
- Load time: 36.743043084s
- Async catch-up time: 39.066862958s
- Catch-up complete: true
- `ps` RSS: 1,312,325,632 bytes
- Process footprint metric: 157,849,384 bytes
- Live malloc metric: 86,710,752 bytes
- vmmap footprint: 158,544,691 bytes
- vmmap peak footprint: 753,401,856 bytes
- vmmap mapped-file resident: 641,309,081 bytes
- vmmap malloc allocated: 27,577,548 bytes
- Final full-text segment files: 8
- Final full-text segment bytes: 641,265,468

## Read

RSS remains dominated by file-backed mapped segment residency. The process footprint and live malloc numbers are much smaller than RSS, while mapped-file resident bytes track final segment bytes closely.
