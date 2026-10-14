import json, subprocess, array, hashlib
from pathlib import Path

root = Path(
    "/Users/ajroetker/go/src/github.com/antflydb/antfly/.worktrees/native-video-design"
)
src = root / "tools/embeddinggemma2/testdata/gbr-formats"
cases = json.loads((src / "formats.json").read_text())["cases"]
out = []
for row in cases:
    depth = row["depth"]
    n = row["width"] * row["height"]
    path = src / (row["id"] + ".webm")
    fmt = "gbrp" + (str(depth) + "le" if depth > 8 else "")
    raw = subprocess.check_output(
        [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            str(path),
            "-pix_fmt",
            fmt,
            "-f",
            "rawvideo",
            "-",
        ]
    )
    rgb = subprocess.check_output(
        [
            "ffmpeg",
            "-v",
            "error",
            "-i",
            str(path),
            "-pix_fmt",
            "rgb24",
            "-f",
            "rawvideo",
            "-",
        ]
    )
    values = raw if depth == 8 else array.array("H", raw)
    actual = bytearray()
    for frame in range(12):
        base = frame * 3 * n
        for i in range(n):
            g, b, r = [values[base + c * n + i] for c in range(3)]
            if depth == 8:
                result = [r, g, b]
            else:
                y = (
                    8414 * r + 16519 * g + 3208 * b + (16 << (depth + 7)) + (1 << depth)
                ) >> (depth + 1)
                u = (
                    (
                        -4865 * r
                        - 9528 * g
                        + 14392 * b
                        + (128 << (depth + 7))
                        + (1 << depth)
                    )
                    >> (depth + 1)
                ) - 8192
                v = (
                    (
                        14392 * r
                        - 12061 * g
                        - 2332 * b
                        + (128 << (depth + 7))
                        + (1 << depth)
                    )
                    >> (depth + 1)
                ) - 8192
                y = (y - 1024) * 9539
                result = [
                    max(0, min(255, (value + (1 << 18)) >> 19))
                    for value in [y + v * 13075, y - u * 3209 - v * 6660, y + u * 16525]
                ]
            actual.extend(result)
    mismatch = sum(a != b for a, b in zip(actual, rgb))
    assert len(actual) == len(rgb)
    print(row["id"], mismatch, flush=True)
    out.append(
        dict(
            case=row["id"],
            mismatched_rgb_samples=mismatch,
            reference_rgb_sha256=hashlib.sha256(rgb).hexdigest(),
            native_arithmetic_rgb_sha256=hashlib.sha256(actual).hexdigest(),
        )
    )
assert all(row["mismatched_rgb_samples"] == 0 for row in out)
Path("/private/tmp/gbr-encoded-pixel-oracle.json").write_text(
    json.dumps(out, indent=2) + "\n"
)
