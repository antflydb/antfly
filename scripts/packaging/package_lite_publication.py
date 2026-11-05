"""Build the POSIX publication worker alongside the matching native binary."""

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def package(output):
    scripts = Path(__file__).resolve().parents[1]
    entries = {"__main__.py": (scripts / "antfly-publication").read_bytes()}
    for file in (scripts / "lite_publication").iterdir():
        if file.suffix in (".py", ".json") and not file.name.startswith("test_"):
            entries["lite_publication/" + file.name] = file.read_bytes()
    with output.open("wb") as stream:
        stream.write(b"#!/usr/bin/env python3\n")
        with ZipFile(stream, "w") as archive:
            for name, contents in sorted(entries.items()):
                archive.writestr(ZipInfo(name), contents, compress_type=ZIP_DEFLATED)
    output.chmod(0o755)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    package(parser.parse_args().output)
