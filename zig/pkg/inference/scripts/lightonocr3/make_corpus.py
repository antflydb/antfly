# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# dependencies = ["pillow"]
# ///
"""Render synthetic, non-sensitive OCR pages. Run with uv run."""

import argparse
import json
import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--font",
        type=Path,
        default=Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--dense", action="store_true", help="Add a high-resolution small-print table"
    )
    args = parser.parse_args()
    default_root = Path(__file__).resolve().parent / "corpus"
    root = args.output_dir or default_root
    root.mkdir(parents=True, exist_ok=True)
    cases = [
        {
            "name": "receipt",
            "min_grounding_boxes": 3,
            "size": [864, 640],
            "font_size": 34,
            "lines": [
                "ANTFLY MARKET",
                "Receipt 40872",
                "Date: 2026-10-21",
                "",
                "Coffee             USD 4.50",
                "Notebook           USD 7.25",
                "",
                "Subtotal          USD 11.75",
                "Tax                USD 0.94",
                "Total             USD 12.69",
            ],
            "required": ["40872", "4.50", "7.25", "11.75", "0.94", "12.69"],
        },
        {
            "name": "letter",
            "size": [1100, 720],
            "font_size": 34,
            "lines": [
                "Delivery confirmation",
                "",
                "Reference: ORDER-76291",
                "",
                "The replacement parts will arrive on Friday.",
                "Please leave the package at reception.",
                "",
                "Contact: Morgan Lee",
                "Telephone: 555-0148",
            ],
            "required": [
                "ORDER-76291",
                "Friday",
                "reception",
                "Morgan Lee",
                "555-0148",
            ],
        },
        {
            "name": "shipping",
            "size": [768, 864],
            "font_size": 36,
            "lines": [
                "EXPRESS DELIVERY",
                "",
                "SHIP TO:",
                "Avery Chen",
                "184 Cedar Street",
                "Portland OR 97205",
                "",
                "Tracking: 987654321012",
                "Weight: 2.40 kg",
                "Service: NEXT DAY",
            ],
            "required": [
                "Avery Chen",
                "184",
                "97205",
                "987654321012",
                "2.40",
                "NEXT DAY",
            ],
        },
    ]
    manifest = [
        {
            "name": "invoice",
            "image": os.path.relpath(
                default_root / "../../../src/models/testdata/lightonocr3/invoice.png",
                root,
            ),
            "expected": "Antfly local OCR\n\nInvoice 12345\n\nTotal USD 42.00",
            "required": ["Antfly", "12345", "42.00"],
            "max_new_tokens": 128,
        }
    ]
    for case in cases:
        image = Image.new("RGB", case["size"], "white")
        draw = ImageDraw.Draw(image)
        font = ImageFont.truetype(str(args.font), case["font_size"])
        y = 38
        for line in case["lines"]:
            if draw.textlength(line, font=font) > image.width - 76:
                raise ValueError(f"Text exceeds page width: {line}")
            draw.text((38, y), line, fill="black", font=font)
            y += case["font_size"] + 15
        if y > image.height:
            raise ValueError("Text exceeds page height")
        image.save(root / (case["name"] + ".png"))
        manifest.append(
            {
                "name": case["name"],
                "image": case["name"] + ".png",
                "expected": "\n".join(case["lines"]),
                "required": case["required"],
                "max_new_tokens": 256,
                **(
                    {"min_grounding_boxes": case["min_grounding_boxes"]}
                    if "min_grounding_boxes" in case
                    else {}
                ),
            }
        )
    if args.dense:
        rows = [("Item", "Reference", "Quantity", "Unit price")]
        rows += [
            (
                f"Replacement part {i:02}",
                f"SKU-{1200 + i}",
                str(i % 5 + 1),
                f"USD {10 + i}.25",
            )
            for i in range(1, 33)
        ]
        image = Image.new("RGB", (1800, 2400), "white")
        draw = ImageDraw.Draw(image)
        font = ImageFont.truetype(str(args.font), 24)
        for row_index, row in enumerate(rows):
            y = 120 + row_index * 58
            draw.line((80, y, 1690, y), fill="gray", width=1)
            for x, field in zip((100, 640, 1100, 1400), row):
                draw.text((x, y + 14), field, fill="black", font=font)
        image.save(root / "dense-table.png")
        manifest.append(
            {
                "name": "dense-table",
                "image": "dense-table.png",
                "expected": "\n".join(" ".join(row) for row in rows),
                "required": ["SKU-1201", "SKU-1216", "SKU-1232", "USD 42.25"],
                "max_new_tokens": 3072,
                "expected_table_rows": rows,
            }
        )
    (root / "cases.json").write_text(
        json.dumps(
            {
                "description": f"{len(manifest)} synthetic OCR smoke pages; not a representative accuracy corpus.",
                "license": "Apache-2.0",
                "font": args.font.name,
                "cases": manifest,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
