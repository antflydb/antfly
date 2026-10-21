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
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--font",
        type=Path,
        default=Path("/System/Library/Fonts/Supplemental/Arial.ttf"),
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent / "corpus"
    root.mkdir(exist_ok=True)
    cases = [
        {
            "name": "receipt",
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
            "image": "../../../src/models/testdata/lightonocr3/invoice.png",
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
            }
        )
    (root / "cases.json").write_text(
        json.dumps(
            {
                "description": "Four synthetic OCR smoke pages; not a representative accuracy corpus.",
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
