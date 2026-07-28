from __future__ import annotations

import prepare_hdc_wands_associations as associations


def test_product_aliases_and_query_phrases_form_independent_fields():
    product = associations.parse_product_features(
        "basecolor : navy blue|primarymaterial : solid wood|"
        "dsprimaryproductstyle : modern|producttype : coffee table|"
        "color : coffee"
    )
    assert ("color", "navy blue") in product
    assert ("material", "solid wood") in product
    assert ("style", "modern") in product
    assert all(field != "product_type" for field, _ in product)
    assert ("color", "coffee") not in product

    supported = {
        "color": {"blue", "navy blue"},
        "material": {"wood", "solid wood"},
        "style": {"modern"},
        "shape": set(),
        "finish": {"blue"},
        "pattern": set(),
        "size": set(),
    }
    query = associations.query_associations(
        "modern navy blue chair made of solid wood",
        supported,
    )
    assert query == {
        ("color", "navy blue"),
        ("material", "solid wood"),
        ("style", "modern"),
    }


def test_encode_rows_is_canonical_and_uses_csr_offsets():
    rows = [
        {("material", "wood"), ("color", "blue")},
        set(),
        {("color", "red")},
    ]
    offsets, records = associations.encode_rows(
        rows,
        {"color": 0, "material": 1},
        {"color": {"blue": 0, "red": 1}, "material": {"wood": 0}},
    )
    assert offsets == [0, 2, 2, 3]
    assert records == [(0, 0), (1, 0), (0, 1)]
