from __future__ import annotations

import struct

import numpy as np

import prepare_hdc_wands_fixture as fixture


def test_load_and_write_fixture(tmp_path):
    (tmp_path / "product.csv").write_text(
        "product_id\tproduct_name\tproduct_class\tcategory hierarchy\t"
        "product_description\tproduct_features\trating_count\taverage_rating\treview_count\n"
        "17\tchair\tChairs\tFurniture / Chairs\tcomfortable seat\t\t0\t0\t0\n",
        encoding="utf-8",
    )
    (tmp_path / "query.csv").write_text(
        "query_id\tquery\tquery_class\n23\tcomfortable chair\tChairs\n",
        encoding="utf-8",
    )
    (tmp_path / "label.csv").write_text(
        "id\tquery_id\tproduct_id\tlabel\n0\t23\t17\tExact\n",
        encoding="utf-8",
    )

    products, queries, qrels = fixture.load_dataset(tmp_path, None, None)
    assert products[0].text == "chair. comfortable seat"
    assert queries[0].query_class == "Chairs"
    assert qrels == [fixture.Qrel(query_index=0, product_index=0, gain=2)]

    class_ids, _ = fixture.intern(
        [products[0].product_class, queries[0].query_class]
    )
    category_ids, _ = fixture.intern([products[0].category])
    output = tmp_path / "fixture.afhw"
    fixture.write_fixture(
        output,
        np.asarray([[0.25, 0.5]], dtype="<f4"),
        np.asarray([[0.75, 1.0]], dtype="<f4"),
        products,
        queries,
        qrels,
        class_ids[:1],
        category_ids,
        class_ids[1:],
        32,
    )

    raw = output.read_bytes()
    assert fixture.HEADER.unpack(raw[: fixture.HEADER.size]) == (
        fixture.MAGIC,
        fixture.VERSION,
        2,
        1,
        1,
        1,
        32,
    )
    qrel_offset = fixture.HEADER.size + (4 * 2 * 2) + (4 * 5)
    assert struct.unpack("<IIB3x", raw[qrel_offset:]) == (0, 0, 2)
