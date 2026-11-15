import os
import sys

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "ingestion")
)

from streamvault.transforms import (
    available_stock,
    build_inventory_snapshot,
    dedupe_latest,
    event_date,
    parse_envelope,
    to_inventory_row,
    to_order_row,
)


def order_event(op, after, before=None, ts_ms=1730000000000):
    return {
        "payload": {
            "op": op,
            "after": after,
            "before": before,
            "source": {"ts_ms": ts_ms},
        }
    }


SAMPLE_ORDER = {
    "order_id": 7,
    "customer_id": 42,
    "status": "paid",
    "total_cents": 2599,
    "placed_at": "2026-10-27T10:15:00Z",
    "updated_at": "2026-10-27T11:02:00Z",
}


def test_parse_envelope_reads_after_image():
    parsed = parse_envelope(order_event("c", SAMPLE_ORDER))
    assert parsed["op"] == "c"
    assert parsed["after"]["status"] == "paid"
    assert parsed["source_ts_ms"] == 1730000000000


def test_snapshot_event_has_no_before_image():
    parsed = parse_envelope(order_event("r", SAMPLE_ORDER))
    assert parsed["before"] == {}
    assert parsed["after"]["order_id"] == 7


def test_to_order_row_shapes_fields():
    row = to_order_row(order_event("u", SAMPLE_ORDER))
    assert row["order_id"] == 7
    assert row["deleted"] is False
    assert row["event_date"] == "2024-10-27"


def test_delete_event_becomes_tombstone_from_before_image():
    row = to_order_row(order_event("d", None, before=SAMPLE_ORDER))
    assert row["deleted"] is True
    assert row["order_id"] == 7


def test_event_date_uses_utc():
    assert event_date(1730000000000) == "2024-10-27"


def test_dedupe_keeps_highest_source_timestamp():
    older = to_order_row(order_event("c", SAMPLE_ORDER, ts_ms=1000))
    newer_image = dict(SAMPLE_ORDER, status="shipped")
    newer = to_order_row(order_event("u", newer_image, ts_ms=2000))
    winners = dedupe_latest([newer, older], ("order_id",))
    assert len(winners) == 1
    assert winners[0]["status"] == "shipped"


def test_dedupe_keeps_distinct_keys():
    first = to_order_row(order_event("c", SAMPLE_ORDER, ts_ms=1000))
    second_image = dict(SAMPLE_ORDER, order_id=8)
    second = to_order_row(order_event("c", second_image, ts_ms=1000))
    winners = dedupe_latest([first, second], ("order_id",))
    assert len(winners) == 2


def test_inventory_row_and_available_stock():
    event = {
        "payload": {
            "op": "u",
            "after": {
                "sku": "SKU-0001",
                "warehouse": "reno",
                "on_hand": 50,
                "reserved": 12,
                "updated_at": "2026-10-27T11:02:00Z",
            },
            "before": None,
            "source": {"ts_ms": 1730000000000},
        }
    }
    row = to_inventory_row(event)
    assert row["sku"] == "SKU-0001"
    assert available_stock(row) == 38


def test_available_stock_never_negative_and_zero_when_deleted():
    row = {"on_hand": 3, "reserved": 9, "deleted": False}
    assert available_stock(row) == 0
    row["deleted"] = True
    assert available_stock(row) == 0


def inventory_row(sku, warehouse, on_hand, reserved, ts_ms, deleted=False):
    return {
        "sku": sku,
        "warehouse": warehouse,
        "on_hand": on_hand,
        "reserved": reserved,
        "updated_at": "2026-11-08T18:00:00Z",
        "deleted": deleted,
        "source_ts_ms": ts_ms,
    }


def test_snapshot_freezes_positions_with_available_counts():
    rows = [
        inventory_row("SKU-0001", "reno", 50, 12, 1000),
        inventory_row("SKU-0002", "reno", 4, 9, 1000),
    ]
    snapshot = build_inventory_snapshot(rows, "2026-11-08")
    assert len(snapshot) == 2
    assert snapshot[0]["snapshot_date"] == "2026-11-08"
    assert snapshot[0]["available"] == 38
    assert snapshot[1]["available"] == 0


def test_snapshot_drops_deleted_rows():
    rows = [
        inventory_row("SKU-0001", "reno", 50, 12, 1000),
        inventory_row("SKU-0009", "reno", 8, 0, 1000, deleted=True),
    ]
    snapshot = build_inventory_snapshot(rows, "2026-11-08")
    assert [row["sku"] for row in snapshot] == ["SKU-0001"]


def test_snapshot_keeps_newest_version_of_a_pair():
    older = inventory_row("SKU-0001", "reno", 50, 12, 1000)
    newer = inventory_row("SKU-0001", "reno", 44, 10, 2000)
    snapshot = build_inventory_snapshot([newer, older], "2026-11-08")
    assert len(snapshot) == 1
    assert snapshot[0]["on_hand"] == 44
    assert snapshot[0]["available"] == 34


def test_snapshot_sorts_by_warehouse_then_sku_and_handles_empty():
    rows = [
        inventory_row("SKU-0002", "tulsa", 5, 1, 1000),
        inventory_row("SKU-0001", "reno", 5, 1, 1000),
        inventory_row("SKU-0001", "tulsa", 5, 1, 1000),
    ]
    snapshot = build_inventory_snapshot(rows, "2026-11-08")
    assert [(row["warehouse"], row["sku"]) for row in snapshot] == [
        ("reno", "SKU-0001"),
        ("tulsa", "SKU-0001"),
        ("tulsa", "SKU-0002"),
    ]
    assert build_inventory_snapshot([], "2026-11-08") == []
