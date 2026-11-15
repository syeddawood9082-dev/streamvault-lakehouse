"""Transforms between Debezium change events and lakehouse rows.

The functions here take plain dictionaries and return plain dictionaries.
Spark stays out of this module so the logic can be tested on its own.
"""

from datetime import datetime, timezone

ORDER_FIELDS = (
    "order_id",
    "customer_id",
    "status",
    "total_cents",
    "placed_at",
    "updated_at",
)

INVENTORY_FIELDS = (
    "sku",
    "warehouse",
    "on_hand",
    "reserved",
    "updated_at",
)


def parse_envelope(raw):
    """Pull the useful parts out of one Debezium change event.

    Returns a dictionary with the operation, the row image after the
    change, the row image before the change, and the source timestamp
    in epoch milliseconds. Snapshot reads report op r and carry no
    before image.
    """
    payload = raw.get("payload", raw)
    op = payload.get("op", "r")
    after = payload.get("after") or {}
    before = payload.get("before") or {}
    source = payload.get("source") or {}
    return {
        "op": op,
        "after": after,
        "before": before,
        "source_ts_ms": source.get("ts_ms", 0),
    }


def event_date(epoch_ms):
    """Return the UTC calendar date for an epoch millisecond value."""
    moment = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return moment.date().isoformat()


def to_order_row(event):
    """Shape one parsed event into an orders table row.

    Delete events become tombstone rows with the deleted flag set so
    the lakehouse keeps a record of the removal.
    """
    parsed = parse_envelope(event)
    image = parsed["before"] if parsed["op"] == "d" else parsed["after"]
    row = {field: image.get(field) for field in ORDER_FIELDS}
    row["deleted"] = parsed["op"] == "d"
    row["source_ts_ms"] = parsed["source_ts_ms"]
    row["event_date"] = event_date(parsed["source_ts_ms"])
    return row


def to_inventory_row(event):
    """Shape one parsed event into an inventory table row."""
    parsed = parse_envelope(event)
    image = parsed["before"] if parsed["op"] == "d" else parsed["after"]
    row = {field: image.get(field) for field in INVENTORY_FIELDS}
    row["deleted"] = parsed["op"] == "d"
    row["source_ts_ms"] = parsed["source_ts_ms"]
    return row


def dedupe_latest(rows, key_fields):
    """Keep the newest version of each keyed row.

    Rows are grouped by the key fields and the row with the highest
    source timestamp wins. Ties keep the row seen last, which matches
    the order records arrive from the log.
    """
    winners = {}
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        current = winners.get(key)
        if current is None or row["source_ts_ms"] >= current["source_ts_ms"]:
            winners[key] = row
    return list(winners.values())


def available_stock(row):
    """Units that can still be sold for one inventory row."""
    if row.get("deleted"):
        return 0
    return max(int(row.get("on_hand") or 0) - int(row.get("reserved") or 0), 0)


def build_inventory_snapshot(rows, snapshot_date):
    """Freeze one end of day stock position per sku and warehouse.

    Takes lakehouse inventory rows and returns one snapshot row for
    each sku and warehouse pair. Tombstone rows are dropped because a
    removed item has no end of day position. When a pair shows up
    more than once the newest version wins, the same rule the
    lakehouse write path uses. Each snapshot row carries the snapshot
    date and the sellable count so a finished day can be compared
    against the next one without rerunning the pipeline. Rows come
    out sorted by warehouse then sku.
    """
    snapshot = []
    for row in dedupe_latest(rows, ("sku", "warehouse")):
        if row.get("deleted"):
            continue
        snapshot.append(
            {
                "snapshot_date": snapshot_date,
                "sku": row.get("sku"),
                "warehouse": row.get("warehouse"),
                "on_hand": int(row.get("on_hand") or 0),
                "reserved": int(row.get("reserved") or 0),
                "available": available_stock(row),
                "updated_at": row.get("updated_at"),
            }
        )
    snapshot.sort(key=lambda item: (item["warehouse"] or "", item["sku"] or ""))
    return snapshot
