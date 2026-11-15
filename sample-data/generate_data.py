"""Generate synthetic orders and inventory for the local stack.

Writes a SQL file with insert statements, then applies it to the
Postgres container started by docker compose. Rerunning the script
replaces the previous seed data.
"""

import argparse
import random
import subprocess
import sys
from datetime import datetime, timedelta, timezone

STATUSES = ("placed", "paid", "picked", "shipped", "delivered", "cancelled")
WAREHOUSES = ("reno", "dallas", "newark")


def iso(moment):
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_sql(rows, seed):
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    lines = ["delete from orders;", "delete from inventory;"]
    for order_id in range(1, rows + 1):
        placed = now - timedelta(days=rng.randint(0, 13), hours=rng.randint(0, 23))
        updated = placed + timedelta(hours=rng.randint(0, 30))
        status = rng.choice(STATUSES)
        total = rng.randint(900, 48000)
        lines.append(
            "insert into orders values "
            f"({order_id}, {rng.randint(100, 999)}, '{status}', {total}, "
            f"'{iso(placed)}', '{iso(updated)}');"
        )
    for index in range(1, 41):
        sku = f"SKU-{index:04d}"
        for warehouse in WAREHOUSES:
            on_hand = rng.randint(0, 400)
            reserved = rng.randint(0, min(on_hand, 60))
            lines.append(
                "insert into inventory values "
                f"('{sku}', '{warehouse}', {on_hand}, {reserved}, '{iso(now)}');"
            )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="sample-data/seed.sql")
    args = parser.parse_args()

    sql = build_sql(args.rows, args.seed)
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write(sql)
    print(f"Wrote {args.out}")

    apply = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "streamvault", "-d", "streamvault",
        ],
        input=(
            open("sample-data/schema.sql", encoding="utf-8").read() + sql
        ).encode(),
        capture_output=True,
    )
    if apply.returncode != 0:
        sys.stderr.write(apply.stderr.decode())
        print("Seed file written. Apply it once the stack is running.")
        return 1
    print("Seed data applied to the local Postgres container.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
