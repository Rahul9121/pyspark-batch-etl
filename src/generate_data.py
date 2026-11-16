"""Seed synthetic capital-markets trade CSV with realistic dirt."""
from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

INSTRUMENTS = [
    ("AAPL", "equity", "USD", "0.010000", 1, "XNAS", True),
    ("MSFT", "equity", "USD", "0.010000", 1, "XNAS", True),
    ("GOOGL", "equity", "USD", "0.010000", 1, "XNAS", True),
    ("JPM", "equity", "USD", "0.010000", 1, "XNYS", True),
    ("GS", "equity", "USD", "0.010000", 1, "XNYS", True),
    ("XOM", "equity", "USD", "0.010000", 1, "XNYS", True),
    ("BTC-USD", "crypto", "USD", "0.010000", 1, "CRYP", True),
    ("ETH-USD", "crypto", "USD", "0.010000", 1, "CRYP", True),
    ("EURUSD", "fx", "USD", "0.000100", 1000, "OTC", True),
    ("GBPUSD", "fx", "USD", "0.000100", 1000, "OTC", True),
    ("US10Y", "fixed_income", "USD", "0.001000", 1000, "OTC", True),
    ("ESZ5", "future", "USD", "0.250000", 1, "XCME", True),
    ("NQZ5", "future", "USD", "0.250000", 1, "XCME", True),
    ("CLZ5", "future", "USD", "0.010000", 1, "XNYM", True),
    ("SPY", "etf", "USD", "0.010000", 1, "ARCX", True),
    ("QQQ", "etf", "USD", "0.010000", 1, "XNAS", True),
    ("IWM", "etf", "USD", "0.010000", 1, "ARCX", True),
    ("TLT", "etf", "USD", "0.010000", 1, "XNAS", True),
    ("GLD", "etf", "USD", "0.010000", 1, "ARCX", True),
    ("INACTIVE", "equity", "USD", "0.010000", 1, "XNYS", False),
]

VENUES = ["XNAS", "XNYS", "ARCX", "XCME", "XNYM", "OTC", "CRYP", "DARK"]
SIDES = ["BUY", "SELL", "buy", "Sell", "B", "S"]
BASE_PRICES = {
    "AAPL": 190.0,
    "MSFT": 420.0,
    "GOOGL": 175.0,
    "JPM": 200.0,
    "GS": 450.0,
    "XOM": 110.0,
    "BTC-USD": 65000.0,
    "ETH-USD": 3200.0,
    "EURUSD": 1.08,
    "GBPUSD": 1.27,
    "US10Y": 98.5,
    "ESZ5": 5800.0,
    "NQZ5": 20500.0,
    "CLZ5": 78.0,
    "SPY": 560.0,
    "QQQ": 480.0,
    "IWM": 220.0,
    "TLT": 92.0,
    "GLD": 230.0,
    "INACTIVE": 10.0,
}


def write_instruments(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "symbol",
                "asset_class",
                "currency",
                "tick_size",
                "lot_size",
                "exchange",
                "is_active",
            ]
        )
        for row in INSTRUMENTS:
            w.writerow(
                [
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    "true" if row[6] else "false",
                ]
            )


def generate_trades(
    n: int = 8000,
    seed: int = 42,
    duplicate_rate: float = 0.04,
    null_rate: float = 0.03,
    bad_type_rate: float = 0.02,
    late_record_rate: float = 0.05,
) -> list[dict]:
    rng = random.Random(seed)
    symbols = [i[0] for i in INSTRUMENTS if i[6]]
    start = datetime(2024, 11, 1, 9, 30, 0)
    rows: list[dict] = []
    for i in range(n):
        sym = rng.choice(symbols)
        ts = start + timedelta(seconds=rng.randint(0, 86400 * 10))
        if rng.random() < late_record_rate:
            ts = start + timedelta(days=12 + rng.randint(0, 3), seconds=rng.randint(0, 3600))
        side = rng.choice(SIDES)
        px = BASE_PRICES[sym] * (1 + rng.uniform(-0.02, 0.02))
        qty = rng.choice([10, 25, 50, 100, 250, 500, 1000])
        venue = rng.choice(VENUES)
        trade_id = f"T{100000 + i}"
        account = f"ACC{rng.randint(1, 40):03d}"
        trader = f"TR{rng.randint(1, 12):02d}"
        notional = round(px * qty, 4)
        row = {
            "trade_id": trade_id,
            "trade_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": sym,
            "side": side,
            "qty": str(qty),
            "price": f"{px:.6f}",
            "venue": venue,
            "account_id": account,
            "trader_id": trader,
            "notional": f"{notional:.4f}",
            "source_file": "gen_batch_01",
        }
        if rng.random() < null_rate:
            field = rng.choice(["price", "qty", "venue", "trader_id"])
            row[field] = ""
        if rng.random() < bad_type_rate:
            row["price"] = rng.choice(["N/A", "abc", "1,234.50", ""])
        if rng.random() < bad_type_rate / 2:
            row["qty"] = rng.choice(["ten", "-5", "0", ""])
        rows.append(row)

    # inject duplicates by trade_id
    n_dup = max(1, int(n * duplicate_rate))
    for _ in range(n_dup):
        base = dict(rng.choice(rows))
        base["source_file"] = "gen_batch_02"
        rows.append(base)

    rng.shuffle(rows)
    return rows


def write_trades(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "trade_id",
        "trade_ts",
        "symbol",
        "side",
        "qty",
        "price",
        "venue",
        "account_id",
        "trader_id",
        "notional",
        "source_file",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    seed = int(os.environ.get("SEED", "42"))
    n = int(os.environ.get("TRADE_ROWS", "8000"))
    write_instruments(ROOT / "data" / "ref" / "instruments.csv")
    rows = generate_trades(n=n, seed=seed)
    write_trades(ROOT / "data" / "raw" / "trades.csv", rows)
    print(f"Wrote {len(rows)} trade rows and {len(INSTRUMENTS)} instruments (seed={seed})")


if __name__ == "__main__":
    main()
