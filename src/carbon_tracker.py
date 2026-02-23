#!/usr/bin/env python3
"""BlackRoad Carbon Tracker — emissions monitoring for tech infrastructure."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BOLD   = "\033[1m"
NC     = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "carbon_tracker.db"

# Default emission factors (kg CO2e per unit)
DEFAULT_FACTORS: Dict[str, Dict] = {
    "server_kwh":       {"factor": 0.233,    "unit": "kWh",
                         "desc": "Grid electricity (global avg)"},
    "cloud_compute_h":  {"factor": 0.016,    "unit": "hour",
                         "desc": "Cloud VM compute (avg)"},
    "data_transfer_gb": {"factor": 0.000060, "unit": "GB",
                         "desc": "Internet data transfer"},
    "storage_tb_month": {"factor": 0.0016,   "unit": "TB/mo",
                         "desc": "Cloud object storage"},
    "flight_km":        {"factor": 0.255,    "unit": "km",
                         "desc": "Short-haul flight"},
    "car_km":           {"factor": 0.171,    "unit": "km",
                         "desc": "Petrol car per km"},
    "train_km":         {"factor": 0.041,    "unit": "km",
                         "desc": "Train travel per km"},
}


# ── Data models ───────────────────────────────────────────────────────────────
@dataclass
class EmissionFactor:
    id: Optional[int]
    name: str
    factor: float           # kg CO2e per unit
    unit: str
    desc: str
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class Activity:
    id: Optional[int]
    category: str
    label: str
    quantity: float
    co2_kg: float
    offset_kg: float
    tags: str
    recorded_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def net_co2(self) -> float:
        return max(0.0, self.co2_kg - self.offset_kg)


@dataclass
class EmissionsReport:
    period_days: int
    total_activities: int
    gross_co2_kg: float
    total_offsets_kg: float
    net_co2_kg: float
    by_category: Dict[str, float]
    top_emitter: str
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# ── Database ──────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS emission_factors ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name TEXT NOT NULL UNIQUE,"
        "  factor REAL NOT NULL,"
        "  unit TEXT NOT NULL,"
        "  desc TEXT DEFAULT '',"
        "  updated_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS activities ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  category TEXT NOT NULL,"
        "  label TEXT NOT NULL,"
        "  quantity REAL NOT NULL,"
        "  co2_kg REAL NOT NULL,"
        "  offset_kg REAL DEFAULT 0,"
        "  tags TEXT DEFAULT '',"
        "  recorded_at TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_act_cat ON activities(category);"
        "CREATE INDEX IF NOT EXISTS idx_act_ts  ON activities(recorded_at);"
    )
    conn.commit()

    # Seed default factors if the table is empty
    if conn.execute(
            "SELECT COUNT(*) FROM emission_factors").fetchone()[0] == 0:
        ts = datetime.utcnow().isoformat()
        conn.executemany(
            "INSERT OR IGNORE INTO emission_factors"
            " (name, factor, unit, desc, updated_at) VALUES (?,?,?,?,?)",
            [(k, v["factor"], v["unit"], v["desc"], ts)
             for k, v in DEFAULT_FACTORS.items()],
        )
        conn.commit()
    return conn


# ── Tracker ───────────────────────────────────────────────────────────────────
class CarbonTracker:
    """Track, calculate, and report infrastructure carbon emissions."""

    def __init__(self) -> None:
        self._db = _get_db()

    # ── Emission factors ───────────────────────────────────────────────────
    def set_factor(self, name: str, factor: float,
                   unit: str, desc: str = "") -> EmissionFactor:
        """Upsert an emission factor."""
        ts = datetime.utcnow().isoformat()
        existing = self._db.execute(
            "SELECT id FROM emission_factors WHERE name=?", (name,)
        ).fetchone()
        if existing:
            self._db.execute(
                "UPDATE emission_factors"
                " SET factor=?, unit=?, desc=?, updated_at=? WHERE name=?",
                (factor, unit, desc, ts, name),
            )
            self._db.commit()
            eid = existing["id"]
        else:
            cur = self._db.execute(
                "INSERT INTO emission_factors"
                " (name, factor, unit, desc, updated_at) VALUES (?,?,?,?,?)",
                (name, factor, unit, desc, ts),
            )
            self._db.commit()
            eid = cur.lastrowid
        return EmissionFactor(
            id=eid, name=name, factor=factor,
            unit=unit, desc=desc, updated_at=ts)

    def _resolve_factor(self, category: str) -> float:
        row = self._db.execute(
            "SELECT factor FROM emission_factors WHERE name=?", (category,)
        ).fetchone()
        if not row:
            raise KeyError(
                f"No emission factor for '{category}'. "
                f"Use 'add factor' to define it. "
                f"Known: {', '.join(DEFAULT_FACTORS)}"
            )
        return row["factor"]

    # ── Activity logging ───────────────────────────────────────────────────
    def log_activity(self, category: str, label: str,
                     quantity: float, offset_kg: float = 0.0,
                     tags: str = "") -> Activity:
        """Compute and persist a CO2e emission for a recorded activity."""
        factor = self._resolve_factor(category)
        co2_kg = round(factor * quantity, 6)
        ts = datetime.utcnow().isoformat()
        cur = self._db.execute(
            "INSERT INTO activities"
            " (category, label, quantity, co2_kg,"
            "  offset_kg, tags, recorded_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (category, label, quantity, co2_kg, offset_kg, tags, ts),
        )
        self._db.commit()
        return Activity(
            id=cur.lastrowid, category=category,
            label=label, quantity=quantity,
            co2_kg=co2_kg, offset_kg=offset_kg,
            tags=tags, recorded_at=ts,
        )

    # ── Reporting ──────────────────────────────────────────────────────────
    def report(self, days: int = 30) -> EmissionsReport:
        """Aggregate emissions over a rolling window."""
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            "SELECT category, co2_kg, offset_kg"
            " FROM activities WHERE recorded_at >= ?",
            (since,),
        ).fetchall()
        if not rows:
            return EmissionsReport(
                period_days=days, total_activities=0,
                gross_co2_kg=0, total_offsets_kg=0, net_co2_kg=0,
                by_category={}, top_emitter="none",
            )
        by_cat: Dict[str, float] = {}
        gross = offsets = 0.0
        for r in rows:
            gross   += r["co2_kg"]
            offsets += r["offset_kg"]
            by_cat[r["category"]] = (
                by_cat.get(r["category"], 0.0) + r["co2_kg"]
            )
        net = max(0.0, gross - offsets)
        top = max(by_cat, key=by_cat.get) if by_cat else "none"
        return EmissionsReport(
            period_days=days,
            total_activities=len(rows),
            gross_co2_kg=round(gross, 4),
            total_offsets_kg=round(offsets, 4),
            net_co2_kg=round(net, 4),
            by_category={k: round(v, 4)
                         for k, v in sorted(by_cat.items(),
                                            key=lambda x: -x[1])},
            top_emitter=top,
        )

    # ── Listing ────────────────────────────────────────────────────────────
    def list_activities(self, limit: int = 25) -> List[dict]:
        rows = self._db.execute(
            "SELECT * FROM activities ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_factors(self) -> List[dict]:
        rows = self._db.execute(
            "SELECT * FROM emission_factors ORDER BY name").fetchall()
        return [dict(r) for r in rows]

    def export(self, output: str = "carbon_export.json",
               days: int = 90) -> Path:
        """Export report + raw activities to JSON."""
        rpt = self.report(days=days)
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = self._db.execute(
            "SELECT * FROM activities WHERE recorded_at >= ?"
            " ORDER BY recorded_at",
            (since,),
        ).fetchall()
        data = {
            "exported_at": datetime.utcnow().isoformat(),
            "report": {
                "period_days":     rpt.period_days,
                "total_activities": rpt.total_activities,
                "gross_co2_kg":    rpt.gross_co2_kg,
                "total_offsets_kg": rpt.total_offsets_kg,
                "net_co2_kg":      rpt.net_co2_kg,
                "by_category":     rpt.by_category,
                "top_emitter":     rpt.top_emitter,
            },
            "activities": [dict(r) for r in rows],
        }
        out = Path(output)
        out.write_text(json.dumps(data, indent=2))
        return out


# ── Helpers ───────────────────────────────────────────────────────────────────
def _co2_bar(val: float, max_val: float, width: int = 20) -> str:
    if max_val == 0:
        return " " * width
    filled = int((val / max_val) * width)
    bar = "\u2588" * filled + "\u2591" * (width - filled)
    colour = RED if filled > width * 0.7 else (
        YELLOW if filled > width * 0.4 else GREEN)
    return f"{colour}{bar}{NC}"


# ── CLI ───────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        prog="carbon_tracker",
        description=(f"{BOLD}BlackRoad Carbon Tracker{NC}"
                     " — infrastructure emissions"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", metavar="command")

    p_list = sub.add_parser("list", help="List activities or emission factors")
    p_list.add_argument(
        "--type", choices=["activities", "factors"], default="activities")
    p_list.add_argument("--limit", type=int, default=25)

    p_add = sub.add_parser("add", help="Log an activity or set a factor")
    p_add.add_argument("type", choices=["activity", "factor"])
    p_add.add_argument("category", help="Category (e.g. server_kwh)")
    p_add.add_argument("quantity", type=float, help="Units consumed")
    p_add.add_argument("--label",       default="")
    p_add.add_argument("--offset",      type=float, default=0.0,
                       help="Offset kg CO2e to subtract")
    p_add.add_argument("--tags",        default="")
    p_add.add_argument("--factor",      type=float,
                       help="kg CO2e per unit (when type=factor)")
    p_add.add_argument("--unit",        default="unit",
                       help="Unit label (when type=factor)")
    p_add.add_argument("--description", default="")

    p_status = sub.add_parser("status", help="Show emissions report")
    p_status.add_argument("--days", type=int, default=30)

    p_export = sub.add_parser("export", help="Export data to JSON")
    p_export.add_argument("--output", default="carbon_export.json")
    p_export.add_argument("--days",   type=int, default=90)

    args = parser.parse_args()
    tracker = CarbonTracker()

    if args.cmd == "list":
        if args.type == "factors":
            factors = tracker.list_factors()
            print(f"\n{BOLD}{CYAN}\u26a1 Emission Factors ({len(factors)}){NC}\n")
            for f in factors:
                print(f"  {CYAN}{f['name']:<26}{NC}"
                      f" {f['factor']:.6f} kg CO\u2082e/{f['unit']:<12}"
                      f" {f['desc']}")
        else:
            rows = tracker.list_activities(args.limit)
            print(f"\n{BOLD}{CYAN}\U0001f331 Activities ({len(rows)}){NC}\n")
            for r in rows:
                ts  = r["recorded_at"][:19].replace("T", " ")
                net = max(0.0, r["co2_kg"] - r["offset_kg"])
                colour = RED if net > 1 else YELLOW if net > 0.1 else GREEN
                print(f"  {r['category']:<22}  {r['label']:<24}"
                      f"  {colour}{net:.4f} kg CO\u2082e{NC}  {ts}")
        print()

    elif args.cmd == "add":
        if args.type == "factor":
            if args.factor is None:
                print(f"{RED}\u2717 --factor required for type=factor{NC}",
                      file=sys.stderr)
                sys.exit(1)
            ef = tracker.set_factor(
                args.category, args.factor, args.unit, args.description)
            print(f"{GREEN}\u2705 Factor set{NC}"
                  f"  {ef.name}: {ef.factor} kg CO\u2082e/{ef.unit}")
        else:
            try:
                act = tracker.log_activity(
                    args.category,
                    args.label or args.category,
                    args.quantity,
                    offset_kg=args.offset,
                    tags=args.tags,
                )
            except KeyError as exc:
                print(f"{RED}\u2717 {exc}{NC}", file=sys.stderr)
                sys.exit(1)
            colour = RED if act.co2_kg > 1 else YELLOW if act.co2_kg > 0.1 else GREEN
            print(f"{GREEN}\u2705 Logged{NC}  {act.category} \u00d7{act.quantity}"
                  f"  \u2192 {colour}{act.co2_kg:.4f} kg CO\u2082e{NC}")

    elif args.cmd == "status":
        rpt = tracker.report(days=args.days)
        max_val = max(rpt.by_category.values()) if rpt.by_category else 1.0
        print(f"\n{BOLD}{CYAN}\U0001f30d Carbon Report"
              f" — last {rpt.period_days} days{NC}\n")
        print(f"  Activities  : {rpt.total_activities}")
        print(f"  Gross CO\u2082e  : {BOLD}{rpt.gross_co2_kg:.4f} kg{NC}")
        print(f"  Offsets     : {GREEN}{rpt.total_offsets_kg:.4f} kg{NC}")
        print(f"  Net CO\u2082e    : {RED}{rpt.net_co2_kg:.4f} kg{NC}")
        print(f"  Top emitter : {YELLOW}{rpt.top_emitter}{NC}")
        if rpt.by_category:
            print(f"\n  {BOLD}By Category:{NC}")
            for cat, val in rpt.by_category.items():
                bar = _co2_bar(val, max_val)
                print(f"  {cat:<24} {bar}  {val:.4f} kg")
        print()

    elif args.cmd == "export":
        out = tracker.export(args.output, days=args.days)
        print(f"{GREEN}\u2705 Exported \u2192{NC} {out}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
