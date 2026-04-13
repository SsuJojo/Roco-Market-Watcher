import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "monitor_history.csv"
CSV_HEADERS = [
    "date",
    "time",
    "source_url",
    "slot_label",
    "name",
    "quantity",
    "price",
    "status",
    "desc",
    "raw",
]
TIME_ORDER = {"全天": 0, "8-12": 1, "12-16": 2, "16-20": 3, "20-24": 4}


def _normalize_row(row: dict) -> dict:
    return {header: row.get(header, "") for header in CSV_HEADERS}


def _row_sort_key(row: dict) -> tuple:
    return (
        row.get("date", ""),
        TIME_ORDER.get(row.get("time", ""), 99),
        row.get("name", ""),
        str(row.get("quantity", "")),
        str(row.get("price", "")),
        row.get("source_url", ""),
        row.get("raw", ""),
    )


def _slot_key(row: dict) -> tuple[str, str]:
    return (row.get("date", ""), row.get("time", ""))


def _build_rows(parsed: dict) -> list[dict]:
    rows: list[dict] = []
    scan_date = parsed.get("published_at") or ""

    for slot in parsed.get("slots", []):
        items = [item for item in slot.get("items", []) if item.get("status") == "active"]
        if not items:
            continue
        for item in items:
            rows.append(
                {
                    "date": scan_date,
                    "time": slot.get("time", ""),
                    "source_url": item.get("source_url", parsed.get("source_url", "")),
                    "slot_label": slot.get("label", ""),
                    "name": item.get("name", ""),
                    "quantity": item.get("quantity", ""),
                    "price": item.get("price", ""),
                    "status": item.get("status", ""),
                    "desc": item.get("desc", ""),
                    "raw": item.get("raw", ""),
                }
            )

    unique_rows: list[dict] = []
    seen: set[tuple] = set()
    for row in sorted(rows, key=_row_sort_key):
        normalized = _normalize_row(row)
        signature = tuple(normalized.get(header, "") for header in CSV_HEADERS)
        if signature in seen:
            continue
        seen.add(signature)
        unique_rows.append(normalized)
    return unique_rows


def _read_existing_rows() -> list[dict]:
    if not CSV_PATH.exists():
        return []

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows = []
        for row in reader:
            normalized = _normalize_row(row)
            if normalized.get("status") != "active":
                continue
            if not normalized.get("date") or not normalized.get("time"):
                continue
            rows.append(normalized)
        return rows


def persist_scan(parsed: dict) -> str:
    DATA_DIR.mkdir(exist_ok=True)
    new_rows = _build_rows(parsed)
    new_slot_keys = {_slot_key(row) for row in new_rows}
    existing_rows = _read_existing_rows()

    kept_rows = [row for row in existing_rows if _slot_key(row) not in new_slot_keys]
    merged_rows = kept_rows + new_rows

    unique_rows: list[dict] = []
    seen: set[tuple] = set()
    for row in sorted(merged_rows, key=_row_sort_key):
        signature = tuple(row.get(header, "") for header in CSV_HEADERS)
        if signature in seen:
            continue
        seen.add(signature)
        unique_rows.append(row)

    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        if unique_rows:
            writer.writerows(unique_rows)

    return str(CSV_PATH)
