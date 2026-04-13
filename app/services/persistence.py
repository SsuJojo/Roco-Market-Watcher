import csv
import json
import logging
from collections import OrderedDict
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "monitor_history.csv"
CSV_HEADERS = [
    "date",
    "time",
    "name",
    "quantity",
    "price",
    "status",
    "desc",
    "raw",
]
TIME_ORDER = {"全天": 0, "8-12": 1, "12-16": 2, "16-20": 3, "20-24": 4}
CURRENT_ACTIVE_STATUS = "active"
DEFAULT_INACTIVE_STATUS = "pending"


def _format_block(title: str, content: str) -> str:
    return f"\n{'=' * 24} {title} {'=' * 24}\n{content}\n{'=' * 60}"


def _log_json(title: str, payload) -> None:
    logger.info(_format_block(title, json.dumps(payload, ensure_ascii=False, indent=2)))


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _current_time_range(now: datetime | None = None) -> str | None:
    current = now or datetime.now()
    hour = current.hour
    if 8 <= hour < 12:
        return "8-12"
    if 12 <= hour < 16:
        return "12-16"
    if 16 <= hour < 20:
        return "16-20"
    if 20 <= hour < 24:
        return "20-24"
    return None


def _normalize_scalar(value):
    if value is None:
        return ""
    return value


def _normalize_text(value) -> str:
    normalized = _normalize_scalar(value)
    if normalized == "":
        return ""
    return str(normalized).strip()


def _normalize_date(value) -> str:
    text = _normalize_text(value).replace("/", "-")
    if not text:
        return _today_str()
    parts = text.split("-")
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        return f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    return _today_str()


def _normalize_time(value) -> str:
    text = _normalize_text(value).replace("－", "-").replace("—", "-")
    if text in TIME_ORDER:
        return text
    if "-" not in text:
        return ""
    parts = [part.strip() for part in text.split("-")]
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return ""
    normalized = f"{int(parts[0])}-{int(parts[1])}"
    return normalized if normalized in TIME_ORDER else ""


def _normalize_name(value) -> str:
    return _normalize_text(value)


def _normalize_status(value, row_time: str) -> str:
    current_time = _current_time_range()
    if row_time and current_time and row_time == current_time:
        return CURRENT_ACTIVE_STATUS
    status = _normalize_text(value).lower()
    if status in {CURRENT_ACTIVE_STATUS, "empty", DEFAULT_INACTIVE_STATUS}:
        return status
    return DEFAULT_INACTIVE_STATUS


def _normalize_row(row: dict) -> dict | None:
    normalized = {
        "date": _normalize_date(row.get("date")),
        "time": _normalize_time(row.get("time")),
        "name": _normalize_name(row.get("name")),
        "quantity": _normalize_scalar(row.get("quantity")),
        "price": _normalize_scalar(row.get("price")),
        "status": DEFAULT_INACTIVE_STATUS,
        "desc": _normalize_text(row.get("desc")),
        "raw": _normalize_text(row.get("raw")),
    }
    if not normalized["name"] or not normalized["time"]:
        return None
    normalized["status"] = _normalize_status(row.get("status"), normalized["time"])
    return {header: normalized.get(header, "") for header in CSV_HEADERS}


def _unique_row_key(row: dict) -> tuple[str, str, str]:
    return (row.get("date", ""), row.get("time", ""), row.get("name", "").strip().lower())


def _row_sort_key(row: dict) -> tuple:
    return (
        row.get("date", ""),
        TIME_ORDER.get(row.get("time", ""), 99),
        row.get("name", ""),
        str(row.get("quantity", "")),
        str(row.get("price", "")),
        row.get("raw", ""),
    )


def _rows_from_merged(parsed: dict) -> list[dict]:
    rows: list[dict] = []
    scan_date = _normalize_date(parsed.get("published_at"))
    for slot in parsed.get("slots", []):
        slot_time = _normalize_time(slot.get("time"))
        if not slot_time:
            continue
        for item in slot.get("items", []):
            normalized = _normalize_row(
                {
                    "date": scan_date,
                    "time": slot_time,
                    "name": item.get("name"),
                    "quantity": item.get("quantity"),
                    "price": item.get("price"),
                    "status": item.get("status"),
                    "desc": item.get("desc"),
                    "raw": item.get("raw"),
                }
            )
            if normalized:
                rows.append(normalized)
    return _dedupe_rows(rows)


def _rows_from_postprocessed(postprocessed: dict) -> list[dict]:
    rows: list[dict] = []
    for row in postprocessed.get("rows", []):
        normalized = _normalize_row(row)
        if normalized:
            rows.append(normalized)
    return _dedupe_rows(rows)


def _dedupe_rows(rows: list[dict]) -> list[dict]:
    deduped: OrderedDict[tuple[str, str, str], dict] = OrderedDict()
    for row in sorted(rows, key=_row_sort_key):
        deduped[_unique_row_key(row)] = row
    return list(deduped.values())


def _read_existing_rows() -> list[dict]:
    if not CSV_PATH.exists():
        return []

    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows: list[dict] = []
        for row in reader:
            normalized = _normalize_row(row)
            if normalized:
                rows.append(normalized)
        return rows


def _write_rows(rows: list[dict]) -> None:
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def _persist_rows(new_rows: list[dict]) -> str:
    DATA_DIR.mkdir(exist_ok=True)
    existing_rows = _read_existing_rows()

    row_map: OrderedDict[tuple[str, str, str], dict] = OrderedDict()
    for row in sorted(existing_rows, key=_row_sort_key):
        row_map[_unique_row_key(row)] = row
    for row in sorted(new_rows, key=_row_sort_key):
        row_map[_unique_row_key(row)] = row

    final_rows = sorted(row_map.values(), key=_row_sort_key)

    _log_json("CSV 本次新增行", new_rows)
    _log_json("CSV 读取到的旧数据", existing_rows)
    _log_json("CSV 最终写入数据", final_rows)

    _write_rows(final_rows)
    logger.info(_format_block("CSV 写入完成", f"文件路径: {CSV_PATH}\n最终行数: {len(final_rows)}"))
    return str(CSV_PATH)


def persist_postprocessed_scan(postprocessed: dict) -> str:
    return _persist_rows(_rows_from_postprocessed(postprocessed))


def persist_scan(parsed: dict) -> str:
    return _persist_rows(_rows_from_merged(parsed))


def _empty_merged() -> dict:
    return {
        "title": "远行商人汇总",
        "published_at": "",
        "current_time": None,
        "normalized_text": "",
        "slots": [],
        "matches": [],
        "matched": False,
        "source_length": 0,
        "source_url": "",
        "source_urls": [],
    }


def load_merged_from_csv() -> dict:
    rows = _read_existing_rows()
    merged = _empty_merged()
    if not rows:
        return merged

    merged["published_at"] = max((row.get("date", "") for row in rows), default="")
    merged["current_time"] = _current_time_range()

    slot_map: OrderedDict[tuple[str, str], dict] = OrderedDict()
    for row in sorted(rows, key=_row_sort_key):
        slot_key = (row.get("date", ""), row.get("time", ""))
        slot = slot_map.setdefault(
            slot_key,
            {
                "id": row.get("time", ""),
                "label": row.get("time", ""),
                "time": row.get("time", ""),
                "items": [],
                "notes": [],
                "status": "active",
            },
        )
        slot["items"].append(
            {
                "name": row.get("name", ""),
                "quantity": row.get("quantity") or None,
                "price": row.get("price") or None,
                "status": row.get("status", DEFAULT_INACTIVE_STATUS) or DEFAULT_INACTIVE_STATUS,
                "desc": row.get("desc", ""),
                "raw": row.get("raw", ""),
            }
        )

    merged["slots"] = list(slot_map.values())
    return merged


def load_cached_scan(listen: list[str]) -> dict:
    merged = load_merged_from_csv()
    matches = []
    current_time = _current_time_range()
    for slot in merged.get("slots", []):
        if current_time and slot.get("time") != current_time:
            continue
        for item in slot.get("items", []):
            if item.get("status") != CURRENT_ACTIVE_STATUS:
                continue
            for name in listen:
                item_name = item.get("name", "")
                if name and name in item_name:
                    matches.append(
                        {
                            "name": item_name,
                            "listen": name,
                            "time": slot.get("time", ""),
                            "slot_label": slot.get("label", ""),
                            "quantity": item.get("quantity"),
                            "price": item.get("price"),
                            "desc": item.get("desc", ""),
                            "raw": item.get("raw", ""),
                        }
                    )
    merged["matches"] = matches
    merged["matched"] = bool(matches)
    return {
        "merged": merged,
        "triggered": bool(matches),
        "csv_paths": [str(CSV_PATH)],
    }
