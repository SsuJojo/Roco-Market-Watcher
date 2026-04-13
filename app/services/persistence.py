import csv
import json
import logging
from collections import OrderedDict
from pathlib import Path


logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]


def _format_block(title: str, content: str) -> str:
    return f"\n{'=' * 24} {title} {'=' * 24}\n{content}\n{'=' * 60}"


def _log_json(title: str, payload) -> None:
    logger.info(_format_block(title, json.dumps(payload, ensure_ascii=False, indent=2)))


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

    slot_map: "OrderedDict[tuple[str, str, str], dict]" = OrderedDict()
    source_urls: list[str] = []
    seen_sources: set[str] = set()

    for row in sorted(rows, key=_row_sort_key):
        source_url = row.get("source_url", "")
        if source_url and source_url not in seen_sources:
            seen_sources.add(source_url)
            source_urls.append(source_url)

        slot_key = (row.get("date", ""), row.get("time", ""), row.get("slot_label", ""))
        slot = slot_map.setdefault(
            slot_key,
            {
                "id": row.get("time", ""),
                "label": row.get("slot_label", "") or row.get("time", ""),
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
                "status": row.get("status", "active") or "active",
                "desc": row.get("desc", ""),
                "raw": row.get("raw", ""),
                "source_url": source_url,
            }
        )

    merged["slots"] = list(slot_map.values())
    merged["source_urls"] = source_urls
    merged["source_url"] = source_urls[0] if len(source_urls) == 1 else ""
    return merged


def load_cached_scan(listen: list[str]) -> dict:
    merged = load_merged_from_csv()
    matches = []
    current_time = next((slot.get("time", "") for slot in merged.get("slots", []) if slot.get("time")), "")
    for slot in merged.get("slots", []):
        if current_time and slot.get("time") != current_time:
            continue
        for item in slot.get("items", []):
            if item.get("status") != "active":
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

    _log_json("CSV 本次新增行", new_rows)
    _log_json("CSV 本次覆盖的时间槽", sorted(list(new_slot_keys)))
    _log_json("CSV 读取到的旧数据", existing_rows)
    _log_json("CSV 保留的旧数据", kept_rows)
    _log_json("CSV 最终写入数据", unique_rows)

    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        if unique_rows:
            writer.writerows(unique_rows)

    logger.info(
        _format_block(
            "CSV 写入完成",
            f"文件路径: {CSV_PATH}\n覆盖时间槽: {json.dumps(sorted(list(new_slot_keys)), ensure_ascii=False)}\n最终行数: {len(unique_rows)}",
        )
    )
    return str(CSV_PATH)
