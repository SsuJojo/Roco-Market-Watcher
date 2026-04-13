import json
from pathlib import Path
from fastapi import APIRouter, Response
from app.services.fetcher import fetch_html
from app.services.llm_parser import merge_parsed_sources, parse_article_content, render_markdown
from app.services.rules import should_notify
from app.services.notifier import send_openclaw_message
from app.services.persistence import persist_scan

router = APIRouter(tags=["monitor"])
ROOT = Path(__file__).resolve().parents[2]
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
def _scan_once() -> dict:
    fetch_config = CONFIG.get("fetch", {})
    sources = fetch_config.get("sources") or []
    if not sources and fetch_config.get("url"):
        sources = [{"url": fetch_config["url"], "class": fetch_config.get("class")}]

    listen = CONFIG.get("listen", [])

    results = []
    for source in sources:
        html = fetch_html(source["url"], fetch_config.get("headers"))
        parse_config = {"article_class": source.get("class")}
        parsed = parse_article_content(html, parse_config, listen)
        parsed["source_url"] = source["url"]
        results.append(parsed)

    merged = merge_parsed_sources(results, listen, CONFIG.get("llm", {}))
    triggered = should_notify(merged, listen)
    csv_path = persist_scan(merged)

    return {
        "sources": results,
        "merged": merged,
        "triggered": triggered,
        "csv_paths": [csv_path],
    }


@router.post("/scan")
def scan():
    result = _scan_once()
    if result["triggered"] and CONFIG.get("notify", {}).get("enabled"):
        message = render_markdown(result["merged"])
        send_openclaw_message(CONFIG["notify"]["command"], message)
    return result


@router.get("/json")
def scan_json():
    return _scan_once()


@router.get("/md")
def scan_markdown():
    result = _scan_once()
    markdown = render_markdown(result["merged"])
    return Response(markdown, media_type="text/markdown; charset=utf-8")
