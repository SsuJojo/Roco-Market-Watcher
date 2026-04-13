import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Response

from app.services.bili_fetcher import extract_uid, fetch_bili_video_titles
from app.services.fetcher import fetch_html
from app.services.llm_parser import LLMParseError, _strip_comments, merge_parsed_sources, parse_article_content, post_process_scan_result, render_markdown
from app.services.notifier import send_openclaw_message
from app.services.persistence import load_cached_scan, persist_postprocessed_scan
from app.services.rules import should_notify

logger = logging.getLogger(__name__)

router = APIRouter(tags=["monitor"])
ROOT = Path(__file__).resolve().parents[2]
CONFIG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))


def _format_block(title: str, content: str) -> str:
    return f"\n{'=' * 24} {title} {'=' * 24}\n{content}\n{'=' * 60}"


def _log_json(title: str, payload) -> None:
    logger.info(_format_block(title, json.dumps(payload, ensure_ascii=False, indent=2)))


def _log_request(request: Request) -> None:
    logger.info(
        _format_block(
            "收到请求",
            f"方法: {request.method}\n路径: {request.url.path}\nIP: {_client_ip(request)}",
        )
    )


def _log_error(title: str, request: Request) -> None:
    logger.exception(
        _format_block(
            title,
            f"路径: {request.url.path}\nIP: {_client_ip(request)}",
        )
    )


def _client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _scan_once() -> dict:
    fetch_config = CONFIG.get("fetch", {})
    sources = fetch_config.get("sources") or []
    if not sources and fetch_config.get("url"):
        sources = [{"url": fetch_config["url"], "class": fetch_config.get("class")}]

    listen = CONFIG.get("listen", [])
    llm_config = CONFIG.get("llm", {})

    results = []
    for source in sources:
        url = source["url"]
        uid = extract_uid(url)
        if uid:
            sessdata = (fetch_config.get("bilibili") or {}).get("sessdata")
            bili_payload = fetch_bili_video_titles(uid, sessdata=sessdata)
            titles = bili_payload.get("titles", [])
            if not titles:
                logger.warning(
                    "No same-day Bilibili titles found for uid=%s method=%s error=%s",
                    uid,
                    bili_payload.get("method"),
                    bili_payload.get("error"),
                )
                continue

            logger.info(
                "Using Bilibili source uid=%s method=%s day=%s count=%s",
                uid,
                bili_payload.get("method"),
                bili_payload.get("day"),
                len(titles),
            )
            results.append({
                "source_url": url,
                "videos": bili_payload.get("videos", []),
                "titles": titles,
                "triggered": False,
            })
            continue
        else:
            html = fetch_html(url, fetch_config.get("headers"))
            parse_config = {"article_class": source.get("class")}

        parsed = parse_article_content(html, parse_config, listen, llm_config, url)
        results.append(parsed)

    if not results:
        return []

    if all(isinstance(item, dict) and "videos" in item for item in results):
        videos = []
        for item in results:
            videos.extend(item.get("videos", []))
        return videos

    merged = merge_parsed_sources(results, listen, llm_config)
    postprocessed = post_process_scan_result(merged, llm_config, listen)
    triggered = should_notify(merged, listen)
    csv_path = persist_postprocessed_scan(postprocessed)

    return _strip_comments(
        {
            "sources": results,
            "merged": merged,
            "postprocessed": postprocessed,
            "triggered": triggered,
            "csv_paths": [csv_path],
        }
    )


def _scan_or_raise(request: Request) -> dict:
    try:
        return _scan_once()
    except LLMParseError as exc:
        _log_error("扫描失败（LLM 解析错误）", request)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        _log_error("扫描失败（未处理异常）", request)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def run_startup_scan() -> dict:
    return _scan_once()


def _load_cached_result() -> dict:
    return load_cached_scan(CONFIG.get("listen", []))


@router.get("/scan")
@router.post("/scan")
def scan(request: Request):
    _log_request(request)
    _log_json("扫描来源配置", CONFIG.get("fetch", {}).get("sources") or CONFIG.get("fetch", {}))

    result = _scan_or_raise(request)
    if isinstance(result, dict) and result["triggered"] and CONFIG.get("notify", {}).get("enabled"):
        message = render_markdown(result["merged"])
        send_openclaw_message(CONFIG["notify"]["command"], message)

    _log_json("接口返回内容", result)
    return result


@router.get("/json")
def scan_json(request: Request):
    _log_request(request)

    result = _load_cached_result()
    _log_json("接口返回内容", result)
    return result


@router.get("/md")
def scan_markdown(request: Request):
    _log_request(request)

    result = _load_cached_result()
    markdown = render_markdown(result["merged"])
    logger.info(_format_block("接口返回 Markdown", markdown))
    return Response(markdown, media_type="text/markdown; charset=utf-8")
