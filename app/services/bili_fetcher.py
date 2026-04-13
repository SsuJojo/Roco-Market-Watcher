import asyncio
import html
import json
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.request import getproxies

# Fix path for bilibili_api
LI_PATH = Path(__file__).resolve().parents[1] / "libs" / "bili-api"
if str(LI_PATH) not in sys.path:
    sys.path.append(str(LI_PATH))

from bilibili_api import Credential, ass, user, video

logger = logging.getLogger(__name__)


def _system_proxy_map() -> dict[str, str]:
    proxies = getproxies()
    normalized: dict[str, str] = {}
    for key in ("http", "https"):
        value = proxies.get(key)
        if value:
            normalized[key] = str(value)
    return normalized


def _apply_system_proxy_env() -> dict[str, str]:
    env_proxy_keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
    if any(os.environ.get(key) for key in env_proxy_keys):
        return {}

    proxies = _system_proxy_map()
    if proxies.get("http"):
        os.environ.setdefault("HTTP_PROXY", proxies["http"])
        os.environ.setdefault("http_proxy", proxies["http"])
    if proxies.get("https"):
        os.environ.setdefault("HTTPS_PROXY", proxies["https"])
        os.environ.setdefault("https_proxy", proxies["https"])
    return proxies


def _normalize_title(value: object) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_same_day(value: object, today: datetime) -> bool:
    if value in (None, ""):
        return False
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value).date() == today.date()
        except (OverflowError, OSError, ValueError):
            return False

    text = str(value).strip()
    if not text:
        return False

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date() == today.date()
        except ValueError:
            continue
    return False


def _extract_titles_from_videos(result: dict, today: datetime) -> list[str]:
    vlist = result.get("list", {}).get("vlist", [])
    titles: list[str] = []
    for video in vlist:
        if not isinstance(video, dict) or not _is_same_day(video.get("created"), today):
            continue
        title = _normalize_title(video.get("title"))
        if title:
            titles.append(title)
    return titles


def _extract_media_list_items(result: dict, today: datetime) -> list[dict]:
    containers = []
    if isinstance(result, dict):
        containers.extend(
            value for value in [result.get("items"), result.get("list"), result.get("media_list")] if isinstance(value, list)
        )
        data = result.get("data")
        if isinstance(data, dict):
            containers.extend(
                value for value in [data.get("items"), data.get("list"), data.get("media_list")] if isinstance(value, list)
            )

    items_out: list[dict] = []
    for items in containers:
        for item in items:
            if not isinstance(item, dict):
                continue
            pubdate = item.get("pubtime") or item.get("pub_date") or item.get("ctime") or item.get("created")
            if not _is_same_day(pubdate, today):
                continue
            items_out.append(item)
    return items_out



def _extract_titles_from_media_list(result: dict, today: datetime) -> list[str]:
    titles: list[str] = []
    for item in _extract_media_list_items(result, today):
        title = _normalize_title(item.get("title") or item.get("name"))
        if title:
            titles.append(title)
    return titles


def _today_label(today: datetime) -> str:
    return today.strftime("%Y-%m-%d")



def _clean_subtitle_text(text: object) -> str:
    if not text:
        return ""
    value = str(text)
    value = html.unescape(value)
    value = value.replace("\r", "\n")
    value = re.sub(r"\n+", "\n", value)
    return value.strip()



def _build_credential(sessdata: str | None) -> Credential | None:
    if not sessdata:
        return None
    return Credential(sessdata=sessdata)



def _media_item_to_video_entry(item: dict) -> dict | None:
    title = _normalize_title(item.get("title") or item.get("name"))
    bvid = item.get("bv_id") or item.get("bvid")
    aid = item.get("id") or item.get("aid")
    pages = item.get("pages") or []
    cid = None
    if pages and isinstance(pages[0], dict):
        cid = pages[0].get("id") or pages[0].get("cid")
    if not title or not bvid or not cid:
        return None
    return {"title": title, "bvid": bvid, "aid": aid, "cid": cid}



async def _fetch_video_subtitle_text(video_entry: dict, credential: Credential | None) -> str:
    if credential is None:
        return ""

    v = video.Video(bvid=video_entry["bvid"], credential=credential)
    subtitle = await v.get_subtitle(cid=video_entry["cid"])
    subtitles = subtitle.get("subtitles") if isinstance(subtitle, dict) else None
    if not subtitles:
        return ""

    try:
        subtitle_obj = await ass.request_subtitle(obj=v, cid=video_entry["cid"], credential=credential)
        subtitle_json = json.loads(subtitle_obj.to_simple_json_str())
        lines = [item.get("content", "") for item in subtitle_json if isinstance(item, dict) and item.get("content")]
        return _clean_subtitle_text("\n".join(lines))
    except Exception:
        first_subtitle = subtitles[0] if isinstance(subtitles, list) and subtitles else {}
        subtitle_url = first_subtitle.get("subtitle_url") if isinstance(first_subtitle, dict) else None
        if subtitle_url:
            from bilibili_api.utils.network import Api

            raw = await Api(url=subtitle_url, method="GET").request(raw=True)
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            subtitle_payload = json.loads(raw)
            lines = [item.get("content", "") for item in subtitle_payload.get("body", []) if isinstance(item, dict) and item.get("content")]
            return _clean_subtitle_text("\n".join(lines))
        return ""



def _build_empty_payload(uid: int, error: str | None = None) -> dict:
    payload = {"uid": uid, "method": None, "titles": [], "videos": []}
    if error:
        payload["error"] = error
    return payload


async def _get_bili_video_payload(uid: int, sessdata: str | None = None) -> dict:
    proxies = _apply_system_proxy_env()
    if proxies:
        logger.info("Applied system proxy for Bilibili fetch uid=%s proxies=%s", uid, json.dumps(proxies, ensure_ascii=False))
    credential = _build_credential(sessdata)
    u = user.User(uid, credential=credential)
    today = datetime.now()

    try:
        media_list_result = await u.get_media_list()
        media_items = _extract_media_list_items(media_list_result, today)
        videos: list[dict] = []
        for item in media_items:
            video_entry = _media_item_to_video_entry(item)
            if video_entry is None:
                continue
            try:
                subtitle_text = await _fetch_video_subtitle_text(video_entry, credential)
            except Exception as exc:
                logger.warning("Bilibili subtitle fetch failed bvid=%s cid=%s: %s", video_entry["bvid"], video_entry["cid"], exc)
                subtitle_text = ""
            videos.append({"title": video_entry["title"], "subtitle": subtitle_text})

        titles = [item["title"] for item in videos]
        if titles:
            logger.info(
                "Bilibili titles fetched uid=%s method=%s day=%s count=%s sample=%s",
                uid,
                "get_media_list",
                _today_label(today),
                len(titles),
                json.dumps(titles[:5], ensure_ascii=False),
            )
            return {"uid": uid, "method": "get_media_list", "titles": titles, "videos": videos, "day": _today_label(today)}
        logger.warning(
            "Bilibili get_media_list returned no same-day titles for uid=%s day=%s raw_keys=%s",
            uid,
            _today_label(today),
            sorted(media_list_result.keys()) if isinstance(media_list_result, dict) else type(media_list_result).__name__,
        )
        raise RuntimeError(f"Failed to fetch same-day Bilibili titles for uid={uid}: no same-day titles returned")
    except Exception as exc:
        logger.warning("Bilibili get_media_list failed for uid=%s: %s", uid, exc)
        raise RuntimeError(f"Failed to fetch same-day Bilibili titles for uid={uid}: get_media_list={exc}") from exc


def _run_sync(coro) -> dict:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    import threading

    result: dict = {}
    error: Exception | None = None

    def runner() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(coro)
        except Exception as exc:
            error = exc

    thread = threading.Thread(target=runner)
    thread.start()
    thread.join()
    if error is not None:
        raise error
    return result


def fetch_bili_video_titles(uid: int, sessdata: str | None = None) -> dict:
    """Sync wrapper for Bilibili video title fetching."""
    try:
        return _run_sync(_get_bili_video_payload(uid, sessdata=sessdata))
    except Exception as exc:
        logger.error("Error in fetch_bili_video_titles uid=%s: %s", uid, exc)
        return _build_empty_payload(uid, str(exc))


def fetch_bili_video_titles_via_media_list(uid: int) -> dict:
    async def _run() -> dict:
        u = user.User(uid)
        today = datetime.now()
        media_list_result = await u.get_media_list()
        titles = _extract_titles_from_media_list(media_list_result, today)
        return {"uid": uid, "method": "get_media_list", "titles": titles, "day": _today_label(today)}

    try:
        return _run_sync(_run())
    except Exception as exc:
        logger.error("Error in fetch_bili_video_titles_via_media_list uid=%s: %s", uid, exc)
        return _build_empty_payload(uid, str(exc))


def fetch_bili_video_titles_via_videos(uid: int) -> dict:
    async def _run() -> dict:
        u = user.User(uid)
        today = datetime.now()
        videos_result = await u.get_videos()
        titles = _extract_titles_from_videos(videos_result, today)
        return {"uid": uid, "method": "get_videos", "titles": titles, "day": _today_label(today)}

    try:
        return _run_sync(_run())
    except Exception as exc:
        logger.error("Error in fetch_bili_video_titles_via_videos uid=%s: %s", uid, exc)
        return _build_empty_payload(uid, str(exc))


def get_bili_titles_text(uid: int, sessdata: str | None = None) -> tuple[str, dict]:
    payload = fetch_bili_video_titles(uid, sessdata=sessdata)
    return "\n".join(payload.get("titles", [])), payload


def extract_uid(url: str) -> int | None:
    """Extract UID from a Bilibili space URL."""
    match = re.search(r"space\.bilibili\.com/(\d+)", url)
    if match:
        return int(match.group(1))
    return None
