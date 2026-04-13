import asyncio
import html
import json
import logging
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

# Fix path for bilibili_api
LI_PATH = Path(__file__).resolve().parents[1] / "libs" / "bili-api"
if str(LI_PATH) not in sys.path:
    sys.path.append(str(LI_PATH))

from bilibili_api import user

logger = logging.getLogger(__name__)


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


def _extract_titles_from_media_list(result: dict, today: datetime) -> list[str]:
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

    titles: list[str] = []
    for items in containers:
        for item in items:
            if not isinstance(item, dict):
                continue
            pubdate = item.get("pubtime") or item.get("pub_date") or item.get("ctime") or item.get("created")
            if not _is_same_day(pubdate, today):
                continue
            title = _normalize_title(item.get("title") or item.get("name"))
            if title:
                titles.append(title)
    return titles


def _today_label(today: datetime) -> str:
    return today.strftime("%Y-%m-%d")


def _build_empty_payload(uid: int, error: str | None = None) -> dict:
    payload = {"uid": uid, "method": None, "titles": []}
    if error:
        payload["error"] = error
    return payload


async def _get_bili_video_payload(uid: int) -> dict:
    u = user.User(uid)
    today = datetime.now()
    attempts: list[tuple[str, object]] = []

    try:
        videos_result = await u.get_videos()
        titles = _extract_titles_from_videos(videos_result, today)
        attempts.append(("get_videos", None))
        if titles:
            logger.info(
                "Bilibili titles fetched uid=%s method=%s day=%s count=%s sample=%s",
                uid,
                "get_videos",
                _today_label(today),
                len(titles),
                json.dumps(titles[:5], ensure_ascii=False),
            )
            return {"uid": uid, "method": "get_videos", "titles": titles, "day": _today_label(today)}
        logger.warning("Bilibili get_videos returned no same-day titles for uid=%s day=%s", uid, _today_label(today))
    except Exception as exc:
        attempts.append(("get_videos", exc))
        logger.warning("Bilibili get_videos failed for uid=%s: %s", uid, exc)

    try:
        media_list_result = await u.get_media_list()
        titles = _extract_titles_from_media_list(media_list_result, today)
        attempts.append(("get_media_list", None))
        if titles:
            logger.info(
                "Bilibili titles fetched uid=%s method=%s day=%s count=%s sample=%s",
                uid,
                "get_media_list",
                _today_label(today),
                len(titles),
                json.dumps(titles[:5], ensure_ascii=False),
            )
            return {"uid": uid, "method": "get_media_list", "titles": titles, "day": _today_label(today)}
        logger.warning(
            "Bilibili get_media_list returned no same-day titles for uid=%s day=%s raw_keys=%s",
            uid,
            _today_label(today),
            sorted(media_list_result.keys()) if isinstance(media_list_result, dict) else type(media_list_result).__name__,
        )
    except Exception as exc:
        attempts.append(("get_media_list", exc))
        logger.warning("Bilibili get_media_list failed for uid=%s: %s", uid, exc)

    errors = "; ".join(f"{name}={err}" for name, err in attempts if err is not None) or "no same-day titles returned"
    raise RuntimeError(f"Failed to fetch same-day Bilibili titles for uid={uid}: {errors}")


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


def fetch_bili_video_titles(uid: int) -> dict:
    """Sync wrapper for Bilibili video title fetching."""
    try:
        return _run_sync(_get_bili_video_payload(uid))
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


def get_bili_titles_text(uid: int) -> tuple[str, dict]:
    payload = fetch_bili_video_titles(uid)
    return "\n".join(payload.get("titles", [])), payload


def extract_uid(url: str) -> int | None:
    """Extract UID from a Bilibili space URL."""
    match = re.search(r"space\.bilibili\.com/(\d+)", url)
    if match:
        return int(match.group(1))
    return None
