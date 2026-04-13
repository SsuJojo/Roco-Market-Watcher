import json
import logging

import requests


logger = logging.getLogger(__name__)
HTML_LOG_LIMIT = 3000


def _format_block(title: str, content: str) -> str:
    return f"\n{'=' * 24} {title} {'=' * 24}\n{content}\n{'=' * 60}"


def _truncate_text(value: str, limit: int = HTML_LOG_LIMIT) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}\n\n...（已截断，原文共 {len(value)} 字符，仅展示前 {limit} 字符）"


def _normalize_headers(headers: dict[str, str] | None) -> dict[str, str] | None:
    if not headers:
        return None
    return {
        str(key): str(value)
        for key, value in headers.items()
        if key and not str(key).startswith("_") and value is not None
    }


def fetch_html(url: str, headers: dict[str, str] | None = None) -> str:
    normalized_headers = _normalize_headers(headers)
    logger.info(
        _format_block(
            "开始拉取网页",
            f"URL: {url}\n请求头: {json.dumps(normalized_headers, ensure_ascii=False, indent=2) if normalized_headers else 'null'}",
        )
    )
    resp = requests.get(url, headers=normalized_headers, timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    logger.info(_format_block("拉取结果", f"URL: {url}\n状态码: {resp.status_code}\n编码: {resp.encoding}"))
    logger.info(_format_block("网页原文（缩略）", _truncate_text(resp.text)))
    return resp.text


__all__ = ["fetch_html"]
