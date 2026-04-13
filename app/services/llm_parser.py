import json
import logging
import re
from datetime import datetime
from html import unescape
from typing import Any

from openai import APIError, OpenAI


logger = logging.getLogger(__name__)


def _format_block(title: str, content: str) -> str:
    return f"\n{'=' * 24} {title} {'=' * 24}\n{content}\n{'=' * 60}"


def _log_json(title: str, payload) -> None:
    logger.info(_format_block(title, json.dumps(payload, ensure_ascii=False, indent=2)))




TIME_ORDER = {"全天": 0, "8-12": 1, "12-16": 2, "16-20": 3, "20-24": 4}
ALLOWED_MERGE_FIELDS = {"name", "quantity", "price", "desc", "raw", "status", "source_url"}
POSTPROCESS_ROW_FIELDS = {"date", "time", "name", "quantity", "price", "status", "desc", "raw"}
VALID_ITEM_STATUS = {"active", "empty", "pending"}
VALID_SLOT_STATUS = {"active", "empty", "pending"}
DEFAULT_POSTPROCESS_PROMPT = "根据 merged 结果生成 CSV 行数据"


class LLMParseError(RuntimeError):
    pass


def _normalize_merge_name(value: str) -> str:
    return re.sub(r"\s+", "", value or "").strip().lower()


def _normalize_merge_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _normalize_merge_int(value) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        return int(value.strip())
    return None


def _item_score(item: dict) -> tuple:
    return (
        1 if item.get("price") not in (None, "") else 0,
        len(item.get("desc", "")),
        len(item.get("raw", "")),
    )


def _fallback_merge_item(candidates: list[dict], merge_strategy: str = "fallback") -> dict:
    merged = {}
    source_urls: list[str] = []

    for candidate in candidates:
        source_url = candidate.get("source_url")
        if source_url and source_url not in source_urls:
            source_urls.append(source_url)
        if not merged or _item_score(candidate) > _item_score(merged):
            merged = dict(candidate)

    if not merged:
        return {}

    best_desc = max((candidate.get("desc", "") for candidate in candidates), key=len, default="")
    best_raw = max((candidate.get("raw", "") for candidate in candidates), key=len, default="")
    best_price = next((candidate.get("price") for candidate in candidates if candidate.get("price") not in (None, "")), None)

    result = dict(merged)
    result["name"] = _normalize_merge_text(result.get("name"))
    result["quantity"] = _normalize_merge_int(result.get("quantity"))
    result["price"] = best_price if best_price is not None else _normalize_merge_int(result.get("price"))
    result["desc"] = _normalize_merge_text(best_desc or result.get("desc", ""))
    result["raw"] = _normalize_merge_text(best_raw or result.get("raw", ""))
    result["status"] = "active"
    result["source_urls"] = source_urls
    result["source_url"] = _normalize_merge_text(result.get("source_url") or (source_urls[0] if source_urls else ""))
    result["merge_strategy"] = merge_strategy
    return result


def _candidate_signature(candidate: dict) -> tuple:
    return (
        _normalize_merge_name(candidate.get("name", "")),
        _normalize_merge_int(candidate.get("quantity")),
        _normalize_merge_int(candidate.get("price")),
        _normalize_merge_text(candidate.get("desc")),
        _normalize_merge_text(candidate.get("raw")),
        _normalize_merge_text(candidate.get("status") or "active") or "active",
    )


def _needs_llm_merge(candidates: list[dict]) -> bool:
    if len(candidates) <= 1:
        return False
    return len({_candidate_signature(candidate) for candidate in candidates}) > 1


def _validate_llm_merge_result(merged: dict, fallback: dict, candidates: list[dict]) -> dict | None:
    if not isinstance(merged, dict):
        return None
    if set(merged) - ALLOWED_MERGE_FIELDS:
        return None

    normalized_name = _normalize_merge_name(merged.get("name"))
    fallback_name = _normalize_merge_name(fallback.get("name"))
    candidate_names = {_normalize_merge_name(candidate.get("name")) for candidate in candidates}
    if not normalized_name or normalized_name != fallback_name or normalized_name not in candidate_names:
        return None

    quantity = _normalize_merge_int(merged.get("quantity"))
    fallback_quantity = _normalize_merge_int(fallback.get("quantity"))
    if quantity != fallback_quantity:
        return None

    price = _normalize_merge_int(merged.get("price"))
    desc = _normalize_merge_text(merged.get("desc"))
    raw = _normalize_merge_text(merged.get("raw"))
    status = _normalize_merge_text(merged.get("status") or "active")
    source_url = _normalize_merge_text(merged.get("source_url"))
    source_urls = fallback.get("source_urls", [])

    if status != "active":
        return None
    if source_url and source_url not in source_urls:
        return None

    result = dict(fallback)
    result["name"] = fallback.get("name", "")
    result["quantity"] = fallback_quantity
    result["price"] = price
    result["desc"] = desc
    result["raw"] = raw
    result["status"] = "active"
    result["source_url"] = source_url or fallback.get("source_url", "")
    result["source_urls"] = source_urls
    result["merge_strategy"] = "llm"
    return result


def _merge_item_candidates(slot: dict, candidates: list[dict], llm_config: dict | None = None) -> dict:
    merge_strategy = "single_source" if len(candidates) == 1 else "fallback"
    fallback = _fallback_merge_item(candidates, merge_strategy=merge_strategy)
    if len(candidates) <= 1 or not llm_config or not _needs_llm_merge(candidates):
        return fallback

    try:
        merged = _call_merge_llm(slot, candidates, llm_config)
    except Exception:
        return _fallback_merge_item(candidates, merge_strategy="fallback")

    validated = _validate_llm_merge_result(merged, _fallback_merge_item(candidates, merge_strategy="fallback"), candidates)
    if not validated:
        return _fallback_merge_item(candidates, merge_strategy="fallback")
    return validated


def _openai_client(llm_config: dict) -> OpenAI:
    api_key = (llm_config.get("api_key") or "").strip() or None
    base_url = (llm_config.get("base_url") or "").rstrip("/") or None
    return OpenAI(api_key=api_key, base_url=base_url)


def _require_llm_config(llm_config: dict | None) -> tuple[str, str]:
    config = llm_config or {}
    base_url = (config.get("base_url") or "").rstrip("/")
    model = (config.get("model") or "").strip()
    if not base_url:
        raise LLMParseError("llm.base_url is required for parsing")
    if not model and not _get_model_candidates(config):
        raise LLMParseError("llm.model is required for parsing")
    return base_url, model


def _get_model_candidates(config: dict) -> list[str]:
    candidates = config.get("models") or config.get("model_candidates") or []
    if isinstance(candidates, list):
        return [str(item).strip() for item in candidates if str(item).strip()]
    return []


def _iter_models(config: dict) -> list[str]:
    primary = (config.get("model") or "").strip()
    candidates = _get_model_candidates(config)
    models = [primary] if primary else []
    for candidate in candidates:
        if candidate not in models:
            models.append(candidate)
    return models


def _build_merge_prompt(slot: dict, candidates: list[dict]) -> str:
    candidate_lines = []
    for index, candidate in enumerate(candidates, start=1):
        candidate_lines.append(
            json.dumps(
                {
                    "candidate_index": index,
                    "slot_time": slot.get("time", ""),
                    "slot_label": slot.get("label", ""),
                    "name": _normalize_merge_text(candidate.get("name", "")),
                    "quantity": _normalize_merge_int(candidate.get("quantity")),
                    "price": _normalize_merge_int(candidate.get("price")),
                    "desc": _normalize_merge_text(candidate.get("desc", "")),
                    "raw": _normalize_merge_text(candidate.get("raw", "")),
                    "status": "active",
                    "source_url": _normalize_merge_text(candidate.get("source_url", "")),
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(
        [
            "你要把同一时间段、同一商品的多个来源候选记录合并成一条最终记录。",
            "只能基于给定候选记录作答，不能猜测、补造、改名或改数量。",
            "只输出一个 JSON object，不能输出 Markdown、解释、代码块或额外字段。",
            "字段只能是 name、quantity、price、desc、raw、status、source_url。",
            "name 必须与候选商品名一致；quantity 必须与候选数量一致；status 必须固定为 active。",
            "source_url 必须直接从候选 source_url 中选择一个。",
            "price 只能填整数或 null；desc/raw 只能填字符串，可为空字符串。",
            "如果候选互补，就保留更完整且不冲突的信息；如果冲突，优先更具体、更像原文商品行的值。",
            '严格输出: {"name": str, "quantity": int|null, "price": int|null, "desc": str, "raw": str, "status": "active", "source_url": str}',
            "候选记录:",
            *candidate_lines,
        ]
    )


def _call_merge_llm(slot: dict, candidates: list[dict], llm_config: dict) -> dict | None:
    client = _openai_client(llm_config)
    models = _iter_models(llm_config)
    if not models:
        return None

    for candidate in models:
        try:
            response = client.chat.completions.create(
                model=candidate,
                messages=[
                    {"role": "system", "content": "你是一个严格输出 JSON 的数据合并器。"},
                    {"role": "user", "content": _build_merge_prompt(slot, candidates)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
                timeout=30,
            )
        except APIError as exc:
            message = str(exc)
            if "model" in message.lower() and any(token in message.lower() for token in ("not found", "does not exist", "unsupported", "invalid")):
                logger.warning("Merge LLM model unavailable, trying fallback model=%s error=%s", candidate, message)
                continue
            raise

        content = response.choices[0].message.content
        if not content:
            return None
        return json.loads(content)

    return None


def merge_parsed_sources(sources: list[dict], listen: list[str], llm_config: dict | None = None) -> dict:
    merged = {
        "title": "远行商人汇总",
        "published_at": "",
        "current_time": None,
        "normalized_text": "",
        "slots": [],
        "matches": [],
        "matched": False,
        "source_length": sum(item.get("source_length", 0) for item in sources),
        "source_url": "",
        "source_urls": [item.get("source_url", "") for item in sources if item.get("source_url")],
    }
    if not sources:
        return merged

    published_values = [item.get("published_at", "") for item in sources if item.get("published_at")]
    merged["published_at"] = max(published_values) if published_values else ""
    merged["current_time"] = next((item.get("current_time") for item in sources if item.get("current_time")), None)
    merged["normalized_text"] = "\n\n".join(
        item.get("normalized_text", "") for item in sources if item.get("normalized_text")
    )

    slot_map: dict[str, dict] = {}
    for source in sources:
        for slot in source.get("slots", []):
            time_key = slot.get("time", "")
            target = slot_map.setdefault(
                time_key,
                {
                    "id": slot.get("id", time_key),
                    "label": slot.get("label", ""),
                    "time": time_key,
                    "items": [],
                    "notes": [],
                    "status": "active",
                },
            )
            target.setdefault("_candidate_groups", {})
            for item in slot.get("items", []):
                candidate = dict(item)
                candidate["source_url"] = source.get("source_url", "")
                item_key = (_normalize_merge_name(candidate.get("name", "")), str(candidate.get("quantity", "")))
                target["_candidate_groups"].setdefault(item_key, []).append(candidate)

    merged_slots: list[dict] = []
    for slot in sorted(slot_map.values(), key=lambda slot: TIME_ORDER.get(slot.get("time", ""), 99)):
        candidate_groups = slot.pop("_candidate_groups", {})
        items = [
            _merge_item_candidates(slot, candidates, llm_config)
            for _, candidates in sorted(candidate_groups.items(), key=lambda entry: entry[0])
        ]
        slot["items"] = sorted(items, key=lambda item: (item.get("name", ""), str(item.get("quantity", ""))))
        merged_slots.append(slot)

    merged["slots"] = merged_slots
    merged["matches"] = _build_matches(merged_slots, listen)
    merged["matched"] = bool(merged["matches"])
    return merged


def _clean_text(value: str) -> str:
    value = unescape(value)
    value = re.sub(r"<[^>]+>", "", value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\r", "\n", value)
    value = re.sub(r"\n\s*\n+", "\n", value)
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip()


def _extract_first(pattern: str, html: str) -> str | None:
    match = re.search(pattern, html, re.I | re.S)
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_title(html: str) -> str | None:
    patterns = [
        r'<h1 class="widget-article-title">(.*?)</h1>',
        r'<h1[^>]*>(.*?)</h1>',
        r'<title>(.*?)</title>',
    ]
    for pattern in patterns:
        value = _extract_first(pattern, html)
        if value:
            return value
    return None


def _extract_published_at(html: str) -> str | None:
    patterns = [
        r'<span class="widget-article-info-num"><i[^>]*></i>(.*?)</span>',
        r'(20\d{2}-\d{1,2}-\d{1,2}\s+\d{2}:\d{2}:\d{2})',
        r'(20\d{2}-\d{1,2}-\d{1,2})',
    ]
    for pattern in patterns:
        value = _extract_first(pattern, html)
        if value:
            return value.split()[0]
    return None


def _extract_by_class(html: str, tag: str, article_class: str) -> str | None:
    class_names = [name for name in article_class.split() if name]
    if not class_names:
        return None
    class_pattern = r'[^\"]*'.join(re.escape(name) for name in class_names)
    pattern = rf'<{tag}[^>]*class="[^"]*{class_pattern}[^"]*"[^>]*>([\s\S]*?)</{tag}>'
    match = re.search(pattern, html, re.I)
    return match.group(1) if match else None


def _trim_article_html(article_html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", "", article_html, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", "", text, flags=re.I)
    stop_markers = [
        r'<div[^>]*class="[^"]*detail_keyword[^"]*"[^>]*>',
        r'<div[^>]*class="[^"]*relate_news[^"]*"[^>]*>',
        r'<div[^>]*class="[^"]*xg_gl[^"]*"[^>]*>',
        r'<div[^>]*class="[^"]*content_recommend[^"]*"[^>]*>',
        r'攻略汇总',
        r'相关攻略MORE\+',
        r'相关下载MORE\+',
        r'热门推荐',
        r'关于游侠',
        r'CopyRight ©',
        r'// 数据处理',
    ]
    cut_index = None
    for marker in stop_markers:
        match = re.search(marker, text, re.I)
        if match:
            cut_index = match.start() if cut_index is None else min(cut_index, match.start())
    if cut_index is not None:
        text = text[:cut_index]
    return text


def _extract_article_html(html: str, article_class: str | None = None) -> str:
    article_class = (article_class or "").strip()

    if article_class:
        for tag in ("article", "div", "section"):
            extracted = _extract_by_class(html, tag, article_class)
            if extracted:
                return _trim_article_html(extracted)

    specialized_patterns = [
        r'<article[^>]*class="[^"]*widget-article[^"]*"[^>]*>([\s\S]*?)</article>',
        r'<div[^>]*class="[^"]*ss-html-container article-content J-photoSwiper[^"]*"[^>]*>([\s\S]*?)<div class="vg-item vg-p">',
        r'<div[^>]*class="[^"]*ArticleBody[^"]*"[^>]*>([\s\S]*?)<div[^>]*class="[^"]*detail_keyword[^"]*"[^>]*>',
    ]
    for pattern in specialized_patterns:
        match = re.search(pattern, html, re.I)
        if match:
            return _trim_article_html(match.group(1))

    return _trim_article_html(html)


def normalize_article_text(article_html: str) -> str:
    text = article_html
    text = re.sub(r"<blockquote[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"</blockquote>", "\n", text, flags=re.I)
    text = re.sub(r"<h[1-6][^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"</h[1-6]>", "\n", text, flags=re.I)
    text = re.sub(r"<li[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"</li>", "\n", text, flags=re.I)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = _clean_text(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    return "\n".join(lines)


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


def _build_matches(slots: list[dict], listen: list[str]) -> list[dict]:
    current_time = _current_time_range()
    matches: list[dict] = []
    for slot in slots:
        if slot["time"] != current_time:
            continue
        for item in slot["items"]:
            if item["status"] != "active":
                continue
            for name in listen:
                if name and name in item["name"]:
                    matches.append(
                        {
                            "name": item["name"],
                            "listen": name,
                            "time": slot["time"],
                            "slot_label": slot["label"],
                            "quantity": item["quantity"],
                            "price": item["price"],
                            "desc": item["desc"],
                            "raw": item["raw"],
                        }
                    )
    return matches


def _strip_comments(value):
    if isinstance(value, dict):
        return {
            key: _strip_comments(val)
            for key, val in value.items()
            if not str(key).startswith("_")
        }
    if isinstance(value, list):
        return [_strip_comments(item) for item in value]
    return value


def _parse_json_object(content: str) -> dict:
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise LLMParseError(f"LLM returned invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise LLMParseError("LLM response must be a JSON object")
    return parsed


def _coerce_int(value) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    match = re.search(r"-?\d+", str(value))
    return int(match.group()) if match else None


def _coerce_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_date(value: Any) -> str:
    text = _coerce_str(value)
    if not text:
        return datetime.now().strftime("%Y-%m-%d")
    match = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", text)
    if match:
        year, month, day = match.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return datetime.now().strftime("%Y-%m-%d")


def _normalize_time_slot(value: Any, fallback: str = "") -> str:
    text = _coerce_str(value)
    normalized = text.replace("：", ":").replace("－", "-").replace("—", "-")
    if normalized in TIME_ORDER:
        return normalized
    if re.fullmatch(r"\d{1,2}\s*-\s*\d{1,2}", normalized):
        start, end = [part.strip() for part in normalized.split("-")]
        candidate = f"{int(start)}-{int(end)}"
        if candidate in TIME_ORDER:
            return candidate
    if fallback and fallback in TIME_ORDER:
        return fallback
    return ""


def _normalize_postprocess_status(value: Any, row_time: str, current_time: str | None) -> str:
    status = _coerce_str(value).lower()
    if row_time and current_time and row_time == current_time:
        return "active"
    if status in VALID_ITEM_STATUS:
        return status
    return "pending"


def _normalize_postprocess_row(row: Any, merged: dict) -> dict | None:
    if not isinstance(row, dict):
        return None

    current_time = merged.get("current_time")
    published_at = _normalize_date(merged.get("published_at"))
    row_time = _normalize_time_slot(row.get("time"), fallback=current_time or "")
    name = _coerce_str(row.get("name"))
    if not name:
        return None

    normalized = {
        "date": _normalize_date(row.get("date") or published_at),
        "time": row_time,
        "name": name,
        "quantity": _coerce_int(row.get("quantity")),
        "price": _coerce_int(row.get("price")),
        "status": _normalize_postprocess_status(row.get("status"), row_time, current_time),
        "desc": _coerce_str(row.get("desc")),
        "raw": _coerce_str(row.get("raw")),
    }
    if not normalized["time"]:
        return None
    return normalized


def _normalize_item(item: dict, source_url: str) -> dict | None:
    if not isinstance(item, dict):
        return None

    name = _coerce_str(item.get("name"))
    raw = _coerce_str(item.get("raw"))
    if not name and not raw:
        return None

    status = _coerce_str(item.get("status")) or "active"
    if status not in VALID_ITEM_STATUS:
        status = "active"

    return {
        "name": name or raw,
        "quantity": _coerce_int(item.get("quantity")),
        "price": _coerce_int(item.get("price")),
        "desc": _coerce_str(item.get("desc")),
        "raw": raw or name,
        "status": status,
        "source_url": _coerce_str(item.get("source_url")) or source_url,
    }


def _normalize_slot(slot: dict, index: int, source_url: str) -> dict | None:
    if not isinstance(slot, dict):
        return None

    time_value = _coerce_str(slot.get("time"))
    label = _coerce_str(slot.get("label")) or time_value or f"slot-{index}"
    status = _coerce_str(slot.get("status")) or "active"
    if status not in VALID_SLOT_STATUS:
        status = "active"

    raw_items = slot.get("items", [])
    if not isinstance(raw_items, list):
        return None

    items = []
    for item in raw_items:
        normalized_item = _normalize_item(item, source_url)
        if normalized_item:
            items.append(normalized_item)

    if not items:
        return None

    raw_notes = slot.get("notes", [])
    notes = []
    if isinstance(raw_notes, list):
        for note in raw_notes:
            note_text = _coerce_str(note)
            if note_text:
                notes.append(note_text)

    return {
        "id": _coerce_str(slot.get("id")) or time_value or f"slot-{index}",
        "label": label,
        "time": time_value,
        "items": items,
        "notes": notes,
        "status": status,
    }


def _build_parse_prompt(clean_text: str, source_url: str, listen: list[str]) -> str:
    listen_json = json.dumps([name for name in listen if name], ensure_ascii=False)
    return "\n".join(
        [
            "你会收到某个网页提取并清洗后的正文纯文本。",
            "请只基于给定正文提取结构化商品信息，不要猜测正文里不存在的信息。",
            "不要依赖网页标签或 DOM，因为这些已经被清洗掉了。",
            "目标是识别文章中的售卖时间段和对应商品，并返回严格 JSON。不要 Markdown，不要解释。",
            "如果正文里没有足够信息，返回空 slots。",
            "时间字段优先使用这些值：全天、8-12、12-16、16-20、20-24。",
            "每个 item 输出字段：name, quantity, price, desc, raw, status。",
            "quantity/price 无法确认时填 null。status 只能是 active、missed 之一。",
            "输出 JSON 结构示例:",
            '{"title": "", "published_at": "", "slots": [{"id": "", "label": "", "time": "", "items": [{"name": "", "quantity": null, "price": null, "desc": "", "raw": "", "status": "active"}], "notes": [], "status": "active"}]}',
            f"当前监听关键词仅供参考，不要求你生成 matches: {listen_json}",
            f"来源 URL: {source_url}",
            "正文开始:",
            clean_text,
        ]
    )


def _call_json_llm(messages: list[dict], llm_config: dict | None, log_context: str, error_prefix: str) -> tuple[dict, str]:
    config = llm_config or {}
    _require_llm_config(config)
    client = _openai_client(config)

    models = _iter_models(config)
    if not models:
        raise LLMParseError("llm.model is required for parsing")

    last_error = None
    for candidate in models:
        payload = {
            "model": candidate,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": 0,
        }
        _log_json("LLM 请求 payload", payload)
        try:
            response = client.chat.completions.create(
                model=candidate,
                messages=messages,
                response_format={"type": "json_object"},
                temperature=0,
                timeout=60,
            )
            content = response.choices[0].message.content
        except APIError as exc:
            message = str(exc)
            if "model" in message.lower() and any(token in message.lower() for token in ("not found", "does not exist", "unsupported", "invalid")):
                logger.warning("LLM model unavailable, trying fallback model=%s error=%s", candidate, message)
                last_error = LLMParseError(f"LLM model unavailable: {candidate}")
                continue
            last_error = LLMParseError(f"{error_prefix}: {exc}")
            break
        except (AttributeError, IndexError, TypeError, ValueError) as exc:
            last_error = LLMParseError(f"Unexpected LLM response shape: {exc}")
            break

        if not content:
            last_error = LLMParseError("LLM returned empty content")
            break

        logger.info(_format_block("LLM 原始输出", f"来源: {log_context}\n{content}"))
        return _parse_json_object(content), content

    raise last_error or LLMParseError(error_prefix)


def _call_parse_llm(clean_text: str, llm_config: dict | None, source_url: str, listen: list[str]) -> tuple[dict, str]:
    messages = [
        {"role": "system", "content": "你是一个严格输出 JSON 的网页正文结构化解析器。"},
        {"role": "user", "content": _build_parse_prompt(clean_text, source_url, listen)},
    ]
    return _call_json_llm(messages, llm_config, source_url, "LLM request failed")


def _build_postprocess_prompt(merged: dict, listen: list[str]) -> str:
    listen_json = json.dumps([name for name in listen if name], ensure_ascii=False)
    merged_json = json.dumps(_strip_comments(merged), ensure_ascii=False, indent=2)
    return "\n".join(
        [
            "你会收到 scan 接口 merged 字段的 JSON。",
            "请把它转换成结构化 JSON 行数据，严格输出 JSON object，不要 Markdown、不要解释、不要代码块。",
            "输出格式固定为: {'rows': [{'date': 'YYYY-MM-DD', 'time': '8-12|12-16|16-20|20-24|全天', 'name': str, 'quantity': int|null, 'price': int|null, 'status': 'active|pending|empty', 'desc': str, 'raw': str}]}。",
            "约束:",
            "1. date 不可为空，优先使用 merged.published_at；没有就用今天日期。",
            "2. time 必须是时间段，只能使用：全天、8-12、12-16、16-20、20-24。",
            "3. name 不可为空。",
            "4. quantity/price 获取不到时填 null。",
            "5. desc 是你基于视频标题/字幕/上下文整理出的描述。",
            "6. raw 是该行依赖的原始文本，尽量保留来源标题或原句。",
            "7. 同一个 date + time + name 只保留一条最合理结果。",
            f"当前监听关键词: {listen_json}",
            "merged JSON:",
            merged_json,
        ]
    )


def _normalize_postprocess_result(result: dict, merged: dict) -> dict:
    raw_rows = result.get("rows", [])
    if raw_rows is None:
        raw_rows = []
    if not isinstance(raw_rows, list):
        raise LLMParseError("LLM response field 'rows' must be a list")

    rows = []
    seen: set[tuple[str, str, str]] = set()
    for raw_row in raw_rows:
        row = _normalize_postprocess_row(raw_row, merged)
        if not row:
            continue
        key = (row["date"], row["time"], _normalize_merge_name(row["name"]))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    rows.sort(key=lambda row: (row["date"], TIME_ORDER.get(row["time"], 99), row["name"]))
    return {"rows": rows}


def post_process_scan_result(merged: dict, llm_config: dict | None, listen: list[str]) -> dict:
    messages = [
        {"role": "system", "content": "你是一个严格输出 JSON 的 CSV 行整理器。"},
        {"role": "user", "content": _build_postprocess_prompt(merged, listen)},
    ]
    llm_result, llm_raw_content = _call_json_llm(messages, llm_config, merged.get("source_url") or "merged", "LLM post-process failed")
    _log_json("LLM 二次处理后的 JSON", {"result": llm_result})
    postprocessed = _normalize_postprocess_result(llm_result, merged)
    postprocessed["llm_raw_content"] = llm_raw_content
    return postprocessed


def _normalize_llm_result(result: dict, normalized_text: str, source_length: int, source_url: str, listen: list[str]) -> dict:
    raw_slots = result.get("slots", [])
    if raw_slots is None:
        raw_slots = []
    if not isinstance(raw_slots, list):
        raise LLMParseError("LLM response field 'slots' must be a list")

    slots = []
    for index, slot in enumerate(raw_slots, start=1):
        normalized_slot = _normalize_slot(slot, index, source_url)
        if normalized_slot:
            slots.append(normalized_slot)

    parsed = {
        "title": _coerce_str(result.get("title")) or None,
        "published_at": _coerce_str(result.get("published_at")) or None,
        "current_time": _current_time_range(),
        "normalized_text": normalized_text,
        "slots": sorted(slots, key=lambda slot: TIME_ORDER.get(slot.get("time", ""), 99)),
        "source_length": source_length,
        "source_url": source_url,
    }
    parsed["matches"] = _build_matches(parsed["slots"], listen)
    parsed["matched"] = bool(parsed["matches"])
    return _strip_comments(parsed)


def parse_article_content(html: str, parse_config: dict, listen: list[str], llm_config: dict, source_url: str) -> dict:
    article_html = _extract_article_html(html, parse_config.get("article_class"))
    logger.info(_format_block("正文区域 HTML", f"来源: {source_url}\n{article_html}"))

    normalized_text = normalize_article_text(article_html)
    logger.info(_format_block("清洗后的正文", f"来源: {source_url}\n{normalized_text}"))
    if not normalized_text:
        raise LLMParseError("Article text is empty after cleaning")

    llm_result, llm_raw_content = _call_parse_llm(normalized_text, llm_config, source_url, listen)
    _log_json("LLM 解析后的 JSON", {"source_url": source_url, "result": llm_result})

    parsed = _normalize_llm_result(llm_result, normalized_text, len(html), source_url, listen)
    parsed["_debug"] = {
        "raw_html": html,
        "article_html": article_html,
        "normalized_text_before_llm": normalized_text,
        "llm_raw_content": llm_raw_content,
    }

    if not parsed["title"]:
        parsed["title"] = _extract_title(html)
    if not parsed["published_at"]:
        parsed["published_at"] = _extract_published_at(html)

    _log_json("标准化后的解析结果", {"source_url": source_url, "result": _strip_comments(parsed)})
    return parsed


def render_markdown(parsed: dict) -> str:
    lines: list[str] = []
    if parsed.get("title"):
        lines.append(f"# {parsed['title']}")
        lines.append("")
    if parsed.get("published_at"):
        lines.append(f"更新时间：{parsed['published_at']}")
        lines.append("")

    for slot in parsed.get("slots", []):
        lines.append(f"## {slot['label']}")
        lines.append("")

        for item in slot.get("items", []):
            quantity = f"*{item['quantity']}" if item.get("quantity") is not None else ""
            price = f"｜价格{item['price']}" if item.get("price") is not None else ""
            lines.append(f"- {item['name']}{quantity}{price}")
            if item.get("desc"):
                lines.append(f"  - {item['desc']}")

        lines.append("")

    markdown = "\n".join(lines).strip()
    logger.info(_format_block("解析后的 Markdown", markdown))
    return markdown


def parse_with_llm(html: str, llm_config: dict, products: list[dict]) -> dict:
    listen = [item.get("name", "") for item in products if item.get("name")]
    return parse_article_content(html, {"article_class": llm_config.get("article_class")}, listen, llm_config, "")


def llm_input_from_html(html: str, article_class: str | None = None) -> str:
    article_html = _extract_article_html(html, article_class)
    return normalize_article_text(article_html)
