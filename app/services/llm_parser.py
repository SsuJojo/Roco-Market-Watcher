import json
import re
from datetime import datetime
from html import unescape

import requests


SECTION_PATTERNS = [
    (r"全天售卖|全天销售商品", "all_day", "全天售卖", "全天"),
    (r"8\s*(?:am)?\s*[-—–~到至]\s*12\s*(?:pm)?|8-12", "8-12", "8-12限时", "8-12"),
    (r"12\s*(?:pm)?\s*[-—–~到至]\s*16|12-16", "12-16", "12-16限时", "12-16"),
    (r"16\s*[-—–~到至]\s*20|16-20", "16-20", "16-20限时", "16-20"),
    (r"20\s*[-—–~到至]\s*24|20-24", "20-24", "20-24限时", "20-24"),
]
TIME_ORDER = {"全天": 0, "8-12": 1, "12-16": 2, "16-20": 3, "20-24": 4}
DESC_TOKENS = ["推荐买", "使用途径", "捕捉", "培养材料", "合成", "获取方式", "随意", "血脉"]


def _normalize_merge_name(value: str) -> str:
    return re.sub(r"\s+", "", value or "").strip().lower()


def _item_score(item: dict) -> tuple:
    return (
        1 if item.get("price") not in (None, "") else 0,
        len(item.get("desc", "")),
        len(item.get("raw", "")),
    )


def _fallback_merge_item(candidates: list[dict]) -> dict:
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
    result["price"] = best_price if best_price is not None else result.get("price")
    result["desc"] = best_desc or result.get("desc", "")
    result["raw"] = best_raw or result.get("raw", "")
    result["source_urls"] = source_urls
    result["source_url"] = result.get("source_url") or (source_urls[0] if source_urls else "")
    return result


def _llm_headers(llm_config: dict) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    api_key = (llm_config.get("api_key") or "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _build_merge_prompt(slot: dict, candidates: list[dict]) -> str:
    candidate_lines = []
    for index, candidate in enumerate(candidates, start=1):
        candidate_lines.append(
            json.dumps(
                {
                    "candidate_index": index,
                    "slot_time": slot.get("time", ""),
                    "slot_label": slot.get("label", ""),
                    "name": candidate.get("name", ""),
                    "quantity": candidate.get("quantity"),
                    "price": candidate.get("price"),
                    "desc": candidate.get("desc", ""),
                    "raw": candidate.get("raw", ""),
                    "status": candidate.get("status", "active"),
                    "source_url": candidate.get("source_url", ""),
                },
                ensure_ascii=False,
            )
        )

    return "\n".join(
        [
            "你要把同一时间段、同一商品的多个来源候选记录合并成一条最终记录。",
            "只基于给定结构化字段判断，不要猜测不存在的信息。",
            "优先保留：明确价格、最完整且不冲突的描述、最可读的原始行。",
            "如果多个来源互补，就合并；如果冲突，优先更具体、更完整、更像原文商品行的值。",
            "返回严格 JSON，不要 Markdown，不要解释。",
            '输出格式: {"name": str, "quantity": int|null, "price": int|null, "desc": str, "raw": str, "status": "active", "source_url": str}',
            "候选记录:",
            *candidate_lines,
        ]
    )


def _call_merge_llm(slot: dict, candidates: list[dict], llm_config: dict) -> dict | None:
    base_url = (llm_config.get("base_url") or "").rstrip("/")
    model = (llm_config.get("model") or "").strip()
    if not base_url or not model:
        return None

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一个严格输出 JSON 的数据合并器。"},
            {"role": "user", "content": _build_merge_prompt(slot, candidates)},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0,
    }

    response = requests.post(
        f"{base_url}/chat/completions",
        headers=_llm_headers(llm_config),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    content = data["choices"][0]["message"]["content"]
    if not content:
        return None
    return json.loads(content)


def _merge_item_candidates(slot: dict, candidates: list[dict], llm_config: dict | None = None) -> dict:
    fallback = _fallback_merge_item(candidates)
    if len(candidates) <= 1 or not llm_config:
        return fallback

    try:
        merged = _call_merge_llm(slot, candidates, llm_config)
    except Exception:
        return fallback

    if not isinstance(merged, dict):
        return fallback

    result = dict(fallback)
    for field in ("name", "quantity", "price", "desc", "raw", "status", "source_url"):
        value = merged.get(field)
        if value in (None, "") and field in {"desc", "raw"}:
            continue
        if value is not None:
            result[field] = value

    result["status"] = "active"
    result["source_urls"] = fallback.get("source_urls", [])
    result["source_url"] = result.get("source_url") or fallback.get("source_url", "")
    return result


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


def _normalize_section(line: str) -> tuple[str, str, str] | None:
    normalized = line.replace("【", "").replace("】", "").replace("（", "(").replace("）", ")")
    for pattern, section_id, display_name, time_value in SECTION_PATTERNS:
        if re.search(pattern, normalized, re.I):
            return section_id, display_name, time_value
    return None


def _parse_item_line(line: str) -> dict | None:
    text = re.sub(r"^\d+[、.．]\s*", "", line).strip("：: ")
    if text in {"无", "【无】", "未完待续……", "未完待续", "暂无", "未更新", "未更新商品"}:
        return {
            "name": text.replace("【", "").replace("】", ""),
            "quantity": None,
            "price": None,
            "desc": "",
            "raw": line,
            "status": "empty" if "无" in text else "pending",
        }

    price_match = re.search(r"价格\s*(\d+)", text)
    quantity_match = re.search(r"(.+?)[*＊×xX](\d+)", text)

    name = text
    quantity = None
    if quantity_match:
        name = quantity_match.group(1).strip("｜|｜:：（）() ")
        quantity = int(quantity_match.group(2))
    if price_match:
        name = re.sub(r"[｜|]?[（(]?\s*价格\s*\d+.*?$", "", name).strip("｜|｜:：（）() ")

    if not quantity_match and not price_match:
        return None

    return {
        "name": name,
        "quantity": quantity,
        "price": int(price_match.group(1)) if price_match else None,
        "desc": "",
        "raw": line,
        "status": "active",
    }


def _parse_slots(normalized_text: str) -> list[dict]:
    lines = [line.strip() for line in normalized_text.split("\n") if line.strip()]
    slots: list[dict] = []
    current: dict | None = None

    for line in lines:
        section = _normalize_section(line)
        if section:
            current = {
                "id": section[0],
                "label": section[1],
                "time": section[2],
                "items": [],
                "notes": [],
                "status": "active",
            }
            slots.append(current)
            continue

        if current is None:
            continue

        item = _parse_item_line(line)
        if item:
            current["items"].append(item)
            if item["status"] != "active":
                current["status"] = item["status"]
            continue

        if current["items"] and any(token in line for token in DESC_TOKENS):
            current["items"][-1]["desc"] = line
            continue

        if current["items"] and line.startswith("PS"):
            break

        if line in {"攻略汇总", "官网网址", "热门推荐"}:
            break

        current["notes"].append(line)

    return slots


def _sort_slots(slots: list[dict]) -> list[dict]:
    return sorted(slots, key=lambda slot: TIME_ORDER.get(slot.get("time", ""), 99))


def _active_slots(slots: list[dict]) -> list[dict]:
    active_slots: list[dict] = []
    for slot in _sort_slots(slots):
        items = [item for item in slot.get("items", []) if item.get("status") == "active"]
        if not items:
            continue
        active_slots.append(
            {
                **slot,
                "items": items,
                "notes": [],
                "status": "active",
            }
        )
    return active_slots


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


def parse_article_content(html: str, parse_config: dict, listen: list[str]) -> dict:
    title = _extract_title(html)
    published_at = _extract_published_at(html)
    article_html = _extract_article_html(html, parse_config.get("article_class"))
    normalized_text = normalize_article_text(article_html)
    slots = _parse_slots(normalized_text)
    matches = _build_matches(slots, listen)

    return _strip_comments({
        "title": title,
        "published_at": published_at,
        "current_time": _current_time_range(),
        "normalized_text": normalized_text,
        "slots": _active_slots(slots),
        "matches": matches,
        "matched": bool(matches),
        "source_length": len(html),
    })


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

    return "\n".join(lines).strip()


def parse_with_llm(html: str, llm_config: dict, products: list[dict]) -> dict:
    listen = [item.get("name", "") for item in products if item.get("name")]
    return parse_article_content(html, llm_config, listen)


def llm_input_from_html(html: str, article_class: str | None = None) -> str:
    article_html = _extract_article_html(html, article_class)
    return normalize_article_text(article_html)
