def should_notify(parsed: dict, listen: list[str]) -> bool:
    return bool(parsed.get("matches"))
