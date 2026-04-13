import requests


def _normalize_headers(headers: dict[str, str] | None) -> dict[str, str] | None:
    if not headers:
        return None
    return {
        str(key): str(value)
        for key, value in headers.items()
        if key and not str(key).startswith("_") and value is not None
    }


def fetch_html(url: str, headers: dict[str, str] | None = None) -> str:
    resp = requests.get(url, headers=_normalize_headers(headers), timeout=20)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text
