from pydantic import BaseModel
from typing import Any

class ParsedItem(BaseModel):
    name: str
    price: str | None = None
    raw: dict[str, Any] | None = None

class ParseResponse(BaseModel):
    items: list[ParsedItem]
    triggered: bool = False
