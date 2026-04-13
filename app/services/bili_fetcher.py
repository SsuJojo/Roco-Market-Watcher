import asyncio
import logging
import re
import sys
from pathlib import Path

# Fix path for bilibili_api
LI_PATH = Path(__file__).resolve().parents[1] / "libs" / "bili-api"
if str(LI_PATH) not in sys.path:
    sys.path.append(str(LI_PATH))

from bilibili_api import user

logger = logging.getLogger(__name__)

async def _get_video_list(uid: int) -> list[str]:
    """Get video titles for a given UID using bilibili_api."""
    try:
        u = user.User(uid)
        res = await u.get_videos()
        # The response structure usually has 'list' containing video info
        # inside 'list' is 'vlist' which is a list of videos
        vlist = res.get("list", {}).get("vlist", [])
        titles = [v.get("title", "") for v in vlist if v.get("title")]
        return titles
    except Exception as e:
        logger.error(f"Failed to fetch Bilibili videos for UID {uid}: {e}")
        return []

def fetch_bili_video_titles(uid: int) -> list[str]:
    """Sync wrapper for _get_video_list."""
    try:
        # Check if there is an existing event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # This should not happen in a standard FastAPI sync route
                # But if it does, we might need another approach
                return [] 
        except Exception:
            pass
            
        return asyncio.run(_get_video_list(uid))
    except Exception as e:
        logger.error(f"Error in fetch_bili_video_titles: {e}")
        return []

def extract_uid(url: str) -> int | None:
    """Extract UID from a Bilibili space URL."""
    match = re.search(r"space\.bilibili\.com/(\d+)", url)
    if match:
        return int(match.group(1))
    return None
