import asyncio
import sys

sys.path.append("app/libs/bili-api")

from bilibili_api import user
from bilibili_api.utils import network


async def main():
    u = user.User(3546898422041050)
    api = network.Api(**user.API["info"]["video"], credential=u.credential).update_params(
        mid=u.get_uid(),
        ps=30,
        tid=0,
        pn=1,
        keyword="",
        order=user.VideoOrder.PUBDATE.value,
        order_avoided=True,
        platform="web",
        w_webid=await u.get_access_id(),
    )
    client = network.get_client()
    resp = await client.request(**(await api._prepare_request()))
    raw = resp.raw
    idx = raw.find(b'"title"')
    print("status", resp.code)
    print("content_type", resp.headers.get("content-type"))
    print("idx", idx)
    print(raw[idx:idx + 240])
    print(raw[idx:idx + 240].decode("utf-8", errors="replace"))


asyncio.run(main())
