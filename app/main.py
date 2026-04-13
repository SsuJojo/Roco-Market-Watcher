import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routers import health, monitor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        monitor.run_startup_scan()
    except Exception:
        logger.exception("Startup scan failed")
    yield


app = FastAPI(title="洛克王国远行商人监控", version="0.1.0", lifespan=lifespan)
app.include_router(health.router)
app.include_router(monitor.router, prefix="/api")
