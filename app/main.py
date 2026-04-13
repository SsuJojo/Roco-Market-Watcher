from fastapi import FastAPI
from app.routers import health, monitor

app = FastAPI(title="洛克王国远行商人监控", version="0.1.0")
app.include_router(health.router)
app.include_router(monitor.router, prefix="/api")
