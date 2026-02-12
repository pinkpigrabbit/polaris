from __future__ import annotations

from fastapi import FastAPI

from .api.corporate_actions import router as ca_router
from .api.instruments import router as instrument_router
from .api.nav import router as nav_router
from .api.process import router as process_router
from .api.staging import router as staging_router


app = FastAPI(title="polaris")
app.include_router(staging_router)
app.include_router(process_router)
app.include_router(nav_router)
app.include_router(ca_router)
app.include_router(instrument_router)
