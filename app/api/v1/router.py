from fastapi import APIRouter

from app.api.v1.upload import router as upload_router
from app.api.v1.search import router as search_router
from app.api.v1.logs import router as logs_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(upload_router)
api_router.include_router(search_router)
api_router.include_router(logs_router)