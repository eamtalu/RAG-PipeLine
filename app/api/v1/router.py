from fastapi import APIRouter

from app.api.v1.upload import router as upload_router
from app.api.v1.search import router as search_router
from app.api.v1.logs import router as logs_router
from app.api.v1.log_sources import router as log_sources_router
from app.api.v1.saved_views import router as saved_views_router
from app.api.v1.customers import router as customers_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(upload_router)
api_router.include_router(search_router)
api_router.include_router(logs_router)
api_router.include_router(log_sources_router)
api_router.include_router(saved_views_router)
api_router.include_router(customers_router)