from fastapi import APIRouter, Depends

from app.api.service_auth import require_service_caller

from app.api.v1.upload import router as upload_router
from app.api.v1.search import router as search_router
from app.api.v1.logs import router as logs_router
from app.api.v1.log_sources import router as log_sources_router
from app.api.v1.saved_views import router as saved_views_router
from app.api.v1.customers import router as customers_router
from app.api.v1.notifications import router as notifications_router
from app.api.v1.analytics import router as analytics_router
from app.api.v1.teams import router as teams_router
from app.api.v1.teams_access import router as teams_access_router
from app.api.v1.analytics_agent import router as analytics_agent_router
from app.api.v1.analytics_forecast import router as analytics_forecast_router
from app.api.v1.analytics_at_risk import router as analytics_at_risk_router

# Every route under /api/v1 resolves its caller first; /health is on the app itself and stays open.
api_router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_service_caller)])
api_router.include_router(upload_router)
api_router.include_router(search_router)
api_router.include_router(logs_router)
api_router.include_router(log_sources_router)
api_router.include_router(saved_views_router)
api_router.include_router(customers_router)
api_router.include_router(notifications_router)
api_router.include_router(analytics_router)
api_router.include_router(analytics_agent_router)
api_router.include_router(analytics_forecast_router)
api_router.include_router(analytics_at_risk_router)
api_router.include_router(teams_router)
api_router.include_router(teams_access_router)
