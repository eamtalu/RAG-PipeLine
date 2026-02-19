from fastapi import APIRouter

from app.api.upload import router as upload_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(upload_router)
