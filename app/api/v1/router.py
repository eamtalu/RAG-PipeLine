from fastapi import APIRouter

from app.api.v1.upload import router as upload_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(upload_router)

#include more if needed
#api_router.include_router(another_upload_router)