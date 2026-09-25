from fastapi import APIRouter
from .objects import router as objects_router
from .nodes import router as nodes_router
from .replication import router as replication_router
from .integrity import router as integrity_router
from .rebalance import router as rebalance_router
from .system import router as system_router
from .demo import router as demo_router
from .resistore import router as resistore_router
from .s3_gateway import router as s3_router
from .chaos import router as chaos_router

api_router = APIRouter()
api_router.include_router(objects_router)
api_router.include_router(nodes_router)
api_router.include_router(replication_router)
api_router.include_router(integrity_router)
api_router.include_router(rebalance_router)
api_router.include_router(demo_router)
api_router.include_router(resistore_router)
api_router.include_router(system_router)
api_router.include_router(s3_router)
api_router.include_router(chaos_router)

__all__ = ["api_router"]
