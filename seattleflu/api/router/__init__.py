"""
Blueprints for API routes.
"""
from . import enrollment

routers = [
    enrollment,
]

blueprints = [ router.blueprint for router in routers ] # type: ignore
