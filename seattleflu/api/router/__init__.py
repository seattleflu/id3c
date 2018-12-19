"""
Blueprints for API routes.
"""
from . import enrollment, scan

routers = [
    enrollment,
    scan,
]

blueprints = [ router.blueprint for router in routers ] # type: ignore
