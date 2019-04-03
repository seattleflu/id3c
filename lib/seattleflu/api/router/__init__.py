"""
Blueprints for API routes.
"""
from . import root, enrollment, scan

routers = [
    root,
    enrollment,
    scan,
]

blueprints = [ router.blueprint for router in routers ] # type: ignore
