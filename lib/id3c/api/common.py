"""
Shared functions used in route logic.
"""

import json
from hashlib import sha1
from datetime import datetime

def add_provenance(document, request):
    """
    Adds a provenance field to a document based on an incoming request
    """
    # calculate hash based on serialized sample record
    digest = sha1(json.dumps(document, sort_keys=True).encode()).hexdigest()

    provenance = {
        "source": request.referrer or request.remote_addr or "?",
        "method": request.method,
        "path": request.path,
        "timestamp": f'{datetime.now():%Y-%m-%d %H:%M:%S%z}',
        "sha1sum": digest
    }
    document.update(_provenance=provenance)
