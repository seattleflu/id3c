VERIFY_BARCODE_USES_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "barcode": {
                "type": "string"
            },
            "use": {
                "type": "string"
            }
        },
        "required": [
            "barcode",
            "use"
        ]
    }
}

POST_SAMPLE_SCHEMA = {
    "type": "object",
    "properties": {
        "sample_id": {
            "type": "string",
            "minLength": 8,
            "maxLength": 8
        },
        "collection_id": {
            "type": "string",
            "minLength": 8,
            "maxLength": 8
        },
        "collection_date": {
            "type": "string",
            "format": "date"
        },
        "sample_origin": {
            "type": "string"
        },
        "swab_site": {
            "type": "string"
        },
        "clia_id": {
            "type": "string",
            "minLength": 8,
            "maxLength": 8
        },
        "received_date": {
            "type": "string",
            "format": "date"
        },
        "aliquot_a": {
            "type": "string"
        },
        "aliquot_b": {
            "type": "string"
        },
        "aliquot_c": {
            "type": "string"
        },
        "aliquoted_date": {
            "type": "string",
            "format": "date"
        },
        "rack_a": {
            "type": "string"
        },
        "rack_a_nickname": {
            "type": "string"
        },
        "rack_b": {
            "type": "string"
        },
        "rack_b_nickname": {
            "type": "string"
        },
        "rack_c": {
            "type": "string"
        },
        "rack_c_nickname": {
            "type": "string"
        },
        "swab_type": {
            "type": "string",
            "enum": ["ans", "mtb", "np", "tiny", "unk", "none"]
        },
        "collection_matrix": {
            "type": "string",
            "enum": ["dry", "utm_vtm", "pbs", "none"]
        },
        "notes": {
            "type": "string"
        }
    },
    "anyOf": [
        { "required":
            [ "sample_id" ] },
        { "required":
            [ "collection_id" ] }
    ],
    "additionalProperties": False
}

POST_INCIDENT_SCHEMA = {
    "type": "object",
    "properties": {
        "collection": {
            "type": "string",
            "minLength": 8,
            "maxLength": 8
        },
        "incident_date": {
            "type": "string",
            "format": "date"
        },
        "failure_type": {
            "type": "string"
        },
        "swab_type": {
            "type": "string",
            "enum": ["ans", "mtb", "np", "tiny", "unk", "none"]
        },
        "collection_matrix": {
            "type": "string",
            "enum": ["dry", "utm_vtm", "pbs", "none"]
        },
        "corrective_action": {
            "type": "string",
            "enum": ["discarded", "continued processing"]
        }
    },
    "required":
        [ "collection" ],
    "additionalProperties": False
}
