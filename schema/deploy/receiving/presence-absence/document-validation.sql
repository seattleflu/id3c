-- Deploy seattleflu/schema:receiving/presence-absence/document-validation to pg
-- requires: receiving/presence-absence
-- requires: functions/validate_json_schema

-- This constraint is in a separate change from the table definition so that it
-- may be easily reworked with sqitch later.

begin;

alter table receiving.presence_absence
    add constraint presence_absence_document_validates check (
        validate_json_schema($$
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "$id": "https://seattleflu.org/_schema/presence-absence.json",
              "type": "object",
              "title": "presence/absence document",
              "required": [
                "calls"
              ],
              "properties": {
                "calls": {
                  "type": "array",
                  "title": "set of call results",
                  "minItems": 1,
                  "items": {
                    "type": "object",
                    "title": "individual call result for a (source, target) pair",
                    "required": [
                      "source",
                      "target",
                      "present"
                    ],
                    "properties": {
                      "source": {
                        "type": "object",
                        "title": "source material characterized by this call",
                        "description": "details such as unique identifiers",
                        "minProperties": 1
                      },
                      "target": {
                        "type": "object",
                        "title": "known target material being probed for within the source material",
                        "description": "details such as unique identifiers",
                        "minProperties": 1
                      },
                      "present": {
                        "type": ["boolean", "null"],
                        "title": "result of presence/absence call",
                        "description": "present (true), absent (false), indeterminate (null)"
                      }
                    }
                  }
                }
              }
            } $$,
            document::jsonb
        ) is true
    );

commit;
