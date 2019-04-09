-- Revert seattleflu/schema:functions/validate_json_schema from pg

begin;

drop function validate_json_schema(jsonb, jsonb, jsonb);
drop function _validate_json_schema_type(text, jsonb);

commit;
