-- Verify seattleflu/schema:warehouse/schema on pg

begin;

set local role id3c;

select 1/pg_catalog.has_schema_privilege('warehouse', 'usage')::int;

rollback;
