-- Verify seattleflu/schema:warehouse/schema on pg

begin;

select 1/pg_catalog.has_schema_privilege('warehouse', 'usage')::int;

rollback;
