-- Verify seattleflu/schema:shipping/schema on pg

begin;

select 1/pg_catalog.has_schema_privilege('shipping', 'usage')::int;

rollback;
