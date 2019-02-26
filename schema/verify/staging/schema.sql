-- Verify seattleflu/schema:staging/schema on pg

begin;

select 1/pg_catalog.has_schema_privilege('staging', 'usage')::int;

rollback;
