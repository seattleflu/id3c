-- Verify seattleflu/schema:staging/enrollment on pg

begin;

select pg_catalog.has_table_privilege('staging.enrollment', 'select');

rollback;
