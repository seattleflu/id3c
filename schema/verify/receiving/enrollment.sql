-- Verify seattleflu/schema:receiving/enrollment on pg

begin;

select pg_catalog.has_table_privilege('receiving.enrollment', 'select');

rollback;
