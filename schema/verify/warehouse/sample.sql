-- Verify seattleflu/schema:warehouse/sample on pg

begin;

select pg_catalog.has_table_privilege('warehouse.sample', 'select');

rollback;
