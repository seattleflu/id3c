-- Verify seattleflu/schema:warehouse/individual on pg

begin;

select pg_catalog.has_table_privilege('warehouse.individual', 'select');

rollback;
