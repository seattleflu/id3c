-- Verify seattleflu/schema:warehouse/target on pg

begin;

select pg_catalog.has_table_privilege('warehouse.target', 'select');

rollback;
