-- Verify seattleflu/schema:warehouse/encounter on pg

begin;

select pg_catalog.has_table_privilege('warehouse.encounter', 'select');

rollback;
