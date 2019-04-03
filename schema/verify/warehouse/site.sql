-- Verify seattleflu/schema:warehouse/site on pg

begin;

select pg_catalog.has_table_privilege('warehouse.site', 'select');

rollback;
