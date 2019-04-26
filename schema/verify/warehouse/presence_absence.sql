-- Verify seattleflu/schema:warehouse/presence_absence on pg

begin;

select pg_catalog.has_table_privilege('warehouse.presence_absence', 'select');

rollback;
