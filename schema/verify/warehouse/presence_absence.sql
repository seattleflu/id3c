-- Verify seattleflu/schema:warehouse/presence_absence on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('warehouse.presence_absence', 'select');

rollback;
