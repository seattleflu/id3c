-- Verify seattleflu/schema:warehouse/individual on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('warehouse.individual', 'select');

rollback;
