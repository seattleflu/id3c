-- Verify seattleflu/schema:warehouse/target on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('warehouse.target', 'select');

rollback;
