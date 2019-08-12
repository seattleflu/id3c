-- Verify seattleflu/schema:warehouse/sample on pg

begin;

set local role id3c;

select pg_catalog.has_table_privilege('warehouse.sample', 'select');

rollback;
