-- Verify seattleflu/schema:warehouse/assigned-sex on pg

begin;

set local role id3c;

select pg_catalog.pg_type_is_visible('warehouse.assigned_sex'::regtype);

rollback;
