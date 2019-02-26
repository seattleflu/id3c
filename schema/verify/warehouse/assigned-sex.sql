-- Verify seattleflu/schema:warehouse/assigned-sex on pg

begin;

select pg_catalog.pg_type_is_visible('warehouse.assigned_sex'::regtype);

rollback;
