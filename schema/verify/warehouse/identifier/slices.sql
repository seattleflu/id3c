-- Verify seattleflu/schema:warehouse/identifier/slices on pg

begin;

select slices from warehouse.identifier limit 1;

rollback;
