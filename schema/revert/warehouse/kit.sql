-- Revert seattleflu/schema:warehouse/kit from pg

begin;

drop table warehouse.kit;

commit;
