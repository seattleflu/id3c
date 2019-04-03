-- Revert seattleflu/schema:warehouse/encounter from pg

begin;

drop table warehouse.encounter;

commit;
