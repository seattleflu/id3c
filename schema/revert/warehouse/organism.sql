-- Revert seattleflu/schema:warehouse/organism from pg

begin;

drop table warehouse.organism;

commit;
