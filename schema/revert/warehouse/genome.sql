-- Revert seattleflu/schema:warehouse/genome from pg

begin;

drop table warehouse.genome;

commit;
