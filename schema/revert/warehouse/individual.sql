-- Revert seattleflu/schema:warehouse/individual from pg

begin;

drop table warehouse.individual;

commit;
