-- Revert seattleflu/schema:warehouse/target from pg

begin;

drop table warehouse.target;

commit;
