-- Revert seattleflu/schema:warehouse/sample from pg

begin;

drop table warehouse.sample;

commit;
