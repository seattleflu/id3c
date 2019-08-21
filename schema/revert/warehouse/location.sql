-- Revert seattleflu/schema:warehouse/location from pg

begin;

drop table warehouse.location;

commit;
