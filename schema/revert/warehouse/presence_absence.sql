-- Revert seattleflu/schema:warehouse/presence_absence from pg

begin;

drop table warehouse.presence_absence;

commit;
