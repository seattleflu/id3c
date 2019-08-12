-- Revert seattleflu/schema:warehouse/presence_absence from pg

begin;

set local role id3c;

drop table warehouse.presence_absence;

commit;
