-- Revert seattleflu/schema:warehouse/encounter from pg

begin;

set local role id3c;

drop table warehouse.encounter;

commit;
