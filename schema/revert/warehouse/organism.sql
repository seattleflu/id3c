-- Revert seattleflu/schema:warehouse/organism from pg

begin;

set local role id3c;

drop table warehouse.organism;

commit;
