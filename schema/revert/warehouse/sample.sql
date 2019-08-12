-- Revert seattleflu/schema:warehouse/sample from pg

begin;

set local role id3c;

drop table warehouse.sample;

commit;
