-- Revert seattleflu/schema:warehouse/kit from pg

begin;

set local role id3c;

drop table warehouse.kit;

commit;
