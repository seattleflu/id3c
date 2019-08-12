-- Revert seattleflu/schema:warehouse/individual from pg

begin;

set local role id3c;

drop table warehouse.individual;

commit;
