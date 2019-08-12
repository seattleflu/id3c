-- Revert seattleflu/schema:warehouse/target from pg

begin;

set local role id3c;

drop table warehouse.target;

commit;
