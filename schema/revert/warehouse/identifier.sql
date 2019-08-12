-- Revert seattleflu/schema:warehouse/identifier from pg

begin;

set local role id3c;

drop table warehouse.identifier;
drop table warehouse.identifier_set;

commit;
