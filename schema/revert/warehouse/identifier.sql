-- Revert seattleflu/schema:warehouse/identifier from pg

begin;

drop table warehouse.identifier;
drop table warehouse.identifier_set;

commit;
