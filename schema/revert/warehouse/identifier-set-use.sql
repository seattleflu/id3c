-- Revert seattleflu/schema:warehouse/identifier-set-use from pg

begin;

drop table warehouse.identifier_set_use;

commit;
