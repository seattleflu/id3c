-- Revert seattleflu/schema:warehouse/schema from pg

begin;

drop schema warehouse;

commit;
