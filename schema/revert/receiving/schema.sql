-- Revert seattleflu/schema:receiving/schema from pg

begin;

drop schema receiving;

commit;
