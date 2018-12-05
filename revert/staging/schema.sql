-- Revert seattleflu/schema:staging/schema from pg

begin;

drop schema staging;

commit;
