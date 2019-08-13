-- Revert seattleflu/schema:citext from pg

begin;

drop extension citext;

commit;
