-- Revert seattleflu/schema:ltree from pg

begin;

drop extension ltree;

commit;
