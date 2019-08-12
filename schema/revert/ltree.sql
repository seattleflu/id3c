-- Revert seattleflu/schema:ltree from pg

begin;

set local role id3c;

drop extension ltree;

commit;
