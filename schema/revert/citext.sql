-- Revert seattleflu/schema:citext from pg

begin;

set local role id3c;

drop extension citext;

commit;
