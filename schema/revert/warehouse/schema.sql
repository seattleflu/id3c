-- Revert seattleflu/schema:warehouse/schema from pg

begin;

set local role id3c;

drop schema warehouse;

commit;
