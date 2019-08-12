-- Revert seattleflu/schema:receiving/schema from pg

begin;

set local role id3c;

drop schema receiving;

commit;
