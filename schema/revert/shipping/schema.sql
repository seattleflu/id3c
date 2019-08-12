-- Revert seattleflu/schema:shipping/schema from pg

begin;

set local role id3c;

drop schema shipping;

commit;
