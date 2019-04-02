-- Revert seattleflu/schema:shipping/schema from pg

begin;

drop schema shipping;

commit;
