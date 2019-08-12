-- Revert seattleflu/schema:roles/longitudinal-processor/create from pg

begin;

set local role id3c;

drop role "longitudinal-processor";

commit;
