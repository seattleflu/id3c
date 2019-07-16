-- Revert seattleflu/schema:roles/longitudinal-processor/create from pg

begin;

drop role "longitudinal-processor";

commit;
