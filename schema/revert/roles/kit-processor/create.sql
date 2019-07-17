-- Revert seattleflu/schema:roles/kit-processor/create from pg

begin;

drop role "kit-processor";

commit;
