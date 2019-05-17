-- Revert seattleflu/schema:roles/manifest-processor/create from pg

begin;

drop role "manifest-processor";

commit;
