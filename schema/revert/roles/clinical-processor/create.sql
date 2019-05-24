-- Revert seattleflu/schema:roles/clinical-processor/create from pg

begin;

drop role "clinical-processor";

commit;
