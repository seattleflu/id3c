-- Revert seattleflu/schema:roles/clinical-processor/create from pg

begin;

set local role id3c;

drop role "clinical-processor";

commit;
