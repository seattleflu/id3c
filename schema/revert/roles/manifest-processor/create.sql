-- Revert seattleflu/schema:roles/manifest-processor/create from pg

begin;

set local role id3c;

drop role "manifest-processor";

commit;
