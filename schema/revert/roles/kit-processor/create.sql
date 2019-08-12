-- Revert seattleflu/schema:roles/kit-processor/create from pg

begin;

set local role id3c;

drop role "kit-processor";

commit;
