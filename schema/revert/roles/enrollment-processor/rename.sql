-- Revert seattleflu/schema:roles/enrollment-processor/rename from pg

begin;

set local role id3c;

alter role "enrollment-processor" rename to enrollment_processor;

commit;
