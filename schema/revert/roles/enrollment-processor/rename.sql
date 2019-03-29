-- Revert seattleflu/schema:roles/enrollment-processor/rename from pg

begin;

alter role "enrollment-processor" rename to enrollment_processor;

commit;
