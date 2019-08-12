-- Deploy seattleflu/schema:roles/enrollment-processor/rename to pg
-- requires: roles/enrollment-processor

begin;

set local role id3c;

alter role enrollment_processor rename to "enrollment-processor";

commit;
