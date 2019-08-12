-- Revert seattleflu/schema:roles/enrollment-processor/comment from pg

begin;

set local role id3c;

comment on role enrollment_processor is null;

commit;
