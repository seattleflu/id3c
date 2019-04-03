-- Revert seattleflu/schema:roles/enrollment-processor/comment from pg

begin;

comment on role enrollment_processor is null;

commit;
