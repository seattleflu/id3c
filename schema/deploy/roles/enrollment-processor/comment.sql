-- Deploy seattleflu/schema:roles/enrollment-processor/comment to pg
-- requires: roles/enrollment-processor

begin;

comment on role enrollment_processor is 'For enrollment ETL routines';

commit;
