-- Deploy seattleflu/schema:roles/enrollment-processor/comment to pg
-- requires: roles/enrollment-processor

begin;

set local role id3c;

comment on role enrollment_processor is 'For enrollment ETL routines';

commit;
