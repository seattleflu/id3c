-- Deploy seattleflu/schema:roles/reporter/comment to pg
-- requires: roles/reporter

begin;

set local role id3c;

comment on role reporter is 'Read-only access to entire database for reporting and browsing';

commit;
