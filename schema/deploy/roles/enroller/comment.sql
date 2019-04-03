-- Deploy seattleflu/schema:roles/enroller/comment to pg
-- requires: roles/enroller

begin;

comment on role enroller is 'For adding new enrollment records';

commit;
