-- Deploy seattleflu/schema:roles/enroller/comment to pg
-- requires: roles/enroller

begin;

set local role id3c;

comment on role enroller is 'For adding new enrollment records';

commit;
