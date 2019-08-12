-- Revert seattleflu/schema:roles/enroller/comment from pg

begin;

set local role id3c;

comment on role enroller is null;

commit;
