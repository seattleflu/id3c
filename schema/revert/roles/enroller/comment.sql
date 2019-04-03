-- Revert seattleflu/schema:roles/enroller/comment from pg

begin;

comment on role enroller is null;

commit;
