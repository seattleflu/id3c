-- Revert seattleflu/schema:roles/reporter/comment from pg

begin;

set local role id3c;

comment on role reporter is null;

commit;
