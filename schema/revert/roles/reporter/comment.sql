-- Revert seattleflu/schema:roles/reporter/comment from pg

begin;

comment on role reporter is null;

commit;
