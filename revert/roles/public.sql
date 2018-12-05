-- Revert seattleflu/schema:roles/public from pg

begin;

grant create on schema public to public;

commit;
