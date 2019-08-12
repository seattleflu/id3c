-- Revert seattleflu/schema:roles/public from pg

begin;

set local role id3c;

grant create on schema public to public;

commit;
