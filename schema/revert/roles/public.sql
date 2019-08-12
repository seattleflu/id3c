-- Revert seattleflu/schema:roles/public from pg

begin;

set local role id3c;

grant usage on schema sqitch to public;

commit;
