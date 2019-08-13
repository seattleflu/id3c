-- Revert seattleflu/schema:roles/public from pg

begin;

-- Intentionally do not switch to the id3c role since we want to be the same
-- user that sqitch is connecting as in order to alter its schema.

grant usage on schema sqitch to public;

commit;
