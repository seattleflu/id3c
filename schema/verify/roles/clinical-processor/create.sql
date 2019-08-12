-- Verify seattleflu/schema:roles/clinical-processor/create on pg

begin;

set local role id3c;

-- No real need to test that the user was created; the database would have
-- thrown an error if it wasn't.

rollback;
