-- Verify seattleflu/schema:roles/identifier-minter/create on pg

begin;

-- No real need to test that the user was created; the database would have
-- thrown an error if it wasn't.

rollback;
