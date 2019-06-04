-- Verify seattleflu/schema:roles/dumper/grants on pg

begin;

-- The only real test of this is to call pg_dump, which we can't/won't from here.

rollback;
