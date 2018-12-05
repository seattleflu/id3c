-- Deploy seattleflu/schema:roles/public to pg

begin;

-- The "public" role is a pseudo-role which means "any connected database user".
--
-- The "public" schema is part of PostgreSQL's default template1 database and
-- is by default copied as part of new database creation.
--
-- The pseudo-role "public" has default grants on the "public" schema,
-- described at <https://www.postgresql.org/docs/11/sql-grant.html>.
--
-- This revoke works *iff* we're the superuser or the owner of the public
-- schema.  Otherwise, it quietly does nothing!
revoke create on schema public from public;

commit;
