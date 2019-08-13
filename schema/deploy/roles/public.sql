-- Deploy seattleflu/schema:roles/public to pg

begin;

set local role id3c;

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

-- Don't allow all roles to connect to this database.  We will allow just
-- specific roles that privilege later.
--
-- By default, public has connection privileges on all databases.  This default
-- does not appear to be alterable with ALTER DEFAULT PRIVILEGES or by revoking
-- CONNECT from the template1 database before creation of this database.
--
-- The :DBNAME psql variable is documented to always be defined.
revoke connect on database :"DBNAME" from public;

-- Reset to the role that sqitch connects as so that we can alter its schema.
-- This assumes that the current sqitch user is the same as the one which
-- initially created the sqitch schema.  During a first deploy to an empty
-- database, that's true.
reset role;

-- No one needs access to the sqitch schema.
revoke usage on schema sqitch from public;

commit;
