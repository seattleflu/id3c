-- Verify seattleflu/schema:citext on pg

begin;

set local role id3c;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'citext'
   and nspname = 'public';

rollback;
