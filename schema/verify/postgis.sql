-- Verify seattleflu/schema:postgis on pg

begin;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'postgis'
   and nspname = 'public';

rollback;
