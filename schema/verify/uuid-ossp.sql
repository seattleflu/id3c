-- Verify seattleflu/schema:uuid-ossp on pg

begin;

set local role id3c;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'uuid-ossp'
   and nspname = 'public';

rollback;
