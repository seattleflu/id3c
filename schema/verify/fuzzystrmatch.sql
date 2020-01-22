-- Verify seattleflu/schema:fuzzystrmatch on pg

begin;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'fuzzystrmatch'
   and nspname = 'public';

do $$ begin
    assert levenshtein('foo', 'woo') = 1;
end $$;

rollback;
