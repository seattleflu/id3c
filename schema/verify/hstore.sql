-- Verify seattleflu/schema:hstore on pg

begin;

set local role id3c;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'hstore'
   and nspname = 'public';

do $$ begin
    assert 'foo=>bar, baz=>bat'::hstore @> 'baz=>bat';
end $$;

rollback;
