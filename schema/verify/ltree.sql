-- Verify seattleflu/schema:ltree on pg

begin;

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'ltree'
   and nspname = 'public';


do $$ begin
    assert 'A.B'::ltree @> 'A.B.C';
    assert lca('A.B'::ltree, 'A.B.C'::ltree) = 'A';
end $$;

rollback;
