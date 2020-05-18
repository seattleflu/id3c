-- Verify seattleflu/schema:roles/materialized-view-refresher/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('materialized-view-refresher', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('materialized-view-refresher', 'public', 'usage')::int;
select 1/pg_catalog.has_function_privilege('materialized-view-refresher', 'public.refresh_materialized_view(text,text)', 'execute')::int;

select 1/(not pg_catalog.has_function_privilege('public', 'public.refresh_materialized_view(text,text)', 'execute'))::int;

rollback;
