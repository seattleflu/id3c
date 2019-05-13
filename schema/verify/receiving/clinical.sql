-- Verify seattleflu/schema:receiving/clinical on pg

begin;

select pg_catalog.has_table_privilege('receiving.clinical', 'select');

rollback;
