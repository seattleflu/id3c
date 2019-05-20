-- Verify seattleflu/schema:receiving/manifest on pg

begin;

select pg_catalog.has_table_privilege('receiving.manifest', 'select');

rollback;
