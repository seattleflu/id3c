-- Verify seattleflu/schema:staging/scan on pg

begin;

select pg_catalog.has_table_privilege('staging.scan_set', 'select');
select pg_catalog.has_table_privilege('staging.collection', 'select');
select pg_catalog.has_table_privilege('staging.sample', 'select');
select pg_catalog.has_table_privilege('staging.aliquot', 'select');

rollback;
