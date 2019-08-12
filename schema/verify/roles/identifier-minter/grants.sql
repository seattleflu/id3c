-- Verify seattleflu/schema:roles/identifier-minter/grants on pg

begin;

set local role id3c;

select 1/pg_catalog.has_database_privilege('identifier-minter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('identifier-minter', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_table_privilege('identifier-minter', 'warehouse.identifier', 'select,insert')::int;
select 1/pg_catalog.has_table_privilege('identifier-minter', 'warehouse.identifier_set', 'select')::int;

select 1/(not pg_catalog.has_schema_privilege('identifier-minter', 'receiving', 'usage'))::int;
select 1/(not pg_catalog.has_table_privilege('identifier-minter', 'warehouse.identifier', 'update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('identifier-minter', 'warehouse.identifier_set', 'insert,update,delete'))::int;

rollback;
