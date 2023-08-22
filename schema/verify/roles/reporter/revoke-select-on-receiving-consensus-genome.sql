-- Verify seattleflu/schema:roles/reporter/revoke-select-on-receiving-consensus-genome on pg

begin;

select 1/(not pg_catalog.has_table_privilege('reporter', 'receiving.consensus_genome', 'select'))::int;

rollback;
