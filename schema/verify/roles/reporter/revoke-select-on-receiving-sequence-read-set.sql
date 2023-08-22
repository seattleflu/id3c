-- Verify seattleflu/schema:roles/reporter/revoke-select-on-receiving-sequence-read-set on pg

begin;

select 1/(not pg_catalog.has_table_privilege('reporter', 'receiving.sequence_read_set', 'select'))::int;

rollback;
