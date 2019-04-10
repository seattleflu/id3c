-- Verify seattleflu/schema:receiving/sequence-read-set on pg

begin;

select pg_catalog.has_table_privilege('receiving.sequence_read_set', 'select');

rollback;
