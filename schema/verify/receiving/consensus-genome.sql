-- Verify seattleflu/schema:receiving/consensus-genome on pg

begin;

select pg_catalog.has_table_privilege('receiving.consensus_genome', 'select');

rollback;
