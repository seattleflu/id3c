-- Revert seattleflu/schema:warehouse/consensus-genome from pg

begin;

drop table warehouse.consensus_genome;

commit;
