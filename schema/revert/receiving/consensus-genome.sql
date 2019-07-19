-- Revert seattleflu/schema:receiving/consensus-genome from pg

begin;

drop table receiving.consensus_genome;

commit;
