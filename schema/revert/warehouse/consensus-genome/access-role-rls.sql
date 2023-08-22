-- Revert seattleflu/schema:warehouse/consensus-genome/access-role-rls from pg

begin;

alter table warehouse.consensus_genome
    disable row level security;

drop policy consensus_genome_rls
    on warehouse.consensus_genome;

alter table warehouse.consensus_genome
    drop column access_role;

commit;
