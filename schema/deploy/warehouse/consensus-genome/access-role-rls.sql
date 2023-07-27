-- Deploy seattleflu/schema:warehouse/consensus-genome/access-role-rls to pg

begin;

alter table warehouse.consensus_genome
    add access_role regrole;

create policy consensus_genome_rls
    on warehouse.consensus_genome
    for all
    to public
    using (access_role is null or pg_has_role(current_user, access_role, 'usage'));

alter table warehouse.consensus_genome
    enable row level security;

commit;
