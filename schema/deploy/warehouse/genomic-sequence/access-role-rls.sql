-- Deploy seattleflu/schema:warehouse/genomic-sequence/access-role-rls to pg

begin;

alter table warehouse.genomic_sequence
    add access_role regrole;

create policy genomic_sequence_rls
    on warehouse.genomic_sequence
    for all
    to public
    using (access_role is null or pg_has_role(current_user, access_role, 'usage'));

alter table warehouse.genomic_sequence
    enable row level security;

commit;
