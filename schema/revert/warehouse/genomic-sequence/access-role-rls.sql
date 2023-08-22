-- Revert seattleflu/schema:warehouse/genomic-sequence/access-role-rls from pg

begin;

alter table warehouse.genomic_sequence
    disable row level security;

drop policy genomic_sequence_rls
    on warehouse.genomic_sequence;

alter table warehouse.genomic_sequence
    drop column access_role;

commit;
