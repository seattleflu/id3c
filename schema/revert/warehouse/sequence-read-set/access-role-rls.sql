-- Revert seattleflu/schema:warehouse/sequence-read-set/access-role-rls from pg

begin;

alter table warehouse.sequence_read_set
    disable row level security;

drop policy sequence_read_set_rls
    on warehouse.sequence_read_set;

alter table warehouse.sequence_read_set
    drop column access_role;

commit;
