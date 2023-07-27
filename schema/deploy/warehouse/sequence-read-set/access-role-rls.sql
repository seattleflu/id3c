-- Deploy seattleflu/schema:warehouse/sequence-read-set/access-role-rls to pg

begin;

alter table warehouse.sequence_read_set
    add access_role regrole;

create policy sequence_read_set_rls
    on warehouse.sequence_read_set
    for all
    to public
    using (access_role is null or pg_has_role(current_user, access_role, 'usage'));

alter table warehouse.sequence_read_set
    enable row level security;

commit;
