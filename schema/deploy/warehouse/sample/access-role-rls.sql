-- deploy seattleflu/schema:warehouse/sample/access-role-rls to pg

begin;

alter table warehouse.sample
    add access_role regrole;

create policy sample_rls
    on warehouse.sample
    for all
    to public
    using (access_role is null or pg_has_role(current_user, access_role, 'usage'));

alter table warehouse.sample
    enable row level security;

commit;
