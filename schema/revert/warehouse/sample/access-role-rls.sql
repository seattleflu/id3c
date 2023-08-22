-- Revert seattleflu/schema:warehouse/sample/access-role-rls from pg

begin;

alter table warehouse.sample
    disable row level security;

drop policy sample_rls
    on warehouse.sample;

alter table warehouse.sample
    drop column access_role;

commit;
