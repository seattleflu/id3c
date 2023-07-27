-- Verify seattleflu/schema:warehouse/sample/access-role-rls on pg

begin;

    insert into warehouse.sample (identifier, access_role)
        values ('__SAMPLE__', 'postgres');

rollback;
