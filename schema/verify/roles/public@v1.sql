-- Verify seattleflu/schema:roles/public on pg

begin;

do $$ begin
    if pg_catalog.has_schema_privilege('public', 'public', 'create') then
        raise 'public pseudo-role has create on schema "public"';
    end if;
end $$;

rollback;
