-- Verify seattleflu/schema:roles/public on pg

begin;

set local role id3c;

do $$ begin
    if pg_catalog.has_schema_privilege('public', 'public', 'create') then
        raise 'public pseudo-role has create on schema "public"';
    end if;
end $$;

do $$ begin
    if pg_catalog.has_schema_privilege('public', 'sqitch', 'usage') then
        raise 'public pseudo-role has usage on schema "sqitch"';
    end if;
end $$;

rollback;
