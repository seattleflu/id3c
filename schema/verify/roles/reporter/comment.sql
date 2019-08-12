-- Verify seattleflu/schema:roles/reporter/comment on pg

begin;

set local role id3c;

do $$ begin
    if pg_catalog.shobj_description('reporter'::regrole, 'pg_authid') is null then
        raise 'role "reporter" has no comment';
    end if;
end $$;

rollback;
