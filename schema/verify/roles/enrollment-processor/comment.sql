-- Verify seattleflu/schema:roles/enrollment-processor/comment on pg

begin;

set local role id3c;

do $$ begin
    if pg_catalog.shobj_description('enrollment_processor'::regrole, 'pg_authid') is null then
        raise 'role "enrollment_processor" has no comment';
    end if;
end $$;

rollback;
