-- Verify seattleflu/schema:staging/enrollment/document-constraint on pg

begin;

insert into staging.enrollment (document) values ('{}');

do $$ begin
    insert into staging.enrollment (document) values ('[]');
exception
    when check_violation then
        return;
end $$;

rollback;
