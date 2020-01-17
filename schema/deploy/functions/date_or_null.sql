-- Deploy seattleflu/schema:functions/date_or_null to pg

begin;

create or replace function public.date_or_null(input text)
    returns date
    returns null on null input
    language plpgsql as $$
        begin
            return cast(input as date);
        exception
            when invalid_datetime_format or datetime_field_overflow then
                return null;
        end;
    $$
    immutable
    parallel safe;

comment on function public.date_or_null is
    'Tries to cast string to date type, returns null if string has invalid datetime format';

commit;
