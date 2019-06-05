-- Deploy seattleflu/schema:functions/age_conversion to pg

begin;

create or replace function public.age_in_years(age interval) returns numeric
    language sql as $$
        -- Interval periods less than a full month are explicitly ignored.  We
        -- don't expect to store them and don't expect to want to report them.
        select round(
              extract(years from $1)::int
            + extract(months from $1)::int * 1::numeric / 12,
            2
        )
    $$
    returns null on null input
    immutable
    parallel safe;


create or replace function public.age_in_months(age interval) returns integer
    language sql as $$
        -- Interval periods less than a full month are explicitly ignored.  We
        -- don't expect to store them and don't expect to want to report them.
        select extract(years from $1)::int * 12
             + extract(months from $1)::int
    $$
    returns null on null input
    immutable
    parallel safe;

comment on function public.age_in_years is 
    'Converts age interval into numeric age in years';

comment on function public.age_in_months is
    'Converts age interval into integer age in months';

commit;
