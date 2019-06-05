-- Deploy seattleflu/schema:types/intervalrange to pg

begin;

create function public.interval_subtype_diff(x interval, y interval)
    returns float8
    returns null on null input
    language sql as $$
        select extract(epoch from (x - y))
    $$
    immutable
    parallel safe;

create type public.intervalrange as range (
    subtype = interval,
    subtype_diff = interval_subtype_diff
);

comment on function public.interval_subtype_diff is
    'Calculates the difference between two time intervals (durations)';

comment on type public.intervalrange is 
    'Range of time intervals (durations)';

commit;
