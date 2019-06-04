-- Revert seattleflu/schema:types/intervalrange from pg

begin;

drop type public.intervalrange;
drop function public.interval_subtype_diff(interval, interval);

commit;
