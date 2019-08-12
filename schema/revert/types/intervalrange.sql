-- Revert seattleflu/schema:types/intervalrange from pg

begin;

set local role id3c;

drop type public.intervalrange;
drop function public.interval_subtype_diff(interval, interval);

commit;
