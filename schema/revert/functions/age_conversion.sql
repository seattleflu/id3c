-- Revert seattleflu/schema:functions/age_conversion from pg

begin;

drop function public.age_in_years(interval);
drop function public.age_in_months(interval);

commit;
