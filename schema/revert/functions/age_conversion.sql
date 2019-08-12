-- Revert seattleflu/schema:functions/age_conversion from pg

begin;

set local role id3c;

drop function public.age_in_years(interval);
drop function public.age_in_months(interval);

commit;
