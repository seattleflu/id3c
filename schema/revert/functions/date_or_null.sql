-- Revert seattleflu/schema:functions/date_or_null from pg

begin;

drop function public.date_or_null(text);

commit;
