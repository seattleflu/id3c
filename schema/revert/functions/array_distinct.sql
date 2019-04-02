-- Revert seattleflu/schema:functions/array_distinct from pg

begin;

drop function public.array_distinct(anyarray);

commit;
