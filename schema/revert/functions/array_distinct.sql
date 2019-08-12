-- Revert seattleflu/schema:functions/array_distinct from pg

begin;

set local role id3c;

drop function public.array_distinct(anyarray);

commit;
