-- Revert seattleflu/schema:functions/hamming_distance from pg

begin;

drop function public.hamming_distance_ci(text, text);
drop function public.hamming_distance(text, text);

commit;
