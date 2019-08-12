-- Revert seattleflu/schema:functions/hamming_distance from pg

begin;

set local role id3c;

drop function public.hamming_distance_ci(text, text);
drop function public.hamming_distance(text, text);

commit;
