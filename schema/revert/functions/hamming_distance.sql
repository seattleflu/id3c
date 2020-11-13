-- Deploy seattleflu/schema:functions/hamming_distance to pg

begin;

create or replace function public.hamming_distance(a text, b text)
    returns integer
    returns null on null input
    language plpgsql as $$
        declare
            distance integer := 0;
            a_char text;
            b_char text;
        begin
            if length(a) != length(b) then
                raise data_exception using
                    message = 'Strings must be the same length';
            end if;

            for i in 1..length(a) loop
                a_char := substring(a from i for 1);
                b_char := substring(b from i for 1);
                distance := distance + (a_char != b_char)::integer;
            end loop;

            return distance;
        end;
    $$
    immutable
    parallel safe;

create or replace function public.hamming_distance_ci(a text, b text)
    returns integer
    returns null on null input
    language plpgsql as $$
        begin
            return hamming_distance(lower(a), lower(b));
        end;
    $$
    immutable
    parallel safe;

comment on function public.hamming_distance(text, text) is
    'Calculates the Hamming substitution, or edit, distance between two strings of equal length';

comment on function public.hamming_distance_ci(text, text) is
    'Case-insensitive version of hamming_distance() which calls lower() on its arguments';

drop function public.hamming_distance_lte(text, text, integer);

commit;
