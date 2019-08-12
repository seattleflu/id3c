-- Deploy seattleflu/schema:functions/array_distinct to pg

begin;

set local role id3c;

create function public.array_distinct(anyarray)
    returns anyarray
    returns null on null input
    language sql as $$
        -- These gymnastics are necessary because array_agg(distinct ...) and
        -- select distinct aren't order-preserving.
        select coalesce(array_agg(element order by first_index), '{}')
          from (select element, min(ordinality) as first_index
                  from unnest($1) with ordinality
                    as element
                 group by element)
            as distinct_elements
    $$
    immutable
    parallel safe;

comment on function public.array_distinct is
    'Removes repeated elements in an array while preserving original order';

commit;
