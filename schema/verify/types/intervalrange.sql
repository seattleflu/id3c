-- Verify seattleflu/schema:types/intervalrange on pg

begin;

set local role id3c;

select 1/(count(*) = 1)::int
    from pg_type
    where typname = 'intervalrange';

do $$
declare
    bin record;
begin
    create temporary table bins (age_range) as values
         ('(,1 month)'::intervalrange)
        ,('[1 month,6 months)'::intervalrange)
        ,('[6 months,1 year)'::intervalrange)
        ,('[1 year,2 years)'::intervalrange)
        ,('[2 years,4 years)'::intervalrange)
        ,('[4 years,)'::intervalrange)
    ;

    -- Sanity check that range adjacency works
    for bin in select age_range -|- lead(age_range) over (order by age_range) as adjacent from bins limit 5 loop
        assert bin.adjacent;
    end loop;
end
$$;

rollback;
