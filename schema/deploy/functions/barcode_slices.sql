-- Deploy seattleflu/schema:functions/barcode_slices to pg

begin;

create or replace function public.barcode_slices(input citext)
    returns citext[]
    returns null on null input
    as $$
        declare
        slice_width integer := 2;
        slice_count integer;
        slices citext[];

        begin
        slice_count = length(input) - slice_width + 1;
        for i in 1..slice_count loop
          slices[i] = cast(i as text) || '__' || substring(input from i for slice_width);
        end loop;
        return slices;
        end;
        $$

        language plpgsql
        immutable
        parallel safe;

comment on function public.barcode_slices is
    'Builds input text into a text array by moving left to right, slicing the input by the slice_width';

commit;
