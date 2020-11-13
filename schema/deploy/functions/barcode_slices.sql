-- Deploy seattleflu/schema:functions/barcode_slices to pg

begin;

begin;

create or replace function public.barcode_slices(input text)
    returns text[]
    returns null on null input
    as $$
		declare
		slice_width integer := 2;
		slice_count integer;
		slices text[];

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
	parallel unsafe;
	-- Do not mark this function as parallel safe. It consumes a lot of system resources.
	-- In local testing, the machine fell over several times with this set as parallel safe.
	-- Rather than depending on consumers to not use this function with parallelism, mark
	-- the function as parallel unsafe.

comment on function public.barcode_slices is
    'Builds input text into a text array by moving left to right, slicing the input by the slice_width';

commit;
