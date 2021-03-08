-- Revert seattleflu/schema:functions/barcode_slices from pg

begin;

drop function public.barcode_slices(input citext);

commit;
