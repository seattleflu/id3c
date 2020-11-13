-- Deploy seattleflu/schema:warehouse/identifier/slices to pg

begin;

alter table warehouse.identifier
add column if not exists slices text[];

update warehouse.identifier
set slices = public.barcode_slices(barcode);

commit;
