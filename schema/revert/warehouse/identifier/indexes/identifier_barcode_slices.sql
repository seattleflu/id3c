-- Revert seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_slices from pg

begin;

drop index warehouse.identifier_barcode_slices_idx;

commit;
