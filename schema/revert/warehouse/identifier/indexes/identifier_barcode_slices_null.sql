-- Revert seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_slices_null from pg

begin;

drop index warehouse.identifier_barcode_slices_is_null_idx;

commit;
