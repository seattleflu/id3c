-- Revert seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_idx from pg

begin;

drop index warehouse.identifier_barcode_idx;

commit;
