-- Deploy seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_slices to pg
-- requires: functions/barcode_slices

begin;

create index identifier_barcode_slices_idx on warehouse.identifier using gin(barcode_slices(barcode) array_ops);

commit;
