-- Deploy seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_slices_null to pg
-- requires: functions/barcode_slices

begin;

create index identifier_barcode_slices_is_null_idx on warehouse.identifier using btree ((barcode_slices(barcode) is null));

commit;
