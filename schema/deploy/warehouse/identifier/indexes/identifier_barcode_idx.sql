-- Deploy seattleflu/schema:warehouse/identifier/indexes/identifier_barcode_idx to pg
-- requires: warehouse/identifier

begin;

create unique index identifier_barcode_idx on warehouse.identifier using btree (barcode);

commit;
