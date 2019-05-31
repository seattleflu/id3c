-- Revert seattleflu/schema:warehouse/identifier/triggers/barcode-distance-check from pg

begin;

drop trigger identifier_barcode_distance_check_before_insert on warehouse.identifier;
drop trigger identifier_barcode_distance_check_before_update on warehouse.identifier;
drop function warehouse.identifier_barcode_distance_check();

commit;
