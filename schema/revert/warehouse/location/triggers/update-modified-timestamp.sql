-- Revert seattleflu/schema:warehouse/location/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.location;

commit;
