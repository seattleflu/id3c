-- Revert seattleflu/schema:warehouse/encounter/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.encounter;

commit;
