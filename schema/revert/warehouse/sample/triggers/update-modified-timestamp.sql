-- Revert seattleflu/schema:warehouse/sample/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.sample;

commit;
