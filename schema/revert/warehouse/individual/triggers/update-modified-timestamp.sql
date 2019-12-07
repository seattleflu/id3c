-- Revert seattleflu/schema:warehouse/individual/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.individual;

commit;
