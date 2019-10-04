-- Revert seattleflu/schema:warehouse/presence_absence/triggers/update-modified-timestamp from pg

begin;

drop trigger update_modified_timestamp on warehouse.presence_absence;

commit;
