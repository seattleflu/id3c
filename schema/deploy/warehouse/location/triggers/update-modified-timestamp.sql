-- Deploy seattleflu/schema:warehouse/location/triggers/update-modified-timestamp to pg
-- requires: warehouse/location
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.location
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
