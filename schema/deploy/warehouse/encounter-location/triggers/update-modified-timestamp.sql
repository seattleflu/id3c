-- Deploy seattleflu/schema:warehouse/encounter-location/triggers/update-modified-timestamp to pg
-- requires: warehouse/encounter-location
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.encounter_location
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
