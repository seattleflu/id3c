-- Deploy seattleflu/schema:warehouse/encounter/triggers/update-modified-timestamp to pg
-- requires: warehouse/encounter
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.encounter
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
