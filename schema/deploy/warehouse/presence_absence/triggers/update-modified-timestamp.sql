-- Deploy seattleflu/schema:warehouse/presence_absence/triggers/update-modified-timestamp to pg
-- requires: warehouse/presence_absence
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.presence_absence
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
