-- Deploy seattleflu/schema:warehouse/individual/triggers/update-modified-timestamp to pg
-- requires: warehouse/individual
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.individual
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
