-- Deploy seattleflu/schema:warehouse/sample/triggers/update-modified-timestamp to pg
-- requires: warehouse/sample
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.sample
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
