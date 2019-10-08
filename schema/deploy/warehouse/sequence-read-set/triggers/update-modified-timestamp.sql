-- Deploy seattleflu/schema:warehouse/sequence-read-set/triggers/update-modified-timestamp to pg
-- requires: warehouse/sequence-read-set
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.sequence_read_set
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
