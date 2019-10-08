-- Deploy seattleflu/schema:warehouse/genomic-sequence/triggers/update-modified-timestamp to pg
-- requires: warehouse/genomic-sequence
-- requires: functions/update-modified-timestamp

begin;

create trigger update_modified_timestamp
    before update on warehouse.genomic_sequence
    for each row
        execute procedure warehouse.update_modified_timestamp();

commit;
