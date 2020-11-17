-- Revert seattleflu/schema:receiving/longitudinal/indexes/processing-log from pg

begin;

drop index receiving.longitudinal_processing_log_idx;

commit;
