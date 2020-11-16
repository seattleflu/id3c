-- Revert seattleflu/schema:receiving/presence-absence/indexes/processing-log from pg

begin;

drop index receiving.presence_absence_processing_log_idx;

commit;
