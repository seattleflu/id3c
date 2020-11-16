-- Revert seattleflu/schema:receiving/clinical/indexes/processing-log from pg

begin;

drop index receiving.clinical_processing_log_idx;

commit;
